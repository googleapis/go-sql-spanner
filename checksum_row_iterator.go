// Copyright 2021 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package spannerdriver

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"hash"
	"math"
	"reflect"
	"sort"

	"cloud.google.com/go/spanner"
	sppb "cloud.google.com/go/spanner/apiv1/spannerpb"
	"github.com/googleapis/go-sql-spanner/parser"
	"google.golang.org/api/iterator"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/structpb"
)

var errNextAfterSTop = status.Errorf(codes.FailedPrecondition, "Next called after Stop")

var _ rowIterator = &checksumRowIterator{}

// checksumRowIterator implements rowIterator and keeps track of a running
// checksum for all results that have been seen during the iteration of the
// results. This checksum can be used to verify whether a retry returned the
// same results as the initial attempt or not.
type checksumRowIterator struct {
	*spanner.RowIterator
	metadata *sppb.ResultSetMetadata

	ctx      context.Context
	tx       *readWriteTransaction
	stmt     spanner.Statement
	stmtType parser.StatementType
	options  spanner.QueryOptions
	// nc (nextCount) indicates the number of times that next has been called
	// on the iterator. Next() will be called the same number of times during
	// a retry.
	nc int64
	// stopped indicates whether the original iterator was stopped. If it was,
	// the iterator that is created during a retry should also be stopped after
	// the retry has finished.
	stopped bool

	// hash contains the current hash for the results that have been
	// seen. It is calculated as a SHA256 checksum over all rows that so far
	// have been returned.
	hash hash.Hash

	// errIndex and err indicate any error and the index in the result set
	// where the error occurred.
	errIndex int64
	err      error
}

func (it *checksumRowIterator) String() string {
	return it.stmt.SQL
}

func (it *checksumRowIterator) Next() (row *spanner.Row, err error) {
	if it.stopped {
		return nil, errNextAfterSTop
	}
	err = it.tx.runWithRetry(it.ctx, func(ctx context.Context) error {
		row, err = it.RowIterator.Next()
		// spanner.ErrCode returns codes.Ok for nil errors.
		if spanner.ErrCode(err) == codes.Aborted {
			return err
		}
		if err != nil {
			// Register the error that we received and the row where we
			// received it. This will in almost all cases be the first row
			// when the query fails, or the last row when the iterator
			// returns iterator.Done. It can however also happen that the
			// result stream breaks halfway and ends with an error before
			// the end.
			it.err = err
			it.errIndex = it.nc
		}
		it.nc++
		if it.metadata == nil && it.RowIterator.Metadata != nil {
			it.metadata = it.RowIterator.Metadata
			// Initialize the checksum of the iterator by calculating the
			// checksum of the columns that are included in this result. This is
			// also used to detect the possible difference between two empty
			// result sets with a different set of columns.
			it.hash, err = createMetadataChecksum(it.metadata)
			if err != nil {
				return err
			}
		}
		if it.err != nil {
			return it.err
		}
		// Update the current checksum.
		return updateChecksum(it.hash, row)
	})
	return row, err
}

// updateChecksum calculates the following checksum based on a current checksum
// and a new row.
func updateChecksum(hash hash.Hash, row *spanner.Row) error {
	for i := 0; i < row.Size(); i++ {
		var v spanner.GenericColumnValue
		err := row.Column(i, &v)
		if err != nil {
			return err
		}
		hashValue(v.Value, hash)
	}
	return nil
}

var int32Buf [4]byte
var float64Buf [8]byte

func hashValue(value *structpb.Value, digest hash.Hash) {
	switch value.GetKind().(type) {
	case *structpb.Value_StringValue:
		digest.Write(intToByte(int32Buf, len(value.GetStringValue())))
		digest.Write([]byte(value.GetStringValue()))
	case *structpb.Value_NullValue:
		digest.Write([]byte{0})
	case *structpb.Value_NumberValue:
		digest.Write(float64ToByte(float64Buf, value.GetNumberValue()))
	case *structpb.Value_BoolValue:
		if value.GetBoolValue() {
			digest.Write([]byte{1})
		} else {
			digest.Write([]byte{0})
		}
	case *structpb.Value_StructValue:
		fields := make([]string, 0, len(value.GetStructValue().Fields))
		for field, _ := range value.GetStructValue().Fields {
			fields = append(fields, field)
		}
		sort.Strings(fields)
		for _, field := range fields {
			digest.Write(intToByte(int32Buf, len(field)))
			digest.Write([]byte(field))
			hashValue(value.GetStructValue().Fields[field], digest)
		}
	case *structpb.Value_ListValue:
		for _, v := range value.GetListValue().GetValues() {
			hashValue(v, digest)
		}
	}
}

func intToByte(buf [4]byte, v int) []byte {
	binary.BigEndian.PutUint32(buf[:], uint32(v))
	return buf[:]
}

func float64ToByte(buf [8]byte, f float64) []byte {
	binary.BigEndian.PutUint64(buf[:], math.Float64bits(f))
	return buf[:]
}

// createMetadataChecksum calculates the checksum of the metadata of a result.
// Only the column names and types are included in the checksum. Any transaction
// metadata is not included.
func createMetadataChecksum(metadata *sppb.ResultSetMetadata) (hash.Hash, error) {
	digest := sha256.New()
	for _, field := range metadata.RowType.Fields {
		digest.Write(intToByte(int32Buf, len(field.Name)))
		digest.Write([]byte(field.Name))
		digest.Write(intToByte(int32Buf, int(field.Type.Code.Number())))
	}
	return digest, nil
}

// retry implements retriableStatement.retry for queries. It will execute the
// query on a new Spanner transaction and iterate over the same number of rows
// as the initial attempt, and then compare the checksum of the initial and the
// retried iterator. It will also check if any error that was returned by the
// initial iterator was also returned by the new iterator, and that the errors
// were returned by the same row index.
func (it *checksumRowIterator) retry(ctx context.Context, tx *spanner.ReadWriteStmtBasedTransaction) error {
	retryIt := tx.QueryWithOptions(ctx, it.stmt, it.options)
	// If the original iterator had been stopped, we should also always stop the
	// new iterator.
	if it.stopped {
		defer retryIt.Stop()
	}
	// The underlying iterator will be replaced by the new one if the retry succeeds.
	replaceIt := func(err error) error {
		if it.RowIterator != nil {
			it.RowIterator.Stop()
			it.RowIterator = retryIt
		}
		it.metadata = retryIt.Metadata
		return err
	}
	// If the retry fails, we will not replace the underlying iterator and we should
	// stop the iterator that was used by the retry.
	failRetry := func(err error) error {
		retryIt.Stop()
		return err
	}
	// Iterate over the new result set as many times as we iterated over the initial
	// result set. The checksums of the two should be equal. Also, the new result set
	// should return any error on the same index as the original.
	// var newChecksum *[32]byte
	var checksumErr error
	newHash := sha256.New()
	for n := int64(0); n < it.nc; n++ {
		row, err := retryIt.Next()
		if n == 0 && (err == nil || err == iterator.Done) {
			newHash, checksumErr = createMetadataChecksum(retryIt.Metadata)
			if checksumErr != nil {
				return failRetry(checksumErr)
			}
		}
		if err != nil {
			if spanner.ErrCode(err) == codes.Aborted {
				// This fails this retry, but it will trigger a new retry of the
				// entire transaction.
				return failRetry(err)
			}
			if errorsEqualForRetry(err, it.err) && n == it.errIndex {
				// Check that the checksums are also equal.
				if !checksumsEqual(newHash, it.hash) {
					return failRetry(ErrAbortedDueToConcurrentModification)
				}
				return replaceIt(nil)
			}
			return failRetry(ErrAbortedDueToConcurrentModification)
		}
		err = updateChecksum(newHash, row)
		if err != nil {
			return failRetry(err)
		}
	}
	// Check if the initial attempt ended with an error and the current attempt
	// did not. This is normally an indication that the retry returned more
	// results than the initial attempt, and that the initial attempt returned
	// iterator.Done, but it could theoretically be any other error as well.
	if it.err != nil {
		return failRetry(ErrAbortedDueToConcurrentModification)
	}
	if !checksumsEqual(newHash, it.hash) {
		return failRetry(ErrAbortedDueToConcurrentModification)
	}
	// Everything seems to be equal, replace the underlying iterator and return
	// a nil error.
	return replaceIt(nil)
}

func checksumsEqual(h1, h2 hash.Hash) bool {
	if reflect.ValueOf(h1).IsNil() && reflect.ValueOf(h2).IsNil() {
		return true
	}
	c1 := h1.Sum(nil)
	c2 := h2.Sum(nil)
	return bytes.Equal(c1, c2)
}

func (it *checksumRowIterator) Stop() {
	if !it.stopped {
		it.stopped = true
		it.RowIterator.Stop()
		it.RowIterator = nil
	}
}

func (it *checksumRowIterator) Metadata() (*sppb.ResultSetMetadata, error) {
	return it.metadata, nil
}

func (it *checksumRowIterator) ResultSetStats() *sppb.ResultSetStats {
	return createResultSetStats(it.RowIterator, it.stmtType)
}
