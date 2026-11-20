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
	cancel   context.CancelFunc
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
	hash       hash.Hash
	int32Buf   [4]byte
	float64Buf [8]byte

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
	if err := it.tx.ensureReplayed(it.ctx); err != nil {
		return nil, err
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
			it.hash, err = it.createMetadataChecksum(it.metadata)
			if err != nil {
				return err
			}
		}
		if it.err != nil {
			return it.err
		}
		// Update the current checksum.
		return it.updateChecksum(it.hash, row)
	})
	return row, err
}

// updateChecksum calculates the following checksum based on a current checksum
// and a new row.
func (it *checksumRowIterator) updateChecksum(hash hash.Hash, row *spanner.Row) error {
	for i := 0; i < row.Size(); i++ {
		var v spanner.GenericColumnValue
		err := row.Column(i, &v)
		if err != nil {
			return err
		}
		it.hashValue(v.Value, hash)
	}
	return nil
}

func (it *checksumRowIterator) hashValue(value *structpb.Value, digest hash.Hash) {
	switch value.GetKind().(type) {
	case *structpb.Value_StringValue:
		digest.Write(intToByte(it.int32Buf, len(value.GetStringValue())))
		digest.Write([]byte(value.GetStringValue()))
	case *structpb.Value_NullValue:
		digest.Write([]byte{0})
	case *structpb.Value_NumberValue:
		digest.Write(float64ToByte(it.float64Buf, value.GetNumberValue()))
	case *structpb.Value_BoolValue:
		if value.GetBoolValue() {
			digest.Write([]byte{1})
		} else {
			digest.Write([]byte{0})
		}
	case *structpb.Value_StructValue:
		fields := make([]string, 0, len(value.GetStructValue().Fields))
		for field := range value.GetStructValue().Fields {
			fields = append(fields, field)
		}
		sort.Strings(fields)
		for _, field := range fields {
			digest.Write(intToByte(it.int32Buf, len(field)))
			digest.Write([]byte(field))
			it.hashValue(value.GetStructValue().Fields[field], digest)
		}
	case *structpb.Value_ListValue:
		for _, v := range value.GetListValue().GetValues() {
			it.hashValue(v, digest)
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
func (it *checksumRowIterator) createMetadataChecksum(metadata *sppb.ResultSetMetadata) (hash.Hash, error) {
	digest := sha256.New()
	for _, field := range metadata.RowType.Fields {
		digest.Write(intToByte(it.int32Buf, len(field.Name)))
		digest.Write([]byte(field.Name))
		digest.Write(intToByte(it.int32Buf, int(field.Type.Code.Number())))
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
	if err := it.checkRetryContext(ctx); err != nil {
		return err
	}
	queryCtx, stopWatch, iterCancel := it.retryQueryContext(ctx)
	retryIt := tx.QueryWithOptions(queryCtx, it.stmt, it.options)
	replaced := false
	defer func() {
		if !replaced {
			stopWatch()
			retryIt.Stop()
			iterCancel()
		}
	}()

	if err := it.replayAndVerifyRows(ctx, queryCtx, retryIt); err != nil {
		return err
	}
	if err := it.replaceIterator(ctx, queryCtx, retryIt, stopWatch, iterCancel); err != nil {
		return err
	}
	replaced = true
	return nil
}

func (it *checksumRowIterator) checkRetryContext(ctx context.Context) error {
	if isContextExpiredOrCancelled(ctx) {
		return retryContextError(ctx, nil, nil)
	}
	if !it.stopped && isContextExpiredOrCancelled(it.ctx) {
		return retryContextError(ctx, it.ctx, nil)
	}
	return nil
}

// replayAndVerifyRows iterates through retryIt as many times as the original
// attempt and returns ErrAbortedDueToConcurrentModification if the checksum or
// error disagrees.
func (it *checksumRowIterator) replayAndVerifyRows(ctx, queryCtx context.Context, retryIt *spanner.RowIterator) error {
	var checksumErr error
	newHash := sha256.New()
	for n := int64(0); n < it.nc; n++ {
		row, err := retryIt.Next()
		if n == 0 && (err == nil || err == iterator.Done) {
			newHash, checksumErr = it.createMetadataChecksum(retryIt.Metadata)
			if checksumErr != nil {
				return checksumErr
			}
		}
		if err != nil {
			if spanner.ErrCode(err) == codes.Aborted {
				// This fails this retry, but it will trigger a new retry of the
				// entire transaction.
				return err
			}
			if isContextCancelledOrTimedOut(queryCtx, err) || isContextCancelledOrTimedOut(ctx, err) {
				return retryContextError(ctx, queryCtx, err)
			}
			if errorsEqualForRetry(err, it.err) && n == it.errIndex {
				// Check that the checksums are also equal.
				if !checksumsEqual(newHash, it.hash) {
					return ErrAbortedDueToConcurrentModification
				}
				return nil
			}
			return ErrAbortedDueToConcurrentModification
		}
		if err := it.updateChecksum(newHash, row); err != nil {
			return err
		}
	}
	// Check if the initial attempt ended with an error and the current attempt
	// did not. This is normally an indication that the retry returned more
	// results than the initial attempt, and that the initial attempt returned
	// iterator.Done, but it could theoretically be any other error as well.
	if it.err != nil {
		return ErrAbortedDueToConcurrentModification
	}
	if !checksumsEqual(newHash, it.hash) {
		return ErrAbortedDueToConcurrentModification
	}
	return nil
}

func (it *checksumRowIterator) replaceIterator(ctx, queryCtx context.Context, retryIt *spanner.RowIterator, stopWatch func() bool, iterCancel context.CancelFunc) error {
	itCtxExpired := !it.stopped && isContextExpiredOrCancelled(it.ctx)
	if !stopWatch() || isContextExpiredOrCancelled(ctx) || isContextExpiredOrCancelled(queryCtx) || itCtxExpired {
		fallbackCtx := queryCtx
		if itCtxExpired {
			fallbackCtx = it.ctx
		}
		return retryContextError(ctx, fallbackCtx, nil)
	}
	if it.RowIterator != nil && !it.stopped {
		it.RowIterator.Stop()
		it.RowIterator = retryIt
		if it.cancel != nil {
			it.cancel()
		}
		it.cancel = iterCancel
		it.metadata = retryIt.Metadata
	} else {
		iterCancel()
		retryIt.Stop()
	}
	return nil
}

// retryQueryContext returns the context to bind to the retried RowIterator.
// Open iterators are rooted in it.ctx (rather than the ephemeral retryCtx) so
// they survive after the triggering call returns, while watching retryCtx for
// cancellation or expiration for the duration of retry().
func (it *checksumRowIterator) retryQueryContext(retryCtx context.Context) (context.Context, func() bool, context.CancelFunc) {
	noopStop := func() bool { return true }
	if it.stopped {
		if retryCtx == nil {
			retryCtx = context.Background()
		}
		return retryCtx, noopStop, func() {}
	}
	baseCtx := it.ctx
	if baseCtx == nil {
		baseCtx = context.Background()
	}
	iterCtx, iterCancel := context.WithCancel(baseCtx)
	if retryCtx == nil {
		return iterCtx, noopStop, iterCancel
	}
	return iterCtx, context.AfterFunc(retryCtx, iterCancel), iterCancel
}

func checksumsEqual(h1, h2 hash.Hash) bool {
	if h1 == nil || h2 == nil {
		return h1 == h2
	}
	c1 := h1.Sum(nil)
	c2 := h2.Sum(nil)
	return bytes.Equal(c1, c2)
}

func (it *checksumRowIterator) Stop() {
	if !it.stopped {
		it.stopped = true
		if it.RowIterator != nil {
			it.RowIterator.Stop()
			it.RowIterator = nil
		}
		if it.cancel != nil {
			it.cancel()
			it.cancel = nil
		}
	}
}

func (it *checksumRowIterator) Metadata() (*sppb.ResultSetMetadata, error) {
	return it.metadata, nil
}

func (it *checksumRowIterator) ResultSetStats() *sppb.ResultSetStats {
	return createResultSetStats(it.RowIterator, it.stmtType)
}
