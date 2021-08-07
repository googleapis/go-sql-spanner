// Copyright 2021 Google Inc. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
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
	"encoding/gob"

	"cloud.google.com/go/spanner"
	sppb "google.golang.org/genproto/googleapis/spanner/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/structpb"
)

var ErrAbortedDueToConcurrentModification = status.Error(codes.Aborted, "Transaction was aborted due to a concurrent modification")
var errNextAfterSTop = status.Errorf(codes.FailedPrecondition, "Next called after Stop")

func init() {
	gob.Register(structpb.Value_BoolValue{})
	gob.Register(structpb.Value_NumberValue{})
	gob.Register(structpb.Value_StringValue{})
	gob.Register(structpb.Value_NullValue{})
	gob.Register(structpb.Value_ListValue{})
	gob.Register(structpb.Value_StructValue{})
}

type checksumRowIterator struct {
	*spanner.RowIterator

	ctx  context.Context
	tx   *readWriteTransaction
	stmt spanner.Statement
	// nc (nextCount) indicates the number of times that next has been called
	// on the iterator.
	nc       int64
	stopped  bool
	checksum [32]byte
	buffer   *bytes.Buffer
	enc      *gob.Encoder

	// errIndex and err indicate any error and the index in the result set
	// where the error occurred.
	errIndex int64
	err      error
}

func (it *checksumRowIterator) Next() (row *spanner.Row, err error) {
	if it.stopped {
		return nil, errNextAfterSTop
	}
	err = it.tx.runWithRetry(it.ctx, func(ctx context.Context) error {
		row, err = it.RowIterator.Next()
		// spanner.ErrCode returns codes.Ok for nil errors.
		if spanner.ErrCode(err) != codes.Aborted {
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
		}
		if err != nil {
			return err
		}
		// Update the current checksum.
		it.checksum, err = updateChecksum(it.enc, it.buffer, it.checksum, row)
		return err
	})
	return row, err
}

func updateChecksum(enc *gob.Encoder, buffer *bytes.Buffer, currentChecksum [32]byte, row *spanner.Row) ([32]byte, error) {
	buffer.Reset()
	buffer.Write(currentChecksum[:])
	for i := 0; i < row.Size(); i++ {
		var v spanner.GenericColumnValue
		err := row.Column(i, &v)
		if err != nil {
			return currentChecksum, err
		}
		err = enc.Encode(v)
		if err != nil {
			return currentChecksum, err
		}
	}
	return sha256.Sum256(buffer.Bytes()), nil
}

func (it *checksumRowIterator) retry(ctx context.Context, tx *spanner.ReadWriteStmtBasedTransaction) error {
	buffer := &bytes.Buffer{}
	enc := gob.NewEncoder(buffer)
	retryIt := tx.Query(ctx, it.stmt)
	if it.stopped {
		defer retryIt.Stop()
	}
	// The underlying iterator will be replaced by the new one if the retry succeeds.
	replaceIt := func(err error) error {
		if it.RowIterator != nil {
			it.RowIterator.Stop()
			it.RowIterator = retryIt
		}
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
	// should contain as many rows as the initial result.
	var newChecksum [32]byte
	for n := int64(0); n < it.nc; n++ {
		row, err := retryIt.Next()
		if err != nil {
			if spanner.ErrCode(err) == codes.Aborted {
				return failRetry(err)
			}
			if errorsEqualForRetry(err, it.err) && n == it.errIndex {
				// Check that the checksums are also equal.
				if newChecksum != it.checksum {
					return failRetry(ErrAbortedDueToConcurrentModification)
				}
				return replaceIt(nil)
			}
			return failRetry(ErrAbortedDueToConcurrentModification)
		}
		newChecksum, err = updateChecksum(enc, buffer, newChecksum, row)
		if err != nil {
			return failRetry(err)
		}
	}
	// Check if the initial attempt ended with an error and the current attempt
	// did not.
	if it.err != nil {
		return failRetry(ErrAbortedDueToConcurrentModification)
	}
	if newChecksum != it.checksum {
		return failRetry(ErrAbortedDueToConcurrentModification)
	}
	return replaceIt(nil)
}

func (it *checksumRowIterator) Stop() {
	if !it.stopped {
		it.stopped = true
		it.RowIterator.Stop()
		it.RowIterator = nil
	}
}

func (it *checksumRowIterator) Metadata() *sppb.ResultSetMetadata {
	if it.stopped {

	}
	return it.RowIterator.Metadata
}
