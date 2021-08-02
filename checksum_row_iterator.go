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
	"cloud.google.com/go/spanner"
	"context"
	sppb "google.golang.org/genproto/googleapis/spanner/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var errAbortedDueToConcurrentModification = status.Error(codes.Aborted, "Transaction was aborted due to a concurrent modification")

type checksumRowIterator struct {
	*spanner.RowIterator

	ctx  context.Context
	tx   *readWriteTransaction
	stmt spanner.Statement
	// nc (nextCount) indicates the number of times that next has been called
	// on the iterator.
	nc   int64
}

func (it *checksumRowIterator) Next() (row *spanner.Row, err error) {
	err = it.tx.runWithRetry(it.ctx, func(ctx context.Context) error {
		if err != nil {
			err = it.restart()
			if err != nil {
				return err
			}
		}
		row, err = it.RowIterator.Next()
		if spanner.ErrCode(err) != codes.Aborted {
			it.nc++
		}
		return err
	})
	return row, err
}

func (it *checksumRowIterator) restart() error {
	return it.tx.runWithRetry(it.ctx, func(ctx context.Context) error {
		it.RowIterator = it.tx.rwTx.Query(it.ctx, it.stmt)
		for n := int64(0); n < it.nc; n++ {
			_, err := it.RowIterator.Next()
			if err != nil {
				return errAbortedDueToConcurrentModification
			}
		}
		return nil
	})
}

func (it *checksumRowIterator) Stop() {
	it.RowIterator.Stop()
}

func (it *checksumRowIterator) Metadata() *sppb.ResultSetMetadata {
	return it.RowIterator.Metadata
}
