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
	"fmt"
)

type contextTransaction interface {
	Commit() error
	Rollback() error
	Query(ctx context.Context, stmt spanner.Statement) *spanner.RowIterator
	ExecContext(ctx context.Context, stmt spanner.Statement) (int64, error)
}

type readOnlyTransaction struct {
	roTx  *spanner.ReadOnlyTransaction
	close func()
}

func (tx *readOnlyTransaction) Commit() error {
	if tx.roTx != nil {
		tx.roTx.Close()
	}
	tx.close()
	return nil
}

func (tx *readOnlyTransaction) Rollback() error {
	if tx.roTx != nil {
		tx.roTx.Close()
	}
	tx.close()
	return nil
}

func (tx *readOnlyTransaction) Query(ctx context.Context, stmt spanner.Statement) *spanner.RowIterator {
	return tx.roTx.Query(ctx, stmt)
}

func (tx *readOnlyTransaction) ExecContext(ctx context.Context, stmt spanner.Statement) (int64, error) {
	return 0, fmt.Errorf("Read-only transactions cannot write")
}

type readWriteTransaction struct {
	ctx   context.Context
	rwTx  *spanner.ReadWriteStmtBasedTransaction
	close func()
}

func (tx *readWriteTransaction) Commit() (err error) {
	if tx.rwTx != nil {
		_, err = tx.rwTx.Commit(tx.ctx)
	}
	tx.close()
	return err
}

func (tx *readWriteTransaction) Rollback() error {
	if tx.rwTx != nil {
		tx.rwTx.Rollback(tx.ctx)
	}
	tx.close()
	return nil
}

func (tx *readWriteTransaction) Query(ctx context.Context, stmt spanner.Statement) *spanner.RowIterator {
	return tx.rwTx.Query(ctx, stmt)
}

func (tx *readWriteTransaction) ExecContext(ctx context.Context, stmt spanner.Statement) (int64, error) {
	return tx.rwTx.Update(ctx, stmt)
}
