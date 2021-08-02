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
	"context"
	"fmt"
	sppb "google.golang.org/genproto/googleapis/spanner/v1"
	"google.golang.org/grpc/codes"

	"cloud.google.com/go/spanner"
)

// contextTransaction is the combination of both read/write and read-only
// transactions.
type contextTransaction interface {
	Commit() error
	Rollback() error
	Query(ctx context.Context, stmt spanner.Statement) rowIterator
	ExecContext(ctx context.Context, stmt spanner.Statement) (int64, error)
}

type rowIterator interface {
	Next() (*spanner.Row, error)
	Stop()
	Metadata() *sppb.ResultSetMetadata
}

type readOnlyRowIterator struct {
	*spanner.RowIterator
}

func (ri *readOnlyRowIterator) Next() (*spanner.Row, error) {
	return ri.RowIterator.Next()
}

func (ri *readOnlyRowIterator) Stop() {
	ri.RowIterator.Stop()
}

func (ri *readOnlyRowIterator) Metadata() *sppb.ResultSetMetadata {
	return ri.RowIterator.Metadata
}

type readOnlyTransaction struct {
	roTx  *spanner.ReadOnlyTransaction
	close func()
}

func (tx *readOnlyTransaction) Commit() error {
	// Read-only transactions don't really commit, but closing the transaction
	// will return the session to the pool.
	if tx.roTx != nil {
		tx.roTx.Close()
	}
	tx.close()
	return nil
}

func (tx *readOnlyTransaction) Rollback() error {
	// Read-only transactions don't really rollback, but closing the transaction
	// will return the session to the pool.
	if tx.roTx != nil {
		tx.roTx.Close()
	}
	tx.close()
	return nil
}

func (tx *readOnlyTransaction) Query(ctx context.Context, stmt spanner.Statement) rowIterator {
	return &readOnlyRowIterator{tx.roTx.Query(ctx, stmt)}
}

func (tx *readOnlyTransaction) ExecContext(_ context.Context, stmt spanner.Statement) (int64, error) {
	return 0, fmt.Errorf("read-only transactions cannot write")
}

type retriableStatement interface {
	retry(ctx context.Context, tx *spanner.ReadWriteStmtBasedTransaction) error
}

type retriableUpdate struct {
	stmt spanner.Statement
	c    int64
	err  error
}

func (ru *retriableUpdate) retry(ctx context.Context, tx *spanner.ReadWriteStmtBasedTransaction) error {
	c, err := tx.Update(ctx, ru.stmt)
	if err != nil && spanner.ErrCode(err) == codes.Aborted {
		return err
	}
	if err != nil && ru.err == nil {
		return errAbortedDueToConcurrentModification
	}
	if err == nil && ru.err != nil {
		return errAbortedDueToConcurrentModification
	}
	if err != nil && ru.err != nil {
		if spanner.ErrCode(err) != spanner.ErrCode(ru.err) {
			return errAbortedDueToConcurrentModification
		}
		return nil
	}
	if c != ru.c {
		return errAbortedDueToConcurrentModification
	}
	return nil
}

type readWriteTransaction struct {
	ctx     context.Context
	client  *spanner.Client
	rwTx    *spanner.ReadWriteStmtBasedTransaction
	close   func()

	statements []retriableStatement
}

func (tx *readWriteTransaction) runWithRetry(ctx context.Context, f func(ctx context.Context) error) (err error) {
	for {
		if err == nil {
			err = f(ctx)
		}
		if err == errAbortedDueToConcurrentModification {
			return
		}
		if spanner.ErrCode(err) == codes.Aborted {
			err = tx.retry(ctx)
			continue
		}
		return
	}
}

func (tx *readWriteTransaction) retry(ctx context.Context) (err error) {
	tx.rwTx, err = spanner.NewReadWriteStmtBasedTransaction(ctx, tx.client)
	if err != nil {
		return err
	}
	for _, stmt := range tx.statements {
		err = stmt.retry(ctx, tx.rwTx)
		if err != nil {
			return err
		}
	}

	return err
}

func (tx *readWriteTransaction) Commit() (err error) {
	if tx.rwTx != nil {
		err = tx.runWithRetry(tx.ctx, func(ctx context.Context) error {
			_, err := tx.rwTx.Commit(ctx)
			return err
		})
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

func (tx *readWriteTransaction) Query(ctx context.Context, stmt spanner.Statement) rowIterator {
	return &checksumRowIterator{
		RowIterator: tx.rwTx.Query(ctx, stmt),
		ctx:  ctx,
		tx:   tx,
		stmt: stmt,
	}
}

func (tx *readWriteTransaction) ExecContext(ctx context.Context, stmt spanner.Statement) (res int64, err error) {
	err = tx.runWithRetry(ctx, func(ctx context.Context) error {
		res, err = tx.rwTx.Update(ctx, stmt)
		return err
	})
	tx.statements = append(tx.statements, &retriableUpdate{
		stmt: stmt,
		c:    res,
		err:  err,
	})
	return res, err
}
