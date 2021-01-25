// Copyright 2020 Google Inc. All Rights Reserved.
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

	"cloud.google.com/go/spanner"
	"github.com/cloudspannerecosystem/go-sql-spanner/internal"
)

type roTx struct {
	close func()
}

func (tx *roTx) Commit() error {
	tx.close()
	return nil
}

func (tx *roTx) Rollback() error {
	tx.close()
	return nil
}

type rwTx struct {
	connector *internal.RWConnector
	close     func()
}

func (tx *rwTx) Query(ctx context.Context, stmt spanner.Statement) *spanner.RowIterator {
	tx.connector.QueryIn <- &internal.RWQueryMessage{
		Ctx:  ctx,
		Stmt: stmt,
	}
	msg := <-tx.connector.QueryOut
	return msg.It
}

func (tx *rwTx) ExecContext(ctx context.Context, stmt spanner.Statement) (int64, error) {
	tx.connector.ExecIn <- &internal.RWExecMessage{
		Ctx:  ctx,
		Stmt: stmt,
	}
	msg := <-tx.connector.ExecOut
	return msg.Rows, msg.Error
}

func (tx *rwTx) Commit() error {
	tx.connector.CommitIn <- struct{}{}
	err := <-tx.connector.Errors
	if err == nil {
		tx.close()
	}
	return err
}

func (tx *rwTx) Rollback() error {
	tx.connector.RollbackIn <- struct{}{}
	err := <-tx.connector.Errors
	if err == internal.ErrAborted {
		tx.close()
		return nil
	}
	return err
}
