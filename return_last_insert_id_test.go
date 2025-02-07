// Copyright 2025 Google LLC
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
	"context"
	"database/sql"
	"math/bits"
	"testing"

	"cloud.google.com/go/spanner/apiv1/spannerpb"
	"github.com/googleapis/go-sql-spanner/testutil"
)

type queryExecutor interface {
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
}

func TestReturnLastInsertId_NoThenReturn(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	db, server, teardown := setupTestDBConnection(t)
	defer teardown()

	query := "insert into test (value) values ('One')"
	if err := server.TestSpanner.PutStatementResult(query, &testutil.StatementResult{
		Type:        testutil.StatementResultUpdateCount,
		UpdateCount: 1,
	}); err != nil {
		t.Fatal(err)
	}

	for _, useTx := range []bool{true, false} {
		var tx queryExecutor
		var err error
		if useTx {
			tx, err = db.BeginTx(ctx, &sql.TxOptions{})
			if err != nil {
				t.Fatalf("failed to start transaction: %v", err)
			}
		} else {
			tx = db
		}
		res, err := tx.ExecContext(ctx, query)
		if err != nil {
			t.Fatalf("failed to execute statement: %v", err)
		}
		if c, err := res.RowsAffected(); err != nil {
			t.Fatalf("failed to get rows affected: %v", err)
		} else if g, w := c, int64(1); g != w {
			t.Fatalf("affected rows mismatch\n Got: %v\nWant: %v", g, w)
		}
		if _, err := res.LastInsertId(); err != errNoLastInsertId {
			t.Fatalf("missing expected error for last insert id, got %v", err)
		}
		if tx, ok := tx.(*sql.Tx); ok {
			if err := tx.Rollback(); err != nil {
				t.Fatal(err)
			}
		}
	}
}

func TestReturnLastInsertId_WithThenReturn(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	db, server, teardown := setupTestDBConnection(t)
	defer teardown()

	generatedId := int64(bits.Reverse64(uint64(1)))
	query := "insert into test (value) values ('One') then return id"
	if err := server.TestSpanner.PutStatementResult(query, &testutil.StatementResult{
		Type:        testutil.StatementResultResultSet,
		ResultSet:   testutil.CreateSingleColumnInt64ResultSet([]int64{generatedId}, "id"),
		UpdateCount: 1,
	}); err != nil {
		t.Fatal(err)
	}

	for _, useTx := range []bool{true, false} {
		var tx queryExecutor
		var err error
		if useTx {
			tx, err = db.BeginTx(ctx, &sql.TxOptions{})
			if err != nil {
				t.Fatalf("failed to start transaction: %v", err)
			}
		} else {
			tx = db
		}
		res, err := tx.ExecContext(ctx, query)
		if err != nil {
			t.Fatalf("failed to execute statement: %v", err)
		}
		if c, err := res.RowsAffected(); err != nil {
			t.Fatalf("failed to get rows affected: %v", err)
		} else if g, w := c, int64(1); g != w {
			t.Fatalf("affected rows mismatch\n Got: %v\nWant: %v", g, w)
		}
		if id, err := res.LastInsertId(); err != nil {
			t.Fatalf("failed to get LastInsertId: %v", err)
		} else if g, w := id, generatedId; g != w {
			t.Fatalf("affected rows mismatch\n Got: %v\nWant: %v", g, w)
		}
		if tx, ok := tx.(*sql.Tx); ok {
			if err := tx.Commit(); err != nil {
				t.Fatal(err)
			}
		}
	}
}

func TestReturnLastInsertId_WithThenReturnNonInt64Col(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	db, server, teardown := setupTestDBConnection(t)
	defer teardown()

	resultSet := testutil.CreateSelect1ResultSet()
	resultSet.Metadata.RowType.Fields[0].Type.Code = spannerpb.TypeCode_STRING
	query := "insert into test (value) values ('One') then return id"
	if err := server.TestSpanner.PutStatementResult(query, &testutil.StatementResult{
		Type:        testutil.StatementResultResultSet,
		ResultSet:   resultSet,
		UpdateCount: 1,
	}); err != nil {
		t.Fatal(err)
	}

	for _, useTx := range []bool{true, false} {
		var tx queryExecutor
		var err error
		if useTx {
			tx, err = db.BeginTx(ctx, &sql.TxOptions{})
			if err != nil {
				t.Fatalf("failed to start transaction: %v", err)
			}
		} else {
			tx = db
		}
		if _, err := tx.ExecContext(ctx, query); err != errInvalidDmlForExecContext {
			t.Fatalf("missing expected error for ExecContext, got %v", err)
		}
		if tx, ok := tx.(*sql.Tx); ok {
			if err := tx.Rollback(); err != nil {
				t.Fatal(err)
			}
		}
	}
}

func TestReturnLastInsertId_WithThenReturnMultipleColumns(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	db, server, teardown := setupTestDBConnection(t)
	defer teardown()

	resultSet := testutil.CreateTwoColumnResultSet([][2]int64{}, [2]string{"id1", "id2"})
	query := "insert into test (value) values ('One') then return id1, id2"
	if err := server.TestSpanner.PutStatementResult(query, &testutil.StatementResult{
		Type:        testutil.StatementResultResultSet,
		ResultSet:   resultSet,
		UpdateCount: 1,
	}); err != nil {
		t.Fatal(err)
	}

	for _, useTx := range []bool{true, false} {
		var tx queryExecutor
		var err error
		if useTx {
			tx, err = db.BeginTx(ctx, &sql.TxOptions{})
			if err != nil {
				t.Fatalf("failed to start transaction: %v", err)
			}
		} else {
			tx = db
		}
		if _, err := tx.ExecContext(ctx, query); err != errInvalidDmlForExecContext {
			t.Fatalf("missing expected error for ExecContext, got %v", err)
		}
		if tx, ok := tx.(*sql.Tx); ok {
			if err := tx.Rollback(); err != nil {
				t.Fatal(err)
			}
		}
	}
}

func TestReturnLastInsertId_WithThenReturnMultipleRows(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	db, server, teardown := setupTestDBConnection(t)
	defer teardown()

	resultSet := testutil.CreateSingleColumnInt64ResultSet([]int64{1, 2}, "id")
	query := "insert into test (value) values ('One'), ('Two') then return id"
	if err := server.TestSpanner.PutStatementResult(query, &testutil.StatementResult{
		Type:        testutil.StatementResultResultSet,
		ResultSet:   resultSet,
		UpdateCount: 2,
	}); err != nil {
		t.Fatal(err)
	}

	for _, useTx := range []bool{true, false} {
		var tx queryExecutor
		var err error
		if useTx {
			tx, err = db.BeginTx(ctx, &sql.TxOptions{})
			if err != nil {
				t.Fatalf("failed to start transaction: %v", err)
			}
		} else {
			tx = db
		}
		if _, err := tx.ExecContext(ctx, query); err != errInvalidDmlForExecContext {
			t.Fatalf("missing expected error for ExecContext, got %v", err)
		}
		if tx, ok := tx.(*sql.Tx); ok {
			if err := tx.Rollback(); err != nil {
				t.Fatal(err)
			}
		}
	}
}
