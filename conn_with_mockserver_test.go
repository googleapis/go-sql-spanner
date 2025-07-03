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
	"fmt"
	"reflect"
	"testing"

	"cloud.google.com/go/spanner"
	"cloud.google.com/go/spanner/apiv1/spannerpb"
	"github.com/googleapis/go-sql-spanner/testutil"
	"google.golang.org/grpc/codes"
)

func TestBeginTx(t *testing.T) {
	t.Parallel()

	db, server, teardown := setupTestDBConnection(t)
	defer teardown()
	ctx := context.Background()

	tx, _ := db.BeginTx(ctx, &sql.TxOptions{})
	_, _ = tx.ExecContext(ctx, testutil.UpdateBarSetFoo)
	_ = tx.Rollback()

	requests := drainRequestsFromServer(server.TestSpanner)
	beginRequests := requestsOfType(requests, reflect.TypeOf(&spannerpb.BeginTransactionRequest{}))
	if g, w := len(beginRequests), 0; g != w {
		t.Fatalf("begin requests count mismatch\n Got: %v\nWant: %v", g, w)
	}
	executeRequests := requestsOfType(requests, reflect.TypeOf(&spannerpb.ExecuteSqlRequest{}))
	if g, w := len(executeRequests), 1; g != w {
		t.Fatalf("execute requests count mismatch\n Got: %v\nWant: %v", g, w)
	}
	request := executeRequests[0].(*spannerpb.ExecuteSqlRequest)
	if request.GetTransaction() == nil || request.GetTransaction().GetBegin() == nil {
		t.Fatal("missing begin transaction on ExecuteSqlRequest")
	}
	if g, w := request.GetTransaction().GetBegin().GetIsolationLevel(), spannerpb.TransactionOptions_ISOLATION_LEVEL_UNSPECIFIED; g != w {
		t.Fatalf("begin isolation level mismatch\n Got: %v\nWant: %v", g, w)
	}
}

func TestTwoTransactionsOnOneConn(t *testing.T) {
	t.Parallel()

	db, _, teardown := setupTestDBConnection(t)
	defer teardown()
	ctx := context.Background()

	c, _ := db.Conn(ctx)
	tx1, err := c.BeginTx(ctx, &sql.TxOptions{})
	defer tx1.Rollback()
	if err != nil {
		t.Fatal(err)
	}
	_, err = c.BeginTx(ctx, &sql.TxOptions{})
	if g, w := spanner.ErrCode(err), codes.FailedPrecondition; g != w {
		t.Fatalf("BeginTx error code mismatch\n Got: %v\nWant: %v", g, w)
	}
}

func TestExplicitBeginTx(t *testing.T) {
	t.Parallel()

	db, server, teardown := setupTestDBConnectionWithConnectorConfig(t, ConnectorConfig{
		Project:  "p",
		Instance: "i",
		Database: "d",

		BeginTransactionOption: spanner.ExplicitBeginTransaction,
	})
	defer teardown()
	ctx := context.Background()

	for _, readOnly := range []bool{true, false} {
		tx, err := db.BeginTx(ctx, &sql.TxOptions{ReadOnly: readOnly})
		if err != nil {
			t.Fatal(err)
		}
		res, err := tx.QueryContext(ctx, testutil.SelectFooFromBar)
		if err != nil {
			t.Fatal(err)
		}
		for res.Next() {
		}
		if err := res.Err(); err != nil {
			t.Fatal(err)
		}
		if err := tx.Rollback(); err != nil {
			t.Fatal(err)
		}

		requests := drainRequestsFromServer(server.TestSpanner)
		beginRequests := requestsOfType(requests, reflect.TypeOf(&spannerpb.BeginTransactionRequest{}))
		if g, w := len(beginRequests), 1; g != w {
			t.Fatalf("begin requests count mismatch\n Got: %v\nWant: %v", g, w)
		}
		executeRequests := requestsOfType(requests, reflect.TypeOf(&spannerpb.ExecuteSqlRequest{}))
		if g, w := len(executeRequests), 1; g != w {
			t.Fatalf("execute requests count mismatch\n Got: %v\nWant: %v", g, w)
		}
		request := executeRequests[0].(*spannerpb.ExecuteSqlRequest)
		if request.GetTransaction() == nil || request.GetTransaction().GetId() == nil {
			t.Fatal("missing transaction id on ExecuteSqlRequest")
		}
	}
}

func TestBeginTxWithIsolationLevel(t *testing.T) {
	t.Parallel()

	db, server, teardown := setupTestDBConnection(t)
	defer teardown()
	ctx := context.Background()

	for _, level := range []sql.IsolationLevel{
		sql.LevelDefault,
		sql.LevelSnapshot,
		sql.LevelRepeatableRead,
		sql.LevelSerializable,
	} {
		originalLevel := level
		for _, disableRetryAborts := range []bool{true, false} {
			if disableRetryAborts {
				level = WithDisableRetryAborts(originalLevel)
			} else {
				level = originalLevel
			}
			tx, _ := db.BeginTx(ctx, &sql.TxOptions{
				Isolation: level,
			})
			_, _ = tx.ExecContext(ctx, testutil.UpdateBarSetFoo)
			_ = tx.Rollback()

			requests := drainRequestsFromServer(server.TestSpanner)
			beginRequests := requestsOfType(requests, reflect.TypeOf(&spannerpb.BeginTransactionRequest{}))
			if g, w := len(beginRequests), 0; g != w {
				t.Fatalf("begin requests count mismatch\n Got: %v\nWant: %v", g, w)
			}
			executeRequests := requestsOfType(requests, reflect.TypeOf(&spannerpb.ExecuteSqlRequest{}))
			if g, w := len(executeRequests), 1; g != w {
				t.Fatalf("execute requests count mismatch\n Got: %v\nWant: %v", g, w)
			}
			request := executeRequests[0].(*spannerpb.ExecuteSqlRequest)
			if request.GetTransaction() == nil || request.GetTransaction().GetBegin() == nil {
				t.Fatalf("execute request does not have a begin transaction")
			}
			wantIsolationLevel, _ := toProtoIsolationLevel(originalLevel)
			if g, w := request.GetTransaction().GetBegin().GetIsolationLevel(), wantIsolationLevel; g != w {
				t.Fatalf("begin isolation level mismatch\n Got: %v\nWant: %v", g, w)
			}
		}
	}
}

func TestBeginTxWithInvalidIsolationLevel(t *testing.T) {
	t.Parallel()

	db, _, teardown := setupTestDBConnection(t)
	defer teardown()
	ctx := context.Background()

	for _, level := range []sql.IsolationLevel{
		sql.LevelReadUncommitted,
		sql.LevelReadCommitted,
		sql.LevelWriteCommitted,
		sql.LevelLinearizable,
	} {
		originalLevel := level
		for _, disableRetryAborts := range []bool{true, false} {
			if disableRetryAborts {
				level = WithDisableRetryAborts(originalLevel)
			} else {
				level = originalLevel
			}
			_, err := db.BeginTx(ctx, &sql.TxOptions{
				Isolation: level,
			})
			if err == nil {
				t.Fatalf("BeginTx should have failed with invalid isolation level: %v", level)
			}
		}
	}
}

func TestDefaultIsolationLevel(t *testing.T) {
	t.Parallel()

	for _, level := range []sql.IsolationLevel{
		sql.LevelDefault,
		sql.LevelSnapshot,
		sql.LevelRepeatableRead,
		sql.LevelSerializable,
	} {
		db, server, teardown := setupTestDBConnectionWithParams(t, fmt.Sprintf("isolationLevel=%v", level))
		defer teardown()
		ctx := context.Background()

		conn, err := db.Conn(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if err := conn.Raw(func(driverConn interface{}) error {
			spannerConn, ok := driverConn.(SpannerConn)
			if !ok {
				return fmt.Errorf("expected spanner conn, got %T", driverConn)
			}
			if spannerConn.IsolationLevel() != level {
				return fmt.Errorf("expected isolation level %v, got %v", level, spannerConn.IsolationLevel())
			}
			return nil
		}); err != nil {
			t.Fatal(err)
		}

		originalLevel := level
		for _, disableRetryAborts := range []bool{true, false} {
			if disableRetryAborts {
				level = WithDisableRetryAborts(originalLevel)
			} else {
				level = originalLevel
			}
			// Note: No isolation level is passed in here, so it will use the default.
			tx, _ := db.BeginTx(ctx, &sql.TxOptions{})
			_, _ = tx.ExecContext(ctx, testutil.UpdateBarSetFoo)
			_ = tx.Rollback()

			requests := drainRequestsFromServer(server.TestSpanner)
			beginRequests := requestsOfType(requests, reflect.TypeOf(&spannerpb.BeginTransactionRequest{}))
			if g, w := len(beginRequests), 0; g != w {
				t.Fatalf("begin requests count mismatch\n Got: %v\nWant: %v", g, w)
			}
			executeRequests := requestsOfType(requests, reflect.TypeOf(&spannerpb.ExecuteSqlRequest{}))
			if g, w := len(executeRequests), 1; g != w {
				t.Fatalf("execute requests count mismatch\n Got: %v\nWant: %v", g, w)
			}
			request := executeRequests[0].(*spannerpb.ExecuteSqlRequest)
			if request.GetTransaction() == nil || request.GetTransaction().GetBegin() == nil {
				t.Fatalf("ExecuteSqlRequest should have a Begin transaction")
			}
			wantIsolationLevel, _ := toProtoIsolationLevel(originalLevel)
			if g, w := request.GetTransaction().GetBegin().GetIsolationLevel(), wantIsolationLevel; g != w {
				t.Fatalf("begin isolation level mismatch\n Got: %v\nWant: %v", g, w)
			}
		}
	}
}

func TestSetIsolationLevel(t *testing.T) {
	t.Parallel()

	db, _, teardown := setupTestDBConnection(t)
	defer teardown()
	ctx := context.Background()

	// Repeat twice to ensure that the state is reset after closing the connection.
	for i := 0; i < 2; i++ {
		conn, err := db.Conn(ctx)
		if err != nil {
			t.Fatal(err)
		}
		var level sql.IsolationLevel
		_ = conn.Raw(func(driverConn interface{}) error {
			level = driverConn.(SpannerConn).IsolationLevel()
			return nil
		})
		if g, w := level, sql.LevelDefault; g != w {
			t.Fatalf("isolation level mismatch\n Got: %v\nWant: %v", g, w)
		}
		_ = conn.Raw(func(driverConn interface{}) error {
			return driverConn.(SpannerConn).SetIsolationLevel(sql.LevelSnapshot)
		})
		_ = conn.Raw(func(driverConn interface{}) error {
			level = driverConn.(SpannerConn).IsolationLevel()
			return nil
		})
		if g, w := level, sql.LevelSnapshot; g != w {
			t.Fatalf("isolation level mismatch\n Got: %v\nWant: %v", g, w)
		}
		_ = conn.Close()
	}
}

func TestIsolationLevelAutoCommit(t *testing.T) {
	t.Parallel()

	db, server, teardown := setupTestDBConnection(t)
	defer teardown()
	ctx := context.Background()

	for _, level := range []sql.IsolationLevel{
		sql.LevelDefault,
		sql.LevelSnapshot,
		sql.LevelRepeatableRead,
		sql.LevelSerializable,
	} {
		spannerLevel, _ := toProtoIsolationLevel(level)
		_, _ = db.ExecContext(ctx, testutil.UpdateBarSetFoo, ExecOptions{TransactionOptions: spanner.TransactionOptions{
			IsolationLevel: spannerLevel,
		}})

		requests := drainRequestsFromServer(server.TestSpanner)
		executeRequests := requestsOfType(requests, reflect.TypeOf(&spannerpb.ExecuteSqlRequest{}))
		if g, w := len(executeRequests), 1; g != w {
			t.Fatalf("execute requests count mismatch\n Got: %v\nWant: %v", g, w)
		}
		request := executeRequests[0].(*spannerpb.ExecuteSqlRequest)
		wantIsolationLevel, _ := toProtoIsolationLevel(level)
		if g, w := request.Transaction.GetBegin().GetIsolationLevel(), wantIsolationLevel; g != w {
			t.Fatalf("begin isolation level mismatch\n Got: %v\nWant: %v", g, w)
		}
	}
}

func TestDDLUsingQueryContext(t *testing.T) {
	t.Parallel()

	db, _, teardown := setupTestDBConnection(t)
	defer teardown()
	ctx := context.Background()

	// DDL statements should not use the query context.
	_, err := db.QueryContext(ctx, "CREATE TABLE Foo (Bar STRING(100))")
	if err == nil {
		t.Fatal("expected error for DDL statement using QueryContext, got nil")
	}
	if g, w := err.Error(), `spanner: code = "FailedPrecondition", desc = "QueryContext does not support DDL statements, use ExecContext instead"`; g != w {
		t.Fatalf("error mismatch\n Got: %v\nWant: %v", g, w)
	}
}

func TestDDLUsingQueryContextInReadOnlyTx(t *testing.T) {
	t.Parallel()

	db, _, teardown := setupTestDBConnection(t)
	defer teardown()
	ctx := context.Background()
	tx, err := db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback() }()

	// DDL statements should not use the query context in a read-only transaction.
	_, err = tx.QueryContext(ctx, "CREATE TABLE Foo (Bar STRING(100))")
	if err == nil {
		t.Fatal("expected error for DDL statement using QueryContext in read-only transaction, got nil")
	}
	if g, w := err.Error(), `spanner: code = "FailedPrecondition", desc = "QueryContext does not support DDL statements, use ExecContext instead"`; g != w {
		t.Fatalf("error mismatch\n Got: %v\nWant: %v", g, w)
	}
}

func TestDDLUsingQueryContextInReadWriteTransaction(t *testing.T) {
	t.Parallel()

	db, _, teardown := setupTestDBConnection(t)
	defer teardown()
	ctx := context.Background()

	tx, err := db.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback() }()

	// DDL statements should not use the query context in a read-write transaction.
	_, err = tx.QueryContext(ctx, "CREATE TABLE Foo (Bar STRING(100))")
	if err == nil {
		t.Fatal("expected error for DDL statement using QueryContext in read-write transaction, got nil")
	}
	if g, w := err.Error(), `spanner: code = "FailedPrecondition", desc = "QueryContext does not support DDL statements, use ExecContext instead"`; g != w {
		t.Fatalf("error mismatch\n Got: %v\nWant: %v", g, w)
	}
}

func TestRunDmlBatch(t *testing.T) {
	t.Parallel()

	db, _, teardown := setupTestDBConnection(t)
	defer teardown()
	ctx := context.Background()

	conn, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer silentClose(conn)
	if err := conn.Raw(func(driverConn interface{}) error {
		spannerConn, _ := driverConn.(SpannerConn)
		return spannerConn.StartBatchDML()
	}); err != nil {
		t.Fatal(err)
	}
	// Buffer two DML statements.
	for range 2 {
		if _, err := conn.ExecContext(ctx, testutil.UpdateBarSetFoo); err != nil {
			t.Fatal(err)
		}
	}
	var res SpannerResult
	if err := conn.Raw(func(driverConn interface{}) (err error) {
		spannerConn, _ := driverConn.(SpannerConn)
		res, err = spannerConn.RunDmlBatch(ctx)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	affected, err := res.BatchRowsAffected()
	if err != nil {
		t.Fatal(err)
	}
	if g, w := affected, []int64{testutil.UpdateBarSetFooRowCount, testutil.UpdateBarSetFooRowCount}; !reflect.DeepEqual(g, w) {
		t.Fatalf("affected mismatch\n Got: %v\nWant: %v", g, w)
	}
}

func TestSetRetryAbortsInternallyInInactiveTransaction(t *testing.T) {
	t.Parallel()

	db, _, teardown := setupTestDBConnection(t)
	defer teardown()
	ctx := context.Background()

	tx, err := db.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.ExecContext(ctx, "set retry_aborts_internally = false"); err != nil {
		t.Fatal(err)
	}
	_ = tx.Rollback()
}

func TestSetRetryAbortsInternallyInActiveTransaction(t *testing.T) {
	t.Parallel()

	db, _, teardown := setupTestDBConnection(t)
	defer teardown()
	ctx := context.Background()

	tx, err := db.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.ExecContext(ctx, testutil.UpdateBarSetFoo); err != nil {
		t.Fatal(err)
	}
	_, err = tx.ExecContext(ctx, "set retry_aborts_internally = false")
	if g, w := err.Error(), "spanner: code = \"FailedPrecondition\", desc = \"cannot change retry mode while a transaction is active\""; g != w {
		t.Fatalf("error mismatch\n Got: %v\nWant: %v", g, w)
	}
	_ = tx.Rollback()
}
