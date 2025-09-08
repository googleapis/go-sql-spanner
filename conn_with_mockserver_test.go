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

	"cloud.google.com/go/longrunning/autogen/longrunningpb"
	"cloud.google.com/go/spanner"
	"cloud.google.com/go/spanner/admin/database/apiv1/databasepb"
	"cloud.google.com/go/spanner/apiv1/spannerpb"
	"github.com/googleapis/go-sql-spanner/connectionstate"
	"github.com/googleapis/go-sql-spanner/testutil"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/emptypb"
)

func TestBeginTx(t *testing.T) {
	t.Parallel()

	db, server, teardown := setupTestDBConnection(t)
	defer teardown()
	ctx := context.Background()

	tx, _ := db.BeginTx(ctx, &sql.TxOptions{})
	_, _ = tx.ExecContext(ctx, testutil.UpdateBarSetFoo)
	_ = tx.Rollback()

	requests := server.TestSpanner.DrainRequestsFromServer()
	beginRequests := testutil.RequestsOfType(requests, reflect.TypeOf(&spannerpb.BeginTransactionRequest{}))
	if g, w := len(beginRequests), 0; g != w {
		t.Fatalf("begin requests count mismatch\n Got: %v\nWant: %v", g, w)
	}
	executeRequests := testutil.RequestsOfType(requests, reflect.TypeOf(&spannerpb.ExecuteSqlRequest{}))
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
	defer func() { _ = tx1.Rollback() }()
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

		requests := server.TestSpanner.DrainRequestsFromServer()
		beginRequests := testutil.RequestsOfType(requests, reflect.TypeOf(&spannerpb.BeginTransactionRequest{}))
		if g, w := len(beginRequests), 1; g != w {
			t.Fatalf("begin requests count mismatch\n Got: %v\nWant: %v", g, w)
		}
		executeRequests := testutil.RequestsOfType(requests, reflect.TypeOf(&spannerpb.ExecuteSqlRequest{}))
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

			requests := server.TestSpanner.DrainRequestsFromServer()
			beginRequests := testutil.RequestsOfType(requests, reflect.TypeOf(&spannerpb.BeginTransactionRequest{}))
			if g, w := len(beginRequests), 0; g != w {
				t.Fatalf("begin requests count mismatch\n Got: %v\nWant: %v", g, w)
			}
			executeRequests := testutil.RequestsOfType(requests, reflect.TypeOf(&spannerpb.ExecuteSqlRequest{}))
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

			requests := server.TestSpanner.DrainRequestsFromServer()
			beginRequests := testutil.RequestsOfType(requests, reflect.TypeOf(&spannerpb.BeginTransactionRequest{}))
			if g, w := len(beginRequests), 0; g != w {
				t.Fatalf("begin requests count mismatch\n Got: %v\nWant: %v", g, w)
			}
			executeRequests := testutil.RequestsOfType(requests, reflect.TypeOf(&spannerpb.ExecuteSqlRequest{}))
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

		requests := server.TestSpanner.DrainRequestsFromServer()
		executeRequests := testutil.RequestsOfType(requests, reflect.TypeOf(&spannerpb.ExecuteSqlRequest{}))
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

func TestDefaultReadLockMode(t *testing.T) {
	t.Parallel()

	for mode, name := range spannerpb.TransactionOptions_ReadWrite_ReadLockMode_name {
		readLockMode := spannerpb.TransactionOptions_ReadWrite_ReadLockMode(mode)
		db, server, teardown := setupTestDBConnectionWithParams(t, fmt.Sprintf("read_lock_mode=%v", name))
		defer teardown()
		ctx := context.Background()

		c, err := db.Conn(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if err := c.Raw(func(driverConn interface{}) error {
			spannerConn, ok := driverConn.(*conn)
			if !ok {
				return fmt.Errorf("expected spanner conn, got %T", driverConn)
			}
			if spannerConn.ReadLockMode() != readLockMode {
				return fmt.Errorf("expected read lock mode %v, got %v", readLockMode, spannerConn.ReadLockMode())
			}
			return nil
		}); err != nil {
			t.Fatal(err)
		}

		tx, _ := db.BeginTx(ctx, &sql.TxOptions{})
		_, _ = tx.ExecContext(ctx, testutil.UpdateBarSetFoo)
		_ = tx.Rollback()

		requests := server.TestSpanner.DrainRequestsFromServer()
		beginRequests := testutil.RequestsOfType(requests, reflect.TypeOf(&spannerpb.BeginTransactionRequest{}))
		if g, w := len(beginRequests), 0; g != w {
			t.Fatalf("begin requests count mismatch\n Got: %v\nWant: %v", g, w)
		}
		executeRequests := testutil.RequestsOfType(requests, reflect.TypeOf(&spannerpb.ExecuteSqlRequest{}))
		if g, w := len(executeRequests), 1; g != w {
			t.Fatalf("execute requests count mismatch\n Got: %v\nWant: %v", g, w)
		}
		request := executeRequests[0].(*spannerpb.ExecuteSqlRequest)
		if request.GetTransaction() == nil || request.GetTransaction().GetBegin() == nil {
			t.Fatalf("ExecuteSqlRequest should have a Begin transaction")
		}
		if g, w := request.GetTransaction().GetBegin().GetReadWrite().GetReadLockMode(), readLockMode; g != w {
			t.Fatalf("begin read lock mode mismatch\n Got: %v\nWant: %v", g, w)
		}
	}
}

func TestSetReadLockMode(t *testing.T) {
	t.Parallel()

	db, _, teardown := setupTestDBConnection(t)
	defer teardown()
	ctx := context.Background()

	// Repeat twice to ensure that the state is reset after closing the connection.
	for i := 0; i < 2; i++ {
		c, err := db.Conn(ctx)
		if err != nil {
			t.Fatal(err)
		}
		var readLockMode spannerpb.TransactionOptions_ReadWrite_ReadLockMode
		_ = c.Raw(func(driverConn interface{}) error {
			readLockMode = driverConn.(*conn).ReadLockMode()
			return nil
		})
		if g, w := readLockMode, spannerpb.TransactionOptions_ReadWrite_READ_LOCK_MODE_UNSPECIFIED; g != w {
			t.Fatalf("read lock mode mismatch\n Got: %v\nWant: %v", g, w)
		}
		_ = c.Raw(func(driverConn interface{}) error {
			return driverConn.(SpannerConn).SetReadLockMode(spannerpb.TransactionOptions_ReadWrite_OPTIMISTIC)
		})
		_ = c.Raw(func(driverConn interface{}) error {
			readLockMode = driverConn.(SpannerConn).ReadLockMode()
			return nil
		})
		if g, w := readLockMode, spannerpb.TransactionOptions_ReadWrite_OPTIMISTIC; g != w {
			t.Fatalf("read lock mode mismatch\n Got: %v\nWant: %v", g, w)
		}
		_ = c.Close()
	}
}

func TestReadLockModeAutoCommit(t *testing.T) {
	t.Parallel()

	db, server, teardown := setupTestDBConnection(t)
	defer teardown()
	ctx := context.Background()

	for mode := range spannerpb.TransactionOptions_ReadWrite_ReadLockMode_name {
		readLockMode := spannerpb.TransactionOptions_ReadWrite_ReadLockMode(mode)
		_, _ = db.ExecContext(ctx, testutil.UpdateBarSetFoo, ExecOptions{TransactionOptions: spanner.TransactionOptions{
			ReadLockMode: readLockMode,
		}})

		requests := server.TestSpanner.DrainRequestsFromServer()
		executeRequests := testutil.RequestsOfType(requests, reflect.TypeOf(&spannerpb.ExecuteSqlRequest{}))
		if g, w := len(executeRequests), 1; g != w {
			t.Fatalf("execute requests count mismatch\n Got: %v\nWant: %v", g, w)
		}
		request := executeRequests[0].(*spannerpb.ExecuteSqlRequest)
		if g, w := request.Transaction.GetBegin().GetReadWrite().GetReadLockMode(), readLockMode; g != w {
			t.Fatalf("begin read lock mode mismatch\n Got: %v\nWant: %v", g, w)
		}
	}
}

func TestSetLocalReadLockMode(t *testing.T) {
	t.Parallel()

	db, server, teardown := setupTestDBConnection(t)
	// Make sure we only have one connection in the pool.
	db.SetMaxOpenConns(1)
	defer teardown()
	ctx := context.Background()

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.ExecContext(ctx, "set local read_lock_mode='optimistic'"); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.ExecContext(ctx, testutil.UpdateBarSetFoo); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}

	requests := server.TestSpanner.DrainRequestsFromServer()
	beginRequests := testutil.RequestsOfType(requests, reflect.TypeOf(&spannerpb.BeginTransactionRequest{}))
	if g, w := len(beginRequests), 0; g != w {
		t.Fatalf("begin requests count mismatch\n Got: %v\nWant: %v", g, w)
	}
	executeRequests := testutil.RequestsOfType(requests, reflect.TypeOf(&spannerpb.ExecuteSqlRequest{}))
	if g, w := len(executeRequests), 1; g != w {
		t.Fatalf("execute requests count mismatch\n Got: %v\nWant: %v", g, w)
	}
	request := executeRequests[0].(*spannerpb.ExecuteSqlRequest)
	if request.GetTransaction() == nil || request.GetTransaction().GetBegin() == nil {
		t.Fatalf("ExecuteSqlRequest should have a Begin transaction")
	}
	if g, w := request.GetTransaction().GetBegin().GetReadWrite().GetReadLockMode(), spannerpb.TransactionOptions_ReadWrite_OPTIMISTIC; g != w {
		t.Fatalf("begin read lock mode mismatch\n Got: %v\nWant: %v", g, w)
	}

	// Execute another transaction without a specific read lock mode. This should then use the default.
	tx, err = db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.ExecContext(ctx, testutil.UpdateBarSetFoo); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	requests = server.TestSpanner.DrainRequestsFromServer()
	beginRequests = testutil.RequestsOfType(requests, reflect.TypeOf(&spannerpb.BeginTransactionRequest{}))
	if g, w := len(beginRequests), 0; g != w {
		t.Fatalf("begin requests count mismatch\n Got: %v\nWant: %v", g, w)
	}
	executeRequests = testutil.RequestsOfType(requests, reflect.TypeOf(&spannerpb.ExecuteSqlRequest{}))
	if g, w := len(executeRequests), 1; g != w {
		t.Fatalf("execute requests count mismatch\n Got: %v\nWant: %v", g, w)
	}
	request = executeRequests[0].(*spannerpb.ExecuteSqlRequest)
	if request.GetTransaction() == nil || request.GetTransaction().GetBegin() == nil {
		t.Fatalf("ExecuteSqlRequest should have a Begin transaction")
	}
	if g, w := request.GetTransaction().GetBegin().GetReadWrite().GetReadLockMode(), spannerpb.TransactionOptions_ReadWrite_READ_LOCK_MODE_UNSPECIFIED; g != w {
		t.Fatalf("begin read lock mode mismatch\n Got: %v\nWant: %v", g, w)
	}
}

func TestCreateDatabase(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	db, server, teardown := setupTestDBConnection(t)
	defer teardown()

	var expectedResponse = &databasepb.Database{}
	anyMsg, _ := anypb.New(expectedResponse)
	server.TestDatabaseAdmin.SetResps([]proto.Message{
		&longrunningpb.Operation{
			Done:   true,
			Result: &longrunningpb.Operation_Response{Response: anyMsg},
			Name:   "test-operation",
		},
	})

	conn, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer silentClose(conn)

	if _, err = conn.ExecContext(ctx, "create database `foo`"); err != nil {
		t.Fatalf("failed to execute CREATE DATABASE: %v", err)
	}

	requests := server.TestDatabaseAdmin.Reqs()
	if g, w := len(requests), 1; g != w {
		t.Fatalf("requests count mismatch\nGot: %v\nWant: %v", g, w)
	}
	if req, ok := requests[0].(*databasepb.CreateDatabaseRequest); ok {
		if g, w := req.Parent, "projects/p/instances/i"; g != w {
			t.Fatalf("parent mismatch\n Got: %v\nWant: %v", g, w)
		}
	} else {
		t.Fatalf("request type mismatch, got %v", requests[0])
	}
}

func TestDropDatabase(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	db, server, teardown := setupTestDBConnection(t)
	defer teardown()

	server.TestDatabaseAdmin.SetResps([]proto.Message{&emptypb.Empty{}})

	conn, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer silentClose(conn)

	if _, err = conn.ExecContext(ctx, "drop database foo"); err != nil {
		t.Fatalf("failed to execute DROP DATABASE: %v", err)
	}

	requests := server.TestDatabaseAdmin.Reqs()
	if g, w := len(requests), 1; g != w {
		t.Fatalf("requests count mismatch\nGot: %v\nWant: %v", g, w)
	}
	if req, ok := requests[0].(*databasepb.DropDatabaseRequest); ok {
		if g, w := req.Database, "projects/p/instances/i/databases/foo"; g != w {
			t.Fatalf("database name mismatch\n Got: %v\nWant: %v", g, w)
		}
	} else {
		t.Fatalf("request type mismatch, got %v", requests[0])
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
	// It is (now) allowed to change this value during a transaction, but it won't have an effect on the
	// running transaction.
	if _, err := tx.ExecContext(ctx, "set retry_aborts_internally = false"); err != nil {
		t.Fatal(err)
	}
	_ = tx.Rollback()
}

func TestSetAutocommitDMLMode(t *testing.T) {
	t.Parallel()

	for _, tp := range []connectionstate.Type{connectionstate.TypeTransactional, connectionstate.TypeNonTransactional} {
		db, _, teardown := setupTestDBConnectionWithConnectorConfig(t, ConnectorConfig{
			Project:             "p",
			Instance:            "i",
			Database:            "d",
			ConnectionStateType: tp,
		})
		defer teardown()

		conn, err := db.Conn(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = conn.Close() }()

		_ = conn.Raw(func(driverConn interface{}) error {
			c, _ := driverConn.(SpannerConn)
			if g, w := c.AutocommitDMLMode(), Transactional; g != w {
				t.Fatalf("initial value mismatch\n Got: %v\nWant: %v", g, w)
			}
			if err := c.SetAutocommitDMLMode(PartitionedNonAtomic); err != nil {
				t.Fatal(err)
			}
			if g, w := c.AutocommitDMLMode(), PartitionedNonAtomic; g != w {
				t.Fatalf("value mismatch\n Got: %v\nWant: %v", g, w)
			}
			return nil
		})

		// Set the value in a transaction and commit.
		tx, err := conn.BeginTx(context.Background(), &sql.TxOptions{})
		if err != nil {
			t.Fatal(err)
		}
		_ = conn.Raw(func(driverConn interface{}) error {
			c, _ := driverConn.(SpannerConn)
			// The value should be the same as before the transaction started.
			if g, w := c.AutocommitDMLMode(), PartitionedNonAtomic; g != w {
				t.Fatalf("value mismatch\n Got: %v\nWant: %v", g, w)
			}
			// Changes in a transaction should be visible in the transaction.
			if err := c.SetAutocommitDMLMode(Transactional); err != nil {
				t.Fatal(err)
			}
			if g, w := c.AutocommitDMLMode(), Transactional; g != w {
				t.Fatalf("value mismatch\n Got: %v\nWant: %v", g, w)
			}
			return nil
		})
		// Committing the transaction should make the change durable (and is a no-op if the connection state type is
		// non-transactional).
		if err := tx.Commit(); err != nil {
			t.Fatal(err)
		}
		_ = conn.Raw(func(driverConn interface{}) error {
			c, _ := driverConn.(SpannerConn)
			if g, w := c.AutocommitDMLMode(), Transactional; g != w {
				t.Fatalf("value mismatch\n Got: %v\nWant: %v", g, w)
			}
			return nil
		})

		// Set the value in a transaction and rollback.
		tx, err = conn.BeginTx(context.Background(), &sql.TxOptions{})
		if err != nil {
			t.Fatal(err)
		}
		_ = conn.Raw(func(driverConn interface{}) error {
			c, _ := driverConn.(SpannerConn)
			if err := c.SetAutocommitDMLMode(PartitionedNonAtomic); err != nil {
				t.Fatal(err)
			}
			if g, w := c.AutocommitDMLMode(), PartitionedNonAtomic; g != w {
				t.Fatalf("value mismatch\n Got: %v\nWant: %v", g, w)
			}
			return nil
		})
		// Rolling back the transaction will undo the change if the connection state is transactional.
		// In case of non-transactional state, the rollback does not have an effect, as the state change was persisted
		// directly when SetAutocommitDMLMode was called.
		if err := tx.Rollback(); err != nil {
			t.Fatal(err)
		}
		_ = conn.Raw(func(driverConn interface{}) error {
			c, _ := driverConn.(SpannerConn)
			var expected AutocommitDMLMode
			if tp == connectionstate.TypeTransactional {
				expected = Transactional
			} else {
				expected = PartitionedNonAtomic
			}
			if g, w := c.AutocommitDMLMode(), expected; g != w {
				t.Fatalf("value mismatch\n Got: %v\nWant: %v", g, w)
			}
			return nil
		})
	}
}

func TestConfigureConnectionProperty(t *testing.T) {
	t.Parallel()

	type config int
	const (
		configConnString config = iota
		configConnectorConfig
		configConnectorConfigAndParams
	)

	for _, cfg := range []config{configConnString, configConnectorConfig, configConnectorConfigAndParams} {
		var db *sql.DB
		var teardown func()
		switch cfg {
		case configConnString:
			db, _, teardown = setupTestDBConnectionWithParams(t, "isolationLevel=repeatable_read")
		case configConnectorConfig:
			db, _, teardown = setupTestDBConnectionWithConnectorConfig(t, ConnectorConfig{
				Project:        "p",
				Instance:       "i",
				Database:       "d",
				IsolationLevel: sql.LevelRepeatableRead,
			})
		case configConnectorConfigAndParams:
			db, _, teardown = setupTestDBConnectionWithConnectorConfig(t, ConnectorConfig{
				Project:        "p",
				Instance:       "i",
				Database:       "d",
				IsolationLevel: sql.LevelSerializable,
				// The configuration in the params (which come from the connection string)
				// take precedence over the configuration in the ConnectorConfig.
				Params: map[string]string{
					"isolation_level": "repeatable_read",
				},
			})
		default:
			t.Fatalf("invalid config: %v", cfg)
		}
		defer teardown()
		// Limit the number of open connections to 1 to ensure that the test re-uses the same connection.
		db.SetMaxOpenConns(1)

		// Repeat twice to ensure that we get a connection that has been reset
		// when getting a fresh connection from the pool
		for range 2 {
			sqlConn, err := db.Conn(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			_ = sqlConn.Raw(func(driverConn interface{}) error {
				c, _ := driverConn.(SpannerConn)
				if g, w := c.IsolationLevel(), sql.LevelRepeatableRead; g != w {
					t.Fatalf("isolation level mismatch\n Got: %v\nWant: %v", g, w)
				}
				if err := c.SetIsolationLevel(sql.LevelSerializable); err != nil {
					t.Fatal(err)
				}
				if g, w := c.IsolationLevel(), sql.LevelSerializable; g != w {
					t.Fatalf("isolation level mismatch\n Got: %v\nWant: %v", g, w)
				}
				// Reset the connection manually (this should also happen automatically when the connection is closed).
				sc := c.(*conn)
				if err := sc.ResetSession(context.Background()); err != nil {
					t.Fatal(err)
				}
				// Verify that the isolation level is reset to the value it had when the connection was created.
				if g, w := c.IsolationLevel(), sql.LevelRepeatableRead; g != w {
					t.Fatalf("isolation level mismatch\n Got: %v\nWant: %v", g, w)
				}
				// Set the isolation level back to serializable in order to ensure that it is also correctly reset
				// when the connection is closed.
				if err := c.SetIsolationLevel(sql.LevelSerializable); err != nil {
					t.Fatal(err)
				}
				return nil
			})
			if err := sqlConn.Close(); err != nil {
				t.Fatal(err)
			}
		}
	}
}

func TestSetAndShowWithExtension(t *testing.T) {
	t.Parallel()

	db, _, teardown := setupTestDBConnection(t)
	defer teardown()
	ctx := context.Background()

	conn, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close() }()

	// Getting an unknown variable fails, even if it is a variable with an extension.
	if _, err := conn.QueryContext(ctx, "show variable my_extension.my_property"); err == nil {
		t.Fatal("missing expected error")
	}

	// Setting an unknown variable with an extension is allowed.
	if _, err := conn.ExecContext(context.Background(), "set my_extension.my_property='my-value'"); err != nil {
		t.Fatal(err)
	}
	it, err := conn.QueryContext(ctx, "show variable my_extension.my_property")
	if err != nil {
		t.Fatal(err)
	}
	if !it.Next() {
		t.Fatal("expected it.Next to return true")
	}
	var val string
	if err := it.Scan(&val); err != nil {
		t.Fatal(err)
	}
	if g, w := val, "my-value"; g != w {
		t.Fatalf("value mismatch\n Got: %v\nWant: %v", g, w)
	}
	if it.Next() {
		t.Fatal("expected it.Next to return false")
	}
	if err := it.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestSetAndShowIsolationLevel(t *testing.T) {
	t.Parallel()

	db, server, teardown := setupTestDBConnection(t)
	defer teardown()
	ctx := context.Background()

	conn, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close() }()

	// Isolation level should start with 'Default'.
	row := conn.QueryRowContext(ctx, "show variable isolation_level")
	var val string
	if err := row.Scan(&val); err != nil {
		t.Fatal(err)
	}
	if g, w := val, sql.LevelDefault.String(); g != w {
		t.Fatalf("value mismatch\n Got: %v\nWant: %v", g, w)
	}

	// We can set the isolation level using a SET statement.
	if _, err := conn.ExecContext(context.Background(), "set isolation_level = 'repeatable_read'"); err != nil {
		t.Fatal(err)
	}
	// The isolation level should now be 'RepeatableRead'.
	row = conn.QueryRowContext(ctx, "show variable isolation_level")
	if err := row.Scan(&val); err != nil {
		t.Fatal(err)
	}
	if g, w := val, sql.LevelRepeatableRead.String(); g != w {
		t.Fatalf("value mismatch\n Got: %v\nWant: %v", g, w)
	}
	// Verify that this is also the value that the connection uses.
	tx, err := conn.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.ExecContext(ctx, testutil.UpdateBarSetFoo); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	requests := server.TestSpanner.DrainRequestsFromServer()
	executeRequests := testutil.RequestsOfType(requests, reflect.TypeOf(&spannerpb.ExecuteSqlRequest{}))
	if g, w := len(executeRequests), 1; g != w {
		t.Fatalf("execute requests count mismatch\n Got: %v\nWant: %v", g, w)
	}
	request := executeRequests[0].(*spannerpb.ExecuteSqlRequest)
	wantIsolationLevel, _ := toProtoIsolationLevel(sql.LevelRepeatableRead)
	if g, w := request.Transaction.GetBegin().GetIsolationLevel(), wantIsolationLevel; g != w {
		t.Fatalf("begin isolation level mismatch\n Got: %v\nWant: %v", g, w)
	}
}

func TestSetLocalIsolationLevel(t *testing.T) {
	t.Parallel()

	db, server, teardown := setupTestDBConnection(t)
	defer teardown()
	ctx := context.Background()

	// Start a transaction without specifying the isolation level.
	tx, err := db.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		t.Fatal(err)
	}
	// We can set the isolation level only for this transaction using a SET LOCAL statement.
	if _, err := tx.ExecContext(context.Background(), "set local isolation_level = 'repeatable_read'"); err != nil {
		t.Fatal(err)
	}
	// The isolation level should now be 'RepeatableRead'.
	var val string
	row := tx.QueryRowContext(ctx, "show variable isolation_level")
	if err := row.Scan(&val); err != nil {
		t.Fatal(err)
	}
	if g, w := val, sql.LevelRepeatableRead.String(); g != w {
		t.Fatalf("value mismatch\n Got: %v\nWant: %v", g, w)
	}
	// Verify that the transaction actually uses the isolation level.
	if _, err := tx.ExecContext(ctx, testutil.UpdateBarSetFoo); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	requests := server.TestSpanner.DrainRequestsFromServer()
	executeRequests := testutil.RequestsOfType(requests, reflect.TypeOf(&spannerpb.ExecuteSqlRequest{}))
	if g, w := len(executeRequests), 1; g != w {
		t.Fatalf("execute requests count mismatch\n Got: %v\nWant: %v", g, w)
	}
	request := executeRequests[0].(*spannerpb.ExecuteSqlRequest)
	wantIsolationLevel, _ := toProtoIsolationLevel(sql.LevelRepeatableRead)
	if g, w := request.Transaction.GetBegin().GetIsolationLevel(), wantIsolationLevel; g != w {
		t.Fatalf("begin isolation level mismatch\n Got: %v\nWant: %v", g, w)
	}
}

func TestGenericConnectionState_GoogleSQL(t *testing.T) {
	t.Parallel()

	// Create an initial connection state with an initial value for max_commit_delay.
	db, _, teardown := setupTestDBConnectionWithParamsAndDialect(t, "max_commit_delay=100ms", databasepb.DatabaseDialect_GOOGLE_STANDARD_SQL)
	defer teardown()
	ctx := context.Background()

	conn, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}

	// Verify that the initial value of max_commit_delay is used.
	verifyConnectionPropertyValue(t, conn, "max_commit_delay", "100ms")
	// Verify that other connection variables have a default value.
	verifyConnectionPropertyValue(t, conn, "auto_batch_dml", false)

	// Verify that changing a connection variable value outside a transaction works.
	setConnectionPropertyValue(t, conn, "max_commit_delay", "50ms")
	verifyConnectionPropertyValue(t, conn, "max_commit_delay", "50ms")

	// Verify that changing a connection variable in a transaction works and is visible both in the transaction and
	// after the transaction, regardless whether the transaction committed or not. GoogleSQL databases use
	// non-transactional connection state by default.
	for _, commit := range []bool{true, false} {
		tx, err := conn.BeginTx(ctx, &sql.TxOptions{})
		if err != nil {
			t.Fatal(err)
		}
		verifyConnectionPropertyValue(t, conn, "max_commit_delay", "50ms")
		setConnectionPropertyValue(t, conn, "max_commit_delay", "'20ms'")
		verifyConnectionPropertyValue(t, conn, "max_commit_delay", "20ms")
		if commit {
			if err := tx.Commit(); err != nil {
				t.Fatal(err)
			}
		} else {
			if err := tx.Rollback(); err != nil {
				t.Fatal(err)
			}
		}
		verifyConnectionPropertyValue(t, conn, "max_commit_delay", "20ms")
		// Set the value back to 50ms before the next test.
		setConnectionPropertyValue(t, conn, "max_commit_delay", "50ms")
	}
	// Verify that resetting a connection variable in a transaction works and is visible both in the transaction and
	// after the transaction, regardless whether the transaction committed or not. GoogleSQL databases use
	// non-transactional connection state by default.
	for _, commit := range []bool{true, false} {
		tx, err := conn.BeginTx(ctx, &sql.TxOptions{})
		if err != nil {
			t.Fatal(err)
		}
		verifyConnectionPropertyValue(t, conn, "max_commit_delay", "50ms")
		resetConnectionPropertyValue(t, conn, "max_commit_delay")
		verifyConnectionPropertyValue(t, conn, "max_commit_delay", "100ms")
		if commit {
			if err := tx.Commit(); err != nil {
				t.Fatal(err)
			}
		} else {
			if err := tx.Rollback(); err != nil {
				t.Fatal(err)
			}
		}
		verifyConnectionPropertyValue(t, conn, "max_commit_delay", "100ms")
		// Set the value back to 50ms before the next test.
		setConnectionPropertyValue(t, conn, "max_commit_delay", "50ms")
	}
	// Verify that setting a connection variable to NULL in a transaction works and is visible both
	// in the transaction and after the transaction, regardless whether the transaction committed or not.
	// GoogleSQL databases use non-transactional connection state by default.
	for _, commit := range []bool{true, false} {
		tx, err := conn.BeginTx(ctx, &sql.TxOptions{})
		if err != nil {
			t.Fatal(err)
		}
		verifyConnectionPropertyValue(t, conn, "max_commit_delay", "50ms")
		setConnectionPropertyValue(t, conn, "max_commit_delay", "null")
		verifyConnectionPropertyValue(t, conn, "max_commit_delay", "")
		if commit {
			if err := tx.Commit(); err != nil {
				t.Fatal(err)
			}
		} else {
			if err := tx.Rollback(); err != nil {
				t.Fatal(err)
			}
		}
		verifyConnectionPropertyValue(t, conn, "max_commit_delay", "")
		// Set the value back to 50ms before the next test.
		setConnectionPropertyValue(t, conn, "max_commit_delay", "50ms")
	}

	// Verify that RESET sets the value to the original value.
	resetConnectionPropertyValue(t, conn, "max_commit_delay")
	verifyConnectionPropertyValue(t, conn, "max_commit_delay", "100ms")

	// Verify that SET ... = DEFAULT sets the value to the original value.
	setConnectionPropertyValue(t, conn, "max_commit_delay", "50ms")
	setConnectionPropertyValue(t, conn, "max_commit_delay", "default")
	verifyConnectionPropertyValue(t, conn, "max_commit_delay", "100ms")

	// Verify that SET ... = NULL sets the value to the default value for the property.
	setConnectionPropertyValue(t, conn, "max_commit_delay", "null")
	verifyConnectionPropertyValue(t, conn, "max_commit_delay", "")

	// Verify that RESET after setting the value to null sets it back to its original value.
	resetConnectionPropertyValue(t, conn, "max_commit_delay")
	verifyConnectionPropertyValue(t, conn, "max_commit_delay", "100ms")

	// Verify that SET LOCAL is only visible inside the transaction.
	for _, commit := range []bool{true, false} {
		tx, err := conn.BeginTx(ctx, &sql.TxOptions{})
		if err != nil {
			t.Fatal(err)
		}
		setLocalConnectionPropertyValue(t, conn, "max_commit_delay", "'20ms'")
		verifyConnectionPropertyValue(t, conn, "max_commit_delay", "20ms")
		if commit {
			if err := tx.Commit(); err != nil {
				t.Fatal(err)
			}
		} else {
			if err := tx.Rollback(); err != nil {
				t.Fatal(err)
			}
		}
		verifyConnectionPropertyValue(t, conn, "max_commit_delay", "100ms")
	}

	// Verify that SET LOCAL ... = DEFAULT is only visible inside the transaction.
	for _, commit := range []bool{true, false} {
		setConnectionPropertyValue(t, conn, "max_commit_delay", "10ms")
		tx, err := conn.BeginTx(ctx, &sql.TxOptions{})
		if err != nil {
			t.Fatal(err)
		}
		setLocalConnectionPropertyValue(t, conn, "max_commit_delay", "default")
		verifyConnectionPropertyValue(t, conn, "max_commit_delay", "100ms")
		if commit {
			if err := tx.Commit(); err != nil {
				t.Fatal(err)
			}
		} else {
			if err := tx.Rollback(); err != nil {
				t.Fatal(err)
			}
		}
		verifyConnectionPropertyValue(t, conn, "max_commit_delay", "10ms")
	}

	// Verify that SET LOCAL ... = NULL is only visible inside the transaction.
	for _, commit := range []bool{true, false} {
		setConnectionPropertyValue(t, conn, "max_commit_delay", "10ms")
		tx, err := conn.BeginTx(ctx, &sql.TxOptions{})
		if err != nil {
			t.Fatal(err)
		}
		setLocalConnectionPropertyValue(t, conn, "max_commit_delay", "null")
		verifyConnectionPropertyValue(t, conn, "max_commit_delay", "")
		if commit {
			if err := tx.Commit(); err != nil {
				t.Fatal(err)
			}
		} else {
			if err := tx.Rollback(); err != nil {
				t.Fatal(err)
			}
		}
		verifyConnectionPropertyValue(t, conn, "max_commit_delay", "10ms")
	}

	// Verify that showing a non-existing variable always fails.
	verifyShowFails[string](t, conn, "my_property")
	verifyShowFails[string](t, conn, "my_extension.my_property")

	// Verify that setting a non-existing variable is only possible if it has an extension.
	verifySetFails(t, conn, "my_property", "some-value")
	setConnectionPropertyValue(t, conn, "my_extension.my_property", "'some-value'")
	verifyConnectionPropertyValue(t, conn, "my_extension.my_property", "some-value")

	// Resetting a variable that did not exist at startup, removes it from the connection.
	resetConnectionPropertyValue(t, conn, "my_extension.my_property")
	verifyShowFails[string](t, conn, "my_extension.my_property")

	// Verify that SET LOCAL my_extension.my_property = 'some-value' is only visible inside the transaction.
	for _, commit := range []bool{true, false} {
		verifyShowFails[string](t, conn, "my_extension.my_property")
		tx, err := conn.BeginTx(ctx, &sql.TxOptions{})
		if err != nil {
			t.Fatal(err)
		}
		setLocalConnectionPropertyValue(t, conn, "my_extension.my_property", "'some-value'")
		verifyConnectionPropertyValue(t, conn, "my_extension.my_property", "some-value")
		if commit {
			if err := tx.Commit(); err != nil {
				t.Fatal(err)
			}
		} else {
			if err := tx.Rollback(); err != nil {
				t.Fatal(err)
			}
		}
		verifyShowFails[string](t, conn, "my_extension.my_property")
	}

	// Verify that SET LOCAL my_extension.my_property = default temporarily removes the property from the connection.
	for _, commit := range []bool{true, false} {
		setConnectionPropertyValue(t, conn, "my_extension.my_property", "'some-value'")
		verifyConnectionPropertyValue(t, conn, "my_extension.my_property", "some-value")
		tx, err := conn.BeginTx(ctx, &sql.TxOptions{})
		if err != nil {
			t.Fatal(err)
		}
		// This is the equivalent of RESET my_extension.my_property, however RESET LOCAL ... is not valid syntax.
		// This removes the property from the current transaction.
		setLocalConnectionPropertyValue(t, conn, "my_extension.my_property", "default")
		verifyShowFails[string](t, conn, "my_extension.my_property")
		if commit {
			if err := tx.Commit(); err != nil {
				t.Fatal(err)
			}
		} else {
			if err := tx.Rollback(); err != nil {
				t.Fatal(err)
			}
		}
		// The connection property should be visible again after the transaction.
		verifyConnectionPropertyValue(t, conn, "my_extension.my_property", "some-value")
	}

	if err := conn.Close(); err != nil {
		t.Fatal(err)
	}
}
func TestGenericConnectionState_PostgreSQL(t *testing.T) {
	t.Parallel()

	// Create an initial connection state with an initial value for max_commit_delay.
	db, _, teardown := setupTestDBConnectionWithParamsAndDialect(t, "max_commit_delay=100ms", databasepb.DatabaseDialect_POSTGRESQL)
	defer teardown()
	ctx := context.Background()

	conn, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}

	// Verify that the initial value of max_commit_delay is used.
	verifyConnectionPropertyValue(t, conn, "max_commit_delay", "100ms")
	// Verify that other connection variables have a default value.
	verifyConnectionPropertyValue(t, conn, "auto_batch_dml", false)

	// Verify that changing a connection variable value outside a transaction works.
	setConnectionPropertyValue(t, conn, "max_commit_delay", "50ms")
	verifyConnectionPropertyValue(t, conn, "max_commit_delay", "50ms")

	// Verify that changing a connection variable in a transaction works and is visible in the transaction.
	// The change should only be visible after the transaction if the transaction committed,
	// as PostgreSQL uses transactional connection state by default.
	for _, commit := range []bool{true, false} {
		tx, err := conn.BeginTx(ctx, &sql.TxOptions{})
		if err != nil {
			t.Fatal(err)
		}
		verifyConnectionPropertyValue(t, conn, "max_commit_delay", "50ms")
		setConnectionPropertyValue(t, conn, "max_commit_delay", "'20ms'")
		verifyConnectionPropertyValue(t, conn, "max_commit_delay", "20ms")
		if commit {
			if err := tx.Commit(); err != nil {
				t.Fatal(err)
			}
		} else {
			if err := tx.Rollback(); err != nil {
				t.Fatal(err)
			}
		}
		if commit {
			verifyConnectionPropertyValue(t, conn, "max_commit_delay", "20ms")
			// Set the value back to 50ms before the next test.
			setConnectionPropertyValue(t, conn, "max_commit_delay", "50ms")
		} else {
			verifyConnectionPropertyValue(t, conn, "max_commit_delay", "50ms")
		}
	}
	// Verify that resetting a connection variable in a transaction works and is visible in the transaction.
	// The change should only be visible after the transaction if the transaction committed,
	// as PostgreSQL uses transactional connection state by default.
	for _, commit := range []bool{true, false} {
		tx, err := conn.BeginTx(ctx, &sql.TxOptions{})
		if err != nil {
			t.Fatal(err)
		}
		verifyConnectionPropertyValue(t, conn, "max_commit_delay", "50ms")
		resetConnectionPropertyValue(t, conn, "max_commit_delay")
		verifyConnectionPropertyValue(t, conn, "max_commit_delay", "100ms")
		if commit {
			if err := tx.Commit(); err != nil {
				t.Fatal(err)
			}
		} else {
			if err := tx.Rollback(); err != nil {
				t.Fatal(err)
			}
		}
		if commit {
			verifyConnectionPropertyValue(t, conn, "max_commit_delay", "100ms")
			// Set the value back to 50ms before the next test.
			setConnectionPropertyValue(t, conn, "max_commit_delay", "50ms")
		} else {
			verifyConnectionPropertyValue(t, conn, "max_commit_delay", "50ms")
		}
	}
	// Verify that setting a connection variable to NULL in a transaction works and is visible in the transaction.
	// The change should only be visible after the transaction if the transaction committed,
	// as PostgreSQL uses transactional connection state by default.
	for _, commit := range []bool{true, false} {
		tx, err := conn.BeginTx(ctx, &sql.TxOptions{})
		if err != nil {
			t.Fatal(err)
		}
		verifyConnectionPropertyValue(t, conn, "max_commit_delay", "50ms")
		setConnectionPropertyValue(t, conn, "max_commit_delay", "null")
		verifyConnectionPropertyValue(t, conn, "max_commit_delay", "")
		if commit {
			if err := tx.Commit(); err != nil {
				t.Fatal(err)
			}
		} else {
			if err := tx.Rollback(); err != nil {
				t.Fatal(err)
			}
		}
		if commit {
			verifyConnectionPropertyValue(t, conn, "max_commit_delay", "")
			// Set the value back to 50ms before the next test.
			setConnectionPropertyValue(t, conn, "max_commit_delay", "50ms")
		} else {
			verifyConnectionPropertyValue(t, conn, "max_commit_delay", "50ms")
		}
	}

	// Verify that RESET sets the value to the original value.
	resetConnectionPropertyValue(t, conn, "max_commit_delay")
	verifyConnectionPropertyValue(t, conn, "max_commit_delay", "100ms")

	// Verify that SET ... = DEFAULT sets the value to the original value.
	setConnectionPropertyValue(t, conn, "max_commit_delay", "50ms")
	setConnectionPropertyValue(t, conn, "max_commit_delay", "default")
	verifyConnectionPropertyValue(t, conn, "max_commit_delay", "100ms")

	// Verify that SET ... = NULL sets the value to the default value for the property.
	setConnectionPropertyValue(t, conn, "max_commit_delay", "null")
	verifyConnectionPropertyValue(t, conn, "max_commit_delay", "")

	// Verify that RESET after setting the value to null sets it back to its original value.
	resetConnectionPropertyValue(t, conn, "max_commit_delay")
	verifyConnectionPropertyValue(t, conn, "max_commit_delay", "100ms")

	// Verify that SET LOCAL is only visible inside the transaction.
	for _, commit := range []bool{true, false} {
		tx, err := conn.BeginTx(ctx, &sql.TxOptions{})
		if err != nil {
			t.Fatal(err)
		}
		setLocalConnectionPropertyValue(t, conn, "max_commit_delay", "'20ms'")
		verifyConnectionPropertyValue(t, conn, "max_commit_delay", "20ms")
		if commit {
			if err := tx.Commit(); err != nil {
				t.Fatal(err)
			}
		} else {
			if err := tx.Rollback(); err != nil {
				t.Fatal(err)
			}
		}
		verifyConnectionPropertyValue(t, conn, "max_commit_delay", "100ms")
	}

	// Verify that SET LOCAL ... = DEFAULT is only visible inside the transaction.
	for _, commit := range []bool{true, false} {
		setConnectionPropertyValue(t, conn, "max_commit_delay", "10ms")
		tx, err := conn.BeginTx(ctx, &sql.TxOptions{})
		if err != nil {
			t.Fatal(err)
		}
		setLocalConnectionPropertyValue(t, conn, "max_commit_delay", "default")
		verifyConnectionPropertyValue(t, conn, "max_commit_delay", "100ms")
		if commit {
			if err := tx.Commit(); err != nil {
				t.Fatal(err)
			}
		} else {
			if err := tx.Rollback(); err != nil {
				t.Fatal(err)
			}
		}
		verifyConnectionPropertyValue(t, conn, "max_commit_delay", "10ms")
	}

	// Verify that SET LOCAL ... = NULL is only visible inside the transaction.
	for _, commit := range []bool{true, false} {
		setConnectionPropertyValue(t, conn, "max_commit_delay", "10ms")
		tx, err := conn.BeginTx(ctx, &sql.TxOptions{})
		if err != nil {
			t.Fatal(err)
		}
		setLocalConnectionPropertyValue(t, conn, "max_commit_delay", "null")
		verifyConnectionPropertyValue(t, conn, "max_commit_delay", "")
		if commit {
			if err := tx.Commit(); err != nil {
				t.Fatal(err)
			}
		} else {
			if err := tx.Rollback(); err != nil {
				t.Fatal(err)
			}
		}
		verifyConnectionPropertyValue(t, conn, "max_commit_delay", "10ms")
	}

	// Verify that showing a non-existing variable always fails.
	verifyShowFails[string](t, conn, "my_property")
	verifyShowFails[string](t, conn, "my_extension.my_property")

	// Verify that setting a non-existing variable is only possible if it has an extension.
	verifySetFails(t, conn, "my_property", "some-value")
	setConnectionPropertyValue(t, conn, "my_extension.my_property", "'some-value'")
	verifyConnectionPropertyValue(t, conn, "my_extension.my_property", "some-value")

	// Resetting a variable that did not exist at startup, removes it from the connection.
	resetConnectionPropertyValue(t, conn, "my_extension.my_property")
	verifyShowFails[string](t, conn, "my_extension.my_property")

	// Verify that SET LOCAL my_extension.my_property = 'some-value' is only visible inside the transaction.
	for _, commit := range []bool{true, false} {
		verifyShowFails[string](t, conn, "my_extension.my_property")
		tx, err := conn.BeginTx(ctx, &sql.TxOptions{})
		if err != nil {
			t.Fatal(err)
		}
		setLocalConnectionPropertyValue(t, conn, "my_extension.my_property", "'some-value'")
		verifyConnectionPropertyValue(t, conn, "my_extension.my_property", "some-value")
		if commit {
			if err := tx.Commit(); err != nil {
				t.Fatal(err)
			}
		} else {
			if err := tx.Rollback(); err != nil {
				t.Fatal(err)
			}
		}
		verifyShowFails[string](t, conn, "my_extension.my_property")
	}

	// Verify that SET LOCAL my_extension.my_property = default temporarily removes the property from the connection.
	for _, commit := range []bool{true, false} {
		setConnectionPropertyValue(t, conn, "my_extension.my_property", "'some-value'")
		verifyConnectionPropertyValue(t, conn, "my_extension.my_property", "some-value")
		tx, err := conn.BeginTx(ctx, &sql.TxOptions{})
		if err != nil {
			t.Fatal(err)
		}
		// This is the equivalent of RESET my_extension.my_property, however RESET LOCAL ... is not valid syntax.
		// This removes the property from the current transaction.
		setLocalConnectionPropertyValue(t, conn, "my_extension.my_property", "default")
		verifyShowFails[string](t, conn, "my_extension.my_property")
		if commit {
			if err := tx.Commit(); err != nil {
				t.Fatal(err)
			}
		} else {
			if err := tx.Rollback(); err != nil {
				t.Fatal(err)
			}
		}
		// The connection property should be visible again after the transaction.
		verifyConnectionPropertyValue(t, conn, "my_extension.my_property", "some-value")
	}

	if err := conn.Close(); err != nil {
		t.Fatal(err)
	}
}

func verifyConnectionPropertyValue[T comparable](t *testing.T, c *sql.Conn, name string, value T) {
	ctx := context.Background()
	row := c.QueryRowContext(ctx, getShowStatement(c)+name)
	var val T
	if err := row.Scan(&val); err != nil {
		t.Fatal(err)
	}
	if g, w := val, value; g != w {
		t.Fatalf("value mismatch for connection property %q\n Got: %v\nWant: %v", name, g, w)
	}
}

func verifyShowFails[T comparable](t *testing.T, c *sql.Conn, name string) {
	ctx := context.Background()
	row := c.QueryRowContext(ctx, getShowStatement(c)+name)
	var val T
	if err := row.Scan(&val); err == nil {
		t.Fatal("missing expected error")
	}
}

func setConnectionPropertyValue(t *testing.T, conn *sql.Conn, name string, value any) {
	ctx := context.Background()
	if _, err := conn.ExecContext(ctx, fmt.Sprintf("set %s = %s", name, value)); err != nil {
		t.Fatal(err)
	}
}

func verifySetFails(t *testing.T, conn *sql.Conn, name string, value any) {
	ctx := context.Background()
	if _, err := conn.ExecContext(ctx, fmt.Sprintf("set %s = %s", name, value)); err == nil {
		t.Fatal("missing expected error")
	}
}

func setLocalConnectionPropertyValue(t *testing.T, conn *sql.Conn, name string, value any) {
	ctx := context.Background()
	if _, err := conn.ExecContext(ctx, fmt.Sprintf("set local %s = %s", name, value)); err != nil {
		t.Fatal(err)
	}
}

func resetConnectionPropertyValue(t *testing.T, conn *sql.Conn, name string) {
	ctx := context.Background()
	if _, err := conn.ExecContext(ctx, fmt.Sprintf("reset %s", name)); err != nil {
		t.Fatal(err)
	}
}

func getShowStatement(c *sql.Conn) string {
	if getDialect(c) == databasepb.DatabaseDialect_GOOGLE_STANDARD_SQL {
		return "show variable "
	}
	return "show "
}

func getDialect(c *sql.Conn) (dialect databasepb.DatabaseDialect) {
	_ = c.Raw(func(driverConn any) error {
		sc, _ := driverConn.(SpannerConn)
		conn := sc.(*conn)
		dialect = conn.parser.Dialect
		return nil
	})
	return
}
