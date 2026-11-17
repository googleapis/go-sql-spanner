// Copyright 2026 Google LLC
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
	"errors"
	"reflect"
	"strings"
	"testing"

	"cloud.google.com/go/spanner"
	"cloud.google.com/go/spanner/admin/database/apiv1/databasepb"
	spannerpb "cloud.google.com/go/spanner/apiv1/spannerpb"
	"github.com/googleapis/go-sql-spanner/connectionstate"
	"github.com/googleapis/go-sql-spanner/testutil"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestSavepoints_GoogleSQL_CreateRollbackAndRelease(t *testing.T) {
	t.Parallel()

	db, server, teardown := setupTestDBConnection(t)
	defer teardown()
	ctx := context.Background()

	dml1 := "UPDATE Foo SET Val=1 WHERE Id=1"
	dml2 := "UPDATE Foo SET Val=2 WHERE Id=2"
	dml3 := "UPDATE Foo SET Val=3 WHERE Id=3"
	dml4 := "UPDATE Foo SET Val=4 WHERE Id=4"
	server.TestSpanner.PutStatementResult(dml1, &testutil.StatementResult{Type: testutil.StatementResultUpdateCount, UpdateCount: 1})
	server.TestSpanner.PutStatementResult(dml2, &testutil.StatementResult{Type: testutil.StatementResultUpdateCount, UpdateCount: 1})
	server.TestSpanner.PutStatementResult(dml3, &testutil.StatementResult{Type: testutil.StatementResultUpdateCount, UpdateCount: 1})
	server.TestSpanner.PutStatementResult(dml4, &testutil.StatementResult{Type: testutil.StatementResultUpdateCount, UpdateCount: 1})

	tx, err := db.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		t.Fatalf("BeginTx failed: %v", err)
	}

	if _, err := tx.ExecContext(ctx, dml1); err != nil {
		t.Fatalf("dml1 failed: %v", err)
	}
	if _, err := tx.ExecContext(ctx, "SAVEPOINT s1"); err != nil {
		t.Fatalf("savepoint s1 failed: %v", err)
	}
	if _, err := tx.ExecContext(ctx, dml2); err != nil {
		t.Fatalf("dml2 failed: %v", err)
	}
	if _, err := tx.ExecContext(ctx, "SAVEPOINT s2"); err != nil {
		t.Fatalf("savepoint s2 failed: %v", err)
	}
	if _, err := tx.ExecContext(ctx, dml3); err != nil {
		t.Fatalf("dml3 failed: %v", err)
	}
	if _, err := tx.ExecContext(ctx, "ROLLBACK TO SAVEPOINT s2"); err != nil {
		t.Fatalf("rollback to s2 failed: %v", err)
	}
	if _, err := tx.ExecContext(ctx, "RELEASE SAVEPOINT s2"); err != nil {
		t.Fatalf("release s2 failed: %v", err)
	}
	if _, err := tx.ExecContext(ctx, dml4); err != nil {
		t.Fatalf("dml4 failed: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit failed: %v", err)
	}

	requests := server.TestSpanner.DrainRequestsFromServer()
	commitRequests := testutil.RequestsOfType(requests, reflect.TypeOf(&spannerpb.CommitRequest{}))
	if g, w := len(commitRequests), 1; g != w {
		t.Fatalf("commit requests count mismatch\nGot:  %v\nWant: %v", g, w)
	}

	executeRequests := testutil.RequestsOfType(requests, reflect.TypeOf(&spannerpb.ExecuteSqlRequest{}))
	// Expected execution sequence:
	// 1. dml1
	// 2. dml2
	// 3. dml3
	// Rollback to s2 triggers rollback of underlying tx, and replay of dml1 and dml2.
	// 4. dml1 (replayed)
	// 5. dml2 (replayed)
	// 6. dml4
	if g, w := len(executeRequests), 6; g != w {
		t.Fatalf("execute requests count mismatch\nGot:  %v\nWant: %v", g, w)
	}

	expectedSqls := []string{dml1, dml2, dml3, dml1, dml2, dml4}
	for i, expectedSql := range expectedSqls {
		req := executeRequests[i].(*spannerpb.ExecuteSqlRequest)
		if g, w := req.Sql, expectedSql; g != w {
			t.Errorf("execute request %d sql mismatch\nGot:  %v\nWant: %v", i, g, w)
		}
	}

	// Initial transaction begin on req 0
	req0 := executeRequests[0].(*spannerpb.ExecuteSqlRequest)
	if req0.Transaction.GetBegin() == nil {
		t.Errorf("expected request 0 to begin a transaction")
	}

	// Replayed transaction begin on req 3
	req3 := executeRequests[3].(*spannerpb.ExecuteSqlRequest)
	if req3.Transaction.GetBegin() == nil {
		t.Errorf("expected request 3 (replayed dml1) to begin a new transaction")
	}
}

func TestSavepoints_GoogleSQL_CaseInsensitiveNames(t *testing.T) {
	t.Parallel()

	db, _, teardown := setupTestDBConnection(t)
	defer teardown()
	ctx := context.Background()

	tx, err := db.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		t.Fatalf("BeginTx failed: %v", err)
	}

	if _, err := tx.ExecContext(ctx, "SAVEPOINT MySavepoint"); err != nil {
		t.Fatalf("savepoint MySavepoint failed: %v", err)
	}
	if _, err := tx.ExecContext(ctx, "ROLLBACK TO SAVEPOINT mysavepoint"); err != nil {
		t.Fatalf("rollback to mysavepoint failed: %v", err)
	}
	if _, err := tx.ExecContext(ctx, "RELEASE SAVEPOINT MYSAVEPOINT"); err != nil {
		t.Fatalf("release MYSAVEPOINT failed: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit failed: %v", err)
	}
}

func TestSavepoints_GoogleSQL_DuplicateSavepointNameFails(t *testing.T) {
	t.Parallel()

	db, _, teardown := setupTestDBConnection(t)
	defer teardown()
	ctx := context.Background()

	tx, err := db.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		t.Fatalf("BeginTx failed: %v", err)
	}

	if _, err := tx.ExecContext(ctx, "SAVEPOINT s1"); err != nil {
		t.Fatalf("initial savepoint s1 failed: %v", err)
	}
	_, err = tx.ExecContext(ctx, "SAVEPOINT S1")
	if err == nil {
		t.Fatalf("expected error for duplicate savepoint S1, got nil")
	}
	if g, w := spanner.ErrCode(err), codes.InvalidArgument; g != w {
		t.Errorf("error code mismatch\nGot:  %v\nWant: %v", g, w)
	}

	if err := tx.Rollback(); err != nil {
		t.Fatalf("rollback failed: %v", err)
	}
}

func TestSavepoints_PostgreSQL_NestedAndShadowedSavepoints(t *testing.T) {
	t.Parallel()

	db, server, teardown := setupTestDBConnectionWithParamsAndDialect(t, "", databasepb.DatabaseDialect_POSTGRESQL)
	defer teardown()
	ctx := context.Background()

	dml1 := "UPDATE Foo SET Val=1 WHERE Id=1"
	dml2 := "UPDATE Foo SET Val=2 WHERE Id=2"
	dml3 := "UPDATE Foo SET Val=3 WHERE Id=3"
	server.TestSpanner.PutStatementResult(dml1, &testutil.StatementResult{Type: testutil.StatementResultUpdateCount, UpdateCount: 1})
	server.TestSpanner.PutStatementResult(dml2, &testutil.StatementResult{Type: testutil.StatementResultUpdateCount, UpdateCount: 1})
	server.TestSpanner.PutStatementResult(dml3, &testutil.StatementResult{Type: testutil.StatementResultUpdateCount, UpdateCount: 1})

	tx, err := db.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		t.Fatalf("BeginTx failed: %v", err)
	}

	if _, err := tx.ExecContext(ctx, dml1); err != nil {
		t.Fatalf("dml1 failed: %v", err)
	}
	// In PostgreSQL, shadowing a savepoint name is allowed.
	if _, err := tx.ExecContext(ctx, "SAVEPOINT s1"); err != nil {
		t.Fatalf("outer savepoint s1 failed: %v", err)
	}
	if _, err := tx.ExecContext(ctx, dml2); err != nil {
		t.Fatalf("dml2 failed: %v", err)
	}
	if _, err := tx.ExecContext(ctx, "SAVEPOINT s1"); err != nil {
		t.Fatalf("shadowing savepoint s1 failed: %v", err)
	}
	if _, err := tx.ExecContext(ctx, dml3); err != nil {
		t.Fatalf("dml3 failed: %v", err)
	}
	// Rolling back to s1 rolls back to the inner/shadowing s1 (discards dml3, keeps dml1 and dml2).
	if _, err := tx.ExecContext(ctx, "ROLLBACK TO SAVEPOINT s1"); err != nil {
		t.Fatalf("rollback to inner s1 failed: %v", err)
	}
	// Releasing s1 removes the inner s1, exposing the outer s1.
	if _, err := tx.ExecContext(ctx, "RELEASE SAVEPOINT s1"); err != nil {
		t.Fatalf("release inner s1 failed: %v", err)
	}
	// Rolling back to s1 now rolls back to the outer s1 (discards dml2, keeps dml1).
	if _, err := tx.ExecContext(ctx, "ROLLBACK TO SAVEPOINT s1"); err != nil {
		t.Fatalf("rollback to outer s1 failed: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit failed: %v", err)
	}

	requests := server.TestSpanner.DrainRequestsFromServer()
	executeRequests := testutil.RequestsOfType(requests, reflect.TypeOf(&spannerpb.ExecuteSqlRequest{}))
	// Expected executions:
	// 1. dml1
	// 2. dml2
	// 3. dml3
	// First rollback to inner s1: deferred (no replay yet)
	// Second rollback to outer s1: deferred (inner s1 discarded, target is outer s1)
	// Commit: triggers deferred replay of statements up to outer s1 (dml1)
	// 4. dml1 (replayed)
	if g, w := len(executeRequests), 4; g != w {
		t.Fatalf("execute requests count mismatch\nGot:  %v\nWant: %v", g, w)
	}

	expectedSqls := []string{dml1, dml2, dml3, dml1}
	for i, expectedSql := range expectedSqls {
		req := executeRequests[i].(*spannerpb.ExecuteSqlRequest)
		if g, w := req.Sql, expectedSql; g != w {
			t.Errorf("execute request %d sql mismatch\nGot:  %v\nWant: %v", i, g, w)
		}
	}
}

func TestSavepoints_PostgreSQL_SyntaxVariations(t *testing.T) {
	t.Parallel()

	db, _, teardown := setupTestDBConnectionWithParamsAndDialect(t, "", databasepb.DatabaseDialect_POSTGRESQL)
	defer teardown()
	ctx := context.Background()

	tx, err := db.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		t.Fatalf("BeginTx failed: %v", err)
	}

	variants := []struct {
		name string
		sql  string
	}{
		{"SavepointStandard", "SAVEPOINT sp1"},
		{"RollbackWorkToSavepoint", "ROLLBACK WORK TO SAVEPOINT sp1"},
		{"Savepoint2", "SAVEPOINT sp2"},
		{"RollbackTransactionToSavepoint", "ROLLBACK TRANSACTION TO SAVEPOINT sp2"},
		{"SavepointShort", "SAVEPOINT sp3"},
		{"RollbackTo", "ROLLBACK TO sp3"},
		{"SavepointReleaseShort", "SAVEPOINT sp4"},
		{"ReleaseShort", "RELEASE sp4"},
		{"SavepointReleaseFull", "SAVEPOINT sp5"},
		{"ReleaseSavepoint", "RELEASE SAVEPOINT sp5"},
	}

	for _, v := range variants {
		if _, err := tx.ExecContext(ctx, v.sql); err != nil {
			t.Fatalf("variant %q (%q) failed: %v", v.name, v.sql, err)
		}
	}

	if err := tx.Commit(); err != nil {
		t.Fatalf("commit failed: %v", err)
	}
}

func TestSavepoints_PostgreSQLErrorRecovery_RecoverFromFailedStatementViaRollbackToSavepoint(t *testing.T) {
	t.Parallel()

	db, server, teardown := setupTestDBConnectionWithParamsAndDialect(t, "", databasepb.DatabaseDialect_POSTGRESQL)
	defer teardown()
	ctx := context.Background()

	dml1 := "UPDATE Foo SET Val=1 WHERE Id=1"
	dml2 := "UPDATE Foo SET Val=2 WHERE Id=2"
	dml3 := "UPDATE Foo SET Val=3 WHERE Id=3"
	failingDml := "UPDATE Foo SET Val=999 WHERE Invalid=true"
	selectSql := "SELECT Val FROM Foo WHERE Id=1"

	server.TestSpanner.PutStatementResult(dml1, &testutil.StatementResult{Type: testutil.StatementResultUpdateCount, UpdateCount: 1})
	server.TestSpanner.PutStatementResult(dml2, &testutil.StatementResult{Type: testutil.StatementResultUpdateCount, UpdateCount: 1})
	server.TestSpanner.PutStatementResult(dml3, &testutil.StatementResult{Type: testutil.StatementResultUpdateCount, UpdateCount: 1})
	server.TestSpanner.PutStatementResult(failingDml, &testutil.StatementResult{Err: status.Error(codes.InvalidArgument, "invalid column name")})

	conn, err := db.Conn(ctx)
	if err != nil {
		t.Fatalf("db.Conn failed: %v", err)
	}
	defer silentClose(conn)
	setPGInErrorTxBehavior(t, conn, connectionstate.InErrorTxBehaviorEnforceInErrorState)

	if g, w := getPGTransactionState(t, conn), connectionstate.TransactionStateIdle; g != w {
		t.Fatalf("initial transaction state mismatch\nGot:  %v\nWant: %v", g, w)
	}

	tx, err := conn.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("BeginTx failed: %v", err)
	}

	if g, w := getPGTransactionState(t, conn), connectionstate.TransactionStateTransaction; g != w {
		t.Fatalf("state after begin mismatch\nGot:  %v\nWant: %v", g, w)
	}

	if _, err := tx.ExecContext(ctx, "SAVEPOINT sp_recovery"); err != nil {
		t.Fatalf("savepoint sp_recovery failed: %v", err)
	}

	if _, err := tx.ExecContext(ctx, dml1); err != nil {
		t.Fatalf("dml1 failed: %v", err)
	}

	// Execute failing statement.
	_, err = tx.ExecContext(ctx, failingDml)
	if err == nil {
		t.Fatalf("expected error from failingDml, got nil")
	}

	// Verify transaction is now in Failed state ('E').
	if g, w := getPGTransactionState(t, conn), connectionstate.TransactionStateFailed; g != w {
		t.Fatalf("state after failed statement mismatch\nGot:  %v\nWant: %v", g, w)
	}

	// Attempting normal DML while in state 'E' must be rejected with 25P02.
	if _, err := tx.ExecContext(ctx, dml2); err == nil {
		t.Fatalf("expected error executing dml2 while in state 'E', got nil")
	} else {
		if g, w := spanner.ErrCode(err), codes.FailedPrecondition; g != w {
			t.Errorf("error code for dml2 in state 'E' mismatch\nGot:  %v\nWant: %v", g, w)
		}
		if g, w := ToPGSQLState(err), "25P02"; g != w {
			t.Errorf("SQLSTATE for dml2 in state 'E' mismatch\nGot:  %v\nWant: %v", g, w)
		}
	}

	// Attempting Query while in state 'E' must be rejected with 25P02.
	if _, err := tx.QueryContext(ctx, selectSql); err == nil {
		t.Fatalf("expected error executing query while in state 'E', got nil")
	} else {
		if g, w := ToPGSQLState(err), "25P02"; g != w {
			t.Errorf("SQLSTATE for query in state 'E' mismatch\nGot:  %v\nWant: %v", g, w)
		}
	}

	// Attempting SAVEPOINT while in state 'E' must be rejected with 25P02.
	if _, err := tx.ExecContext(ctx, "SAVEPOINT sp_blocked"); err == nil {
		t.Fatalf("expected error executing SAVEPOINT while in state 'E', got nil")
	} else {
		if g, w := ToPGSQLState(err), "25P02"; g != w {
			t.Errorf("SQLSTATE for SAVEPOINT in state 'E' mismatch\nGot:  %v\nWant: %v", g, w)
		}
	}

	// Attempting RELEASE SAVEPOINT while in state 'E' must be rejected with 25P02.
	if _, err := tx.ExecContext(ctx, "RELEASE SAVEPOINT sp_recovery"); err == nil {
		t.Fatalf("expected error executing RELEASE SAVEPOINT while in state 'E', got nil")
	} else {
		if g, w := ToPGSQLState(err), "25P02"; g != w {
			t.Errorf("SQLSTATE for RELEASE SAVEPOINT in state 'E' mismatch\nGot:  %v\nWant: %v", g, w)
		}
	}

	// State must still be Failed ('E').
	if g, w := getPGTransactionState(t, conn), connectionstate.TransactionStateFailed; g != w {
		t.Fatalf("state before rollback to savepoint mismatch\nGot:  %v\nWant: %v", g, w)
	}

	// Executing ROLLBACK TO SAVEPOINT recovers the transaction from 'E' to 'T'.
	if _, err := tx.ExecContext(ctx, "ROLLBACK TO SAVEPOINT sp_recovery"); err != nil {
		t.Fatalf("rollback to savepoint failed: %v", err)
	}

	// Verify transaction state recovered to Transaction ('T').
	if g, w := getPGTransactionState(t, conn), connectionstate.TransactionStateTransaction; g != w {
		t.Fatalf("state after recovery mismatch\nGot:  %v\nWant: %v", g, w)
	}

	// Subsequent DML must now succeed on the recovered transaction.
	if _, err := tx.ExecContext(ctx, dml3); err != nil {
		t.Fatalf("dml3 after recovery failed: %v", err)
	}

	// Commit must succeed.
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit failed: %v", err)
	}

	// Verify transaction state is Idle ('I').
	if g, w := getPGTransactionState(t, conn), connectionstate.TransactionStateIdle; g != w {
		t.Fatalf("state after commit mismatch\nGot:  %v\nWant: %v", g, w)
	}

	requests := server.TestSpanner.DrainRequestsFromServer()
	commitRequests := testutil.RequestsOfType(requests, reflect.TypeOf(&spannerpb.CommitRequest{}))
	if g, w := len(commitRequests), 1; g != w {
		t.Fatalf("commit requests count mismatch\nGot:  %v\nWant: %v", g, w)
	}

	executeRequests := testutil.RequestsOfType(requests, reflect.TypeOf(&spannerpb.ExecuteSqlRequest{}))
	// Expected executions:
	// 1. dml1
	// 2. failingDml
	// Rollback to sp_recovery replays statements before sp_recovery (empty).
	// 3. dml3 (executed on fresh transaction)
	if g, w := len(executeRequests), 3; g != w {
		t.Fatalf("execute requests count mismatch\nGot:  %v\nWant: %v", g, w)
	}
}

func TestSavepoints_PostgreSQLErrorRecovery_FailedRollbackToNonexistentSavepointKeepsErrorState(t *testing.T) {
	t.Parallel()

	db, server, teardown := setupTestDBConnectionWithParamsAndDialect(t, "", databasepb.DatabaseDialect_POSTGRESQL)
	defer teardown()
	ctx := context.Background()

	failingDml := "UPDATE Foo SET Val=999 WHERE Invalid=true"
	server.TestSpanner.PutStatementResult(failingDml, &testutil.StatementResult{Err: status.Error(codes.InvalidArgument, "invalid column name")})

	conn, err := db.Conn(ctx)
	if err != nil {
		t.Fatalf("db.Conn failed: %v", err)
	}
	defer silentClose(conn)
	setPGInErrorTxBehavior(t, conn, connectionstate.InErrorTxBehaviorEnforceInErrorState)

	tx, err := conn.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("BeginTx failed: %v", err)
	}

	if _, err := tx.ExecContext(ctx, "SAVEPOINT sp_valid"); err != nil {
		t.Fatalf("savepoint sp_valid failed: %v", err)
	}

	// Trigger failure.
	_, _ = tx.ExecContext(ctx, failingDml)
	if g, w := getPGTransactionState(t, conn), connectionstate.TransactionStateFailed; g != w {
		t.Fatalf("state after error mismatch\nGot:  %v\nWant: %v", g, w)
	}

	// Attempting rollback to nonexistent savepoint should fail with 3B001.
	_, err = tx.ExecContext(ctx, "ROLLBACK TO SAVEPOINT sp_nonexistent")
	if err == nil {
		t.Fatalf("expected error rolling back to nonexistent savepoint, got nil")
	}
	if g, w := spanner.ErrCode(err), codes.FailedPrecondition; g != w {
		t.Errorf("error code mismatch\nGot:  %v\nWant: %v", g, w)
	}
	if g, w := ToPGSQLState(err), "3B001"; g != w {
		t.Errorf("SQLSTATE mismatch\nGot:  %v\nWant: %v", g, w)
	}

	// Transaction must still be in Failed state ('E').
	if g, w := getPGTransactionState(t, conn), connectionstate.TransactionStateFailed; g != w {
		t.Fatalf("state after failed rollback mismatch\nGot:  %v\nWant: %v", g, w)
	}

	// Subsequent rollback to the valid savepoint sp_valid succeeds and recovers to 'T'.
	if _, err := tx.ExecContext(ctx, "ROLLBACK TO SAVEPOINT sp_valid"); err != nil {
		t.Fatalf("rollback to sp_valid failed: %v", err)
	}
	if g, w := getPGTransactionState(t, conn), connectionstate.TransactionStateTransaction; g != w {
		t.Fatalf("state after recovery mismatch\nGot:  %v\nWant: %v", g, w)
	}

	if err := tx.Commit(); err != nil {
		t.Fatalf("commit failed: %v", err)
	}
}

func TestSavepoints_Errors_OutsideTransaction(t *testing.T) {
	t.Parallel()

	db, _, teardown := setupTestDBConnection(t)
	defer teardown()
	ctx := context.Background()

	tests := []struct {
		name     string
		sql      string
		wantCode codes.Code
		wantSQL  string
	}{
		{
			name:     "SavepointOutsideTx",
			sql:      "SAVEPOINT s1",
			wantCode: codes.FailedPrecondition,
			wantSQL:  "25P01",
		},
		{
			name:     "RollbackToSavepointOutsideTx",
			sql:      "ROLLBACK TO SAVEPOINT s1",
			wantCode: codes.FailedPrecondition,
			wantSQL:  "25P01",
		},
		{
			name:     "ReleaseSavepointOutsideTx",
			sql:      "RELEASE SAVEPOINT s1",
			wantCode: codes.FailedPrecondition,
			wantSQL:  "25P01",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := db.ExecContext(ctx, tc.sql)
			if err == nil {
				t.Fatalf("expected error executing %q outside transaction, got nil", tc.sql)
			}
			if g, w := spanner.ErrCode(err), tc.wantCode; g != w {
				t.Errorf("error code mismatch for %q\nGot:  %v\nWant: %v", tc.sql, g, w)
			}
			if g, w := ToPGSQLState(err), tc.wantSQL; g != w {
				t.Errorf("SQLSTATE mismatch for %q\nGot:  %v\nWant: %v", tc.sql, g, w)
			}

			// Also verify via QueryContext.
			rows, queryErr := db.QueryContext(ctx, tc.sql)
			if queryErr == nil {
				_ = rows.Close()
				t.Fatalf("expected error querying %q outside transaction, got nil", tc.sql)
			}
			if g, w := spanner.ErrCode(queryErr), tc.wantCode; g != w {
				t.Errorf("query error code mismatch for %q\nGot:  %v\nWant: %v", tc.sql, g, w)
			}
		})
	}
}

func TestSavepoints_Errors_InsideTransaction_NonexistentSavepoint(t *testing.T) {
	t.Parallel()

	db, _, teardown := setupTestDBConnection(t)
	defer teardown()
	ctx := context.Background()

	tx, err := db.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		t.Fatalf("BeginTx failed: %v", err)
	}

	// Rollback to nonexistent savepoint
	_, err = tx.ExecContext(ctx, "ROLLBACK TO SAVEPOINT non_existent")
	if err == nil {
		t.Fatalf("expected error rolling back to nonexistent savepoint, got nil")
	}
	if g, w := spanner.ErrCode(err), codes.FailedPrecondition; g != w {
		t.Errorf("error code mismatch\nGot:  %v\nWant: %v", g, w)
	}
	if g, w := ToPGSQLState(err), "3B001"; g != w {
		t.Errorf("SQLSTATE mismatch\nGot:  %v\nWant: %v", g, w)
	}

	// Release nonexistent savepoint
	_, err = tx.ExecContext(ctx, "RELEASE SAVEPOINT non_existent")
	if err == nil {
		t.Fatalf("expected error releasing nonexistent savepoint, got nil")
	}
	if g, w := spanner.ErrCode(err), codes.FailedPrecondition; g != w {
		t.Errorf("error code mismatch\nGot:  %v\nWant: %v", g, w)
	}
	if g, w := ToPGSQLState(err), "3B001"; g != w {
		t.Errorf("SQLSTATE mismatch\nGot:  %v\nWant: %v", g, w)
	}

	if err := tx.Rollback(); err != nil {
		t.Fatalf("rollback failed: %v", err)
	}
}

func TestSavepoints_Errors_ReleasedSavepointCannotBeRolledBack(t *testing.T) {
	t.Parallel()

	db, _, teardown := setupTestDBConnection(t)
	defer teardown()
	ctx := context.Background()

	tx, err := db.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		t.Fatalf("BeginTx failed: %v", err)
	}

	if _, err := tx.ExecContext(ctx, "SAVEPOINT s1"); err != nil {
		t.Fatalf("savepoint s1 failed: %v", err)
	}
	if _, err := tx.ExecContext(ctx, "RELEASE SAVEPOINT s1"); err != nil {
		t.Fatalf("release savepoint s1 failed: %v", err)
	}

	_, err = tx.ExecContext(ctx, "ROLLBACK TO SAVEPOINT s1")
	if err == nil {
		t.Fatalf("expected error rolling back to released savepoint, got nil")
	}
	if g, w := spanner.ErrCode(err), codes.FailedPrecondition; g != w {
		t.Errorf("error code mismatch\nGot:  %v\nWant: %v", g, w)
	}
	if g, w := ToPGSQLState(err), "3B001"; g != w {
		t.Errorf("SQLSTATE mismatch\nGot:  %v\nWant: %v", g, w)
	}

	if err := tx.Rollback(); err != nil {
		t.Fatalf("rollback failed: %v", err)
	}
}

func TestSavepoints_ReadOnlyTransaction(t *testing.T) {
	t.Parallel()

	db, _, teardown := setupTestDBConnection(t)
	defer teardown()
	ctx := context.Background()

	tx, err := db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		t.Fatalf("BeginTx failed: %v", err)
	}

	if _, err := tx.ExecContext(ctx, "SAVEPOINT ro_sp1"); err != nil {
		t.Fatalf("savepoint in read-only tx failed: %v", err)
	}
	if _, err := tx.ExecContext(ctx, "ROLLBACK TO SAVEPOINT ro_sp1"); err != nil {
		t.Fatalf("rollback in read-only tx failed: %v", err)
	}
	if _, err := tx.ExecContext(ctx, "RELEASE SAVEPOINT ro_sp1"); err != nil {
		t.Fatalf("release in read-only tx failed: %v", err)
	}

	// Rolling back to non-existent savepoint in read-only transaction must fail with 3B001.
	_, err = tx.ExecContext(ctx, "ROLLBACK TO SAVEPOINT ro_nonexistent")
	if err == nil {
		t.Fatalf("expected error rolling back to nonexistent savepoint in read-only tx, got nil")
	}
	if g, w := spanner.ErrCode(err), codes.FailedPrecondition; g != w {
		t.Errorf("error code mismatch\nGot:  %v\nWant: %v", g, w)
	}
	if g, w := ToPGSQLState(err), "3B001"; g != w {
		t.Errorf("SQLSTATE mismatch\nGot:  %v\nWant: %v", g, w)
	}

	// Releasing non-existent savepoint in read-only transaction must also fail with 3B001.
	_, err = tx.ExecContext(ctx, "RELEASE SAVEPOINT ro_nonexistent")
	if err == nil {
		t.Fatalf("expected error releasing nonexistent savepoint in read-only tx, got nil")
	}
	if g, w := spanner.ErrCode(err), codes.FailedPrecondition; g != w {
		t.Errorf("error code mismatch\nGot:  %v\nWant: %v", g, w)
	}
	if g, w := ToPGSQLState(err), "3B001"; g != w {
		t.Errorf("SQLSTATE mismatch\nGot:  %v\nWant: %v", g, w)
	}

	if err := tx.Commit(); err != nil {
		t.Fatalf("commit failed: %v", err)
	}
}

func TestSavepoints_DirectClientSideStatements(t *testing.T) {
	t.Parallel()

	db, server, teardown := setupTestDBConnection(t)
	defer teardown()
	ctx := context.Background()

	dml1 := "UPDATE Foo SET Val=1 WHERE Id=1"
	dml2 := "UPDATE Foo SET Val=2 WHERE Id=2"
	server.TestSpanner.PutStatementResult(dml1, &testutil.StatementResult{Type: testutil.StatementResultUpdateCount, UpdateCount: 1})
	server.TestSpanner.PutStatementResult(dml2, &testutil.StatementResult{Type: testutil.StatementResultUpdateCount, UpdateCount: 1})

	conn, err := db.Conn(ctx)
	if err != nil {
		t.Fatalf("db.Conn failed: %v", err)
	}
	defer silentClose(conn)

	// Execute transaction control statements directly via connection.
	if _, err := conn.ExecContext(ctx, "BEGIN"); err != nil {
		t.Fatalf("BEGIN failed: %v", err)
	}
	if _, err := conn.ExecContext(ctx, dml1); err != nil {
		t.Fatalf("dml1 failed: %v", err)
	}
	if _, err := conn.ExecContext(ctx, "SAVEPOINT sp_conn"); err != nil {
		t.Fatalf("SAVEPOINT failed: %v", err)
	}
	if _, err := conn.ExecContext(ctx, dml2); err != nil {
		t.Fatalf("dml2 failed: %v", err)
	}
	if _, err := conn.ExecContext(ctx, "ROLLBACK TO SAVEPOINT sp_conn"); err != nil {
		t.Fatalf("ROLLBACK TO SAVEPOINT failed: %v", err)
	}
	if _, err := conn.ExecContext(ctx, "COMMIT"); err != nil {
		t.Fatalf("COMMIT failed: %v", err)
	}

	requests := server.TestSpanner.DrainRequestsFromServer()
	commitRequests := testutil.RequestsOfType(requests, reflect.TypeOf(&spannerpb.CommitRequest{}))
	if g, w := len(commitRequests), 1; g != w {
		t.Fatalf("commit requests count mismatch\nGot:  %v\nWant: %v", g, w)
	}

	executeRequests := testutil.RequestsOfType(requests, reflect.TypeOf(&spannerpb.ExecuteSqlRequest{}))
	// dml1, dml2, then dml1 (replayed)
	if g, w := len(executeRequests), 3; g != w {
		t.Fatalf("execute requests count mismatch\nGot:  %v\nWant: %v", g, w)
	}
}

func TestSavepoints_QueryContext(t *testing.T) {
	t.Parallel()

	db, _, teardown := setupTestDBConnection(t)
	defer teardown()
	ctx := context.Background()

	tx, err := db.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		t.Fatalf("BeginTx failed: %v", err)
	}

	rows, err := tx.QueryContext(ctx, "SAVEPOINT sp_query")
	if err != nil {
		t.Fatalf("query SAVEPOINT failed: %v", err)
	}
	if rows.Next() {
		t.Errorf("expected no rows from SAVEPOINT query")
	}
	if err := rows.Close(); err != nil {
		t.Fatalf("rows.Close failed: %v", err)
	}

	rows, err = tx.QueryContext(ctx, "ROLLBACK TO SAVEPOINT sp_query")
	if err != nil {
		t.Fatalf("query ROLLBACK TO SAVEPOINT failed: %v", err)
	}
	if rows.Next() {
		t.Errorf("expected no rows from ROLLBACK TO SAVEPOINT query")
	}
	if err := rows.Close(); err != nil {
		t.Fatalf("rows.Close failed: %v", err)
	}

	// Create and release via QueryContext
	rows, err = tx.QueryContext(ctx, "SAVEPOINT sp_query2")
	if err != nil {
		t.Fatalf("query SAVEPOINT sp_query2 failed: %v", err)
	}
	_ = rows.Close()

	rows, err = tx.QueryContext(ctx, "RELEASE SAVEPOINT sp_query2")
	if err != nil {
		t.Fatalf("query RELEASE SAVEPOINT failed: %v", err)
	}
	if rows.Next() {
		t.Errorf("expected no rows from RELEASE SAVEPOINT query")
	}
	if err := rows.Close(); err != nil {
		t.Fatalf("rows.Close failed: %v", err)
	}

	if err := tx.Commit(); err != nil {
		t.Fatalf("commit failed: %v", err)
	}
}

func TestSavepoints_PreparedStatement(t *testing.T) {
	t.Parallel()

	db, _, teardown := setupTestDBConnection(t)
	defer teardown()
	ctx := context.Background()

	tx, err := db.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		t.Fatalf("BeginTx failed: %v", err)
	}

	savepointStmt, err := tx.PrepareContext(ctx, "SAVEPOINT sp_prep")
	if err != nil {
		t.Fatalf("prepare SAVEPOINT failed: %v", err)
	}
	defer silentClose(savepointStmt)
	if _, err := savepointStmt.ExecContext(ctx); err != nil {
		t.Fatalf("exec prepared SAVEPOINT failed: %v", err)
	}

	rollbackStmt, err := tx.PrepareContext(ctx, "ROLLBACK TO SAVEPOINT sp_prep")
	if err != nil {
		t.Fatalf("prepare ROLLBACK TO SAVEPOINT failed: %v", err)
	}
	defer silentClose(rollbackStmt)
	if _, err := rollbackStmt.ExecContext(ctx); err != nil {
		t.Fatalf("exec prepared ROLLBACK TO SAVEPOINT failed: %v", err)
	}

	// Also verify QueryContext on a prepared savepoint statement.
	rows, err := rollbackStmt.QueryContext(ctx)
	if err != nil {
		t.Fatalf("query prepared ROLLBACK TO SAVEPOINT failed: %v", err)
	}
	if rows.Next() {
		t.Errorf("expected no rows from prepared ROLLBACK TO SAVEPOINT query")
	}
	if err := rows.Close(); err != nil {
		t.Fatalf("rows.Close failed: %v", err)
	}

	releaseStmt, err := tx.PrepareContext(ctx, "RELEASE SAVEPOINT sp_prep")
	if err != nil {
		t.Fatalf("prepare RELEASE SAVEPOINT failed: %v", err)
	}
	defer silentClose(releaseStmt)
	if _, err := releaseStmt.ExecContext(ctx); err != nil {
		t.Fatalf("exec prepared RELEASE SAVEPOINT failed: %v", err)
	}

	if err := tx.Commit(); err != nil {
		t.Fatalf("commit failed: %v", err)
	}
}

func TestSavepoints_ActiveDmlBatchConflict(t *testing.T) {
	t.Parallel()

	db, _, teardown := setupTestDBConnection(t)
	defer teardown()
	ctx := context.Background()

	tx, err := db.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		t.Fatalf("BeginTx failed: %v", err)
	}

	if _, err := tx.ExecContext(ctx, "START BATCH DML"); err != nil {
		t.Fatalf("START BATCH DML failed: %v", err)
	}

	// SAVEPOINT during DML batch must fail
	if _, err := tx.ExecContext(ctx, "SAVEPOINT sp_batch"); err == nil {
		t.Fatalf("expected error creating savepoint during active DML batch, got nil")
	} else if g, w := spanner.ErrCode(err), codes.FailedPrecondition; g != w {
		t.Errorf("error code for SAVEPOINT during batch mismatch\nGot:  %v\nWant: %v", g, w)
	}

	// ROLLBACK TO SAVEPOINT during DML batch must fail
	if _, err := tx.ExecContext(ctx, "ROLLBACK TO SAVEPOINT sp_batch"); err == nil {
		t.Fatalf("expected error rolling back savepoint during active DML batch, got nil")
	} else if g, w := spanner.ErrCode(err), codes.FailedPrecondition; g != w {
		t.Errorf("error code for ROLLBACK during batch mismatch\nGot:  %v\nWant: %v", g, w)
	}

	// RELEASE SAVEPOINT during DML batch must fail
	if _, err := tx.ExecContext(ctx, "RELEASE SAVEPOINT sp_batch"); err == nil {
		t.Fatalf("expected error releasing savepoint during active DML batch, got nil")
	} else if g, w := spanner.ErrCode(err), codes.FailedPrecondition; g != w {
		t.Errorf("error code for RELEASE during batch mismatch\nGot:  %v\nWant: %v", g, w)
	}

	if _, err := tx.ExecContext(ctx, "ABORT BATCH"); err != nil {
		t.Fatalf("ABORT BATCH failed: %v", err)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatalf("rollback failed: %v", err)
	}
}

func TestSavepoints_RetriesDisabledConflict(t *testing.T) {
	t.Parallel()

	db, _, teardown := setupTestDBConnection(t)
	defer teardown()
	ctx := context.Background()

	conn, err := db.Conn(ctx)
	if err != nil {
		t.Fatalf("db.Conn failed: %v", err)
	}
	defer silentClose(conn)

	if _, err := conn.ExecContext(ctx, "SET RETRY_ABORTS_INTERNALLY = false"); err != nil {
		t.Fatalf("set RETRY_ABORTS_INTERNALLY failed: %v", err)
	}

	tx, err := conn.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("BeginTx failed: %v", err)
	}

	if _, err := tx.ExecContext(ctx, "SAVEPOINT sp_noretry"); err == nil {
		t.Fatalf("expected error creating savepoint when retries disabled, got nil")
	} else if g, w := spanner.ErrCode(err), codes.FailedPrecondition; g != w {
		t.Errorf("error code mismatch\nGot:  %v\nWant: %v", g, w)
	}

	if err := tx.Rollback(); err != nil {
		t.Fatalf("rollback failed: %v", err)
	}
}

func TestSavepoints_ReplayFailure_ConcurrentModification_GoogleSQL(t *testing.T) {
	t.Parallel()

	db, server, teardown := setupTestDBConnection(t)
	defer teardown()
	ctx := context.Background()

	dml1 := "UPDATE Foo SET Val=1 WHERE Id=1"
	dml2 := "UPDATE Foo SET Val=2 WHERE Id=2"
	server.TestSpanner.PutStatementResult(dml1, &testutil.StatementResult{Type: testutil.StatementResultUpdateCount, UpdateCount: 1})
	server.TestSpanner.PutStatementResult(dml2, &testutil.StatementResult{Type: testutil.StatementResultUpdateCount, UpdateCount: 1})

	tx, err := db.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		t.Fatalf("BeginTx failed: %v", err)
	}

	if _, err := tx.ExecContext(ctx, dml1); err != nil {
		t.Fatalf("dml1 failed: %v", err)
	}
	if _, err := tx.ExecContext(ctx, "SAVEPOINT s1"); err != nil {
		t.Fatalf("savepoint s1 failed: %v", err)
	}
	if _, err := tx.ExecContext(ctx, dml2); err != nil {
		t.Fatalf("dml2 failed: %v", err)
	}

	// Mutate mock server result so replay of dml1 sees a concurrent modification.
	server.TestSpanner.PutStatementResult(dml1, &testutil.StatementResult{Type: testutil.StatementResultUpdateCount, UpdateCount: 2})

	// Rollback to savepoint is deferred/lazy, so the rollback statement itself succeeds locally.
	if _, err = tx.ExecContext(ctx, "ROLLBACK TO SAVEPOINT s1"); err != nil {
		t.Fatalf("expected rollback to s1 to succeed locally, got error: %v", err)
	}

	// The next statement triggers deferred replay, which fails with concurrent modification error.
	_, err = tx.ExecContext(ctx, dml2)
	if err == nil {
		t.Fatalf("expected error from statement triggering replay, got nil")
	}
	if !errors.Is(err, ErrAbortedDueToConcurrentModification) {
		if g, w := spanner.ErrCode(err), codes.Aborted; g != w {
			t.Errorf("error code mismatch\nGot:  %v\nWant: %v", g, w)
		}
	}

	// Subsequent statements in the failed replay transaction must be rejected with FailedPrecondition.
	_, err = tx.ExecContext(ctx, dml2)
	if err == nil {
		t.Fatalf("expected error executing statement after failed replay, got nil")
	}
	if g, w := spanner.ErrCode(err), codes.FailedPrecondition; g != w {
		t.Errorf("error code after failed replay mismatch\nGot:  %v\nWant: %v", g, w)
	}

	_ = tx.Rollback()
}

func TestSavepoints_ReplayFailure_ConcurrentModification_PostgreSQL(t *testing.T) {
	t.Parallel()

	db, server, teardown := setupTestDBConnectionWithParamsAndDialect(t, "", databasepb.DatabaseDialect_POSTGRESQL)
	defer teardown()
	ctx := context.Background()

	dml1 := "UPDATE Foo SET Val=1 WHERE Id=1"
	dml2 := "UPDATE Foo SET Val=2 WHERE Id=2"
	server.TestSpanner.PutStatementResult(dml1, &testutil.StatementResult{Type: testutil.StatementResultUpdateCount, UpdateCount: 1})
	server.TestSpanner.PutStatementResult(dml2, &testutil.StatementResult{Type: testutil.StatementResultUpdateCount, UpdateCount: 1})

	tx, err := db.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		t.Fatalf("BeginTx failed: %v", err)
	}

	if _, err := tx.ExecContext(ctx, dml1); err != nil {
		t.Fatalf("dml1 failed: %v", err)
	}
	if _, err := tx.ExecContext(ctx, "SAVEPOINT s1"); err != nil {
		t.Fatalf("savepoint s1 failed: %v", err)
	}
	if _, err := tx.ExecContext(ctx, dml2); err != nil {
		t.Fatalf("dml2 failed: %v", err)
	}

	server.TestSpanner.PutStatementResult(dml1, &testutil.StatementResult{Type: testutil.StatementResultUpdateCount, UpdateCount: 2})

	// Rollback to savepoint is deferred/lazy, so the rollback statement itself succeeds locally.
	if _, err = tx.ExecContext(ctx, "ROLLBACK TO SAVEPOINT s1"); err != nil {
		t.Fatalf("expected rollback to s1 to succeed locally, got error: %v", err)
	}

	// The next statement triggers deferred replay, which fails with 40001 (serialization failure / concurrent modification).
	_, err = tx.ExecContext(ctx, dml2)
	if err == nil {
		t.Fatalf("expected error from statement triggering replay, got nil")
	}
	if g, w := ToPGSQLState(err), "40001"; g != w {
		t.Errorf("SQLSTATE mismatch for concurrent modification in PG\nGot:  %v\nWant: %v", g, w)
	}

	// Subsequent statements in PG must fail with 25P02.
	_, err = tx.ExecContext(ctx, dml2)
	if err == nil {
		t.Fatalf("expected error executing statement after failed replay, got nil")
	}
	if g, w := ToPGSQLState(err), "25P02"; g != w {
		t.Errorf("SQLSTATE mismatch for statement after failed replay in PG\nGot:  %v\nWant: %v", g, w)
	}

	_ = tx.Rollback()
}

func TestSavepoints_MultiStatementQuery_OutsideTransaction_FailsWithoutImplicitTx(t *testing.T) {
	t.Parallel()

	db, server, teardown := setupTestDBConnection(t)
	defer teardown()
	ctx := context.Background()

	dml := "UPDATE Foo SET Val=1 WHERE Id=1"
	server.TestSpanner.PutStatementResult(dml, &testutil.StatementResult{Type: testutil.StatementResultUpdateCount, UpdateCount: 1})

	// Statements starting with savepoint fail immediately on QueryContext and must not start an implicit transaction.
	firstSavepointSqls := []string{
		"SAVEPOINT s1; SELECT 1;",
		"ROLLBACK TO SAVEPOINT s1; SELECT 1;",
		"RELEASE SAVEPOINT s1; SELECT 1;",
	}
	for _, sqlStr := range firstSavepointSqls {
		rows, err := db.QueryContext(ctx, sqlStr)
		if err == nil {
			_ = rows.Close()
			t.Fatalf("expected error executing %q outside transaction, got nil", sqlStr)
		}
		if g, w := spanner.ErrCode(err), codes.FailedPrecondition; g != w {
			t.Errorf("%q error code mismatch\nGot:  %v\nWant: %v", sqlStr, g, w)
		}
		if g, w := ToPGSQLState(err), "25P01"; g != w {
			t.Errorf("%q SQLSTATE mismatch\nGot:  %v\nWant: %v", sqlStr, g, w)
		}
	}

	// Statements where savepoint is not the first statement fail on NextResultSet with 25P01,
	// and the implicit transaction must be rolled back.
	subsequentSavepointSqls := []string{
		"SELECT 1; SAVEPOINT s1;",
		"SELECT 1; ROLLBACK TO SAVEPOINT s1;",
		"SELECT 1; RELEASE SAVEPOINT s1;",
		"UPDATE Foo SET Val=1 WHERE Id=1; SAVEPOINT s1;",
		"UPDATE Foo SET Val=1 WHERE Id=1; ROLLBACK TO SAVEPOINT s1;",
		"UPDATE Foo SET Val=1 WHERE Id=1; RELEASE SAVEPOINT s1;",
	}
	for _, sqlStr := range subsequentSavepointSqls {
		rows, err := db.QueryContext(ctx, sqlStr)
		if err != nil {
			t.Fatalf("unexpected error on initial QueryContext for %q: %v", sqlStr, err)
		}
		if rows.NextResultSet() {
			_ = rows.Close()
			t.Fatalf("expected NextResultSet to return false for %q", sqlStr)
		}
		err = rows.Err()
		_ = rows.Close()
		if err == nil {
			t.Fatalf("expected error on rows.Err() for %q, got nil", sqlStr)
		}
		if g, w := spanner.ErrCode(err), codes.FailedPrecondition; g != w {
			t.Errorf("%q error code mismatch\nGot:  %v\nWant: %v", sqlStr, g, w)
		}
		if g, w := ToPGSQLState(err), "25P01"; g != w {
			t.Errorf("%q SQLSTATE mismatch\nGot:  %v\nWant: %v", sqlStr, g, w)
		}
	}

	// Verify no transaction was committed.
	requests := server.TestSpanner.DrainRequestsFromServer()
	commitRequests := testutil.RequestsOfType(requests, reflect.TypeOf(&spannerpb.CommitRequest{}))
	if g, w := len(commitRequests), 0; g != w {
		t.Fatalf("commit requests count mismatch\nGot:  %v\nWant: %v", g, w)
	}
}

func TestSavepoints_MultiStatementQuery_InsideTransaction_Succeeds(t *testing.T) {
	t.Parallel()

	db, server, teardown := setupTestDBConnection(t)
	defer teardown()
	ctx := context.Background()

	dml1 := "UPDATE Foo SET Val=1 WHERE Id=1"
	dml2 := "UPDATE Foo SET Val=2 WHERE Id=2"
	server.TestSpanner.PutStatementResult(dml1, &testutil.StatementResult{Type: testutil.StatementResultUpdateCount, UpdateCount: 1})
	server.TestSpanner.PutStatementResult(dml2, &testutil.StatementResult{Type: testutil.StatementResultUpdateCount, UpdateCount: 1})

	tx, err := db.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		t.Fatalf("BeginTx failed: %v", err)
	}

	multiSql := "UPDATE Foo SET Val=1 WHERE Id=1;SAVEPOINT sp_multi;UPDATE Foo SET Val=2 WHERE Id=2;ROLLBACK TO SAVEPOINT sp_multi;"
	rows, err := tx.QueryContext(ctx, multiSql)
	if err != nil {
		t.Fatalf("multi-statement query failed: %v", err)
	}
	resultSetCount := 1
	for rows.NextResultSet() {
		resultSetCount++
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows.Err() failed: %v", err)
	}
	if g, w := resultSetCount, 4; g != w {
		t.Fatalf("result set count mismatch\nGot:  %v\nWant: %v", g, w)
	}
	_ = rows.Close()

	if err := tx.Commit(); err != nil {
		t.Fatalf("commit failed: %v", err)
	}

	requests := server.TestSpanner.DrainRequestsFromServer()
	commitRequests := testutil.RequestsOfType(requests, reflect.TypeOf(&spannerpb.CommitRequest{}))
	if g, w := len(commitRequests), 1; g != w {
		t.Fatalf("commit requests count mismatch\nGot:  %v\nWant: %v", g, w)
	}

	executeRequests := testutil.RequestsOfType(requests, reflect.TypeOf(&spannerpb.ExecuteSqlRequest{}))
	expectedSqls := []string{dml1, dml2, dml1}
	if g, w := len(executeRequests), len(expectedSqls); g != w {
		t.Fatalf("execute requests count mismatch\nGot:  %v\nWant: %v", g, w)
	}
	for i, expectedSql := range expectedSqls {
		req := executeRequests[i].(*spannerpb.ExecuteSqlRequest)
		if g, w := req.Sql, expectedSql; g != w {
			t.Errorf("execute request %d sql mismatch\nGot:  %v\nWant: %v", i, g, w)
		}
	}
}

func TestSavepoints_AutoBatchDml(t *testing.T) {
	t.Parallel()

	db, server, teardown := setupTestDBConnection(t)
	defer teardown()
	ctx := context.Background()

	dml1 := "UPDATE Foo SET Val=1 WHERE Id=1"
	dml2 := "UPDATE Foo SET Val=2 WHERE Id=2"
	server.TestSpanner.PutStatementResult(dml1, &testutil.StatementResult{Type: testutil.StatementResultUpdateCount, UpdateCount: 1})
	server.TestSpanner.PutStatementResult(dml2, &testutil.StatementResult{Type: testutil.StatementResultUpdateCount, UpdateCount: 1})

	conn, err := db.Conn(ctx)
	if err != nil {
		t.Fatalf("db.Conn failed: %v", err)
	}
	defer silentClose(conn)

	if _, err := conn.ExecContext(ctx, "SET AUTO_BATCH_DML = true"); err != nil {
		t.Fatalf("set AUTO_BATCH_DML failed: %v", err)
	}
	if _, err := conn.ExecContext(ctx, "SET AUTO_BATCH_DML_UPDATE_COUNT_VERIFICATION = false"); err != nil {
		t.Fatalf("set AUTO_BATCH_DML_UPDATE_COUNT_VERIFICATION failed: %v", err)
	}

	tx, err := conn.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("BeginTx failed: %v", err)
	}
	defer func() { _ = tx.Rollback() }()

	if _, err := tx.ExecContext(ctx, dml1); err != nil {
		t.Fatalf("dml1 failed: %v", err)
	}
	// Creating savepoint flushes buffered DML (dml1)
	if _, err := tx.ExecContext(ctx, "SAVEPOINT sp_autobatch"); err != nil {
		t.Fatalf("SAVEPOINT failed: %v", err)
	}
	if _, err := tx.ExecContext(ctx, dml2); err != nil {
		t.Fatalf("dml2 failed: %v", err)
	}
	// Rolling back to savepoint aborts uncommitted DML batch (dml2) and replays dml1
	if _, err := tx.ExecContext(ctx, "ROLLBACK TO SAVEPOINT sp_autobatch"); err != nil {
		t.Fatalf("ROLLBACK TO SAVEPOINT failed: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit failed: %v", err)
	}

	requests := server.TestSpanner.DrainRequestsFromServer()
	commitRequests := testutil.RequestsOfType(requests, reflect.TypeOf(&spannerpb.CommitRequest{}))
	if g, w := len(commitRequests), 1; g != w {
		t.Fatalf("commit requests count mismatch\nGot:  %v\nWant: %v", g, w)
	}
}

func TestSavepoints_AutoBatchDml_FlushFailure(t *testing.T) {
	t.Parallel()

	db, server, teardown := setupTestDBConnection(t)
	defer teardown()
	ctx := context.Background()

	dml1 := "UPDATE Foo SET Val=1 WHERE Id=1"
	server.TestSpanner.PutStatementResult(dml1, &testutil.StatementResult{
		Type: testutil.StatementResultError,
		Err:  status.Error(codes.InvalidArgument, "invalid DML statement"),
	})

	conn, err := db.Conn(ctx)
	if err != nil {
		t.Fatalf("db.Conn failed: %v", err)
	}
	defer silentClose(conn)

	if _, err := conn.ExecContext(ctx, "SET AUTO_BATCH_DML = true"); err != nil {
		t.Fatalf("set AUTO_BATCH_DML failed: %v", err)
	}

	tx, err := conn.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("BeginTx failed: %v", err)
	}
	defer func() { _ = tx.Rollback() }()

	if _, err := tx.ExecContext(ctx, dml1); err != nil {
		t.Fatalf("dml1 failed: %v", err)
	}
	// Creating savepoint attempts to flush buffered auto-batch DML, which fails.
	_, err = tx.ExecContext(ctx, "SAVEPOINT sp_autobatch")
	if err == nil {
		t.Fatal("expected error from SAVEPOINT when auto-batch flush fails, got nil")
	}
	if !strings.Contains(err.Error(), "running auto-dml-batch failed") {
		t.Errorf("error message mismatch\nGot:  %v", err)
	}
	if !strings.Contains(err.Error(), "invalid DML statement") {
		t.Errorf("underlying error message mismatch\nGot:  %v", err)
	}
}

func TestSavepoints_QuotedIdentifiers_GoogleSQL_Backticks(t *testing.T) {
	t.Parallel()

	db, _, teardown := setupTestDBConnection(t)
	defer teardown()
	ctx := context.Background()

	tx, err := db.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		t.Fatalf("BeginTx failed: %v", err)
	}

	if _, err := tx.ExecContext(ctx, "SAVEPOINT `my_savepoint`"); err != nil {
		t.Fatalf("SAVEPOINT with backticks failed: %v", err)
	}
	if _, err := tx.ExecContext(ctx, "ROLLBACK TO SAVEPOINT `my_savepoint`"); err != nil {
		t.Fatalf("ROLLBACK TO SAVEPOINT with backticks failed: %v", err)
	}
	if _, err := tx.ExecContext(ctx, "RELEASE SAVEPOINT `my_savepoint`"); err != nil {
		t.Fatalf("RELEASE SAVEPOINT with backticks failed: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit failed: %v", err)
	}
}

func TestSavepoints_QuotedIdentifiers_PostgreSQL_DoubleQuotes(t *testing.T) {
	t.Parallel()

	db, _, teardown := setupTestDBConnectionWithParamsAndDialect(t, "", databasepb.DatabaseDialect_POSTGRESQL)
	defer teardown()
	ctx := context.Background()

	tx, err := db.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		t.Fatalf("BeginTx failed: %v", err)
	}

	if _, err := tx.ExecContext(ctx, `SAVEPOINT "MySavepoint"`); err != nil {
		t.Fatalf("SAVEPOINT with double quotes failed: %v", err)
	}
	if _, err := tx.ExecContext(ctx, `ROLLBACK TO SAVEPOINT "MySavepoint"`); err != nil {
		t.Fatalf("ROLLBACK TO SAVEPOINT with double quotes failed: %v", err)
	}
	if _, err := tx.ExecContext(ctx, `RELEASE SAVEPOINT "MySavepoint"`); err != nil {
		t.Fatalf("RELEASE SAVEPOINT with double quotes failed: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit failed: %v", err)
	}
}

func TestSavepoints_MultipleRollbacksToSameSavepoint(t *testing.T) {
	t.Parallel()

	for _, dialect := range []databasepb.DatabaseDialect{
		databasepb.DatabaseDialect_GOOGLE_STANDARD_SQL,
		databasepb.DatabaseDialect_POSTGRESQL,
	} {
		t.Run(dialect.String(), func(t *testing.T) {
			t.Parallel()

			db, server, teardown := setupTestDBConnectionWithParamsAndDialect(t, "", dialect)
			defer teardown()
			ctx := context.Background()

			dml1 := "UPDATE Foo SET Val=1 WHERE Id=1"
			dml2 := "UPDATE Foo SET Val=2 WHERE Id=2"
			dml3 := "UPDATE Foo SET Val=3 WHERE Id=3"
			dml4 := "UPDATE Foo SET Val=4 WHERE Id=4"
			server.TestSpanner.PutStatementResult(dml1, &testutil.StatementResult{Type: testutil.StatementResultUpdateCount, UpdateCount: 1})
			server.TestSpanner.PutStatementResult(dml2, &testutil.StatementResult{Type: testutil.StatementResultUpdateCount, UpdateCount: 1})
			server.TestSpanner.PutStatementResult(dml3, &testutil.StatementResult{Type: testutil.StatementResultUpdateCount, UpdateCount: 1})
			server.TestSpanner.PutStatementResult(dml4, &testutil.StatementResult{Type: testutil.StatementResultUpdateCount, UpdateCount: 1})

			tx, err := db.BeginTx(ctx, &sql.TxOptions{})
			if err != nil {
				t.Fatalf("BeginTx failed: %v", err)
			}
			defer func() { _ = tx.Rollback() }()

			if _, err := tx.ExecContext(ctx, dml1); err != nil {
				t.Fatalf("dml1 failed: %v", err)
			}
			if _, err := tx.ExecContext(ctx, "SAVEPOINT sp1"); err != nil {
				t.Fatalf("savepoint failed: %v", err)
			}

			// First modification after savepoint
			if _, err := tx.ExecContext(ctx, dml2); err != nil {
				t.Fatalf("dml2 failed: %v", err)
			}
			// First rollback to sp1: rolls back dml2 and replays dml1
			if _, err := tx.ExecContext(ctx, "ROLLBACK TO SAVEPOINT sp1"); err != nil {
				t.Fatalf("first rollback to savepoint failed: %v", err)
			}

			// Second modification after the first rollback to sp1
			if _, err := tx.ExecContext(ctx, dml3); err != nil {
				t.Fatalf("dml3 failed: %v", err)
			}
			// Second rollback to sp1: rolls back dml3 and replays dml1 again
			if _, err := tx.ExecContext(ctx, "ROLLBACK TO SAVEPOINT sp1"); err != nil {
				t.Fatalf("second rollback to savepoint failed: %v", err)
			}

			// Third modification after the second rollback to sp1
			if _, err := tx.ExecContext(ctx, dml4); err != nil {
				t.Fatalf("dml4 failed: %v", err)
			}

			if err := tx.Commit(); err != nil {
				t.Fatalf("commit failed: %v", err)
			}

			requests := server.TestSpanner.DrainRequestsFromServer()
			executeRequests := testutil.RequestsOfType(requests, reflect.TypeOf(&spannerpb.ExecuteSqlRequest{}))
			expectedSqls := []string{dml1, dml2, dml1, dml3, dml1, dml4}
			if g, w := len(executeRequests), len(expectedSqls); g != w {
				t.Fatalf("execute requests count mismatch\nGot:  %v\nWant: %v", g, w)
			}
			for i, expectedSql := range expectedSqls {
				req := executeRequests[i].(*spannerpb.ExecuteSqlRequest)
				if g, w := req.Sql, expectedSql; g != w {
					t.Errorf("execute request %d sql mismatch\nGot:  %v\nWant: %v", i, g, w)
				}
			}

			commitRequests := testutil.RequestsOfType(requests, reflect.TypeOf(&spannerpb.CommitRequest{}))
			if g, w := len(commitRequests), 1; g != w {
				t.Fatalf("commit requests count mismatch\nGot:  %v\nWant: %v", g, w)
			}
		})
	}
}

func TestSavepoints_PostgreSQL_CaseSensitivity(t *testing.T) {
	t.Parallel()

	db, _, teardown := setupTestDBConnectionWithParamsAndDialect(t, "", databasepb.DatabaseDialect_POSTGRESQL)
	defer teardown()
	ctx := context.Background()

	tx, err := db.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		t.Fatalf("BeginTx failed: %v", err)
	}
	defer func() { _ = tx.Rollback() }()

	// Create quoted savepoint with mixed case: "CaseSensitiveSp"
	if _, err := tx.ExecContext(ctx, `SAVEPOINT "CaseSensitiveSp"`); err != nil {
		t.Fatalf("SAVEPOINT failed: %v", err)
	}

	// Unquoted rollback to casesensitivesp folds to lowercase and does not match "CaseSensitiveSp" (fails with 3B001)
	_, err = tx.ExecContext(ctx, "ROLLBACK TO casesensitivesp")
	if err == nil {
		t.Fatalf("expected error rolling back to unquoted casesensitivesp, got nil")
	}
	if g, w := spanner.ErrCode(err), codes.FailedPrecondition; g != w {
		t.Errorf("error code mismatch\nGot:  %v\nWant: %v", g, w)
	}
	if g, w := ToPGSQLState(err), "3B001"; g != w {
		t.Errorf("SQLSTATE mismatch\nGot:  %v\nWant: %v", g, w)
	}

	// Quoted rollback matches exact case and succeeds (recovering the transaction from failed state)
	if _, err := tx.ExecContext(ctx, `ROLLBACK TO "CaseSensitiveSp"`); err != nil {
		t.Fatalf("ROLLBACK TO \"CaseSensitiveSp\" failed: %v", err)
	}

	// Quoted release matches exact case and succeeds
	if _, err := tx.ExecContext(ctx, `RELEASE "CaseSensitiveSp"`); err != nil {
		t.Fatalf("RELEASE \"CaseSensitiveSp\" failed: %v", err)
	}

	if err := tx.Commit(); err != nil {
		t.Fatalf("commit failed: %v", err)
	}
}
