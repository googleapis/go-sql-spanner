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

package api

import (
	"context"
	"fmt"
	"reflect"
	"testing"

	"cloud.google.com/go/spanner"
	"cloud.google.com/go/spanner/apiv1/spannerpb"
	"github.com/googleapis/go-sql-spanner/testutil"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestBeginAndCommit(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	server, teardown := setupMockServer(t)
	defer teardown()
	dsn := fmt.Sprintf("%s/projects/p/instances/i/databases/d?useplaintext=true", server.Address)

	poolId, err := CreatePool(ctx, "test", dsn)
	if err != nil {
		t.Fatalf("CreatePool returned unexpected error: %v", err)
	}
	connId, err := CreateConnection(ctx, poolId)
	if err != nil {
		t.Fatalf("CreateConnection returned unexpected error: %v", err)
	}
	if err := BeginTransaction(ctx, poolId, connId, &spannerpb.TransactionOptions{}); err != nil {
		t.Fatalf("BeginTransaction returned unexpected error: %v", err)
	}

	// Execute a statement in the transaction.
	rowsId, err := Execute(ctx, poolId, connId, &spannerpb.ExecuteSqlRequest{Sql: testutil.UpdateBarSetFoo})
	if err != nil {
		t.Fatalf("Execute returned unexpected error: %v", err)
	}
	stats, err := ResultSetStats(ctx, poolId, connId, rowsId)
	if err != nil {
		t.Fatalf("ResultSetStats returned unexpected error: %v", err)
	}
	if g, w := stats.GetRowCountExact(), int64(testutil.UpdateBarSetFooRowCount); g != w {
		t.Fatalf("row count mismatch for rows %d:%d:%d\n Got: %v\nWant: %v", poolId, connId, rowsId, g, w)
	}
	if err := CloseRows(ctx, poolId, connId, rowsId); err != nil {
		t.Fatalf("CloseRows returned unexpected error: %v", err)
	}

	// Commit the transaction.
	if _, err := Commit(ctx, poolId, connId); err != nil {
		t.Fatalf("Commit returned unexpected error: %v", err)
	}

	// Verify that the statement used the transaction, and that the transaction was started using an inlined begin
	// option on the ExecuteSqlRequest.
	requests := server.TestSpanner.DrainRequestsFromServer()
	beginRequests := testutil.RequestsOfType(requests, reflect.TypeOf(&spannerpb.BeginTransactionRequest{}))
	if g, w := len(beginRequests), 0; g != w {
		t.Fatalf("BeginTransaction request count mismatch\n Got: %v\nWant: %v", g, w)
	}
	executeRequests := testutil.RequestsOfType(requests, reflect.TypeOf(&spannerpb.ExecuteSqlRequest{}))
	if g, w := len(executeRequests), 1; g != w {
		t.Fatalf("Execute request count mismatch\n Got: %v\nWant: %v", g, w)
	}
	executeRequest := executeRequests[0].(*spannerpb.ExecuteSqlRequest)
	if executeRequest.GetTransaction() == nil || executeRequest.GetTransaction().GetBegin() == nil || executeRequest.GetTransaction().GetBegin().GetReadWrite() == nil {
		t.Fatalf("missing BeginTransaction option on request: %v", executeRequest)
	}
	commitRequests := testutil.RequestsOfType(requests, reflect.TypeOf(&spannerpb.CommitRequest{}))
	if g, w := len(commitRequests), 1; g != w {
		t.Fatalf("Commit request count mismatch\n Got: %v\nWant: %v", g, w)
	}

	if err := CloseConnection(ctx, poolId, connId); err != nil {
		t.Fatalf("CloseConnection returned unexpected error: %v", err)
	}
	if err := ClosePool(ctx, poolId); err != nil {
		t.Fatalf("ClosePool returned unexpected error: %v", err)
	}
}

func TestBeginAndRollback(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	server, teardown := setupMockServer(t)
	defer teardown()
	dsn := fmt.Sprintf("%s/projects/p/instances/i/databases/d?useplaintext=true", server.Address)

	poolId, err := CreatePool(ctx, "test", dsn)
	if err != nil {
		t.Fatalf("CreatePool returned unexpected error: %v", err)
	}
	connId, err := CreateConnection(ctx, poolId)
	if err != nil {
		t.Fatalf("CreateConnection returned unexpected error: %v", err)
	}
	if err := BeginTransaction(ctx, poolId, connId, &spannerpb.TransactionOptions{}); err != nil {
		t.Fatalf("BeginTransaction returned unexpected error: %v", err)
	}

	// Execute a statement in the transaction.
	rowsId, err := Execute(ctx, poolId, connId, &spannerpb.ExecuteSqlRequest{Sql: testutil.UpdateBarSetFoo})
	if err != nil {
		t.Fatalf("Execute returned unexpected error: %v", err)
	}
	stats, err := ResultSetStats(ctx, poolId, connId, rowsId)
	if err != nil {
		t.Fatalf("ResultSetStats returned unexpected error: %v", err)
	}
	if g, w := stats.GetRowCountExact(), int64(testutil.UpdateBarSetFooRowCount); g != w {
		t.Fatalf("row count mismatch for rows %d:%d:%d\n Got: %v\nWant: %v", poolId, connId, rowsId, g, w)
	}
	if err := CloseRows(ctx, poolId, connId, rowsId); err != nil {
		t.Fatalf("CloseRows returned unexpected error: %v", err)
	}

	// Rollback the transaction.
	if err := Rollback(ctx, poolId, connId); err != nil {
		t.Fatalf("Rollback returned unexpected error: %v", err)
	}

	// Verify that the transaction was rolled back.
	requests := server.TestSpanner.DrainRequestsFromServer()
	rollbackRequests := testutil.RequestsOfType(requests, reflect.TypeOf(&spannerpb.RollbackRequest{}))
	if g, w := len(rollbackRequests), 1; g != w {
		t.Fatalf("Rollback request count mismatch\n Got: %v\nWant: %v", g, w)
	}
	commitRequests := testutil.RequestsOfType(requests, reflect.TypeOf(&spannerpb.CommitRequest{}))
	if g, w := len(commitRequests), 0; g != w {
		t.Fatalf("Commit request count mismatch\n Got: %v\nWant: %v", g, w)
	}

	if err := CloseConnection(ctx, poolId, connId); err != nil {
		t.Fatalf("CloseConnection returned unexpected error: %v", err)
	}
	if err := ClosePool(ctx, poolId); err != nil {
		t.Fatalf("ClosePool returned unexpected error: %v", err)
	}
}

func TestCommitWithOpenRows(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	server, teardown := setupMockServer(t)
	defer teardown()
	dsn := fmt.Sprintf("%s/projects/p/instances/i/databases/d?useplaintext=true", server.Address)

	poolId, err := CreatePool(ctx, "test", dsn)
	if err != nil {
		t.Fatalf("CreatePool returned unexpected error: %v", err)
	}
	connId, err := CreateConnection(ctx, poolId)
	if err != nil {
		t.Fatalf("CreateConnection returned unexpected error: %v", err)
	}
	if err := BeginTransaction(ctx, poolId, connId, &spannerpb.TransactionOptions{}); err != nil {
		t.Fatalf("BeginTransaction returned unexpected error: %v", err)
	}

	// Execute a statement in the transaction.
	_, err = Execute(ctx, poolId, connId, &spannerpb.ExecuteSqlRequest{Sql: testutil.UpdateBarSetFoo})
	if err != nil {
		t.Fatalf("Execute returned unexpected error: %v", err)
	}

	// Try to commit the transaction without closing the Rows object that was returned during the transaction.
	if _, err := Commit(ctx, poolId, connId); err != nil {
		t.Fatalf("Commit returned unexpected error: %v", err)
	}

	if err := CloseConnection(ctx, poolId, connId); err != nil {
		t.Fatalf("CloseConnection returned unexpected error: %v", err)
	}
	if err := ClosePool(ctx, poolId); err != nil {
		t.Fatalf("ClosePool returned unexpected error: %v", err)
	}
}

func TestCloseConnectionWithOpenTransaction(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	server, teardown := setupMockServer(t)
	defer teardown()
	dsn := fmt.Sprintf("%s/projects/p/instances/i/databases/d?useplaintext=true", server.Address)

	poolId, err := CreatePool(ctx, "test", dsn)
	if err != nil {
		t.Fatalf("CreatePool returned unexpected error: %v", err)
	}
	connId, err := CreateConnection(ctx, poolId)
	if err != nil {
		t.Fatalf("CreateConnection returned unexpected error: %v", err)
	}
	if err := BeginTransaction(ctx, poolId, connId, &spannerpb.TransactionOptions{}); err != nil {
		t.Fatalf("BeginTransaction returned unexpected error: %v", err)
	}
	// Execute a statement in the transaction to activate it.
	_, err = Execute(ctx, poolId, connId, &spannerpb.ExecuteSqlRequest{Sql: testutil.UpdateBarSetFoo})
	if err != nil {
		t.Fatalf("Execute returned unexpected error: %v", err)
	}

	// Close the connection while a transaction is still active.
	if err := CloseConnection(ctx, poolId, connId); err != nil {
		t.Fatalf("CloseConnection returned unexpected error: %v", err)
	}
	if err := ClosePool(ctx, poolId); err != nil {
		t.Fatalf("ClosePool returned unexpected error: %v", err)
	}

	// Verify that the transaction was rolled back when the connection was closed.
	requests := server.TestSpanner.DrainRequestsFromServer()
	rollbackRequests := testutil.RequestsOfType(requests, reflect.TypeOf(&spannerpb.RollbackRequest{}))
	if g, w := len(rollbackRequests), 1; g != w {
		t.Fatalf("Rollback request count mismatch\n Got: %v\nWant: %v", g, w)
	}
}

func TestBeginTransactionWithOpenTransaction(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	server, teardown := setupMockServer(t)
	defer teardown()
	dsn := fmt.Sprintf("%s/projects/p/instances/i/databases/d?useplaintext=true", server.Address)

	poolId, err := CreatePool(ctx, "test", dsn)
	if err != nil {
		t.Fatalf("CreatePool returned unexpected error: %v", err)
	}
	connId, err := CreateConnection(ctx, poolId)
	if err != nil {
		t.Fatalf("CreateConnection returned unexpected error: %v", err)
	}
	if err := BeginTransaction(ctx, poolId, connId, &spannerpb.TransactionOptions{}); err != nil {
		t.Fatalf("BeginTransaction returned unexpected error: %v", err)
	}

	// Try to start a transaction when one is already active.
	err = BeginTransaction(ctx, poolId, connId, &spannerpb.TransactionOptions{})
	if g, w := spanner.ErrCode(err), codes.FailedPrecondition; g != w {
		t.Fatalf("error code mismatch\n Got: %v\nWant: %v", g, w)
	}
}

func TestCommitWithoutTransaction(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	server, teardown := setupMockServer(t)
	defer teardown()
	dsn := fmt.Sprintf("%s/projects/p/instances/i/databases/d?useplaintext=true", server.Address)

	poolId, err := CreatePool(ctx, "test", dsn)
	if err != nil {
		t.Fatalf("CreatePool returned unexpected error: %v", err)
	}
	connId, err := CreateConnection(ctx, poolId)
	if err != nil {
		t.Fatalf("CreateConnection returned unexpected error: %v", err)
	}

	// Try to commit when there is no transaction.
	_, err = Commit(ctx, poolId, connId)
	if g, w := spanner.ErrCode(err), codes.FailedPrecondition; g != w {
		t.Fatalf("error code mismatch\n Got: %v\nWant: %v", g, w)
	}
}

func TestRollbackWithoutTransaction(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	server, teardown := setupMockServer(t)
	defer teardown()
	dsn := fmt.Sprintf("%s/projects/p/instances/i/databases/d?useplaintext=true", server.Address)

	poolId, err := CreatePool(ctx, "test", dsn)
	if err != nil {
		t.Fatalf("CreatePool returned unexpected error: %v", err)
	}
	connId, err := CreateConnection(ctx, poolId)
	if err != nil {
		t.Fatalf("CreateConnection returned unexpected error: %v", err)
	}

	// Try to commit when there is no transaction.
	err = Rollback(ctx, poolId, connId)
	if g, w := spanner.ErrCode(err), codes.FailedPrecondition; g != w {
		t.Fatalf("error code mismatch\n Got: %v\nWant: %v", g, w)
	}
}

func TestReadOnlyTransaction(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	server, teardown := setupMockServer(t)
	defer teardown()
	dsn := fmt.Sprintf("%s/projects/p/instances/i/databases/d?useplaintext=true", server.Address)

	poolId, err := CreatePool(ctx, "test", dsn)
	if err != nil {
		t.Fatalf("CreatePool returned unexpected error: %v", err)
	}
	connId, err := CreateConnection(ctx, poolId)
	if err != nil {
		t.Fatalf("CreateConnection returned unexpected error: %v", err)
	}
	if err := BeginTransaction(ctx, poolId, connId, &spannerpb.TransactionOptions{
		Mode: &spannerpb.TransactionOptions_ReadOnly_{
			ReadOnly: &spannerpb.TransactionOptions_ReadOnly{},
		},
	}); err != nil {
		t.Fatalf("BeginTransaction returned unexpected error: %v", err)
	}

	// Execute a statement in the transaction.
	rowsId, err := Execute(ctx, poolId, connId, &spannerpb.ExecuteSqlRequest{Sql: testutil.SelectFooFromBar})
	if err != nil {
		t.Fatalf("Execute returned unexpected error: %v", err)
	}
	if err := CloseRows(ctx, poolId, connId, rowsId); err != nil {
		t.Fatalf("CloseRows returned unexpected error: %v", err)
	}

	// Commit the transaction.
	if _, err := Commit(ctx, poolId, connId); err != nil {
		t.Fatalf("Commit returned unexpected error: %v", err)
	}

	// Verify that the statement used a read-only transaction, that the transaction was started using an inlined
	// begin option on the ExecuteSqlRequest, and that the Commit call was a no-op.
	requests := server.TestSpanner.DrainRequestsFromServer()
	beginRequests := testutil.RequestsOfType(requests, reflect.TypeOf(&spannerpb.BeginTransactionRequest{}))
	if g, w := len(beginRequests), 0; g != w {
		t.Fatalf("BeginTransaction request count mismatch\n Got: %v\nWant: %v", g, w)
	}
	executeRequests := testutil.RequestsOfType(requests, reflect.TypeOf(&spannerpb.ExecuteSqlRequest{}))
	if g, w := len(executeRequests), 1; g != w {
		t.Fatalf("Execute request count mismatch\n Got: %v\nWant: %v", g, w)
	}
	executeRequest := executeRequests[0].(*spannerpb.ExecuteSqlRequest)
	if executeRequest.GetTransaction() == nil || executeRequest.GetTransaction().GetBegin() == nil || executeRequest.GetTransaction().GetBegin().GetReadOnly() == nil {
		t.Fatalf("missing BeginTransaction option on request: %v", executeRequest)
	}
	commitRequests := testutil.RequestsOfType(requests, reflect.TypeOf(&spannerpb.CommitRequest{}))
	if g, w := len(commitRequests), 0; g != w {
		t.Fatalf("Commit request count mismatch\n Got: %v\nWant: %v", g, w)
	}

	if err := CloseConnection(ctx, poolId, connId); err != nil {
		t.Fatalf("CloseConnection returned unexpected error: %v", err)
	}
	if err := ClosePool(ctx, poolId); err != nil {
		t.Fatalf("ClosePool returned unexpected error: %v", err)
	}
}

func TestDdlInTransaction(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	server, teardown := setupMockServer(t)
	defer teardown()
	dsn := fmt.Sprintf("%s/projects/p/instances/i/databases/d?useplaintext=true", server.Address)

	poolId, err := CreatePool(ctx, "test", dsn)
	if err != nil {
		t.Fatalf("CreatePool returned unexpected error: %v", err)
	}
	connId, err := CreateConnection(ctx, poolId)
	if err != nil {
		t.Fatalf("CreateConnection returned unexpected error: %v", err)
	}
	if err := BeginTransaction(ctx, poolId, connId, &spannerpb.TransactionOptions{}); err != nil {
		t.Fatalf("BeginTransaction returned unexpected error: %v", err)
	}

	// Execute a DDL statement in the transaction. This should fail.
	_, err = Execute(ctx, poolId, connId, &spannerpb.ExecuteSqlRequest{Sql: "create table my_table (id int64 primary key)"})
	if g, w := spanner.ErrCode(err), codes.FailedPrecondition; g != w {
		t.Fatalf("error code mismatch\n Got: %v\nWant: %v", g, w)
	}

	if err := CloseConnection(ctx, poolId, connId); err != nil {
		t.Fatalf("CloseConnection returned unexpected error: %v", err)
	}
	if err := ClosePool(ctx, poolId); err != nil {
		t.Fatalf("ClosePool returned unexpected error: %v", err)
	}
}

func TestTransactionOptionsAsSqlStatements(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	server, teardown := setupMockServer(t)
	defer teardown()
	dsn := fmt.Sprintf("%s/projects/p/instances/i/databases/d?useplaintext=true", server.Address)

	poolId, err := CreatePool(ctx, "test", dsn)
	if err != nil {
		t.Fatalf("CreatePool returned unexpected error: %v", err)
	}
	connId, err := CreateConnection(ctx, poolId)
	if err != nil {
		t.Fatalf("CreateConnection returned unexpected error: %v", err)
	}
	if err := BeginTransaction(ctx, poolId, connId, &spannerpb.TransactionOptions{}); err != nil {
		t.Fatalf("BeginTransaction returned unexpected error: %v", err)
	}

	// Set some local transaction options.
	if rowsId, err := Execute(ctx, poolId, connId, &spannerpb.ExecuteSqlRequest{Sql: "set local transaction_tag = 'my_tag'"}); err != nil {
		t.Fatalf("setting transaction_tag returned unexpected error: %v", err)
	} else {
		_ = CloseRows(ctx, poolId, connId, rowsId)
	}
	if rowsId, err := Execute(ctx, poolId, connId, &spannerpb.ExecuteSqlRequest{Sql: "set local retry_aborts_internally = false"}); err != nil {
		t.Fatalf("setting retry_aborts_internally returned unexpected error: %v", err)
	} else {
		_ = CloseRows(ctx, poolId, connId, rowsId)
	}

	// Execute a statement in the transaction.
	if rowsId, err := Execute(ctx, poolId, connId, &spannerpb.ExecuteSqlRequest{Sql: testutil.UpdateBarSetFoo}); err != nil {
		t.Fatalf("Execute returned unexpected error: %v", err)
	} else {
		_ = CloseRows(ctx, poolId, connId, rowsId)
	}

	// Abort the transaction to verify that the retry_aborts_internally setting was respected.
	server.TestSpanner.PutExecutionTime(testutil.MethodCommitTransaction, testutil.SimulatedExecutionTime{
		Errors: []error{status.Error(codes.Aborted, "Aborted")},
	})

	// Commit the transaction. This should fail with an Aborted error.
	if _, err := Commit(ctx, poolId, connId); err == nil {
		t.Fatal("missing expected error")
	} else {
		if g, w := spanner.ErrCode(err), codes.Aborted; g != w {
			t.Fatalf("error code mismatch\n Got: %v\nWant: %v", g, w)
		}
	}

	// Verify that the transaction_tag setting was respected.
	requests := server.TestSpanner.DrainRequestsFromServer()
	executeRequests := testutil.RequestsOfType(requests, reflect.TypeOf(&spannerpb.ExecuteSqlRequest{}))
	if g, w := len(executeRequests), 1; g != w {
		t.Fatalf("Execute request count mismatch\n Got: %v\nWant: %v", g, w)
	}
	executeRequest := executeRequests[0].(*spannerpb.ExecuteSqlRequest)
	if executeRequest.RequestOptions == nil {
		t.Fatalf("Execute request options not set")
	}
	if g, w := executeRequest.RequestOptions.TransactionTag, "my_tag"; g != w {
		t.Fatalf("TransactionTag mismatch\n Got: %v\nWant: %v", g, w)
	}
	commitRequests := testutil.RequestsOfType(requests, reflect.TypeOf(&spannerpb.CommitRequest{}))
	if g, w := len(commitRequests), 1; g != w {
		t.Fatalf("Commit request count mismatch\n Got: %v\nWant: %v", g, w)
	}
	commitRequest := commitRequests[0].(*spannerpb.CommitRequest)
	if commitRequest.RequestOptions == nil {
		t.Fatalf("Commit request options not set")
	}
	if g, w := commitRequest.RequestOptions.TransactionTag, "my_tag"; g != w {
		t.Fatalf("TransactionTag mismatch\n Got: %v\nWant: %v", g, w)
	}

	if err := CloseConnection(ctx, poolId, connId); err != nil {
		t.Fatalf("CloseConnection returned unexpected error: %v", err)
	}
	if err := ClosePool(ctx, poolId); err != nil {
		t.Fatalf("ClosePool returned unexpected error: %v", err)
	}
}

func TestReadOnlyTransactionOptionsAsSqlStatements(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	server, teardown := setupMockServer(t)
	defer teardown()
	dsn := fmt.Sprintf("%s/projects/p/instances/i/databases/d?useplaintext=true", server.Address)

	poolId, err := CreatePool(ctx, "test", dsn)
	if err != nil {
		t.Fatalf("CreatePool returned unexpected error: %v", err)
	}
	connId, err := CreateConnection(ctx, poolId)
	if err != nil {
		t.Fatalf("CreateConnection returned unexpected error: %v", err)
	}
	// Start a read-only transaction without any further options.
	if err := BeginTransaction(ctx, poolId, connId, &spannerpb.TransactionOptions{
		Mode: &spannerpb.TransactionOptions_ReadOnly_{
			ReadOnly: &spannerpb.TransactionOptions_ReadOnly{},
		},
	}); err != nil {
		t.Fatalf("BeginTransaction returned unexpected error: %v", err)
	}

	// Set a local read-only transaction options.
	if rowsId, err := Execute(ctx, poolId, connId, &spannerpb.ExecuteSqlRequest{Sql: "set local read_only_staleness = 'exact_staleness 10s'"}); err != nil {
		t.Fatalf("setting read_only_staleness returned unexpected error: %v", err)
	} else {
		_ = CloseRows(ctx, poolId, connId, rowsId)
	}

	// Execute a statement in the transaction.
	if rowsId, err := Execute(ctx, poolId, connId, &spannerpb.ExecuteSqlRequest{Sql: testutil.SelectFooFromBar}); err != nil {
		t.Fatalf("Execute returned unexpected error: %v", err)
	} else {
		_ = CloseRows(ctx, poolId, connId, rowsId)
	}

	// Commit the transaction to end it.
	if _, err := Commit(ctx, poolId, connId); err != nil {
		t.Fatalf("commit returned unexpected error: %v", err)
	}

	// Verify that the read-only staleness setting was used.
	requests := server.TestSpanner.DrainRequestsFromServer()
	executeRequests := testutil.RequestsOfType(requests, reflect.TypeOf(&spannerpb.ExecuteSqlRequest{}))
	if g, w := len(executeRequests), 1; g != w {
		t.Fatalf("Execute request count mismatch\n Got: %v\nWant: %v", g, w)
	}
	executeRequest := executeRequests[0].(*spannerpb.ExecuteSqlRequest)
	if executeRequest.GetTransaction() == nil || executeRequest.GetTransaction().GetBegin() == nil || executeRequest.GetTransaction().GetBegin().GetReadOnly() == nil {
		t.Fatal("ExecuteRequest does not contain a BeginTransaction option")
	}

	readOnly := executeRequest.GetTransaction().GetBegin().GetReadOnly()
	if readOnly.GetExactStaleness() == nil {
		t.Fatal("BeginTransaction does not contain a ExactStaleness option")
	}
	if g, w := readOnly.GetExactStaleness().GetSeconds(), int64(10); g != w {
		t.Fatalf("read staleness mismatch\n Got: %v\nWant: %v", g, w)
	}

	// There should be no commit requests, as committing a read-only transaction is a no-op on Spanner.
	commitRequests := testutil.RequestsOfType(requests, reflect.TypeOf(&spannerpb.CommitRequest{}))
	if g, w := len(commitRequests), 0; g != w {
		t.Fatalf("Commit request count mismatch\n Got: %v\nWant: %v", g, w)
	}

	if err := CloseConnection(ctx, poolId, connId); err != nil {
		t.Fatalf("CloseConnection returned unexpected error: %v", err)
	}
	if err := ClosePool(ctx, poolId); err != nil {
		t.Fatalf("ClosePool returned unexpected error: %v", err)
	}
}
