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
	"reflect"
	"strings"
	"testing"

	"cloud.google.com/go/spanner"
	"cloud.google.com/go/spanner/apiv1/spannerpb"
	"github.com/googleapis/go-sql-spanner/testutil"
	"google.golang.org/grpc/codes"
)

type sqlExecContext interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
}

func TestAutoBatchDml_Basics(t *testing.T) {
	t.Parallel()

	insert, _, db, server, teardown := setupAutoBatchDmlTest(t)
	defer teardown()
	ctx := context.Background()

	for n := 0; n < 2; n++ {
		tx, err := db.BeginTx(ctx, &sql.TxOptions{})
		if err != nil {
			t.Fatalf("failed to start transaction: %v", err)
		}
		execContext(t, ctx, tx, insert, 1, 1)
		execContext(t, ctx, tx, insert, 1, 2)
		if err := tx.Commit(); err != nil {
			t.Fatalf("failed to commit: %v", err)
		}

		requests := drainRequestsFromServer(server.TestSpanner)
		batchDmlRequests := requestsOfType(requests, reflect.TypeOf(&spannerpb.ExecuteBatchDmlRequest{}))
		if g, w := len(batchDmlRequests), 1; g != w {
			t.Fatalf("num BatchDML requests mismatch\n Got: %v\nWant: %v", g, w)
		}
		batchDmlRequest := batchDmlRequests[0].(*spannerpb.ExecuteBatchDmlRequest)
		if g, w := len(batchDmlRequest.Statements), 2; g != w {
			t.Fatalf("num statements in batch requests mismatch\n Got: %v\nWant: %v", g, w)
		}
		commitRequests := requestsOfType(requests, reflect.TypeOf(&spannerpb.CommitRequest{}))
		if g, w := len(commitRequests), 1; g != w {
			t.Fatalf("num commit requests mismatch\n Got: %v\nWant: %v", g, w)
		}
	}
}

func TestAutoBatchDml_UpdateCount(t *testing.T) {
	t.Parallel()

	insert, _, db, server, teardown := setupAutoBatchDmlTest(t)
	defer teardown()
	ctx := context.Background()

	// Repeat the test twice to ensure that the update count value that is
	// set is reset when the connection is returned to the pool.
	for n := 0; n < 2; n++ {

		tx, err := db.BeginTx(ctx, &sql.TxOptions{})
		if err != nil {
			t.Fatalf("failed to start transaction: %v", err)
		}
		// Disable batch verification, as the expected update count won't match the actual value.
		if _, err := tx.ExecContext(ctx, "set auto_batch_dml_update_count_verification = false"); err != nil {
			t.Fatalf("failed to disable verification: %v", err)
		}

		execContext(t, ctx, tx, insert, 1, 1)
		if _, err := tx.ExecContext(ctx, "set auto_batch_dml_update_count = 2"); err != nil {
			t.Fatalf("failed to set update count: %v", err)
		}
		execContext(t, ctx, tx, insert, 2, 2)
		if err := tx.Commit(); err != nil {
			t.Fatalf("failed to commit: %v", err)
		}

		requests := drainRequestsFromServer(server.TestSpanner)
		batchDmlRequests := requestsOfType(requests, reflect.TypeOf(&spannerpb.ExecuteBatchDmlRequest{}))
		if g, w := len(batchDmlRequests), 1; g != w {
			t.Fatalf("num BatchDML requests mismatch\n Got: %v\nWant: %v", g, w)
		}
		batchDmlRequest := batchDmlRequests[0].(*spannerpb.ExecuteBatchDmlRequest)
		if g, w := len(batchDmlRequest.Statements), 2; g != w {
			t.Fatalf("num statements in batch requests mismatch\n Got: %v\nWant: %v", g, w)
		}
		commitRequests := requestsOfType(requests, reflect.TypeOf(&spannerpb.CommitRequest{}))
		if g, w := len(commitRequests), 1; g != w {
			t.Fatalf("num commit requests mismatch\n Got: %v\nWant: %v", g, w)
		}
	}
}

func TestAutoBatchDml_UpdateCountVerificationFailsBeforeCommit(t *testing.T) {
	t.Parallel()

	insert, _, db, server, teardown := setupAutoBatchDmlTest(t)
	defer teardown()
	ctx := context.Background()

	for n := 0; n < 2; n++ {
		tx, err := db.BeginTx(ctx, &sql.TxOptions{})
		if err != nil {
			t.Fatalf("failed to start transaction: %v", err)
		}

		// Change the expected update count that should be returned.
		// This will make the batch fail, as the actual update count
		// that is returned when the batch is executed will not match
		// the update count that is set here.
		if _, err := tx.ExecContext(ctx, "set auto_batch_dml_update_count = 2"); err != nil {
			t.Fatalf("failed to set update count: %v", err)
		}
		execContext(t, ctx, tx, insert, 2, 1)
		if g, w := spanner.ErrCode(tx.Commit()), codes.FailedPrecondition; g != w {
			t.Fatalf("error code mismatch\n Got: %v\nWant: %v", g, w)
		}

		requests := drainRequestsFromServer(server.TestSpanner)
		batchDmlRequests := requestsOfType(requests, reflect.TypeOf(&spannerpb.ExecuteBatchDmlRequest{}))
		if g, w := len(batchDmlRequests), 1; g != w {
			t.Fatalf("num BatchDML requests mismatch\n Got: %v\nWant: %v", g, w)
		}
		batchDmlRequest := batchDmlRequests[0].(*spannerpb.ExecuteBatchDmlRequest)
		if g, w := len(batchDmlRequest.Statements), 1; g != w {
			t.Fatalf("num statements in batch requests mismatch\n Got: %v\nWant: %v", g, w)
		}
		commitRequests := requestsOfType(requests, reflect.TypeOf(&spannerpb.CommitRequest{}))
		if g, w := len(commitRequests), 0; g != w {
			t.Fatalf("num commit requests mismatch\n Got: %v\nWant: %v", g, w)
		}
		rollbackRequests := requestsOfType(requests, reflect.TypeOf(&spannerpb.RollbackRequest{}))
		if g, w := len(rollbackRequests), 1; g != w {
			t.Fatalf("num rollback requests mismatch\n Got: %v\nWant: %v", g, w)
		}
	}
}

func TestAutoBatchDml_UpdateCountVerificationFailsBeforeQuery(t *testing.T) {
	t.Parallel()

	insert, query, db, server, teardown := setupAutoBatchDmlTest(t)
	defer teardown()
	ctx := context.Background()

	for n := 0; n < 2; n++ {
		tx, err := db.BeginTx(ctx, &sql.TxOptions{})
		if err != nil {
			t.Fatalf("failed to start transaction: %v", err)
		}
		// Change the expected update count that should be returned.
		// This will make the batch fail, as the actual update count
		// that is returned when the batch is executed will not match
		// the update count that is set here.
		if _, err := tx.ExecContext(ctx, "set auto_batch_dml_update_count = 2"); err != nil {
			t.Fatalf("failed to set update count: %v", err)
		}
		execContext(t, ctx, tx, insert, 2, 1)
		_, err = tx.QueryContext(ctx, query)
		if g, w := spanner.ErrCode(err), codes.FailedPrecondition; g != w {
			t.Fatalf("error code mismatch\n Got: %v\nWant: %v", g, w)
		}
		if err := tx.Commit(); err != nil {
			t.Fatalf("failed to commit transaction: %v", err)
		}

		requests := drainRequestsFromServer(server.TestSpanner)
		batchDmlRequests := requestsOfType(requests, reflect.TypeOf(&spannerpb.ExecuteBatchDmlRequest{}))
		if g, w := len(batchDmlRequests), 1; g != w {
			t.Fatalf("num BatchDML requests mismatch\n Got: %v\nWant: %v", g, w)
		}
		batchDmlRequest := batchDmlRequests[0].(*spannerpb.ExecuteBatchDmlRequest)
		if g, w := len(batchDmlRequest.Statements), 1; g != w {
			t.Fatalf("num statements in batch requests mismatch\n Got: %v\nWant: %v", g, w)
		}
		executeRequests := requestsOfType(requests, reflect.TypeOf(&spannerpb.ExecuteSqlRequest{}))
		if g, w := len(executeRequests), 0; g != w {
			t.Fatalf("num ExecuteSql requests mismatch\n Got: %v\nWant: %v", g, w)
		}
		commitRequests := requestsOfType(requests, reflect.TypeOf(&spannerpb.CommitRequest{}))
		if g, w := len(commitRequests), 1; g != w {
			t.Fatalf("num commit requests mismatch\n Got: %v\nWant: %v", g, w)
		}
		rollbackRequests := requestsOfType(requests, reflect.TypeOf(&spannerpb.RollbackRequest{}))
		if g, w := len(rollbackRequests), 0; g != w {
			t.Fatalf("num rollback requests mismatch\n Got: %v\nWant: %v", g, w)
		}
	}
}

func TestAutoBatchDml_OutsideTransaction(t *testing.T) {
	t.Parallel()

	insert, _, db, server, teardown := setupAutoBatchDmlTest(t)
	defer teardown()
	ctx := context.Background()

	for n := 0; n < 2; n++ {
		execContext(t, ctx, db, insert, 1, 1)
		execContext(t, ctx, db, insert, 1, 2)

		requests := drainRequestsFromServer(server.TestSpanner)
		// DML auto-batching only works in transactions.
		batchDmlRequests := requestsOfType(requests, reflect.TypeOf(&spannerpb.ExecuteBatchDmlRequest{}))
		if g, w := len(batchDmlRequests), 0; g != w {
			t.Fatalf("num BatchDML requests mismatch\n Got: %v\nWant: %v", g, w)
		}
		executeRequests := requestsOfType(requests, reflect.TypeOf(&spannerpb.ExecuteSqlRequest{}))
		if g, w := len(executeRequests), 2; g != w {
			t.Fatalf("num ExecuteSql requests mismatch\n Got: %v\nWant: %v", g, w)
		}
		// Each statement is committed after execution, so we should see 2 commit requests.
		commitRequests := requestsOfType(requests, reflect.TypeOf(&spannerpb.CommitRequest{}))
		if g, w := len(commitRequests), 2; g != w {
			t.Fatalf("num commit requests mismatch\n Got: %v\nWant: %v", g, w)
		}
	}
}

func TestAutoBatchDml_FollowedByQuery(t *testing.T) {
	t.Parallel()

	insert, query, db, server, teardown := setupAutoBatchDmlTest(t)
	defer teardown()
	ctx := context.Background()

	for n := 0; n < 2; n++ {
		tx, err := db.BeginTx(ctx, &sql.TxOptions{})
		if err != nil {
			t.Fatalf("failed to start transaction: %v", err)
		}
		execContext(t, ctx, tx, insert, 1, 1)
		execContext(t, ctx, tx, insert, 1, 2)
		queryContext(t, ctx, tx, query)
		if err := tx.Commit(); err != nil {
			t.Fatalf("failed to commit: %v", err)
		}

		requests := drainRequestsFromServer(server.TestSpanner)
		batchDmlRequests := requestsOfType(requests, reflect.TypeOf(&spannerpb.ExecuteBatchDmlRequest{}))
		if g, w := len(batchDmlRequests), 1; g != w {
			t.Fatalf("num BatchDML requests mismatch\n Got: %v\nWant: %v", g, w)
		}
		executeRequests := requestsOfType(requests, reflect.TypeOf(&spannerpb.ExecuteSqlRequest{}))
		if g, w := len(executeRequests), 1; g != w {
			t.Fatalf("num ExecuteSql requests mismatch\n Got: %v\nWant: %v", g, w)
		}
		commitRequests := requestsOfType(requests, reflect.TypeOf(&spannerpb.CommitRequest{}))
		if g, w := len(commitRequests), 1; g != w {
			t.Fatalf("num commit requests mismatch\n Got: %v\nWant: %v", g, w)
		}
	}
}

func TestAutoBatchDml_FollowedByRollback(t *testing.T) {
	t.Parallel()

	insert, _, db, server, teardown := setupAutoBatchDmlTest(t)
	defer teardown()
	ctx := context.Background()

	for n := 0; n < 2; n++ {
		tx, err := db.BeginTx(ctx, &sql.TxOptions{})
		if err != nil {
			t.Fatalf("failed to start transaction: %v", err)
		}
		execContext(t, ctx, tx, insert, 1, 1)
		execContext(t, ctx, tx, insert, 1, 2)
		if err := tx.Rollback(); err != nil {
			t.Fatalf("failed to rollback: %v", err)
		}

		requests := drainRequestsFromServer(server.TestSpanner)
		batchDmlRequests := requestsOfType(requests, reflect.TypeOf(&spannerpb.ExecuteBatchDmlRequest{}))
		// The batch should be aborted and no requests should be sent to Spanner.
		if g, w := len(batchDmlRequests), 0; g != w {
			t.Fatalf("num BatchDML requests mismatch\n Got: %v\nWant: %v", g, w)
		}
		executeRequests := requestsOfType(requests, reflect.TypeOf(&spannerpb.ExecuteSqlRequest{}))
		if g, w := len(executeRequests), 0; g != w {
			t.Fatalf("num ExecuteSql requests mismatch\n Got: %v\nWant: %v", g, w)
		}
		commitRequests := requestsOfType(requests, reflect.TypeOf(&spannerpb.CommitRequest{}))
		if g, w := len(commitRequests), 0; g != w {
			t.Fatalf("num commit requests mismatch\n Got: %v\nWant: %v", g, w)
		}
		beginRequests := requestsOfType(requests, reflect.TypeOf(&spannerpb.BeginTransactionRequest{}))
		if g, w := len(beginRequests), 0; g != w {
			t.Fatalf("num BeginTransaction requests mismatch\n Got: %v\nWant: %v", g, w)
		}
		rollbackRequests := requestsOfType(requests, reflect.TypeOf(&spannerpb.RollbackRequest{}))
		// There are no rollback requests sent to Spanner, as the transaction is never started.
		if g, w := len(rollbackRequests), 0; g != w {
			t.Fatalf("num rollback requests mismatch\n Got: %v\nWant: %v", g, w)
		}
	}
}
func TestAutoBatchDml_ReadOnlyTx(t *testing.T) {
	t.Parallel()

	insert, _, db, _, teardown := setupAutoBatchDmlTest(t)
	defer teardown()
	ctx := context.Background()

	for n := 0; n < 2; n++ {
		tx, err := db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
		if err != nil {
			t.Fatalf("failed to start transaction: %v", err)
		}
		_, err = tx.ExecContext(ctx, insert, 1)
		if g, w := spanner.ErrCode(err), codes.FailedPrecondition; g != w {
			t.Fatalf("error code mismatch\n Got: %v\nWant: %v", g, w)
		}
		if err := tx.Rollback(); err != nil {
			t.Fatalf("failed to rollback: %v", err)
		}
	}
}

func setupAutoBatchDmlTest(t *testing.T) (insert, query string, db *sql.DB, server *testutil.MockedSpannerInMemTestServer, teardown func()) {
	db, server, teardown = setupTestDBConnectionWithConnectorConfig(t, ConnectorConfig{
		Project:  "p",
		Instance: "i",
		Database: "d",

		AutoBatchDml:            true,
		AutoBatchDmlUpdateCount: int64(1),
	})
	db.SetMaxOpenConns(1)

	query = "select value from test"
	if err := server.TestSpanner.PutStatementResult(
		query,
		&testutil.StatementResult{
			Type:      testutil.StatementResultResultSet,
			ResultSet: testutil.CreateSelect1ResultSet(),
		},
	); err != nil {
		t.Fatal(err)
	}

	insert = "insert into test (value) values (?)"
	if err := server.TestSpanner.PutStatementResult(
		strings.Replace(insert, "?", "@p1", 1),
		&testutil.StatementResult{
			Type:        testutil.StatementResultUpdateCount,
			UpdateCount: 1,
		},
	); err != nil {
		t.Fatal(err)
	}
	return insert, query, db, server, teardown
}

func execContext(t *testing.T, ctx context.Context, tx sqlExecContext, dml string, wantUpdateCount int64, args ...any) {
	if res, err := tx.ExecContext(ctx, dml, args...); err == nil {
		if c, err := res.RowsAffected(); err == nil {
			if g, w := c, wantUpdateCount; g != w {
				t.Fatalf("update count mismatch\n Got: %v\nWant: %v", g, w)
			}
		} else {
			t.Fatalf("failed to get rows affected: %v", err)
		}
	} else {
		t.Fatalf("failed to execute statement: %v", err)
	}
}

func queryContext(t *testing.T, ctx context.Context, tx *sql.Tx, query string, args ...any) {
	rows, err := tx.QueryContext(ctx, query, args...)
	if err != nil {
		t.Fatalf("failed to execute query: %v", err)
	}
	for rows.Next() {
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("failed to iterate over rows: %v", err)
	}
}
