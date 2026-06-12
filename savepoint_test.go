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
	"reflect"
	"testing"

	spannerpb "cloud.google.com/go/spanner/apiv1/spannerpb"
	"github.com/googleapis/go-sql-spanner/testutil"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestSavepoints(t *testing.T) {
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
		t.Fatal(err)
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

	if _, err := tx.ExecContext(ctx, "ROLLBACK TO SAVEPOINT s1"); err != nil {
		t.Fatalf("rollback to s1 failed: %v", err)
	}

	if err := tx.Commit(); err != nil {
		t.Fatalf("commit failed: %v", err)
	}

	requests := server.TestSpanner.DrainRequestsFromServer()
	commitRequests := testutil.RequestsOfType(requests, reflect.TypeOf(&spannerpb.CommitRequest{}))
	if len(commitRequests) != 1 {
		t.Fatalf("expected 1 commit request, got %d", len(commitRequests))
	}

	executeRequests := testutil.RequestsOfType(requests, reflect.TypeOf(&spannerpb.ExecuteSqlRequest{}))
	if g, w := len(executeRequests), 3; g != w {
		t.Fatalf("execute requests count mismatch\n Got: %v\nWant: %v", g, w)
	}

	req1 := executeRequests[0].(*spannerpb.ExecuteSqlRequest)
	req2 := executeRequests[1].(*spannerpb.ExecuteSqlRequest)
	req3 := executeRequests[2].(*spannerpb.ExecuteSqlRequest)

	if req1.Sql != dml1 {
		t.Errorf("expected req1 to be dml1, got %q", req1.Sql)
	}
	if req2.Sql != dml2 {
		t.Errorf("expected req2 to be dml2, got %q", req2.Sql)
	}
	if req3.Sql != dml1 {
		t.Errorf("expected req3 to be dml1 (replayed), got %q", req3.Sql)
	}

	if req1.Transaction.GetBegin() == nil {
		t.Error("expected req1 to begin a transaction")
	}
	if len(req2.Transaction.GetId()) == 0 || req2.Transaction.GetBegin() != nil {
		t.Error("expected req2 to use an existing transaction ID")
	}
	if req3.Transaction.GetBegin() == nil {
		t.Error("expected req3 (replayed) to begin a new transaction")
	}
}

func TestSavepointErrors(t *testing.T) {
	t.Parallel()

	db, _, teardown := setupTestDBConnection(t)
	defer teardown()
	ctx := context.Background()

	// 1. SAVEPOINT outside transaction should fail
	if _, err := db.ExecContext(ctx, "SAVEPOINT s1"); err == nil {
		t.Error("expected error for SAVEPOINT outside transaction")
	} else if g, w := status.Code(err), codes.FailedPrecondition; g != w {
		t.Errorf("error code mismatch\n Got: %v\nWant: %v", g, w)
	}

	tx, err := db.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		t.Fatal(err)
	}

	// 2. ROLLBACK TO non-existent savepoint should fail
	if _, err := tx.ExecContext(ctx, "ROLLBACK TO SAVEPOINT s_none"); err == nil {
		t.Error("expected error for rolling back to non-existent savepoint")
	} else if g, w := status.Code(err), codes.FailedPrecondition; g != w {
		t.Errorf("error code mismatch\n Got: %v\nWant: %v", g, w)
	}

	// 3. RELEASE non-existent savepoint should fail
	if _, err := tx.ExecContext(ctx, "RELEASE SAVEPOINT s_none"); err == nil {
		t.Error("expected error for releasing non-existent savepoint")
	} else if g, w := status.Code(err), codes.FailedPrecondition; g != w {
		t.Errorf("error code mismatch\n Got: %v\nWant: %v", g, w)
	}

	// 4. Create and release should work, then rollback to it should fail
	if _, err := tx.ExecContext(ctx, "SAVEPOINT s1"); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.ExecContext(ctx, "RELEASE SAVEPOINT s1"); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.ExecContext(ctx, "ROLLBACK TO SAVEPOINT s1"); err == nil {
		t.Error("expected error for rolling back to released savepoint")
	} else if g, w := status.Code(err), codes.FailedPrecondition; g != w {
		t.Errorf("error code mismatch\n Got: %v\nWant: %v", g, w)
	}

	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}
}
