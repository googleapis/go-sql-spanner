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
	"database/sql"
	"github.com/google/go-cmp/cmp"
	"github.com/rakyll/go-sql-driver-spanner/testutil"
	sppb "google.golang.org/genproto/googleapis/spanner/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"reflect"
	"testing"
)

func TestCommitAborted(t *testing.T) {
	t.Parallel()

	db, server, teardown := setupTestDbConnection(t)
	defer teardown()

	ctx := context.Background()
	tx, err := db.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		t.Fatalf("begin failed: %v", err)
	}
	server.TestSpanner.PutExecutionTime(testutil.MethodCommitTransaction, testutil.SimulatedExecutionTime{
		Errors: []error{status.Error(codes.Aborted, "Aborted")},
	})
	err = tx.Commit()
	if err != nil {
		t.Fatalf("commit failed: %v", err)
	}
	reqs := drainRequestsFromServer(server.TestSpanner)
	commitReqs := requestsOfType(reqs, reflect.TypeOf(&sppb.CommitRequest{}))
	if g, w := len(commitReqs), 2; g != w {
		t.Fatalf("commit request count mismatch\nGot: %v\nWant: %v", g, w)
	}
}

func TestUpdateAborted(t *testing.T) {
	t.Parallel()

	db, server, teardown := setupTestDbConnection(t)
	defer teardown()

	ctx := context.Background()
	tx, err := db.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		t.Fatalf("begin failed: %v", err)
	}
	server.TestSpanner.PutExecutionTime(testutil.MethodExecuteSql, testutil.SimulatedExecutionTime{
		Errors: []error{status.Error(codes.Aborted, "Aborted")},
	})
	res, err := tx.ExecContext(ctx, testutil.UpdateBarSetFoo)
	if err != nil {
		t.Fatalf("update failed: %v", err)
	}
	affected, err := res.RowsAffected()
	if err != nil {
		t.Fatalf("get affected rows failed: %v", err)
	}
	if affected != testutil.UpdateBarSetFooRowCount {
		t.Fatalf("affected rows mismatch\nGot: %v\nWant: %v", affected, testutil.UpdateBarSetFooRowCount)
	}
	err = tx.Commit()
	if err != nil {
		t.Fatalf("commit failed: %v", err)
	}
	reqs := drainRequestsFromServer(server.TestSpanner)
	execReqs := requestsOfType(reqs, reflect.TypeOf(&sppb.ExecuteSqlRequest{}))
	if g, w := len(execReqs), 2; g != w {
		t.Fatalf("execute request count mismatch\nGot: %v\nWant: %v", g, w)
	}
	commitReqs := requestsOfType(reqs, reflect.TypeOf(&sppb.CommitRequest{}))
	if g, w := len(commitReqs), 1; g != w {
		t.Fatalf("commit request count mismatch\nGot: %v\nWant: %v", g, w)
	}
}

func TestQueryAborted(t *testing.T) {
	testQueryAborted(t, func(server testutil.InMemSpannerServer) {
		server.PutExecutionTime(testutil.MethodExecuteStreamingSql, testutil.SimulatedExecutionTime{
			Errors: []error{status.Error(codes.Aborted, "Aborted")},
		})
	}, 2)
}

func TestQueryAbortedHalfway(t *testing.T) {
	testQueryAborted(t, func(server testutil.InMemSpannerServer) {
		server.AddPartialResultSetError(testutil.SelectFooFromBar, testutil.PartialResultSetExecutionTime{
			ResumeToken: testutil.EncodeResumeToken(2),
			Err: status.Error(codes.Aborted, "Aborted"),
		})
	}, 2)
}

func TestQueryAbortedTwice(t *testing.T) {
	testQueryAborted(t, func(server testutil.InMemSpannerServer) {
		server.AddPartialResultSetError(testutil.SelectFooFromBar, testutil.PartialResultSetExecutionTime{
			ResumeToken: testutil.EncodeResumeToken(1),
			Err: status.Error(codes.Aborted, "Aborted"),
		})
		server.AddPartialResultSetError(testutil.SelectFooFromBar, testutil.PartialResultSetExecutionTime{
			ResumeToken: testutil.EncodeResumeToken(2),
			Err: status.Error(codes.Aborted, "Aborted"),
		})
	}, 3)
}

func testQueryAborted(t *testing.T, setupServer func(server testutil.InMemSpannerServer), wantExecuteCount int) {
	t.Parallel()

	db, server, teardown := setupTestDbConnection(t)
	defer teardown()

	setupServer(server.TestSpanner)
	ctx := context.Background()
	tx, err := db.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		t.Fatalf("begin failed: %v", err)
	}
	rows, err := tx.QueryContext(ctx, testutil.SelectFooFromBar)
	if err != nil {
		t.Fatalf("query failed: %v", err)
	}
	values := make([]int64, 0)
	for rows.Next() {
		err = rows.Err()
		if err != nil {
			t.Fatalf("next failed: %v", err)
		}
		var val int64
		err = rows.Scan(&val)
		if err != nil {
			t.Fatalf("scan failed: %v", err)
		}
		values = append(values, val)
	}
	err = rows.Err()
	if err != nil {
		t.Fatalf("next failed: %v", err)
	}
	if g, w := len(values), 2; g != w {
		t.Fatalf("row count mismatch\nGot: %v\nWant: %v", g, w)
	}
	if !cmp.Equal([]int64{1, 2}, values) {
		t.Fatalf("values mismatch\nGot: %v\nWant: %v", values, []int64{1, 2})
	}
	err = tx.Commit()
	if err != nil {
		t.Fatalf("commit failed: %v", err)
	}
	reqs := drainRequestsFromServer(server.TestSpanner)
	execReqs := requestsOfType(reqs, reflect.TypeOf(&sppb.ExecuteSqlRequest{}))
	if g, w := len(execReqs), wantExecuteCount; g != w {
		t.Fatalf("execute request count mismatch\nGot: %v\nWant: %v", g, w)
	}
	commitReqs := requestsOfType(reqs, reflect.TypeOf(&sppb.CommitRequest{}))
	if g, w := len(commitReqs), 1; g != w {
		t.Fatalf("commit request count mismatch\nGot: %v\nWant: %v", g, w)
	}
}

func TestSecondUpdateAborted(t *testing.T) {
	t.Parallel()

	db, server, teardown := setupTestDbConnection(t)
	defer teardown()

	ctx := context.Background()
	tx, err := db.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		t.Fatalf("begin failed: %v", err)
	}
	// This statement should succeed and is not aborted.
	_, err = tx.ExecContext(ctx, testutil.UpdateSingersSetLastName)
	if err != nil {
		t.Fatalf("update singers failed: %v", err)
	}

	server.TestSpanner.PutExecutionTime(testutil.MethodExecuteSql, testutil.SimulatedExecutionTime{
		Errors: []error{status.Error(codes.Aborted, "Aborted")},
	})
	// This statement will return Aborted, the transaction will be retried internally and the statement is
	// then executed once more and should return the correct value.
	res, err := tx.ExecContext(ctx, testutil.UpdateBarSetFoo)
	if err != nil {
		t.Fatalf("update bar failed: %v", err)
	}
	affected, err := res.RowsAffected()
	if err != nil {
		t.Fatalf("get affected rows failed: %v", err)
	}
	if affected != testutil.UpdateBarSetFooRowCount {
		t.Fatalf("affected rows mismatch\nGot: %v\nWant: %v", affected, testutil.UpdateBarSetFooRowCount)
	}
	err = tx.Commit()
	if err != nil {
		t.Fatalf("commit failed: %v", err)
	}
	reqs := drainRequestsFromServer(server.TestSpanner)
	execReqs := requestsOfType(reqs, reflect.TypeOf(&sppb.ExecuteSqlRequest{}))
	// The server should receive 4 execute statements, as each update statement should
	// be executed twice.
	if g, w := len(execReqs), 4; g != w {
		t.Fatalf("execute request count mismatch\nGot: %v\nWant: %v", g, w)
	}
	commitReqs := requestsOfType(reqs, reflect.TypeOf(&sppb.CommitRequest{}))
	if g, w := len(commitReqs), 1; g != w {
		t.Fatalf("commit request count mismatch\nGot: %v\nWant: %v", g, w)
	}
}

func TestSecondUpdateAborted_FirstResultChanged(t *testing.T) {
	t.Parallel()

	db, server, teardown := setupTestDbConnection(t)
	defer teardown()

	ctx := context.Background()
	tx, err := db.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		t.Fatalf("begin failed: %v", err)
	}
	// The result of this statement will be different during the retry.
	// That will cause the transaction to fail with an errAbortedDueToConcurrentModification error.
	_, err = tx.ExecContext(ctx, testutil.UpdateSingersSetLastName)
	if err != nil {
		t.Fatalf("update singers failed: %v", err)
	}
	server.TestSpanner.PutStatementResult(testutil.UpdateSingersSetLastName, &testutil.StatementResult{
		Type: testutil.StatementResultUpdateCount,
		UpdateCount: 0,
	})

	server.TestSpanner.PutExecutionTime(testutil.MethodExecuteSql, testutil.SimulatedExecutionTime{
		Errors: []error{status.Error(codes.Aborted, "Aborted")},
	})
	// This statement will return Aborted and the transaction will be retried internally. That
	// retry will fail because the result of the first statement is different during the retry.
	_, err = tx.ExecContext(ctx, testutil.UpdateBarSetFoo)
	if err != errAbortedDueToConcurrentModification {
		t.Fatalf("update error mismatch\nGot: %v\nWant: %v", err, errAbortedDueToConcurrentModification)
	}
	reqs := drainRequestsFromServer(server.TestSpanner)
	execReqs := requestsOfType(reqs, reflect.TypeOf(&sppb.ExecuteSqlRequest{}))
	// The server should receive 3 execute statements, as only the first statement is retried.
	if g, w := len(execReqs), 3; g != w {
		t.Fatalf("execute request count mismatch\nGot: %v\nWant: %v", g, w)
	}
}
