// Copyright 2021 Google LLC
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
	"time"

	"cloud.google.com/go/spanner"
	sppb "cloud.google.com/go/spanner/apiv1/spannerpb"
	"github.com/google/go-cmp/cmp"
	"github.com/googleapis/go-sql-spanner/testutil"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestCommitAborted(t *testing.T) {
	t.Parallel()

	db, server, teardown := setupTestDBConnectionWithParams(t, "minSessions=1;maxSessions=1")
	defer teardown()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
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
	reqs := server.TestSpanner.DrainRequestsFromServer()
	commitReqs := testutil.RequestsOfType(reqs, reflect.TypeOf(&sppb.CommitRequest{}))
	if g, w := len(commitReqs), 2; g != w {
		t.Fatalf("commit request count mismatch\nGot: %v\nWant: %v", g, w)
	}

	// Verify that the db is still usable.
	if _, err := db.ExecContext(ctx, testutil.UpdateSingersSetLastName); err != nil {
		t.Fatalf("failed to execute statement after transaction: %v", err)
	}
}

func TestCommitWithMutationsAborted(t *testing.T) {
	t.Parallel()

	db, server, teardown := setupTestDBConnectionWithParams(t, "minSessions=1;maxSessions=1")
	defer teardown()
	ctx := context.Background()

	conn, err := db.Conn(ctx)
	if err != nil {
		t.Fatalf("failed to open connection: %v", err)
	}
	defer func() { _ = conn.Close() }()

	tx, err := conn.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		t.Fatalf("begin failed: %v", err)
	}
	if err := conn.Raw(func(driverConn interface{}) error {
		spannerConn, _ := driverConn.(SpannerConn)
		mutation := spanner.Insert("foo", []string{}, []interface{}{})
		return spannerConn.BufferWrite([]*spanner.Mutation{mutation})
	}); err != nil {
		t.Fatalf("failed to buffer mutations: %v", err)
	}
	// Abort the transaction on the first commit attempt.
	server.TestSpanner.PutExecutionTime(testutil.MethodCommitTransaction, testutil.SimulatedExecutionTime{
		Errors: []error{status.Error(codes.Aborted, "Aborted")},
	})
	err = tx.Commit()
	if err != nil {
		t.Fatalf("commit failed: %v", err)
	}
	reqs := server.TestSpanner.DrainRequestsFromServer()
	commitReqs := testutil.RequestsOfType(reqs, reflect.TypeOf(&sppb.CommitRequest{}))
	if g, w := len(commitReqs), 2; g != w {
		t.Fatalf("commit request count mismatch\nGot: %v\nWant: %v", g, w)
	}
	for _, req := range commitReqs {
		commitReq := req.(*sppb.CommitRequest)
		if g, w := len(commitReq.Mutations), 1; g != w {
			t.Fatalf("mutation count mismatch\n Got: %v\nWant: %v", g, w)
		}
	}
}

func TestCommitAbortedWithInternalRetriesDisabled(t *testing.T) {
	t.Parallel()

	db, server, teardown := setupTestDBConnectionWithParams(t, "retryAbortsInternally=false;minSessions=1;maxSessions=1")
	defer teardown()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	tx, err := db.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		t.Fatalf("begin failed: %v", err)
	}
	server.TestSpanner.PutExecutionTime(testutil.MethodCommitTransaction, testutil.SimulatedExecutionTime{
		Errors: []error{status.Error(codes.Aborted, "Aborted")},
	})
	err = tx.Commit()
	// The aborted error should be propagated to the caller when internal retries have been disabled.
	if g, w := spanner.ErrCode(err), codes.Aborted; g != w {
		t.Fatalf("commit error code mismatch\nGot: %v\nWant: %v", g, w)
	}
	reqs := server.TestSpanner.DrainRequestsFromServer()
	commitReqs := testutil.RequestsOfType(reqs, reflect.TypeOf(&sppb.CommitRequest{}))
	if g, w := len(commitReqs), 1; g != w {
		t.Fatalf("commit request count mismatch\nGot: %v\nWant: %v", g, w)
	}

	// Verify that the db is still usable.
	if _, err := db.ExecContext(ctx, testutil.UpdateSingersSetLastName); err != nil {
		t.Fatalf("failed to execute statement after transaction: %v", err)
	}
}

func TestUpdateAborted(t *testing.T) {
	t.Parallel()

	db, server, teardown := setupTestDBConnectionWithParams(t, "minSessions=1;maxSessions=1")
	defer teardown()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	tx, err := db.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		t.Fatalf("begin failed: %v", err)
	}
	server.TestSpanner.PutExecutionTime(testutil.MethodExecuteStreamingSql, testutil.SimulatedExecutionTime{
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
	reqs := server.TestSpanner.DrainRequestsFromServer()
	execReqs := testutil.RequestsOfType(reqs, reflect.TypeOf(&sppb.ExecuteSqlRequest{}))
	if g, w := len(execReqs), 2; g != w {
		t.Fatalf("execute request count mismatch\nGot: %v\nWant: %v", g, w)
	}
	commitReqs := testutil.RequestsOfType(reqs, reflect.TypeOf(&sppb.CommitRequest{}))
	if g, w := len(commitReqs), 1; g != w {
		t.Fatalf("commit request count mismatch\nGot: %v\nWant: %v", g, w)
	}

	// Verify that the db is still usable.
	if _, err := db.ExecContext(ctx, testutil.UpdateSingersSetLastName); err != nil {
		t.Fatalf("failed to execute statement after transaction: %v", err)
	}
}

func TestBatchUpdateAborted(t *testing.T) {
	t.Parallel()

	db, server, teardown := setupTestDBConnectionWithParams(t, "minSessions=1;maxSessions=1")
	defer teardown()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	tx, err := db.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		t.Fatalf("begin failed: %v", err)
	}
	server.TestSpanner.PutExecutionTime(testutil.MethodExecuteBatchDml, testutil.SimulatedExecutionTime{
		Errors: []error{status.Error(codes.Aborted, "Aborted")},
	})
	if _, err := tx.ExecContext(ctx, "START BATCH DML"); err != nil {
		t.Fatalf("start batch failed: %v", err)
	}
	if _, err := tx.ExecContext(ctx, testutil.UpdateBarSetFoo); err != nil {
		t.Fatalf("update failed: %v", err)
	}
	if _, err := tx.ExecContext(ctx, "RUN BATCH"); err != nil {
		t.Fatalf("run batch failed: %v", err)
	}
	err = tx.Commit()
	if err != nil {
		t.Fatalf("commit failed: %v", err)
	}
	reqs := server.TestSpanner.DrainRequestsFromServer()
	execReqs := testutil.RequestsOfType(reqs, reflect.TypeOf(&sppb.ExecuteBatchDmlRequest{}))
	if g, w := len(execReqs), 2; g != w {
		t.Fatalf("batch request count mismatch\nGot: %v\nWant: %v", g, w)
	}
	commitReqs := testutil.RequestsOfType(reqs, reflect.TypeOf(&sppb.CommitRequest{}))
	if g, w := len(commitReqs), 1; g != w {
		t.Fatalf("commit request count mismatch\nGot: %v\nWant: %v", g, w)
	}

	// Verify that the db is still usable.
	if _, err := db.ExecContext(ctx, testutil.UpdateSingersSetLastName); err != nil {
		t.Fatalf("failed to execute statement after transaction: %v", err)
	}
}

func TestQueryAborted(t *testing.T) {
	testRetryReadWriteTransactionWithQueryWithRetrySuccess(t, func(server testutil.InMemSpannerServer) {
		server.PutExecutionTime(testutil.MethodExecuteStreamingSql, testutil.SimulatedExecutionTime{
			Errors: []error{status.Error(codes.Aborted, "Aborted")},
		})
	}, codes.OK, 0, 2, 1)
}

func TestEmptyQueryAbortedTwice(t *testing.T) {
	testRetryReadWriteTransactionWithQueryWithRetrySuccess(t, func(server testutil.InMemSpannerServer) {
		_ = server.PutStatementResult(testutil.SelectFooFromBar, &testutil.StatementResult{
			Type:      testutil.StatementResultResultSet,
			ResultSet: testutil.CreateSingleColumnInt64ResultSet([]int64{}, "FOO"),
		})
		server.PutExecutionTime(testutil.MethodExecuteStreamingSql, testutil.SimulatedExecutionTime{
			Errors: []error{status.Error(codes.Aborted, "Aborted"), status.Error(codes.Aborted, "Aborted")},
		})
	}, codes.OK, -1, 3, 1)
}

func TestQueryAbortedHalfway(t *testing.T) {
	testRetryReadWriteTransactionWithQueryWithRetrySuccess(t, func(server testutil.InMemSpannerServer) {
		server.AddPartialResultSetError(testutil.SelectFooFromBar, testutil.PartialResultSetExecutionTime{
			ResumeToken: testutil.EncodeResumeToken(2),
			Err:         status.Error(codes.Aborted, "Aborted"),
		})
	}, codes.OK, 0, 2, 1)
}

func TestQueryAbortedTwice(t *testing.T) {
	testRetryReadWriteTransactionWithQueryWithRetrySuccess(t, func(server testutil.InMemSpannerServer) {
		server.AddPartialResultSetError(testutil.SelectFooFromBar, testutil.PartialResultSetExecutionTime{
			ResumeToken: testutil.EncodeResumeToken(1),
			Err:         status.Error(codes.Aborted, "Aborted"),
		})
		server.AddPartialResultSetError(testutil.SelectFooFromBar, testutil.PartialResultSetExecutionTime{
			ResumeToken: testutil.EncodeResumeToken(2),
			Err:         status.Error(codes.Aborted, "Aborted"),
		})
	}, codes.OK, 0, 3, 1)
}

func TestQuery_CommitAborted(t *testing.T) {
	testRetryReadWriteTransactionWithQueryWithRetrySuccess(t, func(server testutil.InMemSpannerServer) {
		server.PutExecutionTime(testutil.MethodCommitTransaction, testutil.SimulatedExecutionTime{
			Errors: []error{status.Error(codes.Aborted, "Aborted")},
		})
	}, codes.OK, 0, 2, 2)
}

func TestQuery_CommitAbortedTwice(t *testing.T) {
	testRetryReadWriteTransactionWithQueryWithRetrySuccess(t, func(server testutil.InMemSpannerServer) {
		server.PutExecutionTime(testutil.MethodCommitTransaction, testutil.SimulatedExecutionTime{
			Errors: []error{status.Error(codes.Aborted, "Aborted"), status.Error(codes.Aborted, "Aborted")},
		})
	}, codes.OK, 0, 3, 3)
}

func TestQueryConsumedHalfway_CommitAborted(t *testing.T) {
	testRetryReadWriteTransactionWithQueryWithRetrySuccess(t, func(server testutil.InMemSpannerServer) {
		server.PutExecutionTime(testutil.MethodCommitTransaction, testutil.SimulatedExecutionTime{
			Errors: []error{status.Error(codes.Aborted, "Aborted")},
		})
	}, codes.OK, 1, 2, 2)
}

// TestQueryConsumedHalfway_RetryContainsMoreResults_CommitAborted tests the
// following scenario:
//  1. The initial attempt returns 2 rows. The application fetches the 2 rows,
//     but does not try to fetch a 3 row. It therefore does not know that the
//     iterator only contains 2 rows and not more.
//  2. The retry attempt returns 3 rows. The retry will also only fetch the 2
//     first rows, and as the initial attempt did not know what was after these
//     2 rows the retry should succeed.
func TestQueryConsumedHalfway_RetryContainsMoreResults_CommitAborted(t *testing.T) {
	testRetryReadWriteTransactionWithQuery(t, func(server testutil.InMemSpannerServer) {
		server.PutExecutionTime(testutil.MethodCommitTransaction, testutil.SimulatedExecutionTime{
			Errors: []error{status.Error(codes.Aborted, "Aborted")},
		})
	}, codes.OK, 2, 2, 2,
		func(server testutil.InMemSpannerServer) {
			_ = server.PutStatementResult(testutil.SelectFooFromBar, &testutil.StatementResult{
				Type:      testutil.StatementResultResultSet,
				ResultSet: testutil.CreateSingleColumnInt64ResultSet([]int64{1, 2, 3}, "FOO"),
			})
		}, nil)
}

func TestQueryWithError_CommitAborted(t *testing.T) {
	testRetryReadWriteTransactionWithQueryWithRetrySuccess(t, func(server testutil.InMemSpannerServer) {
		// Let the query return a Table not found error.
		_ = server.PutStatementResult(testutil.SelectFooFromBar, &testutil.StatementResult{
			Type: testutil.StatementResultError,
			Err:  status.Errorf(codes.NotFound, "Table not found"),
		})
		server.PutExecutionTime(testutil.MethodCommitTransaction, testutil.SimulatedExecutionTime{
			Errors: []error{status.Error(codes.Aborted, "Aborted")},
		})
	}, codes.NotFound, 0, 3, 2)
}

func TestQueryWithErrorHalfway_CommitAborted(t *testing.T) {
	testRetryReadWriteTransactionWithQueryWithRetrySuccess(t, func(server testutil.InMemSpannerServer) {
		// Let the query return an internal error halfway through the stream.
		// Add the error twice so it is also returned during the retry.
		for n := 0; n < 2; n++ {
			server.AddPartialResultSetError(testutil.SelectFooFromBar, testutil.PartialResultSetExecutionTime{
				ResumeToken: testutil.EncodeResumeToken(2),
				Err:         status.Errorf(codes.Internal, "broken stream"),
			})
		}
		server.PutExecutionTime(testutil.MethodCommitTransaction, testutil.SimulatedExecutionTime{
			Errors: []error{status.Errorf(codes.Aborted, "Aborted")},
		})
	}, codes.Internal, 0, 2, 2)
}

func TestQueryAbortedWithMoreResultsDuringRetry(t *testing.T) {
	testRetryReadWriteTransactionWithQuery(t, nil, codes.OK, 0, 2, 1,
		func(server testutil.InMemSpannerServer) {
			server.PutExecutionTime(testutil.MethodCommitTransaction, testutil.SimulatedExecutionTime{
				Errors: []error{status.Errorf(codes.Aborted, "Aborted")},
			})
			// Replace the original query result with a new one with an additional row
			// before the transaction is committed. This will cause the retry to fail.
			_ = server.PutStatementResult(testutil.SelectFooFromBar, &testutil.StatementResult{
				Type:      testutil.StatementResultResultSet,
				ResultSet: testutil.CreateSingleColumnInt64ResultSet([]int64{1, 2, 3}, "FOO"),
			})
		}, ErrAbortedDueToConcurrentModification)
}

// Tests that receiving less results during the retry will cause a failed retry.
func TestQueryAbortedWithLessResultsDuringRetry(t *testing.T) {
	testRetryReadWriteTransactionWithQuery(t, nil, codes.OK, 0, 2, 1,
		func(server testutil.InMemSpannerServer) {
			server.PutExecutionTime(testutil.MethodCommitTransaction, testutil.SimulatedExecutionTime{
				Errors: []error{status.Errorf(codes.Aborted, "Aborted")},
			})
			// Replace the original query result with a new one with an additional row
			// before the transaction is committed. This will cause the retry to fail.
			_ = server.PutStatementResult(testutil.SelectFooFromBar, &testutil.StatementResult{
				Type:      testutil.StatementResultResultSet,
				ResultSet: testutil.CreateSingleColumnInt64ResultSet([]int64{1}, "FOO"),
			})
		}, ErrAbortedDueToConcurrentModification)
}

// TestQueryWithEmptyResult_CommitAborted tests the scenario where a query returns
// the same empty result set during the initial attempt and a retry.
func TestQueryWithEmptyResult_CommitAborted(t *testing.T) {
	testRetryReadWriteTransactionWithQueryWithRetrySuccess(t, func(server testutil.InMemSpannerServer) {
		// Let the query return an empty result set with only one column.
		_ = server.PutStatementResult(testutil.SelectFooFromBar, &testutil.StatementResult{
			Type:      testutil.StatementResultResultSet,
			ResultSet: testutil.CreateSingleColumnInt64ResultSet([]int64{}, "FOO"),
		})
		server.PutExecutionTime(testutil.MethodCommitTransaction, testutil.SimulatedExecutionTime{
			Errors: []error{status.Error(codes.Aborted, "Aborted")},
		})
	}, codes.OK, -1, 2, 2)
}

// TestQueryWithNewColumn_CommitAborted tests the scenario where a query returns
// the same empty result set during a retry, but with a new column.
func TestQueryWithNewColumn_CommitAborted(t *testing.T) {
	testRetryReadWriteTransactionWithQuery(t, func(server testutil.InMemSpannerServer) {
		// Let the query return an empty result set with only one column.
		_ = server.PutStatementResult(testutil.SelectFooFromBar, &testutil.StatementResult{
			Type:      testutil.StatementResultResultSet,
			ResultSet: testutil.CreateSingleColumnInt64ResultSet([]int64{}, "FOO"),
		})
		server.PutExecutionTime(testutil.MethodCommitTransaction, testutil.SimulatedExecutionTime{
			Errors: []error{status.Error(codes.Aborted, "Aborted")},
		})
	}, codes.OK, -1, 2, 1,
		func(server testutil.InMemSpannerServer) {
			// Let the query return an empty result set with two columns during the retry.
			// This should cause a retry failure.
			_ = server.PutStatementResult(testutil.SelectFooFromBar, &testutil.StatementResult{
				Type:      testutil.StatementResultResultSet,
				ResultSet: testutil.CreateTwoColumnResultSet([][2]int64{}, [2]string{"FOO", "BAR"}),
			})
		}, ErrAbortedDueToConcurrentModification)
}

// testRetryReadWriteTransactionWithQueryWithRetrySuccess tests a scenario where a
// transaction with a query is retried and the retry should succeed.
func testRetryReadWriteTransactionWithQueryWithRetrySuccess(t *testing.T, setupServer func(server testutil.InMemSpannerServer),
	wantErrCode codes.Code, numRowsToConsume int, wantSqlExecuteCount int, wantCommitCount int) {
	testRetryReadWriteTransactionWithQuery(t, setupServer, wantErrCode, numRowsToConsume, wantSqlExecuteCount, wantCommitCount, nil, nil)
}

// testRetryReadWriteTransactionWithQuery tests a scenario where a transaction with
// a query is retried. The retry should fail with the given wantCommitErr error, or
// succeed if wantCommitErr is nil.
func testRetryReadWriteTransactionWithQuery(t *testing.T, setupServer func(server testutil.InMemSpannerServer),
	wantErrCode codes.Code, numRowsToConsume int, wantSqlExecuteCount int, wantCommitCount int,
	beforeCommit func(server testutil.InMemSpannerServer), wantCommitErr error) {

	t.Parallel()

	db, server, teardown := setupTestDBConnectionWithParams(t, "minSessions=1;maxSessions=1")
	defer teardown()
	db.SetMaxOpenConns(1)

	if setupServer != nil {
		setupServer(server.TestSpanner)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
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
		if len(values) == numRowsToConsume {
			break
		}
	}
	err = rows.Err()
	if g, w := spanner.ErrCode(err), wantErrCode; g != w {
		t.Fatalf("next error mismatch\nGot: %v\nWant: %v", g, w)
	}
	if wantErrCode == codes.OK {
		if numRowsToConsume > -1 {
			if g, w := len(values), firstNonZero(numRowsToConsume, 2); g != w {
				t.Fatalf("row count mismatch\nGot: %v\nWant: %v", g, w)
			}
			wantValues := ([]int64{1, 2})[:firstNonZero(numRowsToConsume, 2)]
			if !cmp.Equal(wantValues, values) {
				t.Fatalf("values mismatch\nGot: %v\nWant: %v", values, wantValues)
			}
		}
	}
	err = rows.Close()
	if err != nil {
		t.Fatalf("closing iterator failed: %v", err)
	}
	if beforeCommit != nil {
		beforeCommit(server.TestSpanner)
	}
	err = tx.Commit()
	if err != wantCommitErr {
		t.Fatalf("commit error mismatch\nGot: %v\nWant: %v", err, wantCommitErr)
	}
	reqs := server.TestSpanner.DrainRequestsFromServer()
	execReqs := testutil.RequestsOfType(reqs, reflect.TypeOf(&sppb.ExecuteSqlRequest{}))
	if g, w := len(execReqs), wantSqlExecuteCount; g != w {
		t.Fatalf("execute request count mismatch\nGot: %v\nWant: %v", g, w)
	}
	commitReqs := testutil.RequestsOfType(reqs, reflect.TypeOf(&sppb.CommitRequest{}))
	if g, w := len(commitReqs), wantCommitCount; g != w {
		t.Fatalf("commit request count mismatch\nGot: %v\nWant: %v", g, w)
	}

	// Execute another statement to ensure that the session that was used
	// has been returned to the pool.
	if _, err := db.ExecContext(ctx, testutil.UpdateSingersSetLastName); err != nil {
		t.Fatalf("failed to execute statement after transaction: %v", err)
	}
}

// Tests that a query that is aborted halfway the stream will be retried,
// but that the retry will fail if the results in the part of the result
// set that has already been seen will fail the retry.
func TestQueryAbortedHalfway_WithDifferentResultsInFirstHalf(t *testing.T) {
	t.Parallel()

	db, server, teardown := setupTestDBConnectionWithParams(t, "minSessions=1;maxSessions=1")
	defer teardown()
	db.SetMaxOpenConns(1)
	// Ensure that the second call to Next() will fail with an Aborted error.
	server.TestSpanner.AddPartialResultSetError(testutil.SelectFooFromBar, testutil.PartialResultSetExecutionTime{
		ResumeToken: testutil.EncodeResumeToken(2),
		Err:         status.Error(codes.Aborted, "Aborted"),
	})

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	tx, err := db.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		t.Fatalf("begin failed: %v", err)
	}
	rows, err := tx.QueryContext(ctx, testutil.SelectFooFromBar)
	if err != nil {
		t.Fatalf("query failed: %v", err)
	}

	// The first row should succeed.
	next := rows.Next()
	if !next {
		t.Fatalf("next result mismatch\nGot: %v\nWant: %v", next, true)
	}
	err = rows.Err()
	if err != nil {
		t.Fatalf("next failed: %v", err)
	}
	var val int64
	err = rows.Scan(&val)
	if err != nil {
		t.Fatalf("scan failed: %v", err)
	}
	if g, w := val, int64(1); g != w {
		t.Fatalf("value mismatch\nGot: %v\nWant: %v", g, w)
	}
	// Replace the original query result with a new one with a different value
	// for the first row. This should cause the transaction to fail with an
	// ErrAbortedDueToConcurrentModification error.
	_ = server.TestSpanner.PutStatementResult(testutil.SelectFooFromBar, &testutil.StatementResult{
		Type:      testutil.StatementResultResultSet,
		ResultSet: testutil.CreateSingleColumnInt64ResultSet([]int64{2, 2}, "FOO"),
	})

	// This should now fail with an ErrAbortedDueToConcurrentModification error.
	next = rows.Next()
	if next {
		t.Fatalf("next result mismatch\nGot: %v\nWant: %v", next, false)
	}
	if g, w := rows.Err(), ErrAbortedDueToConcurrentModification; g != w {
		t.Fatalf("next error mismatch\nGot: %v\nWant: %v", g, w)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatalf("failed to rollback transaction: %v", err)
	}

	reqs := server.TestSpanner.DrainRequestsFromServer()
	execReqs := testutil.RequestsOfType(reqs, reflect.TypeOf(&sppb.ExecuteSqlRequest{}))
	if g, w := len(execReqs), 2; g != w {
		t.Fatalf("execute request count mismatch\nGot: %v\nWant: %v", g, w)
	}

	// Verify that the db is still usable.
	if _, err := db.ExecContext(ctx, testutil.UpdateSingersSetLastName); err != nil {
		t.Fatalf("failed to execute statement after transaction: %v", err)
	}
}

// Tests that a change in the results of a result set that has not yet been seen
// by the user does not cause a retry failure.
func TestQueryAbortedHalfway_WithDifferentResultsInSecondHalf(t *testing.T) {
	t.Parallel()

	db, server, teardown := setupTestDBConnectionWithParams(t, "minSessions=1;maxSessions=1")
	defer teardown()
	db.SetMaxOpenConns(1)
	// Ensure that the second call to Next() will fail with an Aborted error.
	server.TestSpanner.AddPartialResultSetError(testutil.SelectFooFromBar, testutil.PartialResultSetExecutionTime{
		ResumeToken: testutil.EncodeResumeToken(2),
		Err:         status.Error(codes.Aborted, "Aborted"),
	})

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	tx, err := db.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		t.Fatalf("begin failed: %v", err)
	}
	rows, err := tx.QueryContext(ctx, testutil.SelectFooFromBar)
	if err != nil {
		t.Fatalf("query failed: %v", err)
	}

	// The first row should succeed.
	next := rows.Next()
	if !next {
		t.Fatalf("next result mismatch\nGot: %v\nWant: %v", next, true)
	}
	err = rows.Err()
	if err != nil {
		t.Fatalf("next failed: %v", err)
	}
	var val int64
	err = rows.Scan(&val)
	if err != nil {
		t.Fatalf("scan failed: %v", err)
	}
	if g, w := val, int64(1); g != w {
		t.Fatalf("value mismatch\nGot: %v\nWant: %v", g, w)
	}
	// Replace the original query result with a new one with a different value
	// for the second row. This should not cause the transaction to fail with an
	// ErrAbortedDueToConcurrentModification error as the result has not yet
	// been seen by the user.
	_ = server.TestSpanner.PutStatementResult(testutil.SelectFooFromBar, &testutil.StatementResult{
		Type:      testutil.StatementResultResultSet,
		ResultSet: testutil.CreateSingleColumnInt64ResultSet([]int64{1, 3}, "FOO"),
	})

	// This should succeed and return the new result.
	next = rows.Next()
	if !next {
		t.Fatalf("next result mismatch\nGot: %v\nWant: %v", next, true)
	}
	err = rows.Err()
	if err != nil {
		t.Fatalf("next failed: %v", err)
	}
	err = rows.Scan(&val)
	if err != nil {
		t.Fatalf("scan failed: %v", err)
	}
	if g, w := val, int64(3); g != w {
		t.Fatalf("value mismatch\nGot: %v\nWant: %v", g, w)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("failed to commit transaction: %v", err)
	}

	reqs := server.TestSpanner.DrainRequestsFromServer()
	execReqs := testutil.RequestsOfType(reqs, reflect.TypeOf(&sppb.ExecuteSqlRequest{}))
	if g, w := len(execReqs), 2; g != w {
		t.Fatalf("execute request count mismatch\nGot: %v\nWant: %v", g, w)
	}

	// Verify that the db is still usable.
	if _, err := db.ExecContext(ctx, testutil.UpdateSingersSetLastName); err != nil {
		t.Fatalf("failed to execute statement after transaction: %v", err)
	}
}

func TestSecondUpdateAborted(t *testing.T) {
	t.Parallel()

	db, server, teardown := setupTestDBConnectionWithParams(t, "minSessions=1;maxSessions=1")
	defer teardown()
	db.SetMaxOpenConns(1)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	tx, err := db.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		t.Fatalf("begin failed: %v", err)
	}
	// This statement should succeed and is not aborted.
	_, err = tx.ExecContext(ctx, testutil.UpdateSingersSetLastName)
	if err != nil {
		t.Fatalf("update singers failed: %v", err)
	}

	server.TestSpanner.PutExecutionTime(testutil.MethodExecuteStreamingSql, testutil.SimulatedExecutionTime{
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
	reqs := server.TestSpanner.DrainRequestsFromServer()
	execReqs := testutil.RequestsOfType(reqs, reflect.TypeOf(&sppb.ExecuteSqlRequest{}))
	// The server should receive 4 execute statements, as each update statement should
	// be executed twice.
	if g, w := len(execReqs), 4; g != w {
		t.Fatalf("execute request count mismatch\nGot: %v\nWant: %v", g, w)
	}
	commitReqs := testutil.RequestsOfType(reqs, reflect.TypeOf(&sppb.CommitRequest{}))
	if g, w := len(commitReqs), 1; g != w {
		t.Fatalf("commit request count mismatch\nGot: %v\nWant: %v", g, w)
	}

	// Verify that the db is still usable.
	if _, err := db.ExecContext(ctx, testutil.UpdateSingersSetLastName); err != nil {
		t.Fatalf("failed to execute statement after transaction: %v", err)
	}
}

func TestSecondBatchUpdateAborted(t *testing.T) {
	t.Parallel()

	db, server, teardown := setupTestDBConnectionWithParams(t, "minSessions=1;maxSessions=1")
	defer teardown()
	db.SetMaxOpenConns(1)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	tx, err := db.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		t.Fatalf("begin failed: %v", err)
	}
	if _, err := tx.ExecContext(ctx, "START BATCH DML"); err != nil {
		t.Fatalf("failed to start batch: %v", err)
	}
	if _, err := tx.ExecContext(ctx, testutil.UpdateSingersSetLastName); err != nil {
		t.Fatalf("update singers failed: %v", err)
	}
	if _, err := tx.ExecContext(ctx, "RUN BATCH"); err != nil {
		t.Fatalf("failed to run batch: %v", err)
	}

	server.TestSpanner.PutExecutionTime(testutil.MethodExecuteBatchDml, testutil.SimulatedExecutionTime{
		Errors: []error{status.Error(codes.Aborted, "Aborted")},
	})
	if _, err := tx.ExecContext(ctx, "START BATCH DML"); err != nil {
		t.Fatalf("failed to start batch: %v", err)
	}
	if _, err := tx.ExecContext(ctx, testutil.UpdateBarSetFoo); err != nil {
		t.Fatalf("update bar failed: %v", err)
	}
	// This statement will return Aborted, the transaction will be retried internally and the statement is
	// then executed once more and should return the correct value.
	if _, err := tx.ExecContext(ctx, "RUN BATCH"); err != nil {
		t.Fatalf("failed to run batch: %v", err)
	}

	if err := tx.Commit(); err != nil {
		t.Fatalf("commit failed: %v", err)
	}
	reqs := server.TestSpanner.DrainRequestsFromServer()
	execReqs := testutil.RequestsOfType(reqs, reflect.TypeOf(&sppb.ExecuteBatchDmlRequest{}))
	// The server should receive 4 batch statements, as each update statement should
	// be executed twice.
	if g, w := len(execReqs), 4; g != w {
		t.Fatalf("batch request count mismatch\nGot: %v\nWant: %v", g, w)
	}
	commitReqs := testutil.RequestsOfType(reqs, reflect.TypeOf(&sppb.CommitRequest{}))
	if g, w := len(commitReqs), 1; g != w {
		t.Fatalf("commit request count mismatch\nGot: %v\nWant: %v", g, w)
	}

	// Verify that the db is still usable.
	if _, err := db.ExecContext(ctx, testutil.UpdateSingersSetLastName); err != nil {
		t.Fatalf("failed to execute statement after transaction: %v", err)
	}
}

func TestSecondUpdateAborted_FirstStatementWithSameError(t *testing.T) {
	t.Parallel()

	db, server, teardown := setupTestDBConnectionWithParams(t, "minSessions=1;maxSessions=1")
	defer teardown()
	db.SetMaxOpenConns(1)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	tx, err := db.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		t.Fatalf("begin failed: %v", err)
	}
	_ = server.TestSpanner.PutStatementResult(testutil.UpdateSingersSetLastName, &testutil.StatementResult{
		Type: testutil.StatementResultError,
		Err:  status.Error(codes.NotFound, "Table not found"),
	})
	// This statement should fail with NotFound. That will also be the result
	// during the retry.
	_, err = tx.ExecContext(ctx, testutil.UpdateSingersSetLastName)
	if spanner.ErrCode(err) != codes.NotFound {
		t.Fatalf("error code mismatch\nGot: %v\nWant: %v", spanner.ErrCode(err), codes.NotFound)
	}

	server.TestSpanner.PutExecutionTime(testutil.MethodExecuteStreamingSql, testutil.SimulatedExecutionTime{
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
	reqs := server.TestSpanner.DrainRequestsFromServer()
	execReqs := testutil.RequestsOfType(reqs, reflect.TypeOf(&sppb.ExecuteSqlRequest{}))
	// The server should receive 4 execute statements, as each update statement should
	// be executed twice.
	if g, w := len(execReqs), 4; g != w {
		t.Fatalf("execute request count mismatch\nGot: %v\nWant: %v", g, w)
	}
	commitReqs := testutil.RequestsOfType(reqs, reflect.TypeOf(&sppb.CommitRequest{}))
	if g, w := len(commitReqs), 1; g != w {
		t.Fatalf("commit request count mismatch\nGot: %v\nWant: %v", g, w)
	}

	// Verify that the db is still usable.
	if _, err := db.ExecContext(ctx, testutil.UpdateBarSetFoo); err != nil {
		t.Fatalf("failed to execute statement after transaction: %v", err)
	}
}

func TestSecondUpdateAborted_FirstResultUpdateCountChanged(t *testing.T) {
	testSecondUpdateAborted_FirstResultChanged(t, nil, &testutil.StatementResult{
		Type:        testutil.StatementResultUpdateCount,
		UpdateCount: 0,
	})
}

func TestSecondUpdateAborted_FirstResultFromSuccessToError(t *testing.T) {
	// Simulate that the table has been deleted after the first attempt.
	testSecondUpdateAborted_FirstResultChanged(t, nil, &testutil.StatementResult{
		Type: testutil.StatementResultError,
		Err:  status.Error(codes.NotFound, "Table not found"),
	})
}

func TestSecondUpdateAborted_FirstResultFromErrorToSuccess(t *testing.T) {
	// Simulate that the table has been created after the first attempt.
	testSecondUpdateAborted_FirstResultChanged(t, &testutil.StatementResult{
		Type: testutil.StatementResultError,
		Err:  status.Error(codes.NotFound, "Table not found"),
	}, &testutil.StatementResult{
		Type:        testutil.StatementResultUpdateCount,
		UpdateCount: 1,
	})
}

func TestSecondUpdateAborted_FirstResultFromErrorToOtherError(t *testing.T) {
	// Simulate that the table has been created after the first attempt, but that
	// the user has no permission for the table.
	testSecondUpdateAborted_FirstResultChanged(t, &testutil.StatementResult{
		Type: testutil.StatementResultError,
		Err:  status.Error(codes.NotFound, "Table not found"),
	}, &testutil.StatementResult{
		Type: testutil.StatementResultError,
		Err:  status.Error(codes.PermissionDenied, "No permission for table"),
	})
}

func testSecondUpdateAborted_FirstResultChanged(t *testing.T, firstResult *testutil.StatementResult, secondResult *testutil.StatementResult) {
	t.Parallel()

	db, server, teardown := setupTestDBConnectionWithParams(t, "minSessions=1;maxSessions=1")
	defer teardown()
	if firstResult != nil {
		_ = server.TestSpanner.PutStatementResult(testutil.UpdateSingersSetLastName, firstResult)
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	tx, err := db.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		t.Fatalf("begin failed: %v", err)
	}
	// Ignore the result, as all we care about is whether the retry fails because
	// the result is different during the retry.
	_, _ = tx.ExecContext(ctx, testutil.UpdateSingersSetLastName)
	// Update the result to simulate a different result during the retry.
	_ = server.TestSpanner.PutStatementResult(testutil.UpdateSingersSetLastName, secondResult)

	server.TestSpanner.PutExecutionTime(testutil.MethodExecuteStreamingSql, testutil.SimulatedExecutionTime{
		Errors: []error{status.Error(codes.Aborted, "Aborted")},
	})
	// This statement will return Aborted and the transaction will be retried internally. That
	// retry will fail because the result of the first statement is different during the retry.
	_, err = tx.ExecContext(ctx, testutil.UpdateBarSetFoo)
	if err != ErrAbortedDueToConcurrentModification {
		t.Fatalf("update error mismatch\nGot: %v\nWant: %v", err, ErrAbortedDueToConcurrentModification)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatalf("failed to rollback transaction: %v", err)
	}
	reqs := server.TestSpanner.DrainRequestsFromServer()
	execReqs := testutil.RequestsOfType(reqs, reflect.TypeOf(&sppb.ExecuteSqlRequest{}))
	// The server should receive 3 execute statements, as only the first statement is retried.
	if g, w := len(execReqs), 3; g != w {
		t.Fatalf("execute request count mismatch\nGot: %v\nWant: %v", g, w)
	}

	// Verify that the db is still usable.
	if _, err := db.ExecContext(ctx, testutil.UpdateBarSetFoo); err != nil {
		t.Fatalf("failed to execute statement after transaction: %v", err)
	}
}

func TestBatchUpdateAbortedWithError(t *testing.T) {
	t.Parallel()

	db, server, teardown := setupTestDBConnectionWithParams(t, "minSessions=1;maxSessions=1")
	defer teardown()
	db.SetMaxOpenConns(1)

	// Make sure that one of the DML statements will return an error.
	_ = server.TestSpanner.PutStatementResult(testutil.UpdateSingersSetLastName, &testutil.StatementResult{
		Type: testutil.StatementResultError,
		Err:  status.Error(codes.NotFound, "Table not found"),
	})

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	tx, err := db.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		t.Fatalf("begin failed: %v", err)
	}
	if _, err := tx.ExecContext(ctx, "START BATCH DML"); err != nil {
		t.Fatalf("failed to start batch: %v", err)
	}
	if _, err := tx.ExecContext(ctx, testutil.UpdateBarSetFoo); err != nil {
		t.Fatalf("dml statement failed: %v", err)
	}
	// This statement should fail with NotFound when the batch is executed.
	// That will also be the result during the retry. Note that the error
	// will not be returned now, but when the batch is executed.
	if _, err = tx.ExecContext(ctx, testutil.UpdateSingersSetLastName); err != nil {
		t.Fatalf("dml statement failed: %v", err)
	}
	// Note that even though Spanner returns the row count for a Batch DML that
	// fails halfway, go/sql does not do that, so result will be nil for batch
	// statements that fail. Internally, the driver still keeps track of the row
	// counts that were returned, and uses that in the retry strategy.
	if _, err := tx.ExecContext(ctx, "RUN BATCH"); spanner.ErrCode(err) != codes.NotFound {
		t.Fatalf("error code mismatch\nGot: %v\nWant: %v", spanner.ErrCode(err), codes.NotFound)
	}

	// Abort the transaction. The internal retry should succeed as the same error
	// and the same row count is returned during the retry.
	server.TestSpanner.PutExecutionTime(testutil.MethodCommitTransaction, testutil.SimulatedExecutionTime{
		Errors: []error{status.Error(codes.Aborted, "Aborted")},
	})
	err = tx.Commit()
	if err != nil {
		t.Fatalf("commit failed: %v", err)
	}
	reqs := server.TestSpanner.DrainRequestsFromServer()
	execReqs := testutil.RequestsOfType(reqs, reflect.TypeOf(&sppb.ExecuteBatchDmlRequest{}))
	if g, w := len(execReqs), 2; g != w {
		t.Fatalf("batch request count mismatch\nGot: %v\nWant: %v", g, w)
	}
	commitReqs := testutil.RequestsOfType(reqs, reflect.TypeOf(&sppb.CommitRequest{}))
	if g, w := len(commitReqs), 2; g != w {
		t.Fatalf("commit request count mismatch\nGot: %v\nWant: %v", g, w)
	}

	// Verify that the db is still usable.
	if _, err := db.ExecContext(ctx, testutil.UpdateBarSetFoo); err != nil {
		t.Fatalf("failed to execute statement after transaction: %v", err)
	}
}

func TestBatchUpdateAbortedWithError_DifferentRowCountDuringRetry(t *testing.T) {
	t.Parallel()

	db, server, teardown := setupTestDBConnectionWithParams(t, "minSessions=1;maxSessions=1")
	defer teardown()
	db.SetMaxOpenConns(1)

	// Make sure that one of the DML statements will return an error.
	_ = server.TestSpanner.PutStatementResult(testutil.UpdateSingersSetLastName, &testutil.StatementResult{
		Type: testutil.StatementResultError,
		Err:  status.Error(codes.NotFound, "Table not found"),
	})

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	tx, err := db.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		t.Fatalf("begin failed: %v", err)
	}
	if _, err := tx.ExecContext(ctx, "START BATCH DML"); err != nil {
		t.Fatalf("failed to start batch: %v", err)
	}
	if _, err := tx.ExecContext(ctx, testutil.UpdateBarSetFoo); err != nil {
		t.Fatalf("dml statement failed: %v", err)
	}
	// This statement should fail with NotFound when the batch is executed.
	// That will also be the result during the retry. Note that the error
	// will not be returned now, but when the batch is executed.
	if _, err = tx.ExecContext(ctx, testutil.UpdateSingersSetLastName); err != nil {
		t.Fatalf("dml statement failed: %v", err)
	}
	if _, err := tx.ExecContext(ctx, "RUN BATCH"); spanner.ErrCode(err) != codes.NotFound {
		t.Fatalf("error code mismatch\nGot: %v\nWant: %v", spanner.ErrCode(err), codes.NotFound)
	}

	// Change the returned row count of the first DML statement. This will cause
	// the retry to fail.
	_ = server.TestSpanner.PutStatementResult(testutil.UpdateBarSetFoo, &testutil.StatementResult{
		Type:        testutil.StatementResultUpdateCount,
		UpdateCount: testutil.UpdateBarSetFooRowCount + 1,
	})
	server.TestSpanner.PutExecutionTime(testutil.MethodCommitTransaction, testutil.SimulatedExecutionTime{
		Errors: []error{status.Error(codes.Aborted, "Aborted")},
	})
	err = tx.Commit()
	if err != ErrAbortedDueToConcurrentModification {
		t.Fatalf("commit error mismatch\nGot: %v\nWant: %v", err, ErrAbortedDueToConcurrentModification)
	}
	reqs := server.TestSpanner.DrainRequestsFromServer()
	execReqs := testutil.RequestsOfType(reqs, reflect.TypeOf(&sppb.ExecuteBatchDmlRequest{}))
	if g, w := len(execReqs), 2; g != w {
		t.Fatalf("batch request count mismatch\nGot: %v\nWant: %v", g, w)
	}
	commitReqs := testutil.RequestsOfType(reqs, reflect.TypeOf(&sppb.CommitRequest{}))
	// The commit should be attempted only once.
	if g, w := len(commitReqs), 1; g != w {
		t.Fatalf("commit request count mismatch\nGot: %v\nWant: %v", g, w)
	}

	// Verify that the db is still usable.
	if _, err := db.ExecContext(ctx, testutil.UpdateBarSetFoo); err != nil {
		t.Fatalf("failed to execute statement after transaction: %v", err)
	}
}

func TestBatchUpdateAbortedWithError_DifferentErrorDuringRetry(t *testing.T) {
	t.Parallel()

	db, server, teardown := setupTestDBConnectionWithParams(t, "minSessions=1;maxSessions=1")
	defer teardown()
	db.SetMaxOpenConns(1)

	// Make sure that one of the DML statements will return an error.
	_ = server.TestSpanner.PutStatementResult(testutil.UpdateSingersSetLastName, &testutil.StatementResult{
		Type: testutil.StatementResultError,
		Err:  status.Error(codes.NotFound, "Table not found"),
	})

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	tx, err := db.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		t.Fatalf("begin failed: %v", err)
	}
	if _, err := tx.ExecContext(ctx, "START BATCH DML"); err != nil {
		t.Fatalf("failed to start batch: %v", err)
	}
	if _, err = tx.ExecContext(ctx, testutil.UpdateSingersSetLastName); err != nil {
		t.Fatalf("dml statement failed: %v", err)
	}
	if _, err := tx.ExecContext(ctx, "RUN BATCH"); spanner.ErrCode(err) != codes.NotFound {
		t.Fatalf("error code mismatch\n Got: %v\nWant: %v", spanner.ErrCode(err), codes.NotFound)
	}

	// Remove the error for the DML statement and cause a retry. The missing
	// error for the DML statement should fail the retry.
	_ = server.TestSpanner.PutStatementResult(testutil.UpdateSingersSetLastName, &testutil.StatementResult{
		Type:        testutil.StatementResultUpdateCount,
		UpdateCount: testutil.UpdateSingersSetLastNameRowCount,
	})
	server.TestSpanner.PutExecutionTime(testutil.MethodCommitTransaction, testutil.SimulatedExecutionTime{
		Errors: []error{status.Error(codes.Aborted, "Aborted")},
	})
	err = tx.Commit()
	if err != ErrAbortedDueToConcurrentModification {
		t.Fatalf("commit error mismatch\n Got: %v\nWant: %v", err, ErrAbortedDueToConcurrentModification)
	}
	reqs := server.TestSpanner.DrainRequestsFromServer()
	execReqs := testutil.RequestsOfType(reqs, reflect.TypeOf(&sppb.ExecuteBatchDmlRequest{}))
	// There are 3 ExecuteBatchDmlRequests sent to Spanner:
	// 1. An initial attempt with a BeginTransaction RPC, but this returns a NotFound error.
	//    This causes the transaction to be retried with an explicit BeginTransaction request.
	// 2. Another attempt with a transaction ID.
	// 3. A third attempt after the initial transaction is aborted.
	if g, w := len(execReqs), 3; g != w {
		t.Fatalf("batch request count mismatch\n Got: %v\nWant: %v", g, w)
	}
	commitReqs := testutil.RequestsOfType(reqs, reflect.TypeOf(&sppb.CommitRequest{}))
	// The commit should be attempted only once.
	if g, w := len(commitReqs), 1; g != w {
		t.Fatalf("commit request count mismatch\n Got: %v\nWant: %v", g, w)
	}
	// The first ExecuteBatchDml request should try to use an inline-begin.
	// After that, we should have two BeginTransaction requests.
	req1 := execReqs[0].(*sppb.ExecuteBatchDmlRequest)
	if req1.GetTransaction() == nil || req1.GetTransaction().GetBegin() == nil {
		t.Fatal("the first ExecuteBatchDmlRequest should have a BeginTransaction")
	}
	req2 := execReqs[1].(*sppb.ExecuteBatchDmlRequest)
	if req2.GetTransaction() == nil || req2.GetTransaction().GetId() == nil {
		t.Fatal("the second ExecuteBatchDmlRequest should have a transaction id")
	}
	beginRequests := testutil.RequestsOfType(reqs, reflect.TypeOf(&sppb.BeginTransactionRequest{}))
	if g, w := len(beginRequests), 2; g != w {
		t.Fatalf("begin request count mismatch\n Got: %v\nWant: %v", g, w)
	}
	// Verify that the db is still usable.
	if _, err := db.ExecContext(ctx, testutil.UpdateSingersSetLastName); err != nil {
		t.Fatalf("failed to execute statement after transaction: %v", err)
	}
}

func TestLastInsertId(t *testing.T) {
	t.Parallel()

	db, server, teardown := setupTestDBConnectionWithParams(t, "minSessions=1;maxSessions=1")
	defer teardown()
	db.SetMaxOpenConns(1)

	query := "insert into singers (name) values ('foo') then return id"
	_ = server.TestSpanner.PutStatementResult(query, &testutil.StatementResult{
		Type:        testutil.StatementResultResultSet,
		ResultSet:   testutil.CreateSingleColumnInt64ResultSet([]int64{1}, "id"),
		UpdateCount: 1,
	})

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	tx, err := db.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		t.Fatalf("begin failed: %v", err)
	}
	if res, err := tx.ExecContext(ctx, query); err != nil {
		t.Fatalf("failed to execute statement: %v", err)
	} else {
		if id, err := res.LastInsertId(); err != nil {
			t.Fatalf("failed to get last insert id: %v", err)
		} else if g, w := id, int64(1); g != w {
			t.Fatalf("last insert id mismatch\n Got: %v\nWant: %v", g, w)
		}
		if c, err := res.RowsAffected(); err != nil {
			t.Fatalf("failed to get update count: %v", err)
		} else if g, w := c, int64(1); g != w {
			t.Fatalf("update count mismatch\n Got: %v\nWant: %v", g, w)
		}
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("failed to commit: %v", err)
	}

	// Verify that the db is still usable.
	if _, err := db.ExecContext(ctx, testutil.UpdateSingersSetLastName); err != nil {
		t.Fatalf("failed to execute statement after transaction: %v", err)
	}
}

func TestLastInsertIdChanged(t *testing.T) {
	t.Parallel()

	db, server, teardown := setupTestDBConnectionWithParams(t, "minSessions=1;maxSessions=1")
	defer teardown()
	db.SetMaxOpenConns(1)

	query := "insert into singers (name) values ('foo') then return id"
	_ = server.TestSpanner.PutStatementResult(query, &testutil.StatementResult{
		Type:        testutil.StatementResultResultSet,
		ResultSet:   testutil.CreateSingleColumnInt64ResultSet([]int64{1}, "id"),
		UpdateCount: 1,
	})

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	tx, err := db.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		t.Fatalf("begin failed: %v", err)
	}
	if res, err := tx.ExecContext(ctx, query); err != nil {
		t.Fatalf("failed to execute statement: %v", err)
	} else {
		if id, err := res.LastInsertId(); err != nil {
			t.Fatalf("failed to get last insert id: %v", err)
		} else if g, w := id, int64(1); g != w {
			t.Fatalf("last insert id mismatch\n Got: %v\nWant: %v", g, w)
		}
		if c, err := res.RowsAffected(); err != nil {
			t.Fatalf("failed to get update count: %v", err)
		} else if g, w := c, int64(1); g != w {
			t.Fatalf("update count mismatch\n Got: %v\nWant: %v", g, w)
		}
	}
	// Abort the transaction and change the returned ID.
	server.TestSpanner.PutExecutionTime(testutil.MethodCommitTransaction, testutil.SimulatedExecutionTime{
		Errors: []error{status.Error(codes.Aborted, "Aborted")},
	})
	_ = server.TestSpanner.PutStatementResult(query, &testutil.StatementResult{
		Type:        testutil.StatementResultResultSet,
		ResultSet:   testutil.CreateSingleColumnInt64ResultSet([]int64{2}, "id"),
		UpdateCount: 1,
	})
	if err := tx.Commit(); err != ErrAbortedDueToConcurrentModification {
		t.Fatalf("commit error mismatch\n Got: %v\nWant: %v", err, ErrAbortedDueToConcurrentModification)
	}

	// Verify that the db is still usable.
	if _, err := db.ExecContext(ctx, testutil.UpdateSingersSetLastName); err != nil {
		t.Fatalf("failed to execute statement after transaction: %v", err)
	}
}

func firstNonZero(values ...int) int {
	for _, v := range values {
		if v > 0 {
			return v
		}
	}
	return 0
}
