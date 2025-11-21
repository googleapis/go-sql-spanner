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
	"errors"
	"fmt"
	"reflect"
	"testing"
	"time"

	"cloud.google.com/go/longrunning/autogen/longrunningpb"
	"cloud.google.com/go/spanner"
	"cloud.google.com/go/spanner/admin/database/apiv1/databasepb"
	sppb "cloud.google.com/go/spanner/apiv1/spannerpb"
	"github.com/google/uuid"
	"github.com/googleapis/go-sql-spanner/testutil"
	pbstatus "google.golang.org/genproto/googleapis/rpc/status"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestQueryMultiple(t *testing.T) {
	t.Parallel()

	db, server, teardown := setupTestDBConnection(t)
	defer teardown()
	ctx := context.Background()

	r, err := db.QueryContext(ctx, fmt.Sprintf("%s;%s", testutil.SelectFooFromBar, testutil.SelectFooFromBar))
	if err != nil {
		t.Fatal(err)
	}
	defer silentClose(r)

	for v := range 2 {
		if !r.Next() {
			t.Fatal("r.Next returned false")
		}
		var value int64
		if err := r.Scan(&value); err != nil {
			t.Fatal(err)
		}
		if g, w := value, int64(v+1); g != w {
			t.Fatalf("value mismatch\n Got: %v\nWant: %v", g, w)
		}
	}
	if r.Next() {
		t.Fatal("r.Next returned true")
	}
	if !r.NextResultSet() {
		t.Fatal("r.NextResultSet returned false")
	}
	for v := range 2 {
		if !r.Next() {
			t.Fatal("r.Next returned false")
		}
		var value int64
		if err := r.Scan(&value); err != nil {
			t.Fatal(err)
		}
		if g, w := value, int64(v+1); g != w {
			t.Fatalf("value mismatch\n Got: %v\nWant: %v", g, w)
		}
	}
	if r.Next() {
		t.Fatal("r.Next returned true")
	}
	if r.NextResultSet() {
		t.Fatal("r.NextResultSet returned true")
	}

	// A batch of only queries should use a read-only transaction.
	requests := server.TestSpanner.DrainRequestsFromServer()
	executeRequests := testutil.RequestsOfType(requests, reflect.TypeOf(&sppb.ExecuteSqlRequest{}))
	if g, w := len(executeRequests), 2; g != w {
		t.Fatalf("number of execute requests mismatch\n Got: %v\nWant: %v", g, w)
	}
	request := executeRequests[0].(*sppb.ExecuteSqlRequest)
	if request.GetTransaction() == nil || request.GetTransaction().GetBegin() == nil || request.GetTransaction().GetBegin().GetReadOnly() == nil {
		t.Fatal("expected begin read-only transaction")
	}
	request = executeRequests[1].(*sppb.ExecuteSqlRequest)
	if request.GetTransaction() == nil || request.GetTransaction().GetId() == nil {
		t.Fatal("expected transaction ID")
	}

	// Read-only transactions are not committed or rolled back.
	commitRequests := testutil.RequestsOfType(requests, reflect.TypeOf(&sppb.CommitRequest{}))
	if g, w := len(commitRequests), 0; g != w {
		t.Fatalf("number of commit requests mismatch\n Got: %v\nWant: %v", g, w)
	}
	rollbackRequests := testutil.RequestsOfType(requests, reflect.TypeOf(&sppb.RollbackRequest{}))
	if g, w := len(rollbackRequests), 0; g != w {
		t.Fatalf("number of rollback requests mismatch\n Got: %v\nWant: %v", g, w)
	}
}

func TestQueryMultipleWithTransaction(t *testing.T) {
	t.Parallel()

	db, server, teardown := setupTestDBConnection(t)
	defer teardown()
	ctx := context.Background()

	// Execute a complete transaction as a single SQL string.
	r, err := db.QueryContext(ctx,
		fmt.Sprintf("begin;%s;%s;commit", testutil.SelectFooFromBar, testutil.UpdateBarSetFoo))
	if err != nil {
		t.Fatal(err)
	}
	defer silentClose(r)

	// There should be no rows for 'begin'.
	if r.Next() {
		t.Fatal("r.Next returned true for 'begin'")
	}

	// Get results for the query.
	if !r.NextResultSet() {
		t.Fatal("r.NextResultSet returned false for query")
	}
	// Ensure that we get 2 rows.
	for v := range 2 {
		if !r.Next() {
			t.Fatal("r.Next returned false")
		}
		var value int64
		if err := r.Scan(&value); err != nil {
			t.Fatal(err)
		}
		if g, w := value, int64(v+1); g != w {
			t.Fatalf("value mismatch\n Got: %v\nWant: %v", g, w)
		}
	}
	if r.Next() {
		t.Fatal("r.Next returned true")
	}

	// Get results for the update statement.
	if !r.NextResultSet() {
		t.Fatal("r.NextResultSet returned false for update statement")
	}
	// The update statement should not return any rows.
	if r.Next() {
		t.Fatal("r.Next returned true for update statement")
	}

	// Get the results for 'commit'.
	if !r.NextResultSet() {
		t.Fatal("r.NextResultSet returned false for 'commit'")
	}
	// The commit statement should not return any rows.
	if r.Next() {
		t.Fatal("r.Next returned true for update statement")
	}

	// There should be no more results.
	if r.NextResultSet() {
		t.Fatal("r.Next returned true after commit")
	}

	// Verify that the mock server received a read/write transaction consisting of two queries.
	requests := server.TestSpanner.DrainRequestsFromServer()
	executeRequests := testutil.RequestsOfType(requests, reflect.TypeOf(&sppb.ExecuteSqlRequest{}))
	if g, w := len(executeRequests), 2; g != w {
		t.Fatalf("num execute requests mismatch\n Got: %v\nWant: %v", g, w)
	}
	first := executeRequests[0].(*sppb.ExecuteSqlRequest)
	second := executeRequests[1].(*sppb.ExecuteSqlRequest)
	if first.Transaction == nil || first.Transaction.GetBegin() == nil || first.Transaction.GetBegin().GetReadWrite() == nil {
		t.Fatal("missing BeginTransaction on first request")
	}
	if second.Transaction == nil || second.Transaction.GetId() == nil {
		t.Fatal("missing TransactionId on second request")
	}
	commitRequests := testutil.RequestsOfType(requests, reflect.TypeOf(&sppb.CommitRequest{}))
	if g, w := len(commitRequests), 1; g != w {
		t.Fatalf("num commit requests mismatch\n Got: %v\nWant: %v", g, w)
	}
}

func TestQueryMultipleExistingTransaction(t *testing.T) {
	t.Parallel()

	db, server, teardown := setupTestDBConnection(t)
	defer teardown()
	ctx := context.Background()

	// Execute a multi-statement SQL string on a transaction that has been created using the database/sql API.
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	// Note that it is OK to use DML statements with QueryContext. The database/sql API however does not provide any
	// way to get the update count.
	it, err := tx.QueryContext(ctx, fmt.Sprintf("%s;%s", testutil.SelectFooFromBar, testutil.UpdateBarSetFoo))
	if err != nil {
		t.Fatal(err)
	}
	for range 2 {
		if !it.Next() {
			t.Fatal("it.Next returned false")
		}
	}
	if it.Next() {
		t.Fatal("it.Next returned true")
	}
	if !it.NextResultSet() {
		t.Fatal("it.NextResultSet returned false")
	}
	if it.Next() {
		t.Fatal("it.Next returned true")
	}
	if it.NextResultSet() {
		t.Fatal("it.NextResultSet returned true")
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}

	// Verify that the database/sql transaction was used by both statements.
	requests := server.TestSpanner.DrainRequestsFromServer()
	executeRequests := testutil.RequestsOfType(requests, reflect.TypeOf(&sppb.ExecuteSqlRequest{}))
	if g, w := len(executeRequests), 2; g != w {
		t.Fatalf("num execute requests mismatch\n Got: %v\nWant: %v", g, w)
	}
	first := executeRequests[0].(*sppb.ExecuteSqlRequest)
	second := executeRequests[1].(*sppb.ExecuteSqlRequest)
	if first.Transaction == nil || first.Transaction.GetBegin() == nil || first.Transaction.GetBegin().GetReadWrite() == nil {
		t.Fatal("missing BeginTransaction on first request")
	}
	if second.Transaction == nil || second.Transaction.GetId() == nil {
		t.Fatal("missing TransactionId on second request")
	}
	commitRequests := testutil.RequestsOfType(requests, reflect.TypeOf(&sppb.CommitRequest{}))
	if g, w := len(commitRequests), 1; g != w {
		t.Fatalf("num commit requests mismatch\n Got: %v\nWant: %v", g, w)
	}
}

func TestQueryMultipleWithError(t *testing.T) {
	t.Parallel()

	db, server, teardown := setupTestDBConnection(t)
	defer teardown()
	ctx := context.Background()

	c, _ := db.Conn(ctx)
	defer silentClose(c)

	invalidSql := "select * from non_existing_table"
	_ = server.TestSpanner.PutStatementResult(invalidSql, &testutil.StatementResult{
		Type: testutil.StatementResultError,
		Err:  status.Error(codes.NotFound, "Table not found"),
	})

	// Execute a multi-statement SQL string that fails halfway.
	r, err := c.QueryContext(context.Background(), fmt.Sprintf("%s;%s;%s", testutil.SelectFooFromBar, invalidSql, testutil.SelectFooFromBar))
	if err != nil {
		t.Fatal(err)
	}
	defer silentClose(r)

	// The first statement should succeed.
	if err := r.Err(); err != nil {
		t.Fatal(err)
	}
	for v := range 2 {
		if !r.Next() {
			t.Fatal("r.Next returned false")
		}
		var value int64
		if err := r.Scan(&value); err != nil {
			t.Fatal(err)
		}
		if g, w := value, int64(v+1); g != w {
			t.Fatalf("value mismatch\n Got: %v\nWant: %v", g, w)
		}
	}
	if r.Next() {
		t.Fatal("r.Next returned true")
	}
	if err := r.Err(); err != nil {
		t.Fatal(err)
	}

	// Moving to the next result set will fail, as the second query failed.
	if r.NextResultSet() {
		t.Fatal("r.NextResultSet returned true")
	}
	// We can get the error by calling Err() on the rows object.
	if g, w := spanner.ErrCode(r.Err()), codes.NotFound; g != w {
		t.Fatalf("error code mismatch\n Got: %v\nWant: %v", g, w)
	}
	// Trying to get data from the second query should fail.
	if r.Next() {
		t.Fatal("r.Next returned true for failed query")
	}
	// We should still get the same error.
	if g, w := spanner.ErrCode(r.Err()), codes.NotFound; g != w {
		t.Fatalf("error code mismatch\n Got: %v\nWant: %v", g, w)
	}
	// It should not be possible to move to the next result set, as the current statement failed.
	if r.NextResultSet() {
		t.Fatal("r.NextResultSet returned true")
	}

	// There are no rollback requests, because the block uses a read-only transaction.
	verifyRequests(t, server, expectedRequests{
		numExecuteRequests:  2,
		numBatchDmlRequests: 0,
		numCommitRequests:   0,
		numRollbackRequests: 0,
	})
}

func TestQueryMultipleWithMetadataAndStatsAsResultSets(t *testing.T) {
	t.Parallel()

	db, _, teardown := setupTestDBConnection(t)
	defer teardown()
	ctx := context.Background()

	c, _ := db.Conn(ctx)
	defer silentClose(c)

	// Execute both a query and a DML statement in a single SQL string.
	// Request the driver to return the metadata and stats of each statement as a separate result set.
	// That means that we get 3 result sets per statement, so in total 6 result sets.
	r, err := c.QueryContext(context.Background(),
		fmt.Sprintf("%s;%s", testutil.SelectFooFromBar, testutil.UpdateBarSetFoo),
		ExecOptions{ReturnResultSetStats: true, ReturnResultSetMetadata: true})
	if err != nil {
		t.Fatal(err)
	}
	defer silentClose(r)

	// The first result should contain the metadata of the first query.
	var metadata *sppb.ResultSetMetadata
	if !r.Next() {
		t.Fatal("r.Next returned false")
	}
	if err := r.Scan(&metadata); err != nil {
		t.Fatal(err)
	}
	if g, w := len(metadata.RowType.Fields), 1; g != w {
		t.Fatalf("num fields mismatch\n Got: %v\nWant: %v", g, w)
	}

	// The next result set should contain the data of the first query.
	if !r.NextResultSet() {
		t.Fatal("r.NextResultSet returned false")
	}
	for v := range 2 {
		if !r.Next() {
			t.Fatal("r.Next returned false")
		}
		var value int64
		if err := r.Scan(&value); err != nil {
			t.Fatal(err)
		}
		if g, w := value, int64(v+1); g != w {
			t.Fatalf("value mismatch\n Got: %v\nWant: %v", g, w)
		}
	}
	if r.Next() {
		t.Fatal("r.Next returned true")
	}
	// The next result set should contain the stats of the first query.
	if !r.NextResultSet() {
		t.Fatal("r.NextResultSet returned false")
	}
	var stats *sppb.ResultSetStats
	if !r.Next() {
		t.Fatal("r.Next returned false")
	}
	if err := r.Scan(&stats); err != nil {
		t.Fatal(err)
	}
	if g, w := stats.GetRowCountExact(), int64(0); g != w {
		t.Fatalf("row count mismatch\n Got: %v\nWant: %v", g, w)
	}

	// The next result set should contain the metadata of the second statement, which is a DML statement.
	if !r.NextResultSet() {
		t.Fatal("r.NextResultSet returned false")
	}
	if !r.Next() {
		t.Fatal("r.Next returned false")
	}
	if err := r.Scan(&metadata); err != nil {
		t.Fatal(err)
	}
	// The metadata of a DML statement should contain zero columns.
	if g, w := len(metadata.RowType.Fields), 0; g != w {
		t.Fatalf("num fields mismatch\n Got: %v\nWant: %v", g, w)
	}
	// The next result set should contain the data of the second statement. That should also be empty.
	if !r.NextResultSet() {
		t.Fatal("r.NextResultSet returned false")
	}
	if r.Next() {
		t.Fatal("r.Next returned true for DML statement")
	}
	// The next result set should contain the stats for the DML statement.
	if !r.NextResultSet() {
		t.Fatal("r.NextResultSet returned false")
	}
	if !r.Next() {
		t.Fatal("r.Next returned false")
	}
	if err := r.Scan(&stats); err != nil {
		t.Fatal(err)
	}
	if g, w := stats.GetRowCountExact(), int64(testutil.UpdateBarSetFooRowCount); g != w {
		t.Fatalf("row count mismatch\n Got: %v\nWant: %v", g, w)
	}
	// There should be no more results.
	if r.NextResultSet() {
		t.Fatal("r.NextResultSet returned true")
	}
}

func TestMultipleDmlStatements(t *testing.T) {
	t.Parallel()

	db, server, teardown := setupTestDBConnection(t)
	defer teardown()
	ctx := context.Background()

	dml1 := "insert into my_table (id, value) values (1, 'One')"
	_ = server.TestSpanner.PutStatementResult(dml1, &testutil.StatementResult{
		Type:        testutil.StatementResultUpdateCount,
		UpdateCount: 1,
	})
	dml2 := "insert into my_table (id, value) values (2, 'Two')"
	_ = server.TestSpanner.PutStatementResult(dml2, &testutil.StatementResult{
		Type:        testutil.StatementResultUpdateCount,
		UpdateCount: 1,
	})

	r, err := db.QueryContext(ctx, fmt.Sprintf("%s;%s", dml1, dml2))
	if err != nil {
		t.Fatal(err)
	}
	defer silentClose(r)

	if err := consumeResults(t, r, []expectedResults{{numRows: 0}, {numRows: 0}}); err != nil {
		t.Fatal(err)
	}
	verifyRequests(t, server, expectedRequests{
		numExecuteRequests:  0,
		numBatchDmlRequests: 1,
		numCommitRequests:   1,
		numRollbackRequests: 0,
	})
}

func TestMultipleMixedDmlAndQueryStatements(t *testing.T) {
	t.Parallel()

	db, server, teardown := setupTestDBConnection(t)
	defer teardown()
	ctx := context.Background()

	dml1 := "insert into my_table (id, value) values (1, 'One')"
	_ = server.TestSpanner.PutStatementResult(dml1, &testutil.StatementResult{
		Type:        testutil.StatementResultUpdateCount,
		UpdateCount: 1,
	})
	dml2 := "insert into my_table (id, value) values (2, 'Two')"
	_ = server.TestSpanner.PutStatementResult(dml2, &testutil.StatementResult{
		Type:        testutil.StatementResultUpdateCount,
		UpdateCount: 1,
	})

	r, err := db.QueryContext(ctx, fmt.Sprintf("%s;%s;%s", dml1, testutil.SelectFooFromBar, dml2))
	if err != nil {
		t.Fatal(err)
	}
	defer silentClose(r)

	if err := consumeResults(t, r, []expectedResults{{numRows: 0}, {numRows: 2}, {numRows: 0}}); err != nil {
		t.Fatal(err)
	}
	verifyRequests(t, server, expectedRequests{
		numExecuteRequests:  3,
		numBatchDmlRequests: 0,
		numCommitRequests:   1,
		numRollbackRequests: 0,
	})
}

func TestMixedDmlAndTransactionStatements(t *testing.T) {
	t.Parallel()

	db, server, teardown := setupTestDBConnection(t)
	defer teardown()
	ctx := context.Background()

	dml1 := "insert into my_table (id, value) values (1, 'One')"
	_ = server.TestSpanner.PutStatementResult(dml1, &testutil.StatementResult{
		Type:        testutil.StatementResultUpdateCount,
		UpdateCount: 1,
	})
	dml2 := "insert into my_table (id, value) values (2, 'Two')"
	_ = server.TestSpanner.PutStatementResult(dml2, &testutil.StatementResult{
		Type:        testutil.StatementResultUpdateCount,
		UpdateCount: 1,
	})

	// Execute a DML statement, then a commit, then another DML statement.
	r, err := db.QueryContext(ctx, fmt.Sprintf("%s;commit;%s", dml1, dml2))
	if err != nil {
		t.Fatal(err)
	}
	defer silentClose(r)

	if err := consumeResults(t, r, []expectedResults{{numRows: 0}, {numRows: 0}, {numRows: 0}}); err != nil {
		t.Fatal(err)
	}
	verifyRequests(t, server, expectedRequests{
		numExecuteRequests:  2,
		numBatchDmlRequests: 0,
		numCommitRequests:   2,
		numRollbackRequests: 0,
	})
}

func TestWithOpenExplicitTransactionBlock(t *testing.T) {
	t.Parallel()

	db, server, teardown := setupTestDBConnection(t)
	defer teardown()
	ctx := context.Background()

	dml1 := "insert into my_table (id, value) values (1, 'One')"
	_ = server.TestSpanner.PutStatementResult(dml1, &testutil.StatementResult{
		Type:        testutil.StatementResultUpdateCount,
		UpdateCount: 1,
	})
	dml2 := "insert into my_table (id, value) values (2, 'Two')"
	_ = server.TestSpanner.PutStatementResult(dml2, &testutil.StatementResult{
		Type:        testutil.StatementResultUpdateCount,
		UpdateCount: 1,
	})

	// This string starts an explicit transaction block that is still active at the end of the string.
	// This transaction is not committed or rolled back when the execution finishes.
	r, err := db.QueryContext(ctx, fmt.Sprintf("begin;%s;%s", dml1, dml2))
	if err != nil {
		t.Fatal(err)
	}
	defer silentClose(r)

	if err := consumeResults(t, r, []expectedResults{{numRows: 0}, {numRows: 0}, {numRows: 0}}); err != nil {
		t.Fatal(err)
	}
	verifyRequests(t, server, expectedRequests{
		numExecuteRequests:  0,
		numBatchDmlRequests: 1,
		numCommitRequests:   0,
		numRollbackRequests: 0,
	})
}

func TestMultipleDdlStatements(t *testing.T) {
	t.Parallel()

	db, server, teardown := setupTestDBConnection(t)
	defer teardown()
	ctx := context.Background()

	// Add DDL response.
	var expectedResponse = &emptypb.Empty{}
	anyMsg, _ := anypb.New(expectedResponse)
	server.TestDatabaseAdmin.SetResps([]proto.Message{
		&longrunningpb.Operation{
			Done:   true,
			Result: &longrunningpb.Operation_Response{Response: anyMsg},
			Name:   "test-operation",
		},
	})

	r, err := db.QueryContext(ctx, "create table singers (id int64 primary key, name string(max));"+
		"create table albums (id int64 primary key, title string(max));"+
		"create table tracks (id int64 primary key, title string(max));")
	if err != nil {
		t.Fatal(err)
	}
	defer silentClose(r)

	if err := consumeResults(t, r, []expectedResults{{numRows: 0}, {numRows: 0}, {numRows: 0}}); err != nil {
		t.Fatal(err)
	}
	requests := server.TestDatabaseAdmin.Reqs()
	if g, w := len(requests), 1; g != w {
		t.Fatalf("requests count mismatch\nGot: %v\nWant: %v", g, w)
	}
	if req, ok := requests[0].(*databasepb.UpdateDatabaseDdlRequest); ok {
		if g, w := len(req.Statements), 3; g != w {
			t.Fatalf("statements count mismatch\nGot: %v\nWant: %v", g, w)
		}
	} else {
		t.Fatalf("request type mismatch, got %v", requests[0])
	}
}

func TestDdlSyntaxError(t *testing.T) {
	t.Parallel()

	db, server, teardown := setupTestDBConnection(t)
	defer teardown()
	ctx := context.Background()

	// Add DDL response.
	server.TestDatabaseAdmin.SetResps([]proto.Message{
		&longrunningpb.Operation{
			Done:   true,
			Result: &longrunningpb.Operation_Error{Error: &pbstatus.Status{Code: int32(codes.InvalidArgument), Message: "Syntax error"}},
			Name:   "test-operation",
		},
	})

	_, err := db.QueryContext(ctx, "create table singers (id int64 primary key, name string(max));"+
		"create table albums (id int64 primry key, title string(max));"+
		"create table tracks (id int64 primary key, title string(max));")
	if err == nil {
		t.Fatal("expected error")
	}
	var be *BatchError
	if ok := errors.As(err, &be); !ok {
		t.Fatalf("expected a BatchError")
	}
	if g, w := len(be.BatchUpdateCounts), 0; g != w {
		t.Fatalf("number of successful updates mismatch\nGot: %v\nWant: %v", g, w)
	}

	requests := server.TestDatabaseAdmin.Reqs()
	if g, w := len(requests), 1; g != w {
		t.Fatalf("requests count mismatch\nGot: %v\nWant: %v", g, w)
	}
	if req, ok := requests[0].(*databasepb.UpdateDatabaseDdlRequest); ok {
		if g, w := len(req.Statements), 3; g != w {
			t.Fatalf("statements count mismatch\nGot: %v\nWant: %v", g, w)
		}
	} else {
		t.Fatalf("request type mismatch, got %v", requests[0])
	}
}

func TestDdlTableExistsError(t *testing.T) {
	t.Parallel()

	db, server, teardown := setupTestDBConnection(t)
	defer teardown()
	ctx := context.Background()

	metadata := &databasepb.UpdateDatabaseDdlMetadata{
		Database: "projects/p/instances/i/databases/d",
		Statements: []string{
			"create table singers (id int64 primary key, name string(max))",
			"create table albums (id int64 primary key, title string(max))",
			"create table tracks (id int64 primary key, title string(max))",
		},
		CommitTimestamps: []*timestamppb.Timestamp{
			{Seconds: time.Now().UnixMilli() / 1000, Nanos: 0},
		},
	}
	packedMetadata, err := anypb.New(metadata)
	if err != nil {
		t.Fatal(err)
	}

	// Add DDL response.
	server.TestDatabaseAdmin.SetResps([]proto.Message{
		&longrunningpb.Operation{
			Done:     true,
			Result:   &longrunningpb.Operation_Error{Error: &pbstatus.Status{Code: int32(codes.FailedPrecondition), Message: "Table albums already exists"}},
			Name:     "test-operation",
			Metadata: packedMetadata,
		},
	})

	r, err := db.QueryContext(ctx, "create table singers (id int64 primary key, name string(max));"+
		"create table albums (id int64 primary key, title string(max));"+
		"create table tracks (id int64 primary key, title string(max));")
	if err != nil {
		t.Fatal(err)
	}
	defer silentClose(r)

	err = consumeResults(t, r, []expectedResults{{numRows: 0}})
	if err == nil {
		t.Fatal("expected error")
	}
	var be *BatchError
	if ok := errors.As(err, &be); !ok {
		t.Fatalf("expected a BatchError")
	}
	if g, w := len(be.BatchUpdateCounts), 1; g != w {
		t.Fatalf("number of successful updates mismatch\nGot: %v\nWant: %v", g, w)
	}

	requests := server.TestDatabaseAdmin.Reqs()
	if g, w := len(requests), 1; g != w {
		t.Fatalf("requests count mismatch\nGot: %v\nWant: %v", g, w)
	}
	if req, ok := requests[0].(*databasepb.UpdateDatabaseDdlRequest); ok {
		if g, w := len(req.Statements), 3; g != w {
			t.Fatalf("statements count mismatch\nGot: %v\nWant: %v", g, w)
		}
	} else {
		t.Fatalf("request type mismatch, got %v", requests[0])
	}
}

func TestDmlAndDdlMix(t *testing.T) {
	t.Parallel()

	db, server, teardown := setupTestDBConnection(t)
	defer teardown()
	ctx := context.Background()

	// Add DML response.
	_ = server.TestSpanner.PutStatementResult("insert into session (id, timestamp) values (@p1, current_timestamp)",
		&testutil.StatementResult{Type: testutil.StatementResultUpdateCount, UpdateCount: 1})

	// Add DDL response.
	var expectedResponse = &emptypb.Empty{}
	anyMsg, _ := anypb.New(expectedResponse)
	server.TestDatabaseAdmin.SetResps([]proto.Message{
		&longrunningpb.Operation{
			Done:   true,
			Result: &longrunningpb.Operation_Response{Response: anyMsg},
			Name:   "test-operation",
		},
	})

	sessionId := uuid.New().String()
	r, err := db.QueryContext(ctx, "insert into session (id, timestamp) values (?, current_timestamp);"+
		"insert into session (id, timestamp) values (?, current_timestamp);"+
		"create table singers (id int64 primary key, name string(max));"+
		"create table albums (id int64 primary key, title string(max));"+
		"create table tracks (id int64 primary key, title string(max));", sessionId)
	if err != nil {
		t.Fatal(err)
	}
	defer silentClose(r)

	if err := consumeResults(t, r, []expectedResults{{numRows: 0}, {numRows: 0}, {numRows: 0}, {numRows: 0}, {numRows: 0}}); err != nil {
		t.Fatal(err)
	}
	requests := server.TestSpanner.DrainRequestsFromServer()
	execRequests := testutil.RequestsOfType(requests, reflect.TypeOf(&sppb.ExecuteSqlRequest{}))
	if g, w := len(execRequests), 0; g != w {
		t.Fatalf("execute count mismatch\nGot: %v\nWant: %v", g, w)
	}
	batchRequests := testutil.RequestsOfType(requests, reflect.TypeOf(&sppb.ExecuteBatchDmlRequest{}))
	if g, w := len(batchRequests), 1; g != w {
		t.Fatalf("batch dml count mismatch\nGot: %v\nWant: %v", g, w)
	}
	batchRequest := batchRequests[0].(*sppb.ExecuteBatchDmlRequest)
	if g, w := len(batchRequest.Statements), 2; g != w {
		t.Fatalf("statements count mismatch\nGot: %v\nWant: %v", g, w)
	}
	commitRequests := testutil.RequestsOfType(requests, reflect.TypeOf(&sppb.CommitRequest{}))
	if g, w := len(commitRequests), 1; g != w {
		t.Fatalf("commit count mismatch\nGot: %v\nWant: %v", g, w)
	}

	adminRequests := server.TestDatabaseAdmin.Reqs()
	if g, w := len(adminRequests), 1; g != w {
		t.Fatalf("requests count mismatch\nGot: %v\nWant: %v", g, w)
	}
	if req, ok := adminRequests[0].(*databasepb.UpdateDatabaseDdlRequest); ok {
		if g, w := len(req.Statements), 3; g != w {
			t.Fatalf("statements count mismatch\nGot: %v\nWant: %v", g, w)
		}
	} else {
		t.Fatalf("request type mismatch, got %v", adminRequests[0])
	}
}

type expectedResults struct {
	numRows int
}

func consumeResults(t *testing.T, r *sql.Rows, expected []expectedResults) error {
	for i, want := range expected {
		numRows := 0
		for r.Next() {
			numRows++
		}
		if g, w := numRows, want.numRows; g != w {
			t.Fatalf("%d: number of rows mismatch\n Got: %v\nWant: %v", i, g, w)
		}
		if i < len(expected)-1 && !r.NextResultSet() {
			if err := r.Err(); err != nil {
				t.Fatalf("%d: unexpected error: %v", i, err)
			} else {
				t.Fatalf("%d: r.NextResultSet returned false", i)
			}
		}
	}
	if r.NextResultSet() {
		t.Fatal("r.NextResultSet returned true")
	}
	return r.Err()
}

type expectedRequests struct {
	numExecuteRequests  int
	numBatchDmlRequests int
	numCommitRequests   int
	numRollbackRequests int
}

func verifyRequests(t *testing.T, server *testutil.MockedSpannerInMemTestServer, expected expectedRequests) {
	requests := server.TestSpanner.DrainRequestsFromServer()
	executeRequests := testutil.RequestsOfType(requests, reflect.TypeOf(&sppb.ExecuteSqlRequest{}))
	if g, w := len(executeRequests), expected.numExecuteRequests; g != w {
		t.Fatalf("number of execute requests mismatch\n Got: %v\nWant: %v", g, w)
	}
	batchRequests := testutil.RequestsOfType(requests, reflect.TypeOf(&sppb.ExecuteBatchDmlRequest{}))
	if g, w := len(batchRequests), expected.numBatchDmlRequests; g != w {
		t.Fatalf("number of batch requests mismatch\n Got: %v\nWant: %v", g, w)
	}
	commitRequests := testutil.RequestsOfType(requests, reflect.TypeOf(&sppb.CommitRequest{}))
	if g, w := len(commitRequests), expected.numCommitRequests; g != w {
		t.Fatalf("number of commit requests mismatch\n Got: %v\nWant: %v", g, w)
	}
	rollbackRequests := testutil.RequestsOfType(requests, reflect.TypeOf(&sppb.RollbackRequest{}))
	if g, w := len(rollbackRequests), expected.numRollbackRequests; g != w {
		t.Fatalf("number of rollback requests mismatch\n Got: %v\nWant: %v", g, w)
	}
}
