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
	"fmt"
	"reflect"
	"testing"

	"cloud.google.com/go/spanner"
	sppb "cloud.google.com/go/spanner/apiv1/spannerpb"
	"github.com/googleapis/go-sql-spanner/testutil"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestQueryMultiple(t *testing.T) {
	t.Parallel()

	db, _, teardown := setupTestDBConnection(t)
	defer teardown()
	ctx := context.Background()

	c, _ := db.Conn(ctx)
	defer silentClose(c)

	r, err := c.QueryContext(context.Background(), fmt.Sprintf("%s;%s", testutil.SelectFooFromBar, testutil.SelectFooFromBar))
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
	it, err := tx.QueryContext(ctx, fmt.Sprintf("%s;%s", testutil.UpdateBarSetFoo, testutil.UpdateBarSetFoo))
	if err != nil {
		t.Fatal(err)
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
