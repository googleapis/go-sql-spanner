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

	"cloud.google.com/go/longrunning/autogen/longrunningpb"
	"cloud.google.com/go/spanner"
	"cloud.google.com/go/spanner/apiv1/spannerpb"
	"github.com/googleapis/go-sql-spanner/testutil"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/structpb"
)

func TestExecute(t *testing.T) {
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
	rowsId, err := Execute(ctx, poolId, connId, &spannerpb.ExecuteSqlRequest{
		Sql: testutil.SelectFooFromBar,
	})
	if rowsId == 0 {
		t.Fatal("Execute returned unexpected zero id")
	}
	p, ok := pools.Load(poolId)
	if !ok {
		t.Fatal("pool not found in map")
	}
	pool := p.(*Pool)
	c, ok := pool.connections.Load(connId)
	if !ok {
		t.Fatal("connection not in map")
	}
	connection, ok := c.(*Connection)
	if _, ok := connection.results.Load(rowsId); !ok {
		t.Fatal("result not in map")
	}

	requests := server.TestSpanner.DrainRequestsFromServer()
	executeRequests := testutil.RequestsOfType(requests, reflect.TypeOf(&spannerpb.ExecuteSqlRequest{}))
	if g, w := len(executeRequests), 1; g != w {
		t.Fatalf("num ExecuteSql requests mismatch\n Got: %d\nWant: %d", g, w)
	}

	if err := CloseRows(ctx, poolId, connId, rowsId); err != nil {
		t.Fatalf("CloseRows returned unexpected error: %v", err)
	}
	if _, ok := connection.results.Load(rowsId); ok {
		t.Fatal("rows still in results map")
	}
	if err := CloseConnection(ctx, poolId, connId); err != nil {
		t.Fatalf("CloseConnection returned unexpected error: %v", err)
	}
	// Closing a Rows object after closing its connection should be a no-op.
	if err := CloseRows(ctx, poolId, connId, rowsId); err != nil {
		t.Fatalf("CloseRows returned unexpected error: %v", err)
	}
	if err := ClosePool(ctx, poolId); err != nil {
		t.Fatalf("ClosePool returned unexpected error: %v", err)
	}
}

func TestExecuteMultiStatement(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	server, teardown := setupMockServer(t)
	defer teardown()
	dsn := fmt.Sprintf("%s/projects/p/instances/i/databases/d?useplaintext=true", server.Address)

	// Add a generic successful DDL response to the mock server.
	var expectedResponse = &emptypb.Empty{}
	anyMsg, _ := anypb.New(expectedResponse)
	server.TestDatabaseAdmin.SetResps([]proto.Message{
		&longrunningpb.Operation{
			Done:   true,
			Result: &longrunningpb.Operation_Response{Response: anyMsg},
			Name:   "test-operation",
		},
	})

	poolId, err := CreatePool(ctx, "test", dsn)
	if err != nil {
		t.Fatalf("CreatePool returned unexpected error: %v", err)
	}
	connId, err := CreateConnection(ctx, poolId)
	if err != nil {
		t.Fatalf("CreateConnection returned unexpected error: %v", err)
	}

	type expectedResults struct {
		numRows  int
		affected int64
	}
	type test struct {
		name               string
		sql                string
		numExecuteRequests int
		numBachDmlRequests int
		expectedResults    []expectedResults
	}

	for _, tt := range []test{
		{
			name:               "two queries",
			sql:                fmt.Sprintf("%s;%s", testutil.SelectFooFromBar, testutil.SelectFooFromBar),
			numExecuteRequests: 2,
			expectedResults: []expectedResults{
				{numRows: 2},
				{numRows: 2},
			},
		},
		{
			name:               "three queries",
			sql:                fmt.Sprintf("%s;%s;%s", testutil.SelectFooFromBar, testutil.SelectFooFromBar, testutil.SelectFooFromBar),
			numExecuteRequests: 3,
			expectedResults: []expectedResults{
				{numRows: 2},
				{numRows: 2},
				{numRows: 2},
			},
		},
		{
			name:               "two DML statements",
			sql:                fmt.Sprintf("%s;%s", testutil.UpdateBarSetFoo, testutil.UpdateBarSetFoo),
			numBachDmlRequests: 1,
			expectedResults: []expectedResults{
				{affected: testutil.UpdateBarSetFooRowCount},
				{affected: testutil.UpdateBarSetFooRowCount},
			},
		},
		{
			name:            "two DDL statements",
			sql:             "create table my_table (id int64 primary key, value varchar(max)); create index my_index on my_table (value);",
			expectedResults: []expectedResults{{}, {}},
		},
		{
			name:               "query then DML",
			sql:                fmt.Sprintf("%s;%s", testutil.SelectFooFromBar, testutil.UpdateBarSetFoo),
			numExecuteRequests: 2,
			expectedResults: []expectedResults{
				{numRows: 2},
				{affected: testutil.UpdateBarSetFooRowCount},
			},
		},
		{
			name:               "DML then query",
			sql:                fmt.Sprintf("%s;%s", testutil.UpdateBarSetFoo, testutil.SelectFooFromBar),
			numExecuteRequests: 2,
			expectedResults: []expectedResults{
				{affected: testutil.UpdateBarSetFooRowCount},
				{numRows: 2},
			},
		},
		{
			name:               "query then DDL",
			sql:                fmt.Sprintf("%s;%s", testutil.SelectFooFromBar, "create table my_table (id int64 primary key, value varchar(max));"),
			numExecuteRequests: 1,
			expectedResults: []expectedResults{
				{numRows: 2},
				{},
			},
		},
		{
			name:               "DDL then query",
			sql:                fmt.Sprintf("%s;%s", "create table my_table (id int64 primary key)", testutil.SelectFooFromBar),
			numExecuteRequests: 1,
			expectedResults: []expectedResults{
				{},
				{numRows: 2},
			},
		},
		{
			name:               "DML then DDL",
			sql:                fmt.Sprintf("%s;%s", testutil.UpdateBarSetFoo, "create table my_table (id int64 primary key, value varchar(max));"),
			numExecuteRequests: 1,
			expectedResults: []expectedResults{
				{affected: testutil.UpdateBarSetFooRowCount},
				{},
			},
		},
		{
			name:               "DDL then DML",
			sql:                fmt.Sprintf("%s;%s", "create table my_table (id int64 primary key)", testutil.UpdateBarSetFoo),
			numExecuteRequests: 1,
			expectedResults: []expectedResults{
				{},
				{affected: testutil.UpdateBarSetFooRowCount},
			},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			rowsId, err := Execute(ctx, poolId, connId, &spannerpb.ExecuteSqlRequest{
				Sql: tt.sql,
			})
			if err != nil {
				t.Fatalf("Execute returned unexpected error: %v", err)
			}
			if rowsId == 0 {
				t.Fatal("Execute returned unexpected zero id")
			}

			numResultSets := 0
			for {
				rowCount := 0
				for {
					rowBatch, err := Next(ctx, poolId, connId, rowsId, 1)
					if err != nil {
						t.Fatalf("Next returned unexpected error: %v", err)
					}
					if len(rowBatch) == 0 {
						break
					}
					rowCount++
				}
				if g, w := rowCount, tt.expectedResults[numResultSets].numRows; g != w {
					t.Fatalf("row count mismatch\n Got: %d\nWant: %d", g, w)
				}
				stats, err := ResultSetStats(ctx, poolId, connId, rowsId)
				if err != nil {
					t.Fatalf("ResultSetStats returned unexpected error: %v", err)
				}
				if g, w := stats.GetRowCountExact(), tt.expectedResults[numResultSets].affected; g != w {
					t.Fatalf("update count mismatch\n Got: %d\nWant: %d", g, w)
				}

				numResultSets++

				metadata, err := NextResultSet(ctx, poolId, connId, rowsId)
				if err != nil {
					t.Fatalf("NextResultSet returned unexpected error: %v", err)
				}
				if metadata == nil {
					break
				}
			}
			if g, w := numResultSets, len(tt.expectedResults); g != w {
				t.Fatalf("num result sets mismatch\n Got: %d\nWant: %d", g, w)
			}
			requests := server.TestSpanner.DrainRequestsFromServer()
			executeRequests := testutil.RequestsOfType(requests, reflect.TypeOf(&spannerpb.ExecuteSqlRequest{}))
			batchDmlRequests := testutil.RequestsOfType(requests, reflect.TypeOf(&spannerpb.ExecuteBatchDmlRequest{}))
			if g, w := len(executeRequests), tt.numExecuteRequests; g != w {
				t.Fatalf("num ExecuteSql requests mismatch\n Got: %d\nWant: %d", g, w)
			}
			if g, w := len(batchDmlRequests), tt.numBachDmlRequests; g != w {
				t.Fatalf("num BatchDml requests mismatch\n Got: %d\nWant: %d", g, w)
			}

			if err := CloseRows(ctx, poolId, connId, rowsId); err != nil {
				t.Fatalf("CloseRows returned unexpected error: %v", err)
			}
		})
	}

	if err := CloseConnection(ctx, poolId, connId); err != nil {
		t.Fatalf("CloseConnection returned unexpected error: %v", err)
	}
	if err := ClosePool(ctx, poolId); err != nil {
		t.Fatalf("ClosePool returned unexpected error: %v", err)
	}
}

func TestExecuteMultiStatement_MoveToNextResultSetHalfway(t *testing.T) {
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
	rowsId, err := Execute(ctx, poolId, connId, &spannerpb.ExecuteSqlRequest{
		Sql: fmt.Sprintf("%s;%s", testutil.SelectFooFromBar, testutil.SelectFooFromBar),
	})
	if rowsId == 0 {
		t.Fatal("Execute returned unexpected zero id")
	}

	// Read one row from the first result set.
	rowBatch, err := Next(ctx, poolId, connId, rowsId, 1)
	if err != nil {
		t.Fatalf("Next returned unexpected error: %v", err)
	}
	if len(rowBatch) == 0 {
		t.Fatal("Next returned unexpected empty batch")
	}
	// Then move to the next result set.
	metadata, err := NextResultSet(ctx, poolId, connId, rowsId)
	if err != nil {
		t.Fatalf("NextResultSet returned unexpected error: %v", err)
	}
	if metadata == nil {
		t.Fatal("NextResultSet returned unexpected nil metadata")
	}
	// Try to read two rows from the second result set.
	for range 2 {
		rowBatch, err := Next(ctx, poolId, connId, rowsId, 1)
		if err != nil {
			t.Fatalf("Next returned unexpected error: %v", err)
		}
		if len(rowBatch) == 0 {
			t.Fatal("Next returned unexpected empty batch")
		}
	}

	if err := CloseRows(ctx, poolId, connId, rowsId); err != nil {
		t.Fatalf("CloseRows returned unexpected error: %v", err)
	}
	if err := CloseConnection(ctx, poolId, connId); err != nil {
		t.Fatalf("CloseConnection returned unexpected error: %v", err)
	}
	if err := ClosePool(ctx, poolId); err != nil {
		t.Fatalf("ClosePool returned unexpected error: %v", err)
	}

	requests := server.TestSpanner.DrainRequestsFromServer()
	executeRequests := testutil.RequestsOfType(requests, reflect.TypeOf(&spannerpb.ExecuteSqlRequest{}))
	if g, w := len(executeRequests), 2; g != w {
		t.Fatalf("num ExecuteSql requests mismatch\n Got: %d\nWant: %d", g, w)
	}
}

func TestExecuteMultiStatement_MoveToNextResultSetWithoutReading(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	server, teardown := setupMockServer(t)
	defer teardown()
	dsn := fmt.Sprintf("%s/projects/p/instances/i/databases/d?useplaintext=true", server.Address)

	poolId, err := CreatePool(ctx, "test", dsn)
	if err != nil {
		t.Fatalf("CreatePool returned unexpected error: %v", err)
	}
	defer ClosePool(ctx, poolId)
	connId, err := CreateConnection(ctx, poolId)
	if err != nil {
		t.Fatalf("CreateConnection returned unexpected error: %v", err)
	}
	defer CloseConnection(ctx, poolId, connId)
	rowsId, err := Execute(ctx, poolId, connId, &spannerpb.ExecuteSqlRequest{
		Sql: fmt.Sprintf("%s;%s", testutil.SelectFooFromBar, testutil.SelectFooFromBar),
	})
	if rowsId == 0 {
		t.Fatal("Execute returned unexpected zero id")
	}
	defer CloseRows(ctx, poolId, connId, rowsId)

	// Move to the next result set immediately, without calling Next at all!
	metadata, err := NextResultSet(ctx, poolId, connId, rowsId)
	if err != nil {
		t.Fatalf("NextResultSet returned unexpected error: %v", err)
	}
	if metadata == nil {
		t.Fatal("NextResultSet returned unexpected nil metadata")
	}
	// Try to read two rows from the second result set.
	for range 2 {
		rowBatch, err := Next(ctx, poolId, connId, rowsId, 1)
		if err != nil {
			t.Fatalf("Next returned unexpected error: %v", err)
		}
		if g, w := len(rowBatch), 1; g != w {
			t.Fatalf("Next returned unexpected empty batch\nGot:  %v\nWant: %v", g, w)
		}
	}

	requests := server.TestSpanner.DrainRequestsFromServer()
	executeRequests := testutil.RequestsOfType(requests, reflect.TypeOf(&spannerpb.ExecuteSqlRequest{}))
	if g, w := len(executeRequests), 2; g != w {
		t.Fatalf("num ExecuteSql requests mismatch\nGot:  %v\nWant: %v", g, w)
	}
}

func TestExecuteUnknownConnection(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	_, err := Execute(ctx, -1, -1, &spannerpb.ExecuteSqlRequest{
		Sql: testutil.SelectFooFromBar,
	})
	if g, w := spanner.ErrCode(err), codes.NotFound; g != w {
		t.Fatalf("error code mismatch\n Got: %d\nWant: %d", g, w)
	}
}

func TestCloseRowsTwice(t *testing.T) {
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
	rowsId, err := Execute(ctx, poolId, connId, &spannerpb.ExecuteSqlRequest{
		Sql: testutil.SelectFooFromBar,
	})

	for range 2 {
		if err := CloseRows(ctx, poolId, connId, rowsId); err != nil {
			t.Fatalf("CloseRows returned unexpected error: %v", err)
		}
	}
	if err := CloseConnection(ctx, poolId, connId); err != nil {
		t.Fatalf("CloseConnection returned unexpected error: %v", err)
	}
	if err := ClosePool(ctx, poolId); err != nil {
		t.Fatalf("ClosePool returned unexpected error: %v", err)
	}
}

func TestAnalyze(t *testing.T) {
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
	// Execute a SQL statement in PLAN mode. This only returns the query plan and no results.
	_, err = Execute(ctx, poolId, connId, &spannerpb.ExecuteSqlRequest{
		Sql:       testutil.SelectFooFromBar,
		QueryMode: spannerpb.ExecuteSqlRequest_PLAN,
	})

	requests := server.TestSpanner.DrainRequestsFromServer()
	executeRequests := testutil.RequestsOfType(requests, reflect.TypeOf(&spannerpb.ExecuteSqlRequest{}))
	if g, w := len(executeRequests), 1; g != w {
		t.Fatalf("num ExecuteSql requests mismatch\n Got: %d\nWant: %d", g, w)
	}
	request := executeRequests[0].(*spannerpb.ExecuteSqlRequest)
	if g, w := request.QueryMode, spannerpb.ExecuteSqlRequest_PLAN; g != w {
		t.Fatalf("query Mode mismatch\n Got: %d\nWant: %d", g, w)
	}

	if err := ClosePool(ctx, poolId); err != nil {
		t.Fatalf("ClosePool returned unexpected error: %v", err)
	}
}

func TestNextBatchSize(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	server, teardown := setupMockServer(t)
	defer teardown()
	dsn := fmt.Sprintf("%s/projects/p/instances/i/databases/d?useplaintext=true", server.Address)

	// Set up a query with 10 rows
	values := []int64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
	resultSet := testutil.CreateSingleColumnInt64ResultSet(values, "ID")
	result := &testutil.StatementResult{Type: testutil.StatementResultResultSet, ResultSet: resultSet}
	const query = "SELECT id FROM test_table"
	_ = server.TestSpanner.PutStatementResult(query, result)

	poolId, err := CreatePool(ctx, "test", dsn)
	if err != nil {
		t.Fatalf("CreatePool returned unexpected error: %v", err)
	}
	defer ClosePool(ctx, poolId)

	connId, err := CreateConnection(ctx, poolId)
	if err != nil {
		t.Fatalf("CreateConnection returned unexpected error: %v", err)
	}
	defer CloseConnection(ctx, poolId, connId)

	// We run multiple subtests with different batch sizes.
	for _, batchSize := range []int32{1, 2, 5, 10, 15} {
		t.Run(fmt.Sprintf("batchSize_%d", batchSize), func(t *testing.T) {
			rowsId, err := Execute(ctx, poolId, connId, &spannerpb.ExecuteSqlRequest{
				Sql: query,
			})
			if err != nil {
				t.Fatalf("Execute returned unexpected error: %v", err)
			}
			defer CloseRows(ctx, poolId, connId, rowsId)

			var fetchedValues []int64
			for {
				rowBatch, err := Next(ctx, poolId, connId, rowsId, batchSize)
				if err != nil {
					t.Fatalf("Next returned unexpected error: %v", err)
				}
				if len(rowBatch) == 0 {
					break
				}

				// Assert that the returned batch size is less than or equal to batchSize
				if g, w := int32(len(rowBatch)), batchSize; g > w {
					t.Fatalf("batch size exceeded: got %d, max %d", g, w)
				}

				for _, row := range rowBatch {
					if len(row.Values) != 1 {
						t.Fatalf("row should have exactly 1 column, got %d", len(row.Values))
					}
					var id int64
					strVal := row.Values[0].GetStringValue()
					if _, err := fmt.Sscanf(strVal, "%d", &id); err != nil {
						t.Fatalf("failed to parse value %q: %v", strVal, err)
					}
					fetchedValues = append(fetchedValues, id)
				}
			}

			if g, w := fetchedValues, values; !reflect.DeepEqual(g, w) {
				t.Fatalf("fetched values mismatch\nGot:  %v\nWant: %v", g, w)
			}
		})
	}

	// Test error when batchSize <= 0
	for _, batchSize := range []int32{0, -1, -50} {
		t.Run(fmt.Sprintf("invalid_batchSize_%d", batchSize), func(t *testing.T) {
			rowsId, err := Execute(ctx, poolId, connId, &spannerpb.ExecuteSqlRequest{
				Sql: query,
			})
			if err != nil {
				t.Fatalf("Execute returned unexpected error: %v", err)
			}
			defer CloseRows(ctx, poolId, connId, rowsId)

			_, err = Next(ctx, poolId, connId, rowsId, batchSize)
			if err == nil {
				t.Fatalf("Next with batchSize %d expected error, got nil", batchSize)
			}
			if g, w := spanner.ErrCode(err), codes.InvalidArgument; g != w {
				t.Fatalf("error code mismatch\nGot:  %v\nWant: %v", g, w)
			}
		})
	}

	// Test capping when batchSize > MaxRowsPerBatch
	t.Run("capped_batchSize_large", func(t *testing.T) {
		rowsId, err := Execute(ctx, poolId, connId, &spannerpb.ExecuteSqlRequest{
			Sql: query,
		})
		if err != nil {
			t.Fatalf("Execute returned unexpected error: %v", err)
		}
		defer CloseRows(ctx, poolId, connId, rowsId)

		rowBatch, err := Next(ctx, poolId, connId, rowsId, 200000)
		if err != nil {
			t.Fatalf("Next with large batch size returned unexpected error: %v", err)
		}
		if g, w := len(rowBatch), 10; g != w {
			t.Fatalf("expected all 10 rows to be returned\nGot:  %v\nWant: %v", g, w)
		}
	})
}

func TestExecuteMultiStatement_DifferentColumnCount(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	server, teardown := setupMockServer(t)
	defer teardown()
	dsn := fmt.Sprintf("%s/projects/p/instances/i/databases/d?useplaintext=true", server.Address)

	poolId, err := CreatePool(ctx, "test", dsn)
	if err != nil {
		t.Fatalf("CreatePool returned unexpected error: %v", err)
	}
	defer ClosePool(ctx, poolId)
	connId, err := CreateConnection(ctx, poolId)
	if err != nil {
		t.Fatalf("CreateConnection returned unexpected error: %v", err)
	}
	defer CloseConnection(ctx, poolId, connId)
	rowsId, err := Execute(ctx, poolId, connId, &spannerpb.ExecuteSqlRequest{
		Sql: fmt.Sprintf("%s;%s", testutil.SelectFooFromBar, testutil.SelectSingerIDAlbumIDAlbumTitleFromAlbums),
	})
	if rowsId == 0 {
		t.Fatal("Execute returned unexpected zero id")
	}
	defer CloseRows(ctx, poolId, connId, rowsId)

	// 1. Read first result set (which has 1 column).
	rowBatch, err := Next(ctx, poolId, connId, rowsId, 10)
	if err != nil {
		t.Fatalf("Next returned unexpected error: %v", err)
	}
	if g, w := len(rowBatch), 2; g != w {
		t.Fatalf("first result set row count mismatch\nGot:  %v\nWant: %v", g, w)
	}
	for _, row := range rowBatch {
		if g, w := len(row.Values), 1; g != w {
			t.Fatalf("first result set column count mismatch\nGot:  %v\nWant: %v", g, w)
		}
	}

	// 2. Move to next result set (which has 3 columns).
	metadata, err := NextResultSet(ctx, poolId, connId, rowsId)
	if err != nil {
		t.Fatalf("NextResultSet returned unexpected error: %v", err)
	}
	if metadata == nil {
		t.Fatal("NextResultSet returned unexpected nil metadata")
	}
	if g, w := len(metadata.RowType.Fields), 3; g != w {
		t.Fatalf("second result set metadata columns mismatch\nGot:  %v\nWant: %v", g, w)
	}

	// 3. Read second result set (which has 3 columns).
	rowBatch2, err := Next(ctx, poolId, connId, rowsId, 10)
	if err != nil {
		t.Fatalf("Next returned unexpected error on second result set: %v", err)
	}
	if g, w := len(rowBatch2), int(testutil.SelectSingerIDAlbumIDAlbumTitleFromAlbumsRowCount); g != w {
		t.Fatalf("second result set row count mismatch\nGot:  %v\nWant: %v", g, w)
	}
	for _, row := range rowBatch2 {
		if g, w := len(row.Values), 3; g != w {
			t.Fatalf("second result set column count mismatch\nGot:  %v\nWant: %v", g, w)
		}
	}
}

func TestNext_DoneOnError(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	server, teardown := setupMockServer(t)
	defer teardown()
	dsn := fmt.Sprintf("%s/projects/p/instances/i/databases/d?useplaintext=true", server.Address)

	query := "SELECT * FROM broken_table"
	// Setup query returning 2 rows.
	_ = server.TestSpanner.PutStatementResult(query, &testutil.StatementResult{
		Type:      testutil.StatementResultResultSet,
		ResultSet: testutil.CreateSingleColumnInt64ResultSet([]int64{1, 2}, "c"),
	})

	// Inject error on the second PartialResultSet (ResumeToken = 2).
	server.TestSpanner.AddPartialResultSetError(query, testutil.PartialResultSetExecutionTime{
		ResumeToken: testutil.EncodeResumeToken(2),
		Err:         status.Error(codes.Internal, "broken connection"),
	})

	poolId, err := CreatePool(ctx, "test", dsn)
	if err != nil {
		t.Fatalf("CreatePool returned unexpected error: %v", err)
	}
	defer ClosePool(ctx, poolId)
	connId, err := CreateConnection(ctx, poolId)
	if err != nil {
		t.Fatalf("CreateConnection returned unexpected error: %v", err)
	}
	defer CloseConnection(ctx, poolId, connId)
	rowsId, err := Execute(ctx, poolId, connId, &spannerpb.ExecuteSqlRequest{Sql: query})
	if rowsId == 0 {
		t.Fatal("Execute returned unexpected zero id")
	}
	defer CloseRows(ctx, poolId, connId, rowsId)

	// 1. Fetch first row (batchSize=1) -> should succeed.
	rowBatch, err := Next(ctx, poolId, connId, rowsId, 1)
	if err != nil {
		t.Fatalf("First Next() returned unexpected error: %v", err)
	}
	if g, w := len(rowBatch), 1; g != w {
		t.Fatalf("first row batch count mismatch\nGot:  %v\nWant: %v", g, w)
	}

	// 2. Fetch second row (batchSize=1) -> should return the injected error.
	_, err = Next(ctx, poolId, connId, rowsId, 1)
	if err == nil {
		t.Fatal("Second Next() expected error but got nil")
	}
	if g, w := status.Code(err), codes.Internal; g != w {
		t.Fatalf("error code mismatch\nGot:  %v\nWant: %v", g, w)
	}

	// 3. Fetch third row (batchSize=1) -> since rows.done is now true, it should immediately return EOF (empty batch, no error).
	rowBatch3, err := Next(ctx, poolId, connId, rowsId, 1)
	if err != nil {
		t.Fatalf("Third Next() returned unexpected error: %v", err)
	}
	if g, w := len(rowBatch3), 0; g != w {
		t.Fatalf("third row batch count mismatch\nGot:  %v\nWant: %v", g, w)
	}
}

func TestNextBuffered_DoneOnError(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	server, teardown := setupMockServer(t)
	defer teardown()
	dsn := fmt.Sprintf("%s/projects/p/instances/i/databases/d?useplaintext=true", server.Address)

	query := "SELECT * FROM broken_table"
	// Setup query returning 2 rows.
	_ = server.TestSpanner.PutStatementResult(query, &testutil.StatementResult{
		Type:      testutil.StatementResultResultSet,
		ResultSet: testutil.CreateSingleColumnInt64ResultSet([]int64{1, 2}, "c"),
	})

	// Inject error on the second PartialResultSet (ResumeToken = 2).
	server.TestSpanner.AddPartialResultSetError(query, testutil.PartialResultSetExecutionTime{
		ResumeToken: testutil.EncodeResumeToken(2),
		Err:         status.Error(codes.Internal, "broken connection"),
	})

	poolId, err := CreatePool(ctx, "test", dsn)
	if err != nil {
		t.Fatalf("CreatePool returned unexpected error: %v", err)
	}
	defer ClosePool(ctx, poolId)
	connId, err := CreateConnection(ctx, poolId)
	if err != nil {
		t.Fatalf("CreateConnection returned unexpected error: %v", err)
	}
	defer CloseConnection(ctx, poolId, connId)
	rowsId, err := Execute(ctx, poolId, connId, &spannerpb.ExecuteSqlRequest{Sql: query})
	if rowsId == 0 {
		t.Fatal("Execute returned unexpected zero id")
	}
	defer CloseRows(ctx, poolId, connId, rowsId)

	// 1. Fetch first row -> should succeed.
	row, err := NextBuffered(ctx, poolId, connId, rowsId)
	if err != nil {
		t.Fatalf("First NextBuffered() returned unexpected error: %v", err)
	}
	if row == nil {
		t.Fatal("First NextBuffered() returned unexpected nil row")
	}

	// 2. Fetch second row -> should return the injected error.
	_, err = NextBuffered(ctx, poolId, connId, rowsId)
	if err == nil {
		t.Fatal("Second NextBuffered() expected error but got nil")
	}
	if g, w := status.Code(err), codes.Internal; g != w {
		t.Fatalf("error code mismatch\nGot:  %v\nWant: %v", g, w)
	}

	// 3. Fetch third row -> since rows.done is now true, it should immediately return EOF (nil, no error).
	row3, err := NextBuffered(ctx, poolId, connId, rowsId)
	if err != nil {
		t.Fatalf("Third NextBuffered() returned unexpected error: %v", err)
	}
	if g, w := row3, (*structpb.ListValue)(nil); g != w {
		t.Fatalf("expected third NextBuffered() to return nil (EOF)\nGot:  %v\nWant: %v", g, w)
	}
}

func TestRowsNextAutoClose(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	server, teardown := setupMockServer(t)
	defer teardown()
	dsn := fmt.Sprintf("%s/projects/p/instances/i/databases/d?useplaintext=true", server.Address)

	poolId, err := CreatePool(ctx, "test", dsn)
	if err != nil {
		t.Fatalf("CreatePool returned unexpected error: %v", err)
	}
	defer ClosePool(ctx, poolId)

	p, ok := pools.Load(poolId)
	if !ok {
		t.Fatal("pool not found in map")
	}
	pool := p.(*Pool)
	// Force pool max connections to 1. If the connection is leaked on EOF,
	// the next query will block/timeout.
	pool.db.SetMaxOpenConns(1)

	connId, err := CreateConnection(ctx, poolId)
	if err != nil {
		t.Fatalf("CreateConnection returned unexpected error: %v", err)
	}
	defer CloseConnection(ctx, poolId, connId)

	// Execute a SELECT query that returns rows (SelectFooFromBar has 2 rows).
	rowsId, err := Execute(ctx, poolId, connId, &spannerpb.ExecuteSqlRequest{
		Sql: testutil.SelectFooFromBar,
	})
	if err != nil {
		t.Fatalf("First SELECT query failed: %v", err)
	}

	// Read all rows to EOF (so Next returns empty batch).
	rowCount := 0
	for {
		rowBatch, err := Next(ctx, poolId, connId, rowsId, 1)
		if err != nil {
			t.Fatalf("Next failed: %v", err)
		}
		if len(rowBatch) == 0 {
			break
		}
		rowCount += len(rowBatch)
	}
	if rowCount != 2 {
		t.Fatalf("Expected 2 rows, got: %d", rowCount)
	}

	// At this point, rows.Next() hit EOF and should have automatically closed
	// the underlying database iterator, releasing the connection back to the pool.
	// Let's verify by executing a second query.
	// If the connection was leaked, this call will hang or fail (since MaxOpenConns = 1).
	secondRowsId, err := Execute(ctx, poolId, connId, &spannerpb.ExecuteSqlRequest{
		Sql: testutil.SelectFooFromBar,
	})
	if err != nil {
		t.Fatalf("Second SELECT query failed (likely connection leak on EOF): %v", err)
	}
	_ = CloseRows(ctx, poolId, connId, secondRowsId)
}

func TestExecuteWithBooleanStringParams(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	server, teardown := setupMockServer(t)
	defer teardown()

	sqlText := "SELECT * FROM users WHERE active = @p1"
	_ = server.TestSpanner.PutStatementResult(sqlText, &testutil.StatementResult{
		Type:      testutil.StatementResultResultSet,
		ResultSet: testutil.CreateSingleColumnResultSet([]bool{true}, func(b bool) *structpb.Value { return structpb.NewBoolValue(b) }, "active", spannerpb.TypeCode_BOOL),
	})

	dsn := fmt.Sprintf("%s/projects/p/instances/i/databases/d?useplaintext=true", server.Address)
	poolId, err := CreatePool(ctx, "test", dsn)
	if err != nil {
		t.Fatalf("CreatePool returned unexpected error: %v", err)
	}
	defer ClosePool(ctx, poolId)

	connId, err := CreateConnection(ctx, poolId)
	if err != nil {
		t.Fatalf("CreateConnection returned unexpected error: %v", err)
	}
	defer CloseConnection(ctx, poolId, connId)

	rowsId, err := Execute(ctx, poolId, connId, &spannerpb.ExecuteSqlRequest{
		Sql: sqlText,
		Params: &structpb.Struct{
			Fields: map[string]*structpb.Value{
				"p1": structpb.NewStringValue("t"),
			},
		},
		ParamTypes: map[string]*spannerpb.Type{
			"p1": {Code: spannerpb.TypeCode_BOOL},
		},
	})
	if err != nil {
		t.Fatalf("Execute returned unexpected error: %v", err)
	}
	if rowsId == 0 {
		t.Fatal("Execute returned unexpected zero id")
	}
	defer CloseRows(ctx, poolId, connId, rowsId)

	requests := server.TestSpanner.DrainRequestsFromServer()
	executeRequests := testutil.RequestsOfType(requests, reflect.TypeOf(&spannerpb.ExecuteSqlRequest{}))
	if g, w := len(executeRequests), 1; g != w {
		t.Fatalf("num ExecuteSql requests mismatch\nGot:  %d\nWant: %d", g, w)
	}
	receivedReq := executeRequests[0].(*spannerpb.ExecuteSqlRequest)
	if receivedReq.Params == nil || receivedReq.Params.Fields["p1"] == nil {
		t.Fatal("receivedReq missing p1 parameter")
	}
	if g, w := receivedReq.Params.Fields["p1"].GetBoolValue(), true; g != w {
		t.Errorf("receivedReq p1 boolean value mismatch\nGot:  %v\nWant: %v", g, w)
	}
}
