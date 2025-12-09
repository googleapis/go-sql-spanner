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
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/emptypb"
)

func TestExecute(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	server, teardown := setupMockServer(t)
	defer teardown()
	dsn := fmt.Sprintf("%s/projects/p/instances/i/databases/d?useplaintext=true", server.Address)

	poolId, err := CreatePool(ctx, dsn)
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

	poolId, err := CreatePool(ctx, dsn)
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
					row, err := Next(ctx, poolId, connId, rowsId)
					if err != nil {
						t.Fatalf("Next returned unexpected error: %v", err)
					}
					if row == nil {
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

	poolId, err := CreatePool(ctx, dsn)
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
	row, err := Next(ctx, poolId, connId, rowsId)
	if err != nil {
		t.Fatalf("Next returned unexpected error: %v", err)
	}
	if row == nil {
		t.Fatal("Next returned unexpected nil row")
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
		row, err := Next(ctx, poolId, connId, rowsId)
		if err != nil {
			t.Fatalf("Next returned unexpected error: %v", err)
		}
		if row == nil {
			t.Fatal("Next returned unexpected nil row")
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

	poolId, err := CreatePool(ctx, dsn)
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

	poolId, err := CreatePool(ctx, dsn)
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
