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

	totalRowCount := 0
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
			totalRowCount++
		}
		if g, w := rowCount, 2; g != w {
			t.Fatalf("row count mismatch\n Got: %d\nWant: %d", g, w)
		}
		metadata, err := NextResultSet(ctx, poolId, connId, rowsId)
		if err != nil {
			t.Fatalf("NextResultSet returned unexpected error: %v", err)
		}
		if metadata == nil {
			break
		}
	}
	if g, w := totalRowCount, 4; g != w {
		t.Fatalf("total row count mismatch\n Got: %d\nWant: %d", g, w)
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
