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
	"cloud.google.com/go/spanner/admin/database/apiv1/databasepb"
	"cloud.google.com/go/spanner/apiv1/spannerpb"
	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/googleapis/go-sql-spanner/testutil"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/emptypb"
)

func TestExecuteDmlBatch(t *testing.T) {
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

	// Execute a DML batch.
	request := &spannerpb.ExecuteBatchDmlRequest{Statements: []*spannerpb.ExecuteBatchDmlRequest_Statement{
		{Sql: testutil.UpdateBarSetFoo},
		{Sql: testutil.UpdateBarSetFoo},
	}}
	resp, err := ExecuteBatch(ctx, poolId, connId, request)
	if err != nil {
		t.Fatalf("ExecuteBatch returned unexpected error: %v", err)
	}
	if g, w := len(resp.ResultSets), 2; g != w {
		t.Fatalf("num results mismatch\n Got: %d\nWant: %d", g, w)
	}
	for i, result := range resp.ResultSets {
		if g, w := result.Stats.GetRowCountExact(), int64(testutil.UpdateBarSetFooRowCount); g != w {
			t.Fatalf("%d: update count mismatch\n Got: %d\nWant: %d", i, g, w)
		}
	}

	requests := server.TestSpanner.DrainRequestsFromServer()
	// There should be no ExecuteSql requests.
	executeRequests := testutil.RequestsOfType(requests, reflect.TypeOf(&spannerpb.ExecuteSqlRequest{}))
	if g, w := len(executeRequests), 0; g != w {
		t.Fatalf("Execute request count mismatch\n Got: %v\nWant: %v", g, w)
	}
	batchRequests := testutil.RequestsOfType(requests, reflect.TypeOf(&spannerpb.ExecuteBatchDmlRequest{}))
	if g, w := len(batchRequests), 1; g != w {
		t.Fatalf("Execute batch request count mismatch\n Got: %v\nWant: %v", g, w)
	}

	if err := CloseConnection(ctx, poolId, connId); err != nil {
		t.Fatalf("CloseConnection returned unexpected error: %v", err)
	}
	if err := ClosePool(ctx, poolId); err != nil {
		t.Fatalf("ClosePool returned unexpected error: %v", err)
	}
}

func TestExecuteDdlBatch(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	server, teardown := setupMockServer(t)
	defer teardown()
	dsn := fmt.Sprintf("%s/projects/p/instances/i/databases/d?useplaintext=true", server.Address)
	// Set up a result for a DDL statement on the mock server.
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

	// Execute a DDL batch. This also uses a DML batch request.
	request := &spannerpb.ExecuteBatchDmlRequest{Statements: []*spannerpb.ExecuteBatchDmlRequest_Statement{
		{Sql: "create table my_table (id int64 primary key, value string(100))"},
		{Sql: "create index my_index on my_table (value)"},
	}}
	resp, err := ExecuteBatch(ctx, poolId, connId, request)
	if err != nil {
		t.Fatalf("ExecuteBatch returned unexpected error: %v", err)
	}
	// The response should contain an 'update count' per DDL statement.
	if g, w := len(resp.ResultSets), 2; g != w {
		t.Fatalf("num results mismatch\n Got: %d\nWant: %d", g, w)
	}
	// There is no update count for DDL statements.
	for i, result := range resp.ResultSets {
		emptyStats := &spannerpb.ResultSetStats{}
		if g, w := result.Stats, emptyStats; !cmp.Equal(g, w, cmpopts.IgnoreUnexported(spannerpb.ResultSetStats{})) {
			t.Fatalf("%d: ResultSetStats mismatch\n Got: %v\nWant: %v", i, g, w)
		}
	}

	requests := server.TestSpanner.DrainRequestsFromServer()
	// There should be no ExecuteSql requests.
	executeRequests := testutil.RequestsOfType(requests, reflect.TypeOf(&spannerpb.ExecuteSqlRequest{}))
	if g, w := len(executeRequests), 0; g != w {
		t.Fatalf("Execute request count mismatch\n Got: %v\nWant: %v", g, w)
	}
	// There should also be no ExecuteBatchDml requests.
	batchDmlRequests := testutil.RequestsOfType(requests, reflect.TypeOf(&spannerpb.ExecuteBatchDmlRequest{}))
	if g, w := len(batchDmlRequests), 0; g != w {
		t.Fatalf("ExecuteBatchDmlRequest count mismatch\n Got: %v\nWant: %v", g, w)
	}

	adminRequests := server.TestDatabaseAdmin.Reqs()
	if g, w := len(adminRequests), 1; g != w {
		t.Fatalf("admin request count mismatch\n Got: %v\nWant: %v", g, w)
	}
	ddlRequest := adminRequests[0].(*databasepb.UpdateDatabaseDdlRequest)
	if g, w := len(ddlRequest.Statements), 2; g != w {
		t.Fatalf("DDL statement count mismatch\n Got: %v\nWant: %v", g, w)
	}

	if err := CloseConnection(ctx, poolId, connId); err != nil {
		t.Fatalf("CloseConnection returned unexpected error: %v", err)
	}
	if err := ClosePool(ctx, poolId); err != nil {
		t.Fatalf("ClosePool returned unexpected error: %v", err)
	}
}

func TestExecuteCreateDatabaseInBatch(t *testing.T) {
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

	// If a batch starts with 'CREATE DATABASE', then the following semantics are applied:
	// 1. A CreateDatabase operation is executed.
	// 2. All following DDL statements in the batch are added to the CreateDatabase operation.
	for _, extraStatements := range [][]string{{}, {"create table foo (id int64 primary key, value string(max))", "create index bar on foo (value)"}} {
		// Set up a result for a CreateDatabase operation on the mock server.
		var expectedResponse = &databasepb.Database{}
		anyMsg, _ := anypb.New(expectedResponse)
		server.TestDatabaseAdmin.SetResps([]proto.Message{
			&longrunningpb.Operation{
				Done:   true,
				Result: &longrunningpb.Operation_Response{Response: anyMsg},
				Name:   "test-operation",
			},
		})

		// Execute a DDL batch. This also uses a DML batch request.
		statements := make([]*spannerpb.ExecuteBatchDmlRequest_Statement, 0, len(extraStatements)+1)
		statements = append(statements, &spannerpb.ExecuteBatchDmlRequest_Statement{Sql: "create database `my-database`"})
		for _, ddl := range extraStatements {
			statements = append(statements, &spannerpb.ExecuteBatchDmlRequest_Statement{Sql: ddl})
		}
		request := &spannerpb.ExecuteBatchDmlRequest{Statements: statements}
		resp, err := ExecuteBatch(ctx, poolId, connId, request)
		if err != nil {
			t.Fatalf("ExecuteBatch returned unexpected error: %v", err)
		}
		// The response should contain an 'update count' per DDL statement.
		if g, w := len(resp.ResultSets), 1+len(extraStatements); g != w {
			t.Fatalf("num results mismatch\n Got: %d\nWant: %d", g, w)
		}
		// There is no update count for DDL statements.
		for i, result := range resp.ResultSets {
			emptyStats := &spannerpb.ResultSetStats{}
			if g, w := result.Stats, emptyStats; !cmp.Equal(g, w, cmpopts.IgnoreUnexported(spannerpb.ResultSetStats{})) {
				t.Fatalf("%d: ResultSetStats mismatch\n Got: %v\nWant: %v", i, g, w)
			}
		}

		requests := server.TestSpanner.DrainRequestsFromServer()
		// There should be no ExecuteSql requests.
		executeRequests := testutil.RequestsOfType(requests, reflect.TypeOf(&spannerpb.ExecuteSqlRequest{}))
		if g, w := len(executeRequests), 0; g != w {
			t.Fatalf("Execute request count mismatch\n Got: %v\nWant: %v", g, w)
		}
		// There should also be no ExecuteBatchDml requests.
		batchDmlRequests := testutil.RequestsOfType(requests, reflect.TypeOf(&spannerpb.ExecuteBatchDmlRequest{}))
		if g, w := len(batchDmlRequests), 0; g != w {
			t.Fatalf("ExecuteBatchDmlRequest count mismatch\n Got: %v\nWant: %v", g, w)
		}

		adminRequests := server.TestDatabaseAdmin.Reqs()
		server.TestDatabaseAdmin.SetReqs([]proto.Message{})
		if g, w := len(adminRequests), 1; g != w {
			t.Fatalf("admin request count mismatch\n Got: %v\nWant: %v", g, w)
		}
		createRequest, ok := adminRequests[0].(*databasepb.CreateDatabaseRequest)
		if !ok {
			t.Fatal("request is not a CreateDatabaseRequest")
		}
		if g, w := len(createRequest.ExtraStatements), len(extraStatements); g != w {
			t.Fatalf("DDL statement count mismatch\n Got: %v\nWant: %v", g, w)
		}
	}

	if err := CloseConnection(ctx, poolId, connId); err != nil {
		t.Fatalf("CloseConnection returned unexpected error: %v", err)
	}
	if err := ClosePool(ctx, poolId); err != nil {
		t.Fatalf("ClosePool returned unexpected error: %v", err)
	}
}

func TestExecuteMixedBatch(t *testing.T) {
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

	// Try to execute a batch with mixed DML and DDL statements. This should fail.
	request := &spannerpb.ExecuteBatchDmlRequest{Statements: []*spannerpb.ExecuteBatchDmlRequest_Statement{
		{Sql: "create table my_table (id int64 primary key, value string(100))"},
		{Sql: "update my_table set value = 100 where true"},
	}}
	_, err = ExecuteBatch(ctx, poolId, connId, request)
	if g, w := spanner.ErrCode(err), codes.InvalidArgument; g != w {
		t.Fatalf("error code mismatch\n Got: %v\nWant: %v", g, w)
	}

	if err := CloseConnection(ctx, poolId, connId); err != nil {
		t.Fatalf("CloseConnection returned unexpected error: %v", err)
	}
	if err := ClosePool(ctx, poolId); err != nil {
		t.Fatalf("ClosePool returned unexpected error: %v", err)
	}
}

func TestExecuteDdlBatchInTransaction(t *testing.T) {
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
	if err := BeginTransaction(ctx, poolId, connId, &spannerpb.TransactionOptions{}); err != nil {
		t.Fatalf("BeginTransaction returned unexpected error: %v", err)
	}

	// Try to execute a DDL batch in a transaction. This should fail.
	request := &spannerpb.ExecuteBatchDmlRequest{Statements: []*spannerpb.ExecuteBatchDmlRequest_Statement{
		{Sql: "create table my_table (id int64 primary key, value string(100))"},
		{Sql: "create index my_index on my_table (value)"},
	}}
	_, err = ExecuteBatch(ctx, poolId, connId, request)
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

func TestExecuteQueryInBatch(t *testing.T) {
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

	// Try to execute a batch with queries. This should fail.
	request := &spannerpb.ExecuteBatchDmlRequest{Statements: []*spannerpb.ExecuteBatchDmlRequest_Statement{
		{Sql: "select 1"},
		{Sql: "select 2"},
	}}
	_, err = ExecuteBatch(ctx, poolId, connId, request)
	if g, w := spanner.ErrCode(err), codes.InvalidArgument; g != w {
		t.Fatalf("error code mismatch\n Got: %v\nWant: %v", g, w)
	}
	if g, w := err.Error(), "rpc error: code = InvalidArgument desc = unsupported statement for batching: select 1"; g != w {
		t.Fatalf("error message mismatch\n Got: %v\nWant: %v", g, w)
	}

	if err := CloseConnection(ctx, poolId, connId); err != nil {
		t.Fatalf("CloseConnection returned unexpected error: %v", err)
	}
	if err := ClosePool(ctx, poolId); err != nil {
		t.Fatalf("ClosePool returned unexpected error: %v", err)
	}
}
