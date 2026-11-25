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
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/structpb"
)

func TestExecuteDmlBatch(t *testing.T) {
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

	poolId, err := CreatePool(ctx, "test", dsn)
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

	poolId, err := CreatePool(ctx, "test", dsn)
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

	testCases := []struct {
		name            string
		extraStatements []string
	}{
		{
			name:            "without extra statements",
			extraStatements: nil,
		},
		{
			name: "with extra statements",
			extraStatements: []string{
				"create table foo (id int64 primary key, value string(max))",
				"create index bar on foo (value)",
			},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
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
			defer func() {
				server.TestDatabaseAdmin.SetResps([]proto.Message{})
			}()

			// Execute a DDL batch. This also uses a DML batch request.
			statements := make([]*spannerpb.ExecuteBatchDmlRequest_Statement, 0, len(tc.extraStatements)+1)
			statements = append(statements, &spannerpb.ExecuteBatchDmlRequest_Statement{Sql: "create database `my-database`"})
			for _, ddl := range tc.extraStatements {
				statements = append(statements, &spannerpb.ExecuteBatchDmlRequest_Statement{Sql: ddl})
			}
			request := &spannerpb.ExecuteBatchDmlRequest{Statements: statements}
			resp, err := ExecuteBatch(ctx, poolId, connId, request)
			if err != nil {
				t.Fatalf("ExecuteBatch returned unexpected error: %v", err)
			}
			// The response should contain an 'update count' per DDL statement.
			if g, w := len(resp.ResultSets), 1+len(tc.extraStatements); g != w {
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
			if g, w := len(createRequest.ExtraStatements), len(tc.extraStatements); g != w {
				t.Fatalf("DDL statement count mismatch\n Got: %v\nWant: %v", g, w)
			} else if !cmp.Equal(createRequest.ExtraStatements, tc.extraStatements) {
				t.Fatalf("extra statements mismatch\n Got: %v\nWant: %v", createRequest.ExtraStatements, tc.extraStatements)
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

func TestExecuteMixedBatch(t *testing.T) {
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

	// Try to execute a batch with mixed DML and DDL statements. This should fail.
	request := &spannerpb.ExecuteBatchDmlRequest{Statements: []*spannerpb.ExecuteBatchDmlRequest_Statement{
		{Sql: "create table my_table (id int64 primary key, value string(100))"},
		{Sql: "update my_table set value = 100 where true"},
	}}
	_, err = ExecuteBatch(ctx, poolId, connId, request)
	if g, w := spanner.ErrCode(err), codes.InvalidArgument; g != w {
		t.Fatalf("error code mismatch\n Got: %v\nWant: %v", g, w)
	}
	if g, w := err.Error(), "rpc error: code = InvalidArgument desc = Batches may not contain different types of statements. The first statement is of type DDL. The statement on position 2 is of type DML."; g != w {
		t.Fatalf("error message mismatch\n Got: %v\nWant: %v", g, w)
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

	poolId, err := CreatePool(ctx, "test", dsn)
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

	poolId, err := CreatePool(ctx, "test", dsn)
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

func TestExecuteDmlBatchScript(t *testing.T) {
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

	for _, useTx := range []bool{true, false} {
		if useTx {
			if err := BeginTransaction(ctx, poolId, connId, &spannerpb.TransactionOptions{}); err != nil {
				t.Fatalf("BeginTransaction returned unexpected error: %v", err)
			}
		}
		id, err := Execute(ctx, poolId, connId, &spannerpb.ExecuteSqlRequest{Sql: "start batch dml"})
		if err != nil {
			t.Fatalf("START BATCH DML returned unexpected error: %v", err)
		}
		if err := CloseRows(ctx, poolId, connId, id); err != nil {
			t.Fatalf("CloseRows returned unexpected error: %v", err)
		}
		for range 2 {
			id, err := Execute(ctx, poolId, connId, &spannerpb.ExecuteSqlRequest{Sql: testutil.UpdateBarSetFoo})
			if err != nil {
				t.Fatalf("DML returned unexpected error: %v", err)
			}
			if err := CloseRows(ctx, poolId, connId, id); err != nil {
				t.Fatalf("CloseRows returned unexpected error: %v", err)
			}
		}
		id, err = Execute(ctx, poolId, connId, &spannerpb.ExecuteSqlRequest{Sql: "run batch"})
		if err != nil {
			t.Fatalf("RUN BATCH returned unexpected error: %v", err)
		}
		if err := CloseRows(ctx, poolId, connId, id); err != nil {
			t.Fatalf("CloseRows returned unexpected error: %v", err)
		}
		if useTx {
			if _, err := Commit(ctx, poolId, connId); err != nil {
				t.Fatalf("Commit returned unexpected error: %v", err)
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
	}

	if err := CloseConnection(ctx, poolId, connId); err != nil {
		t.Fatalf("CloseConnection returned unexpected error: %v", err)
	}
	if err := ClosePool(ctx, poolId); err != nil {
		t.Fatalf("ClosePool returned unexpected error: %v", err)
	}
}

func TestExecuteDdlBatchScript(t *testing.T) {
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

	poolId, err := CreatePool(ctx, "test", dsn)
	if err != nil {
		t.Fatalf("CreatePool returned unexpected error: %v", err)
	}
	connId, err := CreateConnection(ctx, poolId)
	if err != nil {
		t.Fatalf("CreateConnection returned unexpected error: %v", err)
	}

	id, err := Execute(ctx, poolId, connId, &spannerpb.ExecuteSqlRequest{Sql: "start batch ddl"})
	if err != nil {
		t.Fatalf("START BATCH DDL returned unexpected error: %v", err)
	}
	if err := CloseRows(ctx, poolId, connId, id); err != nil {
		t.Fatalf("CloseRows returned unexpected error: %v", err)
	}

	id, err = Execute(ctx, poolId, connId, &spannerpb.ExecuteSqlRequest{Sql: "create table my_table (id int64 primary key, value string(100))"})
	if err != nil {
		t.Fatalf("DDL returned unexpected error: %v", err)
	}
	if err := CloseRows(ctx, poolId, connId, id); err != nil {
		t.Fatalf("CloseRows returned unexpected error: %v", err)
	}

	id, err = Execute(ctx, poolId, connId, &spannerpb.ExecuteSqlRequest{Sql: "create index my_index on my_table (value)"})
	if err != nil {
		t.Fatalf("DDL returned unexpected error: %v", err)
	}
	if err := CloseRows(ctx, poolId, connId, id); err != nil {
		t.Fatalf("CloseRows returned unexpected error: %v", err)
	}

	id, err = Execute(ctx, poolId, connId, &spannerpb.ExecuteSqlRequest{Sql: "run batch"})
	if err != nil {
		t.Fatalf("RUN BATCH returned unexpected error: %v", err)
	}
	if err := CloseRows(ctx, poolId, connId, id); err != nil {
		t.Fatalf("CloseRows returned unexpected error: %v", err)
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

func assertSubsequentExecuteRequestOptions(t *testing.T, ctx context.Context, poolId, connId int64, server *testutil.MockedSpannerInMemTestServer, wantPriority spannerpb.RequestOptions_Priority) {
	t.Helper()
	rowsId, err := Execute(ctx, poolId, connId, &spannerpb.ExecuteSqlRequest{Sql: testutil.UpdateBarSetFoo})
	if err != nil {
		t.Fatalf("subsequent Execute returned unexpected error: %v", err)
	}
	if err := CloseRows(ctx, poolId, connId, rowsId); err != nil {
		t.Fatalf("CloseRows returned unexpected error: %v", err)
	}
	requests := server.TestSpanner.DrainRequestsFromServer()
	execRequests := testutil.RequestsOfType(requests, reflect.TypeOf(&spannerpb.ExecuteSqlRequest{}))
	if g, w := len(execRequests), 1; g != w {
		t.Fatalf("subsequent ExecuteSqlRequest count mismatch\nGot:  %v\nWant: %v", g, w)
	}
	if g, w := execRequests[0].(*spannerpb.ExecuteSqlRequest).RequestOptions.GetRequestTag(), ""; g != w {
		t.Errorf("leaked request tag\nGot:  %v\nWant: %v", g, w)
	}
	if g, w := execRequests[0].(*spannerpb.ExecuteSqlRequest).RequestOptions.GetTransactionTag(), ""; g != w {
		t.Errorf("leaked transaction tag\nGot:  %v\nWant: %v", g, w)
	}
	if g, w := execRequests[0].(*spannerpb.ExecuteSqlRequest).RequestOptions.GetPriority(), wantPriority; g != w {
		t.Errorf("priority mismatch\nGot:  %v\nWant: %v", g, w)
	}
}

func TestExecuteBatch_RequestOptions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name                string
		inTransaction       bool
		activateBefore      bool
		setBeforeBegin      bool
		setStatementTag     string
		setTransactionTag   string
		setRpcPriority      string
		requestTag          string
		transactionTag      string
		priority            spannerpb.RequestOptions_Priority
		wantReqTagOnBatch   string
		wantTxTagOnBatch    string
		wantPriorityOnBatch spannerpb.RequestOptions_Priority
		wantTxTagOnCommit   string
		wantNextPriority    spannerpb.RequestOptions_Priority
	}{
		{
			name:                "outside transaction with tags and priority",
			inTransaction:       false,
			requestTag:          "batch_req';--",
			transactionTag:      "batch_tx';--",
			priority:            spannerpb.RequestOptions_PRIORITY_MEDIUM,
			wantReqTagOnBatch:   "batch_req';--",
			wantTxTagOnBatch:    "batch_tx';--",
			wantPriorityOnBatch: spannerpb.RequestOptions_PRIORITY_MEDIUM,
			wantTxTagOnCommit:   "batch_tx';--",
		},
		{
			name:                "inside unactivated transaction with tags and priority",
			inTransaction:       true,
			requestTag:          "tx_batch_req",
			transactionTag:      "tx_batch_tx",
			priority:            spannerpb.RequestOptions_PRIORITY_LOW,
			wantReqTagOnBatch:   "tx_batch_req",
			wantTxTagOnBatch:    "tx_batch_tx",
			wantPriorityOnBatch: spannerpb.RequestOptions_PRIORITY_LOW,
			wantTxTagOnCommit:   "tx_batch_tx",
		},
		{
			name:                "inside already activated transaction preserves initial tx tag",
			inTransaction:       true,
			activateBefore:      true,
			requestTag:          "second_batch_req",
			transactionTag:      "ignored_later_tx_tag",
			priority:            spannerpb.RequestOptions_PRIORITY_HIGH,
			wantReqTagOnBatch:   "second_batch_req",
			wantTxTagOnBatch:    "initial_tx_tag",
			wantPriorityOnBatch: spannerpb.RequestOptions_PRIORITY_HIGH,
			wantTxTagOnCommit:   "initial_tx_tag",
		},
		{
			name:                "outside transaction RequestOptions overrides SET tags and RPC_PRIORITY",
			inTransaction:       false,
			setStatementTag:     "set_stmt_tag",
			setTransactionTag:   "set_tx_tag",
			setRpcPriority:      "PRIORITY_LOW",
			requestTag:          "batch_req_override",
			transactionTag:      "batch_tx_override",
			priority:            spannerpb.RequestOptions_PRIORITY_HIGH,
			wantReqTagOnBatch:   "batch_req_override",
			wantTxTagOnBatch:    "batch_tx_override",
			wantPriorityOnBatch: spannerpb.RequestOptions_PRIORITY_HIGH,
			wantTxTagOnCommit:   "batch_tx_override",
			wantNextPriority:    spannerpb.RequestOptions_PRIORITY_LOW,
		},
		{
			name:                "outside transaction empty RequestOptions preserves SET tags and RPC_PRIORITY",
			inTransaction:       false,
			setStatementTag:     "set_stmt_tag",
			setTransactionTag:   "set_tx_tag",
			setRpcPriority:      "PRIORITY_HIGH",
			wantReqTagOnBatch:   "set_stmt_tag",
			wantTxTagOnBatch:    "set_tx_tag",
			wantPriorityOnBatch: spannerpb.RequestOptions_PRIORITY_HIGH,
			wantTxTagOnCommit:   "set_tx_tag",
			wantNextPriority:    spannerpb.RequestOptions_PRIORITY_HIGH,
		},
		{
			name:                "inside unactivated transaction RequestOptions overrides SET tags set before begin",
			inTransaction:       true,
			setBeforeBegin:      true,
			setStatementTag:     "set_stmt_tag",
			setTransactionTag:   "set_tx_tag",
			requestTag:          "batch_req_override",
			transactionTag:      "batch_tx_override",
			priority:            spannerpb.RequestOptions_PRIORITY_LOW,
			wantReqTagOnBatch:   "batch_req_override",
			wantTxTagOnBatch:    "batch_tx_override",
			wantPriorityOnBatch: spannerpb.RequestOptions_PRIORITY_LOW,
			wantTxTagOnCommit:   "batch_tx_override",
		},
		{
			name:                "inside unactivated transaction RequestOptions overrides SET tags set after begin",
			inTransaction:       true,
			setStatementTag:     "set_stmt_tag",
			setTransactionTag:   "set_tx_tag",
			setRpcPriority:      "PRIORITY_LOW",
			requestTag:          "batch_req_override",
			transactionTag:      "batch_tx_override",
			priority:            spannerpb.RequestOptions_PRIORITY_HIGH,
			wantReqTagOnBatch:   "batch_req_override",
			wantTxTagOnBatch:    "batch_tx_override",
			wantPriorityOnBatch: spannerpb.RequestOptions_PRIORITY_HIGH,
			wantTxTagOnCommit:   "batch_tx_override",
			wantNextPriority:    spannerpb.RequestOptions_PRIORITY_LOW,
		},
		{
			name:                "inside unactivated transaction empty RequestOptions preserves SET tags",
			inTransaction:       true,
			setStatementTag:     "set_stmt_tag",
			setTransactionTag:   "set_tx_tag",
			priority:            spannerpb.RequestOptions_PRIORITY_MEDIUM,
			wantReqTagOnBatch:   "set_stmt_tag",
			wantTxTagOnBatch:    "set_tx_tag",
			wantPriorityOnBatch: spannerpb.RequestOptions_PRIORITY_MEDIUM,
			wantTxTagOnCommit:   "set_tx_tag",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			ctx := context.Background()
			server, teardown := setupMockServer(t)
			defer teardown()
			dsn := fmt.Sprintf("%s/projects/p/instances/i/databases/d?useplaintext=true", server.Address)

			poolId, err := CreatePool(ctx, "test", dsn)
			if err != nil {
				t.Fatalf("CreatePool returned unexpected error: %v", err)
			}
			defer func() { _ = ClosePool(ctx, poolId) }()

			connId, err := CreateConnection(ctx, poolId)
			if err != nil {
				t.Fatalf("CreateConnection returned unexpected error: %v", err)
			}

			applySetTags := func() {
				for _, stmt := range []struct {
					name  string
					value string
				}{
					{name: "statement_tag", value: tc.setStatementTag},
					{name: "transaction_tag", value: tc.setTransactionTag},
					{name: "rpc_priority", value: tc.setRpcPriority},
				} {
					if stmt.value == "" {
						continue
					}
					rowsId, err := Execute(ctx, poolId, connId, &spannerpb.ExecuteSqlRequest{
						Sql: fmt.Sprintf("SET %s = '%s'", stmt.name, stmt.value),
					})
					if err != nil {
						t.Fatalf("SET %s returned unexpected error: %v", stmt.name, err)
					}
					if err := CloseRows(ctx, poolId, connId, rowsId); err != nil {
						t.Fatalf("CloseRows returned unexpected error: %v", err)
					}
				}
			}

			if tc.setBeforeBegin {
				applySetTags()
			}
			if tc.inTransaction {
				if err := BeginTransaction(ctx, poolId, connId, &spannerpb.TransactionOptions{}); err != nil {
					t.Fatalf("BeginTransaction returned unexpected error: %v", err)
				}
				if tc.activateBefore {
					rowsId, err := Execute(ctx, poolId, connId, &spannerpb.ExecuteSqlRequest{
						Sql: testutil.UpdateBarSetFoo,
						RequestOptions: &spannerpb.RequestOptions{
							RequestTag:     "initial_req_tag",
							TransactionTag: "initial_tx_tag",
						},
					})
					if err != nil {
						t.Fatalf("Execute returned unexpected error: %v", err)
					}
					if err := CloseRows(ctx, poolId, connId, rowsId); err != nil {
						t.Fatalf("CloseRows returned unexpected error: %v", err)
					}
				}
			}
			if !tc.setBeforeBegin {
				applySetTags()
			}

			request := &spannerpb.ExecuteBatchDmlRequest{
				Statements: []*spannerpb.ExecuteBatchDmlRequest_Statement{
					{Sql: testutil.UpdateBarSetFoo},
					{Sql: testutil.UpdateBarSetFoo},
				},
				RequestOptions: &spannerpb.RequestOptions{
					RequestTag:     tc.requestTag,
					TransactionTag: tc.transactionTag,
					Priority:       tc.priority,
				},
			}
			if _, err := ExecuteBatch(ctx, poolId, connId, request); err != nil {
				t.Fatalf("ExecuteBatch returned unexpected error: %v", err)
			}
			if tc.inTransaction {
				if _, err := Commit(ctx, poolId, connId); err != nil {
					t.Fatalf("Commit returned unexpected error: %v", err)
				}
			}

			requests := server.TestSpanner.DrainRequestsFromServer()
			batchRequests := testutil.RequestsOfType(requests, reflect.TypeOf(&spannerpb.ExecuteBatchDmlRequest{}))
			if g, w := len(batchRequests), 1; g != w {
				t.Fatalf("ExecuteBatchDmlRequest count mismatch\nGot:  %v\nWant: %v", g, w)
			}
			batchReq := batchRequests[0].(*spannerpb.ExecuteBatchDmlRequest)
			if g, w := batchReq.RequestOptions.GetRequestTag(), tc.wantReqTagOnBatch; g != w {
				t.Errorf("request tag mismatch\nGot:  %v\nWant: %v", g, w)
			}
			if g, w := batchReq.RequestOptions.GetTransactionTag(), tc.wantTxTagOnBatch; g != w {
				t.Errorf("transaction tag mismatch\nGot:  %v\nWant: %v", g, w)
			}
			if g, w := batchReq.RequestOptions.GetPriority(), tc.wantPriorityOnBatch; g != w {
				t.Errorf("priority mismatch\nGot:  %v\nWant: %v", g, w)
			}
			commitRequests := testutil.RequestsOfType(requests, reflect.TypeOf(&spannerpb.CommitRequest{}))
			if g, w := len(commitRequests), 1; g != w {
				t.Fatalf("CommitRequest count mismatch\nGot:  %v\nWant: %v", g, w)
			}
			if g, w := commitRequests[0].(*spannerpb.CommitRequest).RequestOptions.GetTransactionTag(), tc.wantTxTagOnCommit; g != w {
				t.Errorf("commit transaction tag mismatch\nGot:  %v\nWant: %v", g, w)
			}

			assertSubsequentExecuteRequestOptions(t, ctx, poolId, connId, server, tc.wantNextPriority)
		})
	}
}

func TestExecuteBatch_NilRequest(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	server, teardown := setupMockServer(t)
	defer teardown()
	dsn := fmt.Sprintf("%s/projects/p/instances/i/databases/d?useplaintext=true", server.Address)

	poolId, err := CreatePool(ctx, "test", dsn)
	if err != nil {
		t.Fatalf("CreatePool returned unexpected error: %v", err)
	}
	defer func() { _ = ClosePool(ctx, poolId) }()

	connId, err := CreateConnection(ctx, poolId)
	if err != nil {
		t.Fatalf("CreateConnection returned unexpected error: %v", err)
	}

	_, err = ExecuteBatch(ctx, poolId, connId, nil)
	if err == nil {
		t.Fatal("expected error for nil batch request, got nil")
	}
	if g, w := status.Code(err), codes.InvalidArgument; g != w {
		t.Errorf("error code mismatch\nGot:  %v\nWant: %v", g, w)
	}
}

func TestExecuteBatch_RequestOptions_ServerErrorNoLeak(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	server, teardown := setupMockServer(t)
	defer teardown()
	dsn := fmt.Sprintf("%s/projects/p/instances/i/databases/d?useplaintext=true", server.Address)

	poolId, err := CreatePool(ctx, "test", dsn)
	if err != nil {
		t.Fatalf("CreatePool returned unexpected error: %v", err)
	}
	defer func() { _ = ClosePool(ctx, poolId) }()

	connId, err := CreateConnection(ctx, poolId)
	if err != nil {
		t.Fatalf("CreateConnection returned unexpected error: %v", err)
	}

	if err := BeginTransaction(ctx, poolId, connId, &spannerpb.TransactionOptions{}); err != nil {
		t.Fatalf("BeginTransaction returned unexpected error: %v", err)
	}
	rowsId, err := Execute(ctx, poolId, connId, &spannerpb.ExecuteSqlRequest{Sql: "SET TRANSACTION_TAG = 'kept_tx_tag'"})
	if err != nil {
		t.Fatalf("SET TRANSACTION_TAG failed: %v", err)
	}
	if err := CloseRows(ctx, poolId, connId, rowsId); err != nil {
		t.Fatalf("CloseRows failed: %v", err)
	}

	failedBatchSql := "UPDATE Bar SET Foo = 999 WHERE Id = 1"
	if err := server.TestSpanner.PutStatementResult(failedBatchSql, &testutil.StatementResult{
		Type: testutil.StatementResultError,
		Err:  status.Error(codes.AlreadyExists, "row already exists"),
	}); err != nil {
		t.Fatalf("PutStatementResult failed: %v", err)
	}

	_, err = ExecuteBatch(ctx, poolId, connId, &spannerpb.ExecuteBatchDmlRequest{
		Statements: []*spannerpb.ExecuteBatchDmlRequest_Statement{{Sql: failedBatchSql}},
		RequestOptions: &spannerpb.RequestOptions{
			RequestTag:     "failed_req_tag",
			TransactionTag: "failed_tx_tag",
			Priority:       spannerpb.RequestOptions_PRIORITY_LOW,
		},
	})
	if err == nil {
		t.Fatal("expected ExecuteBatch error, got nil")
	}
	if g, w := status.Code(err), codes.AlreadyExists; g != w {
		t.Errorf("error code mismatch\nGot:  %v\nWant: %v", g, w)
	}
	if err := Rollback(ctx, poolId, connId); err != nil {
		t.Fatalf("Rollback failed: %v", err)
	}
	_ = server.TestSpanner.DrainRequestsFromServer()

	assertSubsequentExecuteRequestOptions(t, ctx, poolId, connId, server, spannerpb.RequestOptions_PRIORITY_UNSPECIFIED)
}

func TestExecuteBatch_RequestOptions_ReadOnlyTransactionRejected(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	server, teardown := setupMockServer(t)
	defer teardown()
	dsn := fmt.Sprintf("%s/projects/p/instances/i/databases/d?useplaintext=true", server.Address)

	poolId, err := CreatePool(ctx, "test", dsn)
	if err != nil {
		t.Fatalf("CreatePool returned unexpected error: %v", err)
	}
	defer func() { _ = ClosePool(ctx, poolId) }()

	connId, err := CreateConnection(ctx, poolId)
	if err != nil {
		t.Fatalf("CreateConnection returned unexpected error: %v", err)
	}
	if err := BeginTransaction(ctx, poolId, connId, &spannerpb.TransactionOptions{
		Mode: &spannerpb.TransactionOptions_ReadOnly_{ReadOnly: &spannerpb.TransactionOptions_ReadOnly{}},
	}); err != nil {
		t.Fatalf("BeginTransaction failed: %v", err)
	}
	if _, err := ExecuteBatch(ctx, poolId, connId, &spannerpb.ExecuteBatchDmlRequest{
		Statements: []*spannerpb.ExecuteBatchDmlRequest_Statement{{Sql: testutil.UpdateBarSetFoo}},
		RequestOptions: &spannerpb.RequestOptions{
			RequestTag:     "ro_req_tag",
			TransactionTag: "ro_tx_tag",
			Priority:       spannerpb.RequestOptions_PRIORITY_LOW,
		},
	}); err == nil {
		t.Fatal("expected error for ExecuteBatch in read-only transaction, got nil")
	}
	if err := Rollback(ctx, poolId, connId); err != nil {
		t.Fatalf("Rollback failed: %v", err)
	}
	_ = server.TestSpanner.DrainRequestsFromServer()

	assertSubsequentExecuteRequestOptions(t, ctx, poolId, connId, server, spannerpb.RequestOptions_PRIORITY_UNSPECIFIED)
}

func TestExecuteBatch_RequestOptions_AlreadyInBatchRejected(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	server, teardown := setupMockServer(t)
	defer teardown()
	dsn := fmt.Sprintf("%s/projects/p/instances/i/databases/d?useplaintext=true", server.Address)

	poolId, err := CreatePool(ctx, "test", dsn)
	if err != nil {
		t.Fatalf("CreatePool returned unexpected error: %v", err)
	}
	defer func() { _ = ClosePool(ctx, poolId) }()

	connId, err := CreateConnection(ctx, poolId)
	if err != nil {
		t.Fatalf("CreateConnection returned unexpected error: %v", err)
	}

	rowsId, err := Execute(ctx, poolId, connId, &spannerpb.ExecuteSqlRequest{Sql: "START BATCH DML"})
	if err != nil {
		t.Fatalf("START BATCH DML failed: %v", err)
	}
	if err := CloseRows(ctx, poolId, connId, rowsId); err != nil {
		t.Fatalf("CloseRows failed: %v", err)
	}

	_, err = ExecuteBatch(ctx, poolId, connId, &spannerpb.ExecuteBatchDmlRequest{
		Statements: []*spannerpb.ExecuteBatchDmlRequest_Statement{{Sql: testutil.UpdateBarSetFoo}},
		RequestOptions: &spannerpb.RequestOptions{
			RequestTag:     "nested_req_tag",
			TransactionTag: "nested_tx_tag",
			Priority:       spannerpb.RequestOptions_PRIORITY_HIGH,
		},
	})
	if err == nil {
		t.Fatal("expected error when batch already active, got nil")
	}
	if g, w := status.Code(err), codes.FailedPrecondition; g != w {
		t.Errorf("error code mismatch\nGot:  %v\nWant: %v", g, w)
	}

	rowsId, err = Execute(ctx, poolId, connId, &spannerpb.ExecuteSqlRequest{Sql: "ABORT BATCH"})
	if err != nil {
		t.Fatalf("ABORT BATCH failed: %v", err)
	}
	if err := CloseRows(ctx, poolId, connId, rowsId); err != nil {
		t.Fatalf("CloseRows failed: %v", err)
	}
	_ = server.TestSpanner.DrainRequestsFromServer()

	assertSubsequentExecuteRequestOptions(t, ctx, poolId, connId, server, spannerpb.RequestOptions_PRIORITY_UNSPECIFIED)
}

func TestExecuteBatch_RequestOptions_MidBatchExecFailure(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	server, teardown := setupMockServer(t)
	defer teardown()
	dsn := fmt.Sprintf("%s/projects/p/instances/i/databases/d?useplaintext=true", server.Address)

	poolId, err := CreatePool(ctx, "test", dsn)
	if err != nil {
		t.Fatalf("CreatePool returned unexpected error: %v", err)
	}
	defer func() { _ = ClosePool(ctx, poolId) }()

	connId, err := CreateConnection(ctx, poolId)
	if err != nil {
		t.Fatalf("CreateConnection returned unexpected error: %v", err)
	}

	if _, err := ExecuteBatch(ctx, poolId, connId, &spannerpb.ExecuteBatchDmlRequest{
		Statements: []*spannerpb.ExecuteBatchDmlRequest_Statement{
			{Sql: testutil.UpdateBarSetFoo},
			{
				// Missing @missing causes prepareSpannerStmt to fail during ExecContext
				// inside the buffering loop, before RunDmlBatch is called.
				Sql: "UPDATE Bar SET Foo = @p1 WHERE Id = @missing",
				Params: &structpb.Struct{Fields: map[string]*structpb.Value{
					"p1": structpb.NewStringValue("hello"),
				}},
				ParamTypes: map[string]*spannerpb.Type{
					"p1": {Code: spannerpb.TypeCode_STRING},
				},
			},
		},
		RequestOptions: &spannerpb.RequestOptions{
			RequestTag:     "mid_fail_req",
			TransactionTag: "mid_fail_tx",
			Priority:       spannerpb.RequestOptions_PRIORITY_HIGH,
		},
	}); err == nil {
		t.Fatal("expected error for missing parameter during batch buffering, got nil")
	}

	// Verify ExecContext failed before RunDmlBatch sent any ExecuteBatchDmlRequest.
	requests := server.TestSpanner.DrainRequestsFromServer()
	batchRequests := testutil.RequestsOfType(requests, reflect.TypeOf(&spannerpb.ExecuteBatchDmlRequest{}))
	if g, w := len(batchRequests), 0; g != w {
		t.Fatalf("ExecuteBatchDmlRequest count mismatch\nGot:  %v\nWant: %v", g, w)
	}

	// Verify abortBatchQuietly cleaned up the batch so a subsequent Execute succeeds cleanly with no leaked options.
	assertSubsequentExecuteRequestOptions(t, ctx, poolId, connId, server, spannerpb.RequestOptions_PRIORITY_UNSPECIFIED)
}

func TestExecuteBatch_RequestOptions_ParameterizedDml(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	server, teardown := setupMockServer(t)
	defer teardown()
	paramDml := "UPDATE Bar SET Foo = @p1 WHERE Id = @_p2"
	if err := server.TestSpanner.PutStatementResult(paramDml, &testutil.StatementResult{
		Type:        testutil.StatementResultUpdateCount,
		UpdateCount: 3,
	}); err != nil {
		t.Fatalf("PutStatementResult failed: %v", err)
	}
	dsn := fmt.Sprintf("%s/projects/p/instances/i/databases/d?useplaintext=true", server.Address)

	poolId, err := CreatePool(ctx, "test", dsn)
	if err != nil {
		t.Fatalf("CreatePool returned unexpected error: %v", err)
	}
	defer func() { _ = ClosePool(ctx, poolId) }()

	connId, err := CreateConnection(ctx, poolId)
	if err != nil {
		t.Fatalf("CreateConnection returned unexpected error: %v", err)
	}

	resp, err := ExecuteBatch(ctx, poolId, connId, &spannerpb.ExecuteBatchDmlRequest{
		Statements: []*spannerpb.ExecuteBatchDmlRequest_Statement{
			{
				Sql: paramDml,
				Params: &structpb.Struct{Fields: map[string]*structpb.Value{
					"p1":  structpb.NewStringValue("hello"),
					"_p2": structpb.NewStringValue("42"),
				}},
				ParamTypes: map[string]*spannerpb.Type{
					"p1":  {Code: spannerpb.TypeCode_STRING},
					"_p2": {Code: spannerpb.TypeCode_INT64},
				},
			},
		},
		RequestOptions: &spannerpb.RequestOptions{
			RequestTag:     "param_batch_req",
			TransactionTag: "param_batch_tx",
			Priority:       spannerpb.RequestOptions_PRIORITY_HIGH,
		},
	})
	if err != nil {
		t.Fatalf("ExecuteBatch failed: %v", err)
	}
	if g, w := resp.ResultSets[0].Stats.GetRowCountExact(), int64(3); g != w {
		t.Errorf("row count mismatch\nGot:  %v\nWant: %v", g, w)
	}

	requests := server.TestSpanner.DrainRequestsFromServer()
	batchRequests := testutil.RequestsOfType(requests, reflect.TypeOf(&spannerpb.ExecuteBatchDmlRequest{}))
	if g, w := len(batchRequests), 1; g != w {
		t.Fatalf("ExecuteBatchDmlRequest count mismatch\nGot:  %v\nWant: %v", g, w)
	}
	batchReq := batchRequests[0].(*spannerpb.ExecuteBatchDmlRequest)
	if g, w := batchReq.RequestOptions.GetRequestTag(), "param_batch_req"; g != w {
		t.Errorf("request tag mismatch\nGot:  %v\nWant: %v", g, w)
	}
	if g, w := batchReq.RequestOptions.GetTransactionTag(), "param_batch_tx"; g != w {
		t.Errorf("transaction tag mismatch\nGot:  %v\nWant: %v", g, w)
	}
	if g, w := batchReq.RequestOptions.GetPriority(), spannerpb.RequestOptions_PRIORITY_HIGH; g != w {
		t.Errorf("priority mismatch\nGot:  %v\nWant: %v", g, w)
	}
	if g, w := batchReq.Statements[0].Params.GetFields()["p1"].GetStringValue(), "hello"; g != w {
		t.Errorf("p1 mismatch\nGot:  %v\nWant: %v", g, w)
	}
	if g, w := batchReq.Statements[0].Params.GetFields()["_p2"].GetStringValue(), "42"; g != w {
		t.Errorf("_p2 mismatch\nGot:  %v\nWant: %v", g, w)
	}
}

func TestExecuteBatch_RequestOptions_DdlBatch(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	server, teardown := setupMockServer(t)
	defer teardown()
	expectedResponse := &emptypb.Empty{}
	anyMsg, err := anypb.New(expectedResponse)
	if err != nil {
		t.Fatalf("anypb.New failed: %v", err)
	}
	server.TestDatabaseAdmin.SetResps([]proto.Message{
		&longrunningpb.Operation{
			Done:   true,
			Result: &longrunningpb.Operation_Response{Response: anyMsg},
			Name:   "test-operation",
		},
	})
	dsn := fmt.Sprintf("%s/projects/p/instances/i/databases/d?useplaintext=true", server.Address)

	poolId, err := CreatePool(ctx, "test", dsn)
	if err != nil {
		t.Fatalf("CreatePool returned unexpected error: %v", err)
	}
	defer func() { _ = ClosePool(ctx, poolId) }()

	connId, err := CreateConnection(ctx, poolId)
	if err != nil {
		t.Fatalf("CreateConnection returned unexpected error: %v", err)
	}

	if _, err := ExecuteBatch(ctx, poolId, connId, &spannerpb.ExecuteBatchDmlRequest{
		Statements: []*spannerpb.ExecuteBatchDmlRequest_Statement{
			{Sql: "create table t (id int64 primary key)"},
		},
		RequestOptions: &spannerpb.RequestOptions{
			RequestTag:     "ddl_req_tag",
			TransactionTag: "ddl_tx_tag",
			Priority:       spannerpb.RequestOptions_PRIORITY_LOW,
		},
	}); err != nil {
		t.Fatalf("DDL ExecuteBatch failed: %v", err)
	}
	_ = server.TestSpanner.DrainRequestsFromServer()

	assertSubsequentExecuteRequestOptions(t, ctx, poolId, connId, server, spannerpb.RequestOptions_PRIORITY_UNSPECIFIED)
}

func TestExecuteBatch_RequestOptions_DdlBatchFailure(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	server, teardown := setupMockServer(t)
	defer teardown()
	server.TestDatabaseAdmin.SetErr(status.Error(codes.InvalidArgument, "invalid DDL"))
	dsn := fmt.Sprintf("%s/projects/p/instances/i/databases/d?useplaintext=true", server.Address)

	poolId, err := CreatePool(ctx, "test", dsn)
	if err != nil {
		t.Fatalf("CreatePool returned unexpected error: %v", err)
	}
	defer func() { _ = ClosePool(ctx, poolId) }()

	connId, err := CreateConnection(ctx, poolId)
	if err != nil {
		t.Fatalf("CreateConnection returned unexpected error: %v", err)
	}

	if _, err := ExecuteBatch(ctx, poolId, connId, &spannerpb.ExecuteBatchDmlRequest{
		Statements: []*spannerpb.ExecuteBatchDmlRequest_Statement{
			{Sql: "create table t (id int64 primary key)"},
		},
		RequestOptions: &spannerpb.RequestOptions{
			RequestTag:     "ddl_fail_req",
			TransactionTag: "ddl_fail_tx",
			Priority:       spannerpb.RequestOptions_PRIORITY_LOW,
		},
	}); err == nil {
		t.Fatal("expected error from DDL ExecuteBatch, got nil")
	}
	_ = server.TestSpanner.DrainRequestsFromServer()

	assertSubsequentExecuteRequestOptions(t, ctx, poolId, connId, server, spannerpb.RequestOptions_PRIORITY_UNSPECIFIED)
}

func TestExecuteBatch_RequestOptions_InvalidBatchTypeNoLeak(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	server, teardown := setupMockServer(t)
	defer teardown()
	dsn := fmt.Sprintf("%s/projects/p/instances/i/databases/d?useplaintext=true", server.Address)

	poolId, err := CreatePool(ctx, "test", dsn)
	if err != nil {
		t.Fatalf("CreatePool returned unexpected error: %v", err)
	}
	defer func() { _ = ClosePool(ctx, poolId) }()

	connId, err := CreateConnection(ctx, poolId)
	if err != nil {
		t.Fatalf("CreateConnection returned unexpected error: %v", err)
	}

	if _, err := ExecuteBatch(ctx, poolId, connId, &spannerpb.ExecuteBatchDmlRequest{
		Statements: []*spannerpb.ExecuteBatchDmlRequest_Statement{
			{Sql: testutil.SelectFooFromBar},
		},
		RequestOptions: &spannerpb.RequestOptions{
			RequestTag:     "invalid_type_req",
			TransactionTag: "invalid_type_tx",
			Priority:       spannerpb.RequestOptions_PRIORITY_HIGH,
		},
	}); err == nil {
		t.Fatal("expected error for unsupported query statement in ExecuteBatch, got nil")
	}

	assertSubsequentExecuteRequestOptions(t, ctx, poolId, connId, server, spannerpb.RequestOptions_PRIORITY_UNSPECIFIED)
}

func TestExecuteBatch_RequestOptions_AbortedRetry(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	server, teardown := setupMockServer(t)
	defer teardown()
	dsn := fmt.Sprintf("%s/projects/p/instances/i/databases/d?useplaintext=true", server.Address)

	poolId, err := CreatePool(ctx, "test", dsn)
	if err != nil {
		t.Fatalf("CreatePool returned unexpected error: %v", err)
	}
	defer func() { _ = ClosePool(ctx, poolId) }()

	connId, err := CreateConnection(ctx, poolId)
	if err != nil {
		t.Fatalf("CreateConnection returned unexpected error: %v", err)
	}

	server.TestSpanner.PutExecutionTime(testutil.MethodCommitTransaction, testutil.SimulatedExecutionTime{
		Errors: []error{status.Error(codes.Aborted, "Aborted")},
	})

	if err := BeginTransaction(ctx, poolId, connId, &spannerpb.TransactionOptions{}); err != nil {
		t.Fatalf("BeginTransaction failed: %v", err)
	}
	if _, err := ExecuteBatch(ctx, poolId, connId, &spannerpb.ExecuteBatchDmlRequest{
		Statements: []*spannerpb.ExecuteBatchDmlRequest_Statement{{Sql: testutil.UpdateBarSetFoo}},
		RequestOptions: &spannerpb.RequestOptions{
			RequestTag:     "retry_api_req",
			TransactionTag: "retry_api_tx",
			Priority:       spannerpb.RequestOptions_PRIORITY_HIGH,
		},
	}); err != nil {
		t.Fatalf("ExecuteBatch failed: %v", err)
	}
	if _, err := Commit(ctx, poolId, connId); err != nil {
		t.Fatalf("Commit failed: %v", err)
	}

	requests := server.TestSpanner.DrainRequestsFromServer()
	batchRequests := testutil.RequestsOfType(requests, reflect.TypeOf(&spannerpb.ExecuteBatchDmlRequest{}))
	if g, w := len(batchRequests), 2; g != w {
		t.Fatalf("ExecuteBatchDmlRequest count mismatch\nGot:  %v\nWant: %v", g, w)
	}
	for i, req := range batchRequests {
		batchReq := req.(*spannerpb.ExecuteBatchDmlRequest)
		if g, w := batchReq.RequestOptions.GetRequestTag(), "retry_api_req"; g != w {
			t.Errorf("attempt %d request tag mismatch\nGot:  %v\nWant: %v", i, g, w)
		}
		if g, w := batchReq.RequestOptions.GetTransactionTag(), "retry_api_tx"; g != w {
			t.Errorf("attempt %d transaction tag mismatch\nGot:  %v\nWant: %v", i, g, w)
		}
		if g, w := batchReq.RequestOptions.GetPriority(), spannerpb.RequestOptions_PRIORITY_HIGH; g != w {
			t.Errorf("attempt %d priority mismatch\nGot:  %v\nWant: %v", i, g, w)
		}
	}
	commitRequests := testutil.RequestsOfType(requests, reflect.TypeOf(&spannerpb.CommitRequest{}))
	if g, w := len(commitRequests), 2; g != w {
		t.Fatalf("CommitRequest count mismatch\nGot:  %v\nWant: %v", g, w)
	}
	if g, w := commitRequests[1].(*spannerpb.CommitRequest).RequestOptions.GetTransactionTag(), "retry_api_tx"; g != w {
		t.Errorf("commit transaction tag mismatch\nGot:  %v\nWant: %v", g, w)
	}
}

func TestExecute_RequestOptions_Priority(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	server, teardown := setupMockServer(t)
	defer teardown()
	dsn := fmt.Sprintf("%s/projects/p/instances/i/databases/d?useplaintext=true", server.Address)

	poolId, err := CreatePool(ctx, "test", dsn)
	if err != nil {
		t.Fatalf("CreatePool returned unexpected error: %v", err)
	}
	defer func() { _ = ClosePool(ctx, poolId) }()

	connId, err := CreateConnection(ctx, poolId)
	if err != nil {
		t.Fatalf("CreateConnection returned unexpected error: %v", err)
	}

	rowsId, err := Execute(ctx, poolId, connId, &spannerpb.ExecuteSqlRequest{
		Sql: testutil.SelectFooFromBar,
		RequestOptions: &spannerpb.RequestOptions{
			RequestTag: "single_req_tag",
			Priority:   spannerpb.RequestOptions_PRIORITY_HIGH,
		},
	})
	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}
	if err := CloseRows(ctx, poolId, connId, rowsId); err != nil {
		t.Fatalf("CloseRows failed: %v", err)
	}

	rowsId, err = Execute(ctx, poolId, connId, &spannerpb.ExecuteSqlRequest{Sql: testutil.SelectFooFromBar})
	if err != nil {
		t.Fatalf("subsequent Execute failed: %v", err)
	}
	if err := CloseRows(ctx, poolId, connId, rowsId); err != nil {
		t.Fatalf("CloseRows failed: %v", err)
	}

	requests := server.TestSpanner.DrainRequestsFromServer()
	execRequests := testutil.RequestsOfType(requests, reflect.TypeOf(&spannerpb.ExecuteSqlRequest{}))
	if g, w := len(execRequests), 2; g != w {
		t.Fatalf("ExecuteSqlRequest count mismatch\nGot:  %v\nWant: %v", g, w)
	}
	if g, w := execRequests[0].(*spannerpb.ExecuteSqlRequest).RequestOptions.GetPriority(), spannerpb.RequestOptions_PRIORITY_HIGH; g != w {
		t.Errorf("first request priority mismatch\nGot:  %v\nWant: %v", g, w)
	}
	if g, w := execRequests[1].(*spannerpb.ExecuteSqlRequest).RequestOptions.GetPriority(), spannerpb.RequestOptions_PRIORITY_UNSPECIFIED; g != w {
		t.Errorf("leaked priority on second request\nGot:  %v\nWant: %v", g, w)
	}
}
