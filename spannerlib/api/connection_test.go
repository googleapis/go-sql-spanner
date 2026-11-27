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
	"time"

	"cloud.google.com/go/longrunning/autogen/longrunningpb"
	"cloud.google.com/go/spanner"
	"cloud.google.com/go/spanner/admin/database/apiv1/databasepb"
	"cloud.google.com/go/spanner/apiv1/spannerpb"
	"github.com/googleapis/go-sql-spanner/testutil"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/structpb"
)

func TestCreateAndCloseConnection(t *testing.T) {
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
	if connId == 0 {
		t.Fatal("CreateConnection returned unexpected zero id")
	}
	p, ok := pools.Load(poolId)
	if !ok {
		t.Fatal("pool not found in map")
	}
	pool := p.(*Pool)
	if _, ok := pool.connections.Load(connId); !ok {
		t.Fatal("connection not in map")
	}

	requests := server.TestSpanner.DrainRequestsFromServer()
	createSessionRequests := testutil.RequestsOfType(requests, reflect.TypeOf(&spannerpb.CreateSessionRequest{}))
	if g, w := len(createSessionRequests), 1; g != w {
		t.Fatalf("num CreateSession requests mismatch\n Got: %d\nWant: %d", g, w)
	}

	if err := CloseConnection(ctx, poolId, connId); err != nil {
		t.Fatalf("CloseConnection returned unexpected error: %v", err)
	}
	if _, ok := pool.connections.Load(connId); ok {
		t.Fatal("connection still in map")
	}

	if err := ClosePool(ctx, poolId); err != nil {
		t.Fatalf("ClosePool returned unexpected error: %v", err)
	}
	// Closing a connection after its pool has been closed should be a no-op.
	if err := CloseConnection(ctx, poolId, connId); err != nil {
		t.Fatalf("CloseConnection returned unexpected error: %v", err)
	}
}

func TestCreateConnectionWithUnknownPool(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	// Try to create a connection for an unknown pool.
	_, err := CreateConnection(ctx, -1)
	if g, w := spanner.ErrCode(err), codes.NotFound; g != w {
		t.Fatalf("error code mismatch\n Got: %d\nWant: %d", g, w)
	}
}

func TestCreateTwoConnections(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	server, teardown := setupMockServer(t)
	defer teardown()
	dsn := fmt.Sprintf("%s/projects/p/instances/i/databases/d?useplaintext=true", server.Address)

	poolId, err := CreatePool(ctx, "test", dsn)
	if err != nil {
		t.Fatalf("CreatePool returned unexpected error: %v", err)
	}

	for range 2 {
		connId, err := CreateConnection(ctx, poolId)
		if err != nil {
			t.Fatalf("CreateConnection returned unexpected error: %v", err)
		}
		//goland:noinspection GoDeferInLoop
		defer func() { _ = CloseConnection(ctx, poolId, connId) }()
	}

	// Two connections in one pool should only create one multiplexed session.
	requests := server.TestSpanner.DrainRequestsFromServer()
	createSessionRequests := testutil.RequestsOfType(requests, reflect.TypeOf(&spannerpb.CreateSessionRequest{}))
	if g, w := len(createSessionRequests), 1; g != w {
		t.Fatalf("num CreateSession requests mismatch\n Got: %d\nWant: %d", g, w)
	}

	if err := ClosePool(ctx, poolId); err != nil {
		t.Fatalf("ClosePool returned unexpected error: %v", err)
	}
}

func TestCloseConnectionTwice(t *testing.T) {
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

	for range 2 {
		if err := CloseConnection(ctx, poolId, connId); err != nil {
			t.Fatalf("CloseConnection returned unexpected error: %v", err)
		}
	}
	if err := ClosePool(ctx, poolId); err != nil {
		t.Fatalf("ClosePool returned unexpected error: %v", err)
	}
}

func TestWriteMutations(t *testing.T) {
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

	mutations := &spannerpb.BatchWriteRequest_MutationGroup{Mutations: []*spannerpb.Mutation{
		{Operation: &spannerpb.Mutation_Insert{Insert: &spannerpb.Mutation_Write{
			Table:   "my_table",
			Columns: []string{"id", "value"},
			Values: []*structpb.ListValue{
				{Values: []*structpb.Value{structpb.NewStringValue("1"), structpb.NewStringValue("One")}},
				{Values: []*structpb.Value{structpb.NewStringValue("2"), structpb.NewStringValue("Two")}},
				{Values: []*structpb.Value{structpb.NewStringValue("3"), structpb.NewStringValue("Three")}},
			},
		}}},
		{Operation: &spannerpb.Mutation_Update{Update: &spannerpb.Mutation_Write{
			Table:   "my_table",
			Columns: []string{"id", "value"},
			Values: []*structpb.ListValue{
				{Values: []*structpb.Value{structpb.NewStringValue("0"), structpb.NewStringValue("Zero")}},
			},
		}}},
	}}
	resp, err := WriteMutations(ctx, poolId, connId, mutations)
	if err != nil {
		t.Fatalf("WriteMutations returned unexpected error: %v", err)
	}
	if resp.CommitTimestamp == nil {
		t.Fatalf("CommitTimestamp is nil")
	}
	requests := server.TestSpanner.DrainRequestsFromServer()
	beginRequests := testutil.RequestsOfType(requests, reflect.TypeOf(&spannerpb.BeginTransactionRequest{}))
	if g, w := len(beginRequests), 1; g != w {
		t.Fatalf("num BeginTransaction requests mismatch\n Got: %d\nWant: %d", g, w)
	}
	commitRequests := testutil.RequestsOfType(requests, reflect.TypeOf(&spannerpb.CommitRequest{}))
	if g, w := len(commitRequests), 1; g != w {
		t.Fatalf("num CommitRequests mismatch\n Got: %d\nWant: %d", g, w)
	}
	commitRequest := commitRequests[0].(*spannerpb.CommitRequest)
	if g, w := len(commitRequest.Mutations), 2; g != w {
		t.Fatalf("num mutations mismatch\n Got: %d\nWant: %d", g, w)
	}

	// Write the same mutations in a transaction.
	if err := BeginTransaction(ctx, poolId, connId, &spannerpb.TransactionOptions{}); err != nil {
		t.Fatalf("BeginTransaction returned unexpected error: %v", err)
	}
	resp, err = WriteMutations(ctx, poolId, connId, mutations)
	if err != nil {
		t.Fatalf("WriteMutations returned unexpected error: %v", err)
	}
	if resp != nil {
		t.Fatalf("WriteMutations returned unexpected response: %v", resp)
	}
	resp, err = Commit(ctx, poolId, connId)
	if err != nil {
		t.Fatalf("Commit returned unexpected error: %v", err)
	}
	if resp == nil {
		t.Fatalf("Commit returned nil response")
	}
	if resp.CommitTimestamp == nil {
		t.Fatalf("CommitTimestamp is nil")
	}
	requests = server.TestSpanner.DrainRequestsFromServer()
	beginRequests = testutil.RequestsOfType(requests, reflect.TypeOf(&spannerpb.BeginTransactionRequest{}))
	if g, w := len(beginRequests), 1; g != w {
		t.Fatalf("num BeginTransaction requests mismatch\n Got: %d\nWant: %d", g, w)
	}
	commitRequests = testutil.RequestsOfType(requests, reflect.TypeOf(&spannerpb.CommitRequest{}))
	if g, w := len(commitRequests), 1; g != w {
		t.Fatalf("num CommitRequests mismatch\n Got: %d\nWant: %d", g, w)
	}
	commitRequest = commitRequests[0].(*spannerpb.CommitRequest)
	if g, w := len(commitRequest.Mutations), 2; g != w {
		t.Fatalf("num mutations mismatch\n Got: %d\nWant: %d", g, w)
	}

	if err := ClosePool(ctx, poolId); err != nil {
		t.Fatalf("ClosePool returned unexpected error: %v", err)
	}
}

func TestWriteMutationsInReadOnlyTx(t *testing.T) {
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

	// Start a read-only transaction and try to write mutations to that transaction. That should return an error.
	if err := BeginTransaction(ctx, poolId, connId, &spannerpb.TransactionOptions{
		Mode: &spannerpb.TransactionOptions_ReadOnly_{ReadOnly: &spannerpb.TransactionOptions_ReadOnly{}},
	}); err != nil {
		t.Fatalf("BeginTransaction returned unexpected error: %v", err)
	}

	mutations := &spannerpb.BatchWriteRequest_MutationGroup{Mutations: []*spannerpb.Mutation{
		{Operation: &spannerpb.Mutation_Insert{Insert: &spannerpb.Mutation_Write{
			Table:   "my_table",
			Columns: []string{"id", "value"},
			Values: []*structpb.ListValue{
				{Values: []*structpb.Value{structpb.NewStringValue("1"), structpb.NewStringValue("One")}},
			},
		}}},
	}}
	_, err = WriteMutations(ctx, poolId, connId, mutations)
	if g, w := spanner.ErrCode(err), codes.FailedPrecondition; g != w {
		t.Fatalf("WriteMutations error code mismatch\n Got: %d\nWant: %d", g, w)
	}

	// Committing the read-only transaction should not lead to any commits on Spanner.
	_, err = Commit(ctx, poolId, connId)
	if err != nil {
		t.Fatalf("Commit returned unexpected error: %v", err)
	}
	requests := server.TestSpanner.DrainRequestsFromServer()
	// There should also not be any BeginTransaction requests on Spanner, as the transaction was never really started
	// by a query or other statement.
	beginRequests := testutil.RequestsOfType(requests, reflect.TypeOf(&spannerpb.BeginTransactionRequest{}))
	if g, w := len(beginRequests), 0; g != w {
		t.Fatalf("num BeginTransaction requests mismatch\n Got: %d\nWant: %d", g, w)
	}
	commitRequests := testutil.RequestsOfType(requests, reflect.TypeOf(&spannerpb.CommitRequest{}))
	if g, w := len(commitRequests), 0; g != w {
		t.Fatalf("num CommitRequests mismatch\n Got: %d\nWant: %d", g, w)
	}

	if err := ClosePool(ctx, poolId); err != nil {
		t.Fatalf("ClosePool returned unexpected error: %v", err)
	}
}

func TestTags(t *testing.T) {
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
	if err := BeginTransaction(ctx, poolId, connId, nil); err != nil {
		t.Fatalf("BeginTransaction returned unexpected error: %v", err)
	}
	for i := 0; i < 2; i++ {
		if _, err = Execute(ctx, poolId, connId, &spannerpb.ExecuteSqlRequest{
			Sql: testutil.UpdateBarSetFoo,
			RequestOptions: &spannerpb.RequestOptions{
				RequestTag:     fmt.Sprintf("my_request_tag_%d", i),
				TransactionTag: "my_transaction_tag",
			},
		}); err != nil {
			t.Fatalf("Execute returned unexpected error: %v", err)
		}
	}
	if _, err := Commit(ctx, poolId, connId); err != nil {
		t.Fatalf("Commit returned unexpected error: %v", err)
	}

	requests := server.TestSpanner.DrainRequestsFromServer()
	executeRequests := testutil.RequestsOfType(requests, reflect.TypeOf(&spannerpb.ExecuteSqlRequest{}))
	if g, w := len(executeRequests), 2; g != w {
		t.Fatalf("num ExecuteSql requests mismatch\n Got: %d\nWant: %d", g, w)
	}
	for i, request := range executeRequests {
		if g, w := request.(*spannerpb.ExecuteSqlRequest).RequestOptions.RequestTag, fmt.Sprintf("my_request_tag_%d", i); g != w {
			t.Fatalf("ExecuteSql request tag mismatch\n Got: %v\nWant: %v", g, w)
		}
		if g, w := request.(*spannerpb.ExecuteSqlRequest).RequestOptions.TransactionTag, "my_transaction_tag"; g != w {
			t.Fatalf("ExecuteSql transaction tag mismatch\n Got: %v\nWant: %v", g, w)
		}
	}
	commitRequests := testutil.RequestsOfType(requests, reflect.TypeOf(&spannerpb.CommitRequest{}))
	if g, w := len(commitRequests), 1; g != w {
		t.Fatalf("num CommitRequests mismatch\n Got: %d\nWant: %d", g, w)
	}
	commitRequest := commitRequests[0].(*spannerpb.CommitRequest)
	if g, w := commitRequest.RequestOptions.TransactionTag, "my_transaction_tag"; g != w {
		t.Fatalf("Commit transaction tag mismatch\n Got: %v\nWant: %v", g, w)
	}

	if err := ClosePool(ctx, poolId); err != nil {
		t.Fatalf("ClosePool returned unexpected error: %v", err)
	}
}

func TestStaleQuery(t *testing.T) {
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
	_, err = Execute(ctx, poolId, connId, &spannerpb.ExecuteSqlRequest{
		Sql: testutil.SelectFooFromBar,
		Transaction: &spannerpb.TransactionSelector{
			Selector: &spannerpb.TransactionSelector_SingleUse{
				SingleUse: &spannerpb.TransactionOptions{
					Mode: &spannerpb.TransactionOptions_ReadOnly_{
						ReadOnly: &spannerpb.TransactionOptions_ReadOnly{
							TimestampBound: &spannerpb.TransactionOptions_ReadOnly_MaxStaleness{
								MaxStaleness: &durationpb.Duration{Seconds: 15},
							},
						},
					},
				},
			},
		},
	})

	requests := server.TestSpanner.DrainRequestsFromServer()
	executeRequests := testutil.RequestsOfType(requests, reflect.TypeOf(&spannerpb.ExecuteSqlRequest{}))
	if g, w := len(executeRequests), 1; g != w {
		t.Fatalf("num ExecuteSql requests mismatch\n Got: %d\nWant: %d", g, w)
	}
	request := executeRequests[0].(*spannerpb.ExecuteSqlRequest)
	if g, w := request.Transaction.GetSingleUse().GetReadOnly().GetMaxStaleness().GetSeconds(), int64(15); g != w {
		t.Fatalf("staleness mismatch\n Got: %v\nWant: %v", g, w)
	}

	if err := ClosePool(ctx, poolId); err != nil {
		t.Fatalf("ClosePool returned unexpected error: %v", err)
	}
}

func TestCreateDatabase(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	server, teardown := setupMockServer(t)
	defer teardown()
	dsn := fmt.Sprintf("%s/projects/p/instances/i/databases/d?useplaintext=true", server.Address)

	// Add a CreateDatabase response to the mock server.
	var expectedResponse = &databasepb.Database{}
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

	for _, tt := range []struct {
		name       string
		statements *spannerpb.ExecuteBatchDmlRequest
	}{
		{
			name: "create_database",
			statements: &spannerpb.ExecuteBatchDmlRequest{
				Statements: []*spannerpb.ExecuteBatchDmlRequest_Statement{
					{Sql: "CREATE DATABASE my_db"},
				},
			},
		},
		{
			name: "create_database_with_extra_statements",
			statements: &spannerpb.ExecuteBatchDmlRequest{
				Statements: []*spannerpb.ExecuteBatchDmlRequest_Statement{
					{Sql: "CREATE DATABASE my_db"},
					{Sql: "CREATE TABLE my_table (id int64 primary key, value string(max))"},
					{Sql: "CREATE INDEX my_index ON my_table (value)"},
				},
			},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			_, err = ExecuteBatch(ctx, poolId, connId, tt.statements)
			if err != nil {
				t.Fatalf("CreateDatabase returned unexpected error: %v", err)
			}

			requests := server.TestDatabaseAdmin.Reqs()
			if g, w := len(requests), 1; g != w {
				t.Fatalf("requests count mismatch\nGot: %v\nWant: %v", g, w)
			}
			if req, ok := requests[0].(*databasepb.CreateDatabaseRequest); ok {
				if g, w := req.Parent, "projects/p/instances/i"; g != w {
					t.Fatalf("parent mismatch\n Got: %v\nWant: %v", g, w)
				}
				if g, w := len(req.ExtraStatements), len(tt.statements.Statements)-1; g != w {
					t.Fatalf("extra statements count mismatch\n Got: %v\nWant: %v", g, w)
				}
			} else {
				t.Fatalf("request type mismatch, got %v", requests[0])
			}
			server.TestDatabaseAdmin.SetReqs(make([]proto.Message, 0))
		})
	}

	if err := ClosePool(ctx, poolId); err != nil {
		t.Fatalf("ClosePool returned unexpected error: %v", err)
	}
}

func TestDMLDoesNotLeakConnection(t *testing.T) {
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
	// Force pool max connections to 1. If any DML query leaks a connection,
	// the next query execution will hang/fail.
	pool.db.SetMaxOpenConns(1)

	connId, err := CreateConnection(ctx, poolId)
	if err != nil {
		t.Fatalf("CreateConnection returned unexpected error: %v", err)
	}
	defer CloseConnection(ctx, poolId, connId)

	// 1. Execute a DML statement (UpdateBarSetFoo has no fields, returns 0 rows).
	rowsId, err := Execute(ctx, poolId, connId, &spannerpb.ExecuteSqlRequest{
		Sql: testutil.UpdateBarSetFoo,
	})
	if err != nil {
		t.Fatalf("First DML execute failed: %v", err)
	}
	_ = CloseRows(ctx, poolId, connId, rowsId)

	// 2. Execute a regular SELECT query on the same connection.
	// If the connection was leaked, this call will block indefinitely or fail.
	selectRowsId, err := Execute(ctx, poolId, connId, &spannerpb.ExecuteSqlRequest{
		Sql: testutil.SelectFooFromBar,
	})
	if err != nil {
		t.Fatalf("Second SELECT query failed (likely connection leak): %v", err)
	}
	_ = CloseRows(ctx, poolId, connId, selectRowsId)
}

func TestConnectionCloseCancelsContext(t *testing.T) {
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

	// Put a 5-second simulated minimum delay on ExecuteStreamingSql calls
	// so the query execution blocks.
	server.TestSpanner.PutExecutionTime(testutil.MethodExecuteStreamingSql, testutil.SimulatedExecutionTime{
		MinimumExecutionTime: 5 * time.Second,
	})

	errChan := make(chan error, 1)
	go func() {
		// Run a query that will block.
		_, err := Execute(context.Background(), poolId, connId, &spannerpb.ExecuteSqlRequest{
			Sql: testutil.SelectFooFromBar,
		})
		errChan <- err
	}()

	// Wait 100ms for the query to start and block.
	time.Sleep(100 * time.Millisecond)

	// Close the connection with a 1-second timeout.
	closeCtx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	closeErrChan := make(chan error, 1)
	go func() {
		closeErrChan <- CloseConnection(closeCtx, poolId, connId)
	}()

	// CloseConnection should return quickly (within the 1-second timeout context)
	// and NOT hang waiting for the 5-second query.
	select {
	case closeErr := <-closeErrChan:
		if closeErr != nil {
			t.Fatalf("CloseConnection failed: %v", closeErr)
		}
	case <-time.After(1500 * time.Millisecond):
		t.Fatal("CloseConnection hung/timed out!")
	}

	// Verify that the blocked query was aborted/canceled.
	select {
	case err := <-errChan:
		if err == nil {
			t.Fatal("Blocked query completed successfully, expected cancel error")
		}
		// The error should contain context canceled.
		if g, w := spanner.ErrCode(err), codes.Canceled; g != w {
			t.Fatalf("error code mismatch\nGot:  %v\nWant: %v", g, w)
		}
	case <-time.After(1 * time.Second):
		t.Fatal("Blocked query didn't cancel!")
	}
}

func TestApiNilArguments(t *testing.T) {
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

	tests := []struct {
		name     string
		call     func() error
		wantCode codes.Code
	}{
		{
			name: "WriteMutations nil mutations",
			call: func() error {
				_, err := WriteMutations(ctx, poolId, connId, nil)
				return err
			},
			wantCode: codes.InvalidArgument,
		},
		{
			name: "WriteMutations nil mutation element",
			call: func() error {
				_, err := WriteMutations(ctx, poolId, connId, &spannerpb.BatchWriteRequest_MutationGroup{
					Mutations: []*spannerpb.Mutation{nil},
				})
				return err
			},
			wantCode: codes.InvalidArgument,
		},
		{
			name: "Execute nil request",
			call: func() error {
				_, err := Execute(ctx, poolId, connId, nil)
				return err
			},
			wantCode: codes.InvalidArgument,
		},
		{
			name: "ExecuteBatch nil request",
			call: func() error {
				_, err := ExecuteBatch(ctx, poolId, connId, nil)
				return err
			},
			wantCode: codes.InvalidArgument,
		},
		{
			name: "ExecuteBatch nil statement element",
			call: func() error {
				_, err := ExecuteBatch(ctx, poolId, connId, &spannerpb.ExecuteBatchDmlRequest{
					Statements: []*spannerpb.ExecuteBatchDmlRequest_Statement{nil},
				})
				return err
			},
			wantCode: codes.InvalidArgument,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.call()
			if g, w := spanner.ErrCode(err), tc.wantCode; g != w {
				t.Errorf("error code mismatch\nGot:  %v\nWant: %v (err: %v)", g, w, err)
			}
		})
	}
}

func setupPoolAndConnection(t *testing.T, ctx context.Context, dsn string) (int64, int64) {
	t.Helper()
	poolId, err := CreatePool(ctx, "test", dsn)
	if err != nil {
		t.Fatalf("CreatePool returned unexpected error: %v", err)
	}
	t.Cleanup(func() { _ = ClosePool(ctx, poolId) })
	connId, err := CreateConnection(ctx, poolId)
	if err != nil {
		t.Fatalf("CreateConnection returned unexpected error: %v", err)
	}
	return poolId, connId
}

func insertTestMutations() *spannerpb.BatchWriteRequest_MutationGroup {
	return &spannerpb.BatchWriteRequest_MutationGroup{Mutations: []*spannerpb.Mutation{
		{Operation: &spannerpb.Mutation_Insert{Insert: &spannerpb.Mutation_Write{
			Table:   "my_table",
			Columns: []string{"id", "value"},
			Values: []*structpb.ListValue{
				{Values: []*structpb.Value{structpb.NewStringValue("1"), structpb.NewStringValue("One")}},
			},
		}}},
	}}
}

func TestWriteMutationsWithOptions(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	server, teardown := setupMockServer(t)
	defer teardown()
	dsn := fmt.Sprintf("%s/projects/p/instances/i/databases/d?useplaintext=true", server.Address)
	poolId, connId := setupPoolAndConnection(t, ctx, dsn)

	mutations := insertTestMutations()
	reqOpts := &spannerpb.RequestOptions{
		TransactionTag: "mut_tx_tag';--",
		Priority:       spannerpb.RequestOptions_PRIORITY_LOW,
	}
	resp, err := WriteMutationsWithOptions(ctx, poolId, connId, mutations, reqOpts)
	if err != nil {
		t.Fatalf("WriteMutationsWithOptions returned unexpected error: %v", err)
	}
	if resp == nil || resp.CommitTimestamp == nil {
		t.Fatalf("expected CommitTimestamp, got: %v", resp)
	}
	requests := server.TestSpanner.DrainRequestsFromServer()
	commitRequests := testutil.RequestsOfType(requests, reflect.TypeOf(&spannerpb.CommitRequest{}))
	if g, w := len(commitRequests), 1; g != w {
		t.Fatalf("CommitRequest count mismatch\nGot:  %v\nWant: %v", g, w)
	}
	commitRequest := commitRequests[0].(*spannerpb.CommitRequest)
	if g, w := commitRequest.RequestOptions.GetTransactionTag(), "mut_tx_tag';--"; g != w {
		t.Errorf("commit transaction tag mismatch\nGot:  %v\nWant: %v", g, w)
	}
	if g, w := commitRequest.RequestOptions.GetPriority(), spannerpb.RequestOptions_PRIORITY_LOW; g != w {
		t.Errorf("commit priority mismatch\nGot:  %v\nWant: %v", g, w)
	}

	// Write mutations inside a transaction with RequestOptions.
	if err := BeginTransaction(ctx, poolId, connId, &spannerpb.TransactionOptions{}); err != nil {
		t.Fatalf("BeginTransaction returned unexpected error: %v", err)
	}
	resp, err = WriteMutationsWithOptions(ctx, poolId, connId, mutations, &spannerpb.RequestOptions{
		TransactionTag: "tx_mut_tag",
		Priority:       spannerpb.RequestOptions_PRIORITY_MEDIUM,
	})
	if err != nil {
		t.Fatalf("WriteMutationsWithOptions in tx returned unexpected error: %v", err)
	}
	if resp != nil {
		t.Fatalf("expected nil response when buffering in tx, got: %v", resp)
	}
	if _, err := Commit(ctx, poolId, connId); err != nil {
		t.Fatalf("Commit returned unexpected error: %v", err)
	}
	requests = server.TestSpanner.DrainRequestsFromServer()
	commitRequests = testutil.RequestsOfType(requests, reflect.TypeOf(&spannerpb.CommitRequest{}))
	if g, w := len(commitRequests), 1; g != w {
		t.Fatalf("CommitRequest count mismatch\nGot:  %v\nWant: %v", g, w)
	}
	commitRequest = commitRequests[0].(*spannerpb.CommitRequest)
	if g, w := commitRequest.RequestOptions.GetTransactionTag(), "tx_mut_tag"; g != w {
		t.Errorf("tx commit transaction tag mismatch\nGot:  %v\nWant: %v", g, w)
	}
	if g, w := commitRequest.RequestOptions.GetPriority(), spannerpb.RequestOptions_PRIORITY_MEDIUM; g != w {
		t.Errorf("tx commit priority mismatch\nGot:  %v\nWant: %v", g, w)
	}
}

func TestBeginTransactionWithOptions(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	server, teardown := setupMockServer(t)
	defer teardown()
	dsn := fmt.Sprintf("%s/projects/p/instances/i/databases/d?useplaintext=true", server.Address)
	poolId, connId := setupPoolAndConnection(t, ctx, dsn)

	if err := BeginTransactionWithOptions(ctx, poolId, connId, &spannerpb.TransactionOptions{}, &spannerpb.RequestOptions{
		TransactionTag: "begin_tx_tag';--",
		Priority:       spannerpb.RequestOptions_PRIORITY_HIGH,
	}); err != nil {
		t.Fatalf("BeginTransactionWithOptions returned unexpected error: %v", err)
	}
	rowsId, err := Execute(ctx, poolId, connId, &spannerpb.ExecuteSqlRequest{
		Sql: testutil.UpdateBarSetFoo,
	})
	if err != nil {
		t.Fatalf("Execute returned unexpected error: %v", err)
	}
	if err := CloseRows(ctx, poolId, connId, rowsId); err != nil {
		t.Fatalf("CloseRows returned unexpected error: %v", err)
	}
	if _, err := Commit(ctx, poolId, connId); err != nil {
		t.Fatalf("Commit returned unexpected error: %v", err)
	}

	requests := server.TestSpanner.DrainRequestsFromServer()
	execRequests := testutil.RequestsOfType(requests, reflect.TypeOf(&spannerpb.ExecuteSqlRequest{}))
	if g, w := len(execRequests), 1; g != w {
		t.Fatalf("ExecuteSqlRequest count mismatch\nGot:  %v\nWant: %v", g, w)
	}
	if g, w := execRequests[0].(*spannerpb.ExecuteSqlRequest).RequestOptions.GetTransactionTag(), "begin_tx_tag';--"; g != w {
		t.Errorf("execute transaction tag mismatch\nGot:  %v\nWant: %v", g, w)
	}
	commitRequests := testutil.RequestsOfType(requests, reflect.TypeOf(&spannerpb.CommitRequest{}))
	if g, w := len(commitRequests), 1; g != w {
		t.Fatalf("CommitRequest count mismatch\nGot:  %v\nWant: %v", g, w)
	}
	commitRequest := commitRequests[0].(*spannerpb.CommitRequest)
	if g, w := commitRequest.RequestOptions.GetTransactionTag(), "begin_tx_tag';--"; g != w {
		t.Errorf("commit transaction tag mismatch\nGot:  %v\nWant: %v", g, w)
	}
	if g, w := commitRequest.RequestOptions.GetPriority(), spannerpb.RequestOptions_PRIORITY_HIGH; g != w {
		t.Errorf("commit priority mismatch\nGot:  %v\nWant: %v", g, w)
	}
}

func TestWriteMutationsWithOptions_NilAndErrorNoLeak(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	server, teardown := setupMockServer(t)
	defer teardown()
	dsn := fmt.Sprintf("%s/projects/p/instances/i/databases/d?useplaintext=true", server.Address)
	poolId, connId := setupPoolAndConnection(t, ctx, dsn)

	if _, err := WriteMutationsWithOptions(ctx, poolId, connId, nil, &spannerpb.RequestOptions{
		TransactionTag: "nil_mut_tag",
		Priority:       spannerpb.RequestOptions_PRIORITY_HIGH,
	}); err == nil {
		t.Fatal("expected error for nil mutations, got nil")
	} else if g, w := status.Code(err), codes.InvalidArgument; g != w {
		t.Errorf("error code mismatch\nGot:  %v\nWant: %v", g, w)
	}

	mutations := insertTestMutations()
	server.TestSpanner.PutExecutionTime(testutil.MethodCommitTransaction, testutil.SimulatedExecutionTime{
		Errors: []error{status.Error(codes.PermissionDenied, "permission denied")},
	})
	if _, err := WriteMutationsWithOptions(ctx, poolId, connId, mutations, &spannerpb.RequestOptions{
		TransactionTag: "err_mut_tag",
		Priority:       spannerpb.RequestOptions_PRIORITY_HIGH,
	}); err == nil {
		t.Fatal("expected error from Commit, got nil")
	}
	_ = server.TestSpanner.DrainRequestsFromServer()

	rowsId, err := Execute(ctx, poolId, connId, &spannerpb.ExecuteSqlRequest{Sql: testutil.UpdateBarSetFoo})
	if err != nil {
		t.Fatalf("subsequent Execute failed: %v", err)
	}
	if err := CloseRows(ctx, poolId, connId, rowsId); err != nil {
		t.Fatalf("CloseRows failed: %v", err)
	}

	requests := server.TestSpanner.DrainRequestsFromServer()
	execRequests := testutil.RequestsOfType(requests, reflect.TypeOf(&spannerpb.ExecuteSqlRequest{}))
	if g, w := len(execRequests), 1; g != w {
		t.Fatalf("ExecuteSqlRequest count mismatch\nGot:  %v\nWant: %v", g, w)
	}
	if g, w := execRequests[0].(*spannerpb.ExecuteSqlRequest).RequestOptions.GetTransactionTag(), ""; g != w {
		t.Errorf("leaked transaction tag\nGot:  %v\nWant: %v", g, w)
	}
	if g, w := execRequests[0].(*spannerpb.ExecuteSqlRequest).RequestOptions.GetPriority(), spannerpb.RequestOptions_PRIORITY_UNSPECIFIED; g != w {
		t.Errorf("leaked priority\nGot:  %v\nWant: %v", g, w)
	}
}

func TestBeginTransactionWithOptions_RollbackNoLeak(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	server, teardown := setupMockServer(t)
	defer teardown()
	dsn := fmt.Sprintf("%s/projects/p/instances/i/databases/d?useplaintext=true", server.Address)
	poolId, connId := setupPoolAndConnection(t, ctx, dsn)

	if err := BeginTransactionWithOptions(ctx, poolId, connId, &spannerpb.TransactionOptions{}, &spannerpb.RequestOptions{
		TransactionTag: "rolled_back_tx_tag",
		Priority:       spannerpb.RequestOptions_PRIORITY_HIGH,
	}); err != nil {
		t.Fatalf("BeginTransactionWithOptions failed: %v", err)
	}
	if err := Rollback(ctx, poolId, connId); err != nil {
		t.Fatalf("Rollback failed: %v", err)
	}

	if err := BeginTransaction(ctx, poolId, connId, &spannerpb.TransactionOptions{}); err != nil {
		t.Fatalf("subsequent BeginTransaction failed: %v", err)
	}
	rowsId, err := Execute(ctx, poolId, connId, &spannerpb.ExecuteSqlRequest{Sql: testutil.UpdateBarSetFoo})
	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}
	if err := CloseRows(ctx, poolId, connId, rowsId); err != nil {
		t.Fatalf("CloseRows failed: %v", err)
	}
	if _, err := Commit(ctx, poolId, connId); err != nil {
		t.Fatalf("Commit failed: %v", err)
	}

	requests := server.TestSpanner.DrainRequestsFromServer()
	execRequests := testutil.RequestsOfType(requests, reflect.TypeOf(&spannerpb.ExecuteSqlRequest{}))
	if g, w := len(execRequests), 1; g != w {
		t.Fatalf("ExecuteSqlRequest count mismatch\nGot:  %v\nWant: %v", g, w)
	}
	if g, w := execRequests[0].(*spannerpb.ExecuteSqlRequest).RequestOptions.GetTransactionTag(), ""; g != w {
		t.Errorf("leaked transaction tag to Execute\nGot:  %v\nWant: %v", g, w)
	}
	commitRequests := testutil.RequestsOfType(requests, reflect.TypeOf(&spannerpb.CommitRequest{}))
	if g, w := len(commitRequests), 1; g != w {
		t.Fatalf("CommitRequest count mismatch\nGot:  %v\nWant: %v", g, w)
	}
	commitReq := commitRequests[0].(*spannerpb.CommitRequest)
	if g, w := commitReq.RequestOptions.GetTransactionTag(), ""; g != w {
		t.Errorf("leaked transaction tag to Commit\nGot:  %v\nWant: %v", g, w)
	}
	if g, w := commitReq.RequestOptions.GetPriority(), spannerpb.RequestOptions_PRIORITY_UNSPECIFIED; g != w {
		t.Errorf("leaked priority to Commit\nGot:  %v\nWant: %v", g, w)
	}
}

func TestBeginTransactionWithOptions_AlreadyInTxNoLeak(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	server, teardown := setupMockServer(t)
	defer teardown()
	dsn := fmt.Sprintf("%s/projects/p/instances/i/databases/d?useplaintext=true", server.Address)
	poolId, connId := setupPoolAndConnection(t, ctx, dsn)

	if err := BeginTransactionWithOptions(ctx, poolId, connId, &spannerpb.TransactionOptions{}, &spannerpb.RequestOptions{
		TransactionTag: "orig_api_tx",
		Priority:       spannerpb.RequestOptions_PRIORITY_LOW,
	}); err != nil {
		t.Fatalf("BeginTransactionWithOptions failed: %v", err)
	}
	if err := BeginTransactionWithOptions(ctx, poolId, connId, &spannerpb.TransactionOptions{}, &spannerpb.RequestOptions{
		TransactionTag: "nested_api_tx",
		Priority:       spannerpb.RequestOptions_PRIORITY_HIGH,
	}); err == nil {
		t.Fatal("expected error for nested BeginTransactionWithOptions, got nil")
	}

	rowsId, err := Execute(ctx, poolId, connId, &spannerpb.ExecuteSqlRequest{Sql: testutil.UpdateBarSetFoo})
	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}
	if err := CloseRows(ctx, poolId, connId, rowsId); err != nil {
		t.Fatalf("CloseRows failed: %v", err)
	}
	if _, err := Commit(ctx, poolId, connId); err != nil {
		t.Fatalf("Commit failed: %v", err)
	}

	requests := server.TestSpanner.DrainRequestsFromServer()
	commitRequests := testutil.RequestsOfType(requests, reflect.TypeOf(&spannerpb.CommitRequest{}))
	if g, w := len(commitRequests), 1; g != w {
		t.Fatalf("CommitRequest count mismatch\nGot:  %v\nWant: %v", g, w)
	}
	commitReq := commitRequests[0].(*spannerpb.CommitRequest)
	if g, w := commitReq.RequestOptions.GetTransactionTag(), "orig_api_tx"; g != w {
		t.Errorf("commit transaction tag mismatch\nGot:  %v\nWant: %v", g, w)
	}
	if g, w := commitReq.RequestOptions.GetPriority(), spannerpb.RequestOptions_PRIORITY_LOW; g != w {
		t.Errorf("commit priority mismatch\nGot:  %v\nWant: %v", g, w)
	}
}

func TestBeginTransactionWithOptions_ReadOnlyPartitionedAndAbortedRetry(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	server, teardown := setupMockServer(t)
	defer teardown()
	dsn := fmt.Sprintf("%s/projects/p/instances/i/databases/d?useplaintext=true", server.Address)
	poolId, connId := setupPoolAndConnection(t, ctx, dsn)

	// Partitioned DML transactions are rejected without mutating connection state.
	if err := BeginTransactionWithOptions(ctx, poolId, connId, &spannerpb.TransactionOptions{
		Mode: &spannerpb.TransactionOptions_PartitionedDml_{
			PartitionedDml: &spannerpb.TransactionOptions_PartitionedDml{},
		},
	}, &spannerpb.RequestOptions{
		TransactionTag: "pdml_ignored_tag",
		Priority:       spannerpb.RequestOptions_PRIORITY_HIGH,
	}); err == nil {
		t.Fatal("expected error for PartitionedDml transaction, got nil")
	} else if g, w := status.Code(err), codes.InvalidArgument; g != w {
		t.Errorf("PartitionedDml error code mismatch\nGot:  %v\nWant: %v", g, w)
	}

	// Read-only transaction ignores RequestOptions and does not leak them.
	if err := BeginTransactionWithOptions(ctx, poolId, connId, &spannerpb.TransactionOptions{
		Mode: &spannerpb.TransactionOptions_ReadOnly_{
			ReadOnly: &spannerpb.TransactionOptions_ReadOnly{},
		},
	}, &spannerpb.RequestOptions{
		TransactionTag: "ro_ignored_tag",
		Priority:       spannerpb.RequestOptions_PRIORITY_HIGH,
	}); err != nil {
		t.Fatalf("read-only BeginTransactionWithOptions failed: %v", err)
	}
	if _, err := Commit(ctx, poolId, connId); err != nil {
		t.Fatalf("read-only Commit failed: %v", err)
	}

	// Read-write transaction with Aborted retry on Commit preserves BeginTransactionWithOptions tags/priority.
	abortedSt, _ := status.New(codes.Aborted, "Aborted").WithDetails(&errdetails.RetryInfo{RetryDelay: durationpb.New(time.Nanosecond)})
	server.TestSpanner.PutExecutionTime(testutil.MethodCommitTransaction, testutil.SimulatedExecutionTime{
		Errors: []error{abortedSt.Err()},
	})
	if err := BeginTransactionWithOptions(ctx, poolId, connId, &spannerpb.TransactionOptions{}, &spannerpb.RequestOptions{
		TransactionTag: "retry_begin_tx",
		Priority:       spannerpb.RequestOptions_PRIORITY_MEDIUM,
	}); err != nil {
		t.Fatalf("read-write BeginTransactionWithOptions failed: %v", err)
	}
	rowsId, err := Execute(ctx, poolId, connId, &spannerpb.ExecuteSqlRequest{Sql: testutil.UpdateBarSetFoo})
	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}
	if err := CloseRows(ctx, poolId, connId, rowsId); err != nil {
		t.Fatalf("CloseRows failed: %v", err)
	}
	if _, err := Commit(ctx, poolId, connId); err != nil {
		t.Fatalf("Commit failed: %v", err)
	}

	requests := server.TestSpanner.DrainRequestsFromServer()
	commitRequests := testutil.RequestsOfType(requests, reflect.TypeOf(&spannerpb.CommitRequest{}))
	if g, w := len(commitRequests), 2; g != w {
		t.Fatalf("CommitRequest count mismatch\nGot:  %v\nWant: %v", g, w)
	}
	finalCommit := commitRequests[1].(*spannerpb.CommitRequest)
	if g, w := finalCommit.RequestOptions.GetTransactionTag(), "retry_begin_tx"; g != w {
		t.Errorf("final commit transaction tag mismatch\nGot:  %v\nWant: %v", g, w)
	}
	if g, w := finalCommit.RequestOptions.GetPriority(), spannerpb.RequestOptions_PRIORITY_MEDIUM; g != w {
		t.Errorf("final commit priority mismatch\nGot:  %v\nWant: %v", g, w)
	}
}
