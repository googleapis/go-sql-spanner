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
	"google.golang.org/protobuf/types/known/structpb"
)

func TestCreateAndCloseConnection(t *testing.T) {
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

	poolId, err := CreatePool(ctx, dsn)
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

	poolId, err := CreatePool(ctx, dsn)
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

	poolId, err := CreatePool(ctx, dsn)
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

	poolId, err := CreatePool(ctx, dsn)
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
