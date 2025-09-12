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
