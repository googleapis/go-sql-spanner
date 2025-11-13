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
	"cloud.google.com/go/spanner/admin/database/apiv1/databasepb"
	"cloud.google.com/go/spanner/apiv1/spannerpb"
	"github.com/googleapis/go-sql-spanner/testutil"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestCreateAndClosePool(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	server, teardown := setupMockServer(t)
	defer teardown()
	dsn := fmt.Sprintf("%s/projects/p/instances/i/databases/d?useplaintext=true", server.Address)

	poolId, err := CreatePool(ctx, dsn)
	if err != nil {
		t.Fatalf("CreatePool returned unexpected error: %v", err)
	}
	if poolId == 0 {
		t.Fatal("CreatePool returned unexpected zero id")
	}
	if _, ok := pools.Load(poolId); !ok {
		t.Fatal("pool not found in map")
	}

	requests := server.TestSpanner.DrainRequestsFromServer()
	createSessionRequests := testutil.RequestsOfType(requests, reflect.TypeOf(&spannerpb.CreateSessionRequest{}))
	if g, w := len(createSessionRequests), 1; g != w {
		t.Fatalf("num CreateSession requests mismatch\n Got: %d\nWant: %d", g, w)
	}

	if err := ClosePool(ctx, poolId); err != nil {
		t.Fatalf("ClosePool returned unexpected error: %v", err)
	}
	if _, ok := pools.Load(poolId); ok {
		t.Fatal("closed pool still in map")
	}
}

func TestCreatePoolFails(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	server, teardown := setupMockServer(t)
	defer teardown()
	dsn := fmt.Sprintf("%s/projects/p/instances/i/databases/d?useplaintext=true", server.Address)
	// Simulate that the user is not allowed to connect to this database.
	server.TestSpanner.PutExecutionTime(testutil.MethodCreateSession, testutil.SimulatedExecutionTime{
		Errors:    []error{status.Error(codes.PermissionDenied, "Not allowed")},
		KeepError: true,
	})

	poolId, err := CreatePool(ctx, dsn)
	if g, w := spanner.ErrCode(err), codes.PermissionDenied; g != w {
		t.Fatalf("error code mismatch\n Got: %v\nWant: %v", g, w)
	}
	if g, w := poolId, int64(0); g != w {
		t.Fatalf("poolId mismatch\n Got: %v\nWant: %v", g, w)
	}
}

func TestClosePoolTwice(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	server, teardown := setupMockServer(t)
	defer teardown()
	dsn := fmt.Sprintf("%s/projects/p/instances/i/databases/d?useplaintext=true", server.Address)

	poolId, err := CreatePool(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}

	// Close the pool twice. The second time should be a no-op.
	for i := range 2 {
		if err := ClosePool(ctx, poolId); err != nil {
			t.Fatalf("%d: ClosePool returned unexpected error: %v", i, err)
		}
	}
}

func TestCreateTwoPools(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	server, teardown := setupMockServer(t)
	defer teardown()
	dsn := fmt.Sprintf("%s/projects/p/instances/i/databases/d?useplaintext=true", server.Address)

	for range 2 {
		poolId, err := CreatePool(ctx, dsn)
		if err != nil {
			t.Fatalf("CreatePool returned unexpected error: %v", err)
		}
		//goland:noinspection GoDeferInLoop
		defer func() { _ = ClosePool(ctx, poolId) }()
	}

	// Each new pool should create a new client with its own multiplexed session, even if they are for the same
	// database.
	requests := server.TestSpanner.DrainRequestsFromServer()
	createSessionRequests := testutil.RequestsOfType(requests, reflect.TypeOf(&spannerpb.CreateSessionRequest{}))
	if g, w := len(createSessionRequests), 2; g != w {
		t.Fatalf("num CreateSession requests mismatch\n Got: %d\nWant: %d", g, w)
	}
}

func setupMockServer(t *testing.T) (server *testutil.MockedSpannerInMemTestServer, teardown func()) {
	return setupMockServerWithDialect(t, databasepb.DatabaseDialect_GOOGLE_STANDARD_SQL)
}

func setupMockServerWithDialect(t *testing.T, dialect databasepb.DatabaseDialect) (server *testutil.MockedSpannerInMemTestServer, teardown func()) {
	server, _, serverTeardown := testutil.NewMockedSpannerInMemTestServer(t)
	server.SetupSelectDialectResult(dialect)
	return server, serverTeardown
}
