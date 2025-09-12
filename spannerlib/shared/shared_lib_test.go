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

package main

import (
	"fmt"
	"testing"
	"unsafe"

	"cloud.google.com/go/spanner/admin/database/apiv1/databasepb"
	"github.com/googleapis/go-sql-spanner/testutil"
)

// The tests in this file only verify the happy flow to ensure that everything compiles.
// Corner cases are tested in the lib and api packages.

func TestCreatePool(t *testing.T) {
	t.Parallel()

	server, teardown := setupMockServer(t)
	defer teardown()
	dsn := fmt.Sprintf("%s/projects/p/instances/i/databases/d?useplaintext=true", server.Address)

	mem, code, poolId, length, data := CreatePool(dsn)
	if g, w := mem, int64(0); g != w {
		t.Fatalf("CreatePool mem mismatch\n Got: %v\nWant: %v", g, w)
	}
	if g, w := code, int32(0); g != w {
		t.Fatalf("CreatePool result mismatch\n Got: %v\nWant: %v", g, w)
	}
	if poolId <= int64(0) {
		t.Fatalf("poolId mismatch: %v", poolId)
	}
	if g, w := length, int32(0); g != w {
		t.Fatalf("CreatePool length mismatch\n Got: %v\nWant: %v", g, w)
	}
	if g, w := unsafe.Pointer(nil), data; g != w {
		t.Fatalf("CreatePool data mismatch\n Got: %v\nWant: %v", g, w)
	}

	_, code, _, _, _ = ClosePool(poolId)
	if g, w := code, int32(0); g != w {
		t.Fatalf("ClosePool result mismatch\n Got: %v\nWant: %v", g, w)
	}
}

func TestCreateConnection(t *testing.T) {
	t.Parallel()

	server, teardown := setupMockServer(t)
	defer teardown()
	dsn := fmt.Sprintf("%s/projects/p/instances/i/databases/d?useplaintext=true", server.Address)

	_, code, poolId, _, _ := CreatePool(dsn)
	if g, w := code, int32(0); g != w {
		t.Fatalf("CreatePool result mismatch\n Got: %v\nWant: %v", g, w)
	}
	mem, code, connId, length, data := CreateConnection(poolId)
	if g, w := mem, int64(0); g != w {
		t.Fatalf("CreateConnection mem mismatch\n Got: %v\nWant: %v", g, w)
	}
	if g, w := code, int32(0); g != w {
		t.Fatalf("CreateConnection result mismatch\n Got: %v\nWant: %v", g, w)
	}
	if connId <= int64(0) {
		t.Fatalf("connId mismatch: %v", poolId)
	}
	if g, w := length, int32(0); g != w {
		t.Fatalf("CreateConnection length mismatch\n Got: %v\nWant: %v", g, w)
	}
	if g, w := unsafe.Pointer(nil), data; g != w {
		t.Fatalf("CreateConnection data mismatch\n Got: %v\nWant: %v", g, w)
	}

	_, code, _, _, _ = CloseConnection(poolId, connId)
	if g, w := code, int32(0); g != w {
		t.Fatalf("CloseConnection result mismatch\n Got: %v\nWant: %v", g, w)
	}
	_, code, _, _, _ = ClosePool(poolId)
	if g, w := code, int32(0); g != w {
		t.Fatalf("ClosePool result mismatch\n Got: %v\nWant: %v", g, w)
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
