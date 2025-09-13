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

package lib

import (
	"context"
	"fmt"
	"testing"

	"google.golang.org/grpc/codes"
)

func TestCreateAndCloseConnection(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	server, teardown := setupMockServer(t)
	defer teardown()
	dsn := fmt.Sprintf("%s/projects/p/instances/i/databases/d?useplaintext=true", server.Address)

	poolMsg := CreatePool(ctx, dsn)
	if g, w := poolMsg.Code, int32(0); g != w {
		t.Fatalf("CreatePool result mismatch\n Got: %v\nWant: %v", g, w)
	}

	connMsg := CreateConnection(ctx, poolMsg.ObjectId)
	if g, w := connMsg.Code, int32(0); g != w {
		t.Fatalf("CreateConnection result mismatch\n Got: %v\nWant: %v", g, w)
	}
	if connMsg.ObjectId <= 0 {
		t.Fatalf("connectionId mismatch: %v", connMsg.ObjectId)
	}
	if g, w := connMsg.Length(), int32(0); g != w {
		t.Fatalf("result length mismatch\n Got: %v\nWant: %v", g, w)
	}

	closeMsg := CloseConnection(ctx, poolMsg.ObjectId, connMsg.ObjectId)
	if g, w := closeMsg.Code, int32(0); g != w {
		t.Fatalf("CloseConnection result mismatch\n Got: %v\nWant: %v", g, w)
	}
	closeMsg = ClosePool(ctx, poolMsg.ObjectId)
	if g, w := closeMsg.Code, int32(0); g != w {
		t.Fatalf("ClosePool result mismatch\n Got: %v\nWant: %v", g, w)
	}
}

func TestCreateConnectionWithUnknownPool(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	connMsg := CreateConnection(ctx, -1)
	if g, w := codes.Code(connMsg.Code), codes.NotFound; g != w {
		t.Fatalf("CreateConnection result mismatch\n Got: %v\nWant: %v", g, w)
	}
}
