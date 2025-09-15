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

	"cloud.google.com/go/spanner/apiv1/spannerpb"
	"github.com/googleapis/go-sql-spanner/testutil"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/proto"
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

func TestExecute(t *testing.T) {
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
	request := &spannerpb.ExecuteSqlRequest{
		Sql: testutil.SelectFooFromBar,
	}
	requestBytes, err := proto.Marshal(request)
	if err != nil {
		t.Fatalf("Failed to marshal request: %v", err)
	}
	rowsMsg := Execute(ctx, poolMsg.ObjectId, connMsg.ObjectId, requestBytes)
	if g, w := rowsMsg.Code, int32(0); g != w {
		t.Fatalf("Execute result mismatch\n Got: %v\nWant: %v", g, w)
	}
	if rowsMsg.ObjectId <= 0 {
		t.Fatalf("rowsId mismatch: %v", rowsMsg.ObjectId)
	}
	if g, w := rowsMsg.Length(), int32(0); g != w {
		t.Fatalf("result length mismatch\n Got: %v\nWant: %v", g, w)
	}

	closeMsg := CloseRows(ctx, poolMsg.ObjectId, connMsg.ObjectId, rowsMsg.ObjectId)
	if g, w := closeMsg.Code, int32(0); g != w {
		t.Fatalf("CloseRows result mismatch\n Got: %v\nWant: %v", g, w)
	}
	closeMsg = CloseConnection(ctx, poolMsg.ObjectId, connMsg.ObjectId)
	if g, w := closeMsg.Code, int32(0); g != w {
		t.Fatalf("CloseConnection result mismatch\n Got: %v\nWant: %v", g, w)
	}
	closeMsg = ClosePool(ctx, poolMsg.ObjectId)
	if g, w := closeMsg.Code, int32(0); g != w {
		t.Fatalf("ClosePool result mismatch\n Got: %v\nWant: %v", g, w)
	}
}

func TestExecuteBatch(t *testing.T) {
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
	request := &spannerpb.ExecuteBatchDmlRequest{Statements: []*spannerpb.ExecuteBatchDmlRequest_Statement{
		{Sql: testutil.UpdateBarSetFoo},
		{Sql: testutil.UpdateBarSetFoo},
	}}
	requestBytes, err := proto.Marshal(request)
	if err != nil {
		t.Fatalf("Failed to marshal request: %v", err)
	}
	rowsMsg := ExecuteBatch(ctx, poolMsg.ObjectId, connMsg.ObjectId, requestBytes)
	if g, w := rowsMsg.Code, int32(0); g != w {
		t.Fatalf("ExecuteBatch result mismatch\n Got: %v\nWant: %v", g, w)
	}
	if rowsMsg.Length() == 0 {
		t.Fatal("ExecuteBatch returned no data")
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

func TestBeginAndCommit(t *testing.T) {
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
	txOpts := &spannerpb.TransactionOptions{}
	txOptsBytes, err := proto.Marshal(txOpts)
	if err != nil {
		t.Fatalf("Failed to marshal transaction options: %v", err)
	}
	txMsg := BeginTransaction(ctx, poolMsg.ObjectId, connMsg.ObjectId, txOptsBytes)
	if g, w := txMsg.Code, int32(0); g != w {
		t.Fatalf("BeginTransaction result mismatch\n Got: %v\nWant: %v", g, w)
	}
	if g, w := txMsg.ObjectId, int64(0); g != w {
		t.Fatalf("object ID result mismatch\n Got: %v\nWant: %v", g, w)
	}
	if g, w := txMsg.Length(), int32(0); g != w {
		t.Fatalf("result length mismatch\n Got: %v\nWant: %v", g, w)
	}

	commitMsg := Commit(ctx, poolMsg.ObjectId, connMsg.ObjectId)
	if g, w := commitMsg.Code, int32(0); g != w {
		t.Fatalf("Commit result mismatch\n Got: %v\nWant: %v", g, w)
	}
	if commitMsg.Length() == 0 {
		t.Fatal("Commit return zero length")
	}
	resp := &spannerpb.CommitResponse{}
	if err := proto.Unmarshal(commitMsg.Res, resp); err != nil {
		t.Fatalf("Failed to unmarshal commit response: %v", err)
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

func TestBeginAndRollback(t *testing.T) {
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
	txOpts := &spannerpb.TransactionOptions{}
	txOptsBytes, err := proto.Marshal(txOpts)
	if err != nil {
		t.Fatalf("Failed to marshal transaction options: %v", err)
	}
	txMsg := BeginTransaction(ctx, poolMsg.ObjectId, connMsg.ObjectId, txOptsBytes)
	if g, w := txMsg.Code, int32(0); g != w {
		t.Fatalf("BeginTransaction result mismatch\n Got: %v\nWant: %v", g, w)
	}
	if g, w := txMsg.ObjectId, int64(0); g != w {
		t.Fatalf("object ID result mismatch\n Got: %v\nWant: %v", g, w)
	}
	if g, w := txMsg.Length(), int32(0); g != w {
		t.Fatalf("result length mismatch\n Got: %v\nWant: %v", g, w)
	}

	rollbackMsg := Rollback(ctx, poolMsg.ObjectId, connMsg.ObjectId)
	if g, w := rollbackMsg.Code, int32(0); g != w {
		t.Fatalf("Rollback result mismatch\n Got: %v\nWant: %v", g, w)
	}
	if g, w := rollbackMsg.Length(), int32(0); g != w {
		t.Fatalf("Rollback length mismatch\n Got: %v\nWant: %v", g, w)
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
