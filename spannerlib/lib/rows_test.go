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

	"cloud.google.com/go/longrunning/autogen/longrunningpb"
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

func TestQuery(t *testing.T) {
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

	numRows := 0
	for {
		rowMsg := Next(ctx, poolMsg.ObjectId, connMsg.ObjectId, rowsMsg.ObjectId)
		if g, w := rowMsg.Code, int32(0); g != w {
			t.Fatalf("Next result mismatch\n Got: %v\nWant: %v", g, w)
		}
		// Data length == 0 means end of data.
		if rowMsg.Length() == 0 {
			break
		}
		numRows++
		values := &structpb.ListValue{}
		if err := proto.Unmarshal(rowMsg.Res, values); err != nil {
			t.Fatalf("Failed to unmarshal values: %v", err)
		}
		if g, w := len(values.Values), 1; g != w {
			t.Fatalf("num values mismatch\n Got: %v\nWant: %v", g, w)
		}
		if g, w := values.Values[0].GetStringValue(), fmt.Sprintf("%d", numRows); g != w {
			t.Fatalf("value mismatch\n Got: %v\nWant: %v", g, w)
		}
	}
	if g, w := numRows, 2; g != w {
		t.Fatalf("num rows mismatch\n Got: %v\nWant: %v", g, w)
	}

	// The ResultSetStats should be empty for queries.
	statsMsg := ResultSetStats(ctx, poolMsg.ObjectId, connMsg.ObjectId, rowsMsg.ObjectId)
	if g, w := statsMsg.Code, int32(0); g != w {
		t.Fatalf("ResultSetStats result mismatch\n Got: %v\nWant: %v", g, w)
	}
	stats := &spannerpb.ResultSetStats{}
	if err := proto.Unmarshal(statsMsg.Res, stats); err != nil {
		t.Fatalf("Failed to unmarshal ResultSetStats: %v", err)
	}
	// TODO: Enable when this branch is up to date with main
	//emptyStats := &spannerpb.ResultSetStats{}
	//if g, w := stats, emptyStats; !cmp.Equal(g, w, cmpopts.IgnoreUnexported(spannerpb.ResultSetStats{})) {
	//	t.Fatalf("ResultSetStats mismatch\n Got: %v\nWant: %v", g, w)
	//}

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

func TestDml(t *testing.T) {
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
		Sql: testutil.UpdateBarSetFoo,
	}
	requestBytes, err := proto.Marshal(request)
	if err != nil {
		t.Fatalf("Failed to marshal request: %v", err)
	}
	// Execute is used for all types of statements.
	rowsMsg := Execute(ctx, poolMsg.ObjectId, connMsg.ObjectId, requestBytes)
	if g, w := rowsMsg.Code, int32(0); g != w {
		t.Fatalf("Execute result mismatch\n Got: %v\nWant: %v", g, w)
	}

	// Next should return no rows for a DML statement.
	rowMsg := Next(ctx, poolMsg.ObjectId, connMsg.ObjectId, rowsMsg.ObjectId)
	if g, w := rowMsg.Code, int32(0); g != w {
		t.Fatalf("Next result mismatch\n Got: %v\nWant: %v", g, w)
	}
	// Data length == 0 means end of data (or no data).
	if g, w := rowMsg.Length(), int32(0); g != w {
		t.Fatalf("row length mismatch\n Got: %v\nWant: %v", g, w)
	}

	// The ResultSetStats should contain the update count for DML statements.
	statsMsg := ResultSetStats(ctx, poolMsg.ObjectId, connMsg.ObjectId, rowsMsg.ObjectId)
	if g, w := statsMsg.Code, int32(0); g != w {
		t.Fatalf("ResultSetStats result mismatch\n Got: %v\nWant: %v", g, w)
	}
	stats := &spannerpb.ResultSetStats{}
	if err := proto.Unmarshal(statsMsg.Res, stats); err != nil {
		t.Fatalf("Failed to unmarshal ResultSetStats: %v", err)
	}
	wantStats := &spannerpb.ResultSetStats{
		RowCount: &spannerpb.ResultSetStats_RowCountExact{RowCountExact: testutil.UpdateBarSetFooRowCount},
	}
	if g, w := stats, wantStats; !cmp.Equal(g, w, cmpopts.IgnoreUnexported(spannerpb.ResultSetStats{})) {
		t.Fatalf("ResultSetStats mismatch\n Got: %v\nWant: %v", g, w)
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

func TestDdl(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	server, teardown := setupMockServer(t)
	defer teardown()
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

	dsn := fmt.Sprintf("%s/projects/p/instances/i/databases/d?useplaintext=true", server.Address)

	poolMsg := CreatePool(ctx, dsn)
	if g, w := poolMsg.Code, int32(0); g != w {
		t.Fatalf("CreatePool result mismatch\n Got: %v\nWant: %v", g, w)
	}
	connMsg := CreateConnection(ctx, poolMsg.ObjectId)
	if g, w := connMsg.Code, int32(0); g != w {
		t.Fatalf("CreateConnection result mismatch\n Got: %v\nWant: %v", g, w)
	}
	// The input argument for the library is always an ExecuteSqlRequest, also for DDL statements.
	request := &spannerpb.ExecuteSqlRequest{
		Sql: "CREATE TABLE my_table (id INT64 PRIMARY KEY, value STRING(MAX))",
	}

	requestBytes, err := proto.Marshal(request)
	if err != nil {
		t.Fatalf("Failed to marshal request: %v", err)
	}
	// Execute is used for all types of statements, including DDL.
	rowsMsg := Execute(ctx, poolMsg.ObjectId, connMsg.ObjectId, requestBytes)
	if g, w := rowsMsg.Code, int32(0); g != w {
		t.Fatalf("Execute result mismatch\n Got: %v\nWant: %v", g, w)
	}

	// Next should return no rows for a DDL statement.
	rowMsg := Next(ctx, poolMsg.ObjectId, connMsg.ObjectId, rowsMsg.ObjectId)
	if g, w := rowMsg.Code, int32(0); g != w {
		t.Fatalf("Next result mismatch\n Got: %v\nWant: %v", g, w)
	}
	// Data length == 0 means end of data (or no data).
	if g, w := rowMsg.Length(), int32(0); g != w {
		t.Fatalf("row length mismatch\n Got: %v\nWant: %v", g, w)
	}

	// The ResultSetStats should be empty for DDL statements.
	statsMsg := ResultSetStats(ctx, poolMsg.ObjectId, connMsg.ObjectId, rowsMsg.ObjectId)
	if g, w := statsMsg.Code, int32(0); g != w {
		t.Fatalf("ResultSetStats result mismatch\n Got: %v\nWant: %v", g, w)
	}
	stats := &spannerpb.ResultSetStats{}
	if err := proto.Unmarshal(statsMsg.Res, stats); err != nil {
		t.Fatalf("Failed to unmarshal ResultSetStats: %v", err)
	}
	emptyStats := &spannerpb.ResultSetStats{}
	if g, w := stats, emptyStats; !cmp.Equal(g, w, cmpopts.IgnoreUnexported(spannerpb.ResultSetStats{})) {
		t.Fatalf("ResultSetStats mismatch\n Got: %v\nWant: %v", g, w)
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

func TestQueryError(t *testing.T) {
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

	// Set up a query that returns an error.
	query := "select * from non_existing_table"
	_ = server.TestSpanner.PutStatementResult(query, &testutil.StatementResult{
		Type: testutil.StatementResultError,
		Err:  status.Error(codes.NotFound, "Table not found"),
	})
	// Execute the query that will return an error.
	request := &spannerpb.ExecuteSqlRequest{
		Sql: query,
	}
	requestBytes, err := proto.Marshal(request)
	if err != nil {
		t.Fatalf("Failed to marshal request: %v", err)
	}
	rowsMsg := Execute(ctx, poolMsg.ObjectId, connMsg.ObjectId, requestBytes)
	if g, w := rowsMsg.Code, int32(codes.NotFound); g != w {
		t.Fatalf("Execute result mismatch\n Got: %v\nWant: %v", g, w)
	}
	if g, w := rowsMsg.ObjectId, int64(0); g != w {
		t.Fatalf("rowsId mismatch\n Got: %v\nWant: %v", g, w)
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
