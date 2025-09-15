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
	"reflect"
	"testing"
	"unsafe"

	"cloud.google.com/go/spanner/admin/database/apiv1/databasepb"
	"cloud.google.com/go/spanner/apiv1/spannerpb"
	"github.com/googleapis/go-sql-spanner/testutil"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"
	"spannerlib/api"
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

func TestExecute(t *testing.T) {
	// This test is intentionally not marked as Parallel, as it checks the number of open memory pointers.

	server, teardown := setupMockServer(t)
	defer teardown()
	dsn := fmt.Sprintf("%s/projects/p/instances/i/databases/d?useplaintext=true", server.Address)

	_, code, poolId, _, _ := CreatePool(dsn)
	if g, w := code, int32(0); g != w {
		t.Fatalf("CreatePool result mismatch\n Got: %v\nWant: %v", g, w)
	}
	_, code, connId, _, _ := CreateConnection(poolId)
	if g, w := code, int32(0); g != w {
		t.Fatalf("CreateConnection result mismatch\n Got: %v\nWant: %v", g, w)
	}

	request := &spannerpb.ExecuteSqlRequest{
		// This query returns a result set with one column and two rows.
		// The values in the two rows are 1 and 2.
		Sql: testutil.SelectFooFromBar,
	}
	requestBytes, err := proto.Marshal(request)
	if err != nil {
		t.Fatal(err)
	}
	// Execute returns a reference to a Rows object, not the actual data.
	mem, code, rowsId, length, data := Execute(poolId, connId, requestBytes)
	if g, w := mem, int64(0); g != w {
		t.Fatalf("Execute mem mismatch\n Got: %v\nWant: %v", g, w)
	}
	if g, w := code, int32(0); g != w {
		t.Fatalf("Execute result mismatch\n Got: %v\nWant: %v", g, w)
	}
	if rowsId <= int64(0) {
		t.Fatalf("rowsId mismatch: %v", rowsId)
	}
	if g, w := length, int32(0); g != w {
		t.Fatalf("Execute length mismatch\n Got: %v\nWant: %v", g, w)
	}
	if g, w := unsafe.Pointer(nil), data; g != w {
		t.Fatalf("Execute data mismatch\n Got: %v\nWant: %v", g, w)
	}

	// Get the metadata of the selected rows.
	mem, code, _, length, data = Metadata(poolId, connId, rowsId)
	// Metadata returns actual data, and should therefore return a memory ID that needs to be released.
	if mem == int64(0) {
		t.Fatalf("Metadata mem mismatch: %v", mem)
	}
	if length == int32(0) {
		t.Fatalf("Metadata length mismatch: %v", length)
	}
	// Get a []byte from the pointer to the data and the length.
	metadataBytes := reflect.SliceAt(reflect.TypeOf(byte(0)), data, int(length)).Bytes()
	metadata := &spannerpb.ResultSetMetadata{}
	if err := proto.Unmarshal(metadataBytes, metadata); err != nil {
		t.Fatal(err)
	}
	if g, w := len(metadata.RowType.Fields), 1; g != w {
		t.Fatalf("Metadata field count mismatch\n Got: %v\nWant: %v", g, w)
	}
	if g, w := metadata.RowType.Fields[0].Name, "FOO"; g != w {
		t.Fatalf("Metadata field name mismatch\n Got: %v\nWant: %v", g, w)
	}
	if g, w := metadata.RowType.Fields[0].Type.Code, spannerpb.TypeCode_INT64; g != w {
		t.Fatalf("Metadata type code mismatch\n Got: %v\nWant: %v", g, w)
	}
	// Release the memory.
	if g, w := Release(mem), int32(0); g != w {
		t.Fatalf("Release() result mismatch\n Got: %v\nWant: %v", g, w)
	}

	// Iterate over the rows.
	numRows := 0
	for {
		mem, code, _, length, data = Next(poolId, connId, rowsId /*numRows = */, 1, int32(api.EncodeRowOptionProto))
		// Next returns an empty message if it is the end of the query results.
		if length == 0 {
			break
		}
		numRows++
		// Decode the row.
		rowBytes := reflect.SliceAt(reflect.TypeOf(byte(0)), data, int(length)).Bytes()
		row := &structpb.ListValue{}
		if err := proto.Unmarshal(rowBytes, row); err != nil {
			t.Fatal(err)
		}
		// Release the memory that was held for the row. We can do that as soon as it has
		// been copied into a data structure that is maintained by the 'application'.
		// The 'application' in this case is the test.
		if g, w := Release(mem), int32(0); g != w {
			t.Fatalf("Release() result mismatch\n Got: %v\nWant: %v", g, w)
		}
		// Verify the row data.
		if g, w := len(row.GetValues()), 1; g != w {
			t.Fatalf("num row values mismatch\n Got: %v\nWant: %v", g, w)
		}
		if g, w := row.GetValues()[0].GetStringValue(), fmt.Sprintf("%d", numRows); g != w {
			t.Fatalf("row values mismatch\n Got: %v\nWant: %v", g, w)
		}
	}
	// The result should contain two rows.
	if g, w := numRows, 2; g != w {
		t.Fatalf("num rows mismatch\n Got: %v\nWant: %v", g, w)
	}

	// Get the ResultSetStats. For queries, this is an empty instance.
	mem, code, _, length, data = ResultSetStats(poolId, connId, rowsId)
	if g, w := code, int32(0); g != w {
		t.Fatalf("ResultSetStats result code mismatch\n Got: %v\nWant: %v", g, w)
	}
	if length == int32(0) {
		t.Fatalf("ResultSetStats length mismatch: %v", length)
	}
	statsBytes := reflect.SliceAt(reflect.TypeOf(byte(0)), data, int(length)).Bytes()
	stats := &spannerpb.ResultSetStats{}
	if err := proto.Unmarshal(statsBytes, stats); err != nil {
		t.Fatal(err)
	}
	// TODO: Enable when this branch is up to date with main
	// emptyStats := &spannerpb.ResultSetStats{}
	//if g, w := stats, emptyStats; !cmp.Equal(g, w, cmpopts.IgnoreUnexported(spannerpb.ResultSetStats{})) {
	//	t.Fatalf("ResultSetStats mismatch\n Got: %v\nWant: %v", g, w)
	//}
	if res := Release(mem); res != 0 {
		t.Fatalf("Release() result mismatch\n Got: %v\nWant: %v", res, 0)
	}

	_, code, _, _, _ = CloseRows(poolId, connId, rowsId)
	if g, w := code, int32(0); g != w {
		t.Fatalf("CloseRows result mismatch\n Got: %v\nWant: %v", g, w)
	}
	_, code, _, _, _ = CloseConnection(poolId, connId)
	if g, w := code, int32(0); g != w {
		t.Fatalf("CloseConnection result mismatch\n Got: %v\nWant: %v", g, w)
	}
	_, code, _, _, _ = ClosePool(poolId)
	if g, w := code, int32(0); g != w {
		t.Fatalf("ClosePool result mismatch\n Got: %v\nWant: %v", g, w)
	}

	if g, w := countOpenMemoryPointers(), 0; g != w {
		t.Fatalf("countOpenMemoryPointers() result mismatch\n Got: %v\nWant: %v", g, w)
	}
}

func TestBeginAndCommitTransaction(t *testing.T) {
	t.Parallel()

	server, teardown := setupMockServer(t)
	defer teardown()
	dsn := fmt.Sprintf("%s/projects/p/instances/i/databases/d?useplaintext=true", server.Address)

	_, code, poolId, _, _ := CreatePool(dsn)
	if g, w := code, int32(0); g != w {
		t.Fatalf("CreatePool result mismatch\n Got: %v\nWant: %v", g, w)
	}
	_, code, connId, _, _ := CreateConnection(poolId)
	if g, w := code, int32(0); g != w {
		t.Fatalf("CreateConnection result mismatch\n Got: %v\nWant: %v", g, w)
	}

	txOpts := &spannerpb.TransactionOptions{}
	txOptsBytes, err := proto.Marshal(txOpts)
	if err != nil {
		t.Fatal(err)
	}
	mem, code, id, length, res := BeginTransaction(poolId, connId, txOptsBytes)
	// BeginTransaction should return an empty message.
	// That is, there should be no error code, no ObjectID, and no data.
	verifyEmptyMessage(t, "BeginTransaction", mem, code, id, length, res)

	// Execute a statement in the transaction.
	request := &spannerpb.ExecuteSqlRequest{Sql: testutil.UpdateBarSetFoo}
	requestBytes, err := proto.Marshal(request)
	if err != nil {
		t.Fatal(err)
	}
	_, code, rowsId, _, _ := Execute(poolId, connId, requestBytes)
	if g, w := code, int32(0); g != w {
		t.Fatalf("Execute result mismatch\n Got: %v\nWant: %v", g, w)
	}
	_, code, _, _, _ = CloseRows(poolId, connId, rowsId)
	if g, w := code, int32(0); g != w {
		t.Fatalf("CloseRows result mismatch\n Got: %v\nWant: %v", g, w)
	}

	// Commit returns the CommitResponse (if any).
	mem, code, id, length, res = Commit(poolId, connId)
	verifyDataMessage(t, "Commit", mem, code, id, length, res)

	_, code, _, _, _ = CloseConnection(poolId, connId)
	if g, w := code, int32(0); g != w {
		t.Fatalf("CloseConnection result mismatch\n Got: %v\nWant: %v", g, w)
	}
	_, code, _, _, _ = ClosePool(poolId)
	if g, w := code, int32(0); g != w {
		t.Fatalf("ClosePool result mismatch\n Got: %v\nWant: %v", g, w)
	}
}

func TestBeginAndRollbackTransaction(t *testing.T) {
	t.Parallel()

	server, teardown := setupMockServer(t)
	defer teardown()
	dsn := fmt.Sprintf("%s/projects/p/instances/i/databases/d?useplaintext=true", server.Address)

	_, code, poolId, _, _ := CreatePool(dsn)
	if g, w := code, int32(0); g != w {
		t.Fatalf("CreatePool result mismatch\n Got: %v\nWant: %v", g, w)
	}
	_, code, connId, _, _ := CreateConnection(poolId)
	if g, w := code, int32(0); g != w {
		t.Fatalf("CreateConnection result mismatch\n Got: %v\nWant: %v", g, w)
	}

	txOpts := &spannerpb.TransactionOptions{}
	txOptsBytes, err := proto.Marshal(txOpts)
	if err != nil {
		t.Fatal(err)
	}
	mem, code, id, length, res := BeginTransaction(poolId, connId, txOptsBytes)
	// BeginTransaction should return an empty message.
	// That is, there should be no error code, no ObjectID, and no data.
	verifyEmptyMessage(t, "BeginTransaction", mem, code, id, length, res)

	// Execute a statement in the transaction.
	request := &spannerpb.ExecuteSqlRequest{Sql: testutil.UpdateBarSetFoo}
	requestBytes, err := proto.Marshal(request)
	if err != nil {
		t.Fatal(err)
	}
	_, code, rowsId, _, _ := Execute(poolId, connId, requestBytes)
	if g, w := code, int32(0); g != w {
		t.Fatalf("Execute result mismatch\n Got: %v\nWant: %v", g, w)
	}
	_, code, _, _, _ = CloseRows(poolId, connId, rowsId)
	if g, w := code, int32(0); g != w {
		t.Fatalf("CloseRows result mismatch\n Got: %v\nWant: %v", g, w)
	}

	// Rollback returns nothing.
	mem, code, id, length, res = Rollback(poolId, connId)
	verifyEmptyMessage(t, "Rollback", mem, code, id, length, res)

	_, code, _, _, _ = CloseConnection(poolId, connId)
	if g, w := code, int32(0); g != w {
		t.Fatalf("CloseConnection result mismatch\n Got: %v\nWant: %v", g, w)
	}
	_, code, _, _, _ = ClosePool(poolId)
	if g, w := code, int32(0); g != w {
		t.Fatalf("ClosePool result mismatch\n Got: %v\nWant: %v", g, w)
	}
}

func verifyEmptyMessage(t *testing.T, name string, mem int64, code int32, id int64, length int32, res unsafe.Pointer) {
	if g, w := mem, int64(0); g != w {
		t.Fatalf("%s: mem ID mismatch\n Got: %v\nWant: %v", name, g, w)
	}
	if g, w := code, int32(0); g != w {
		t.Fatalf("%s: result mismatch\n Got: %v\nWant: %v", name, g, w)
	}
	if g, w := id, int64(0); g != w {
		t.Fatalf("%s: ID mismatch\n Got: %v\nWant: %v", name, g, w)
	}
	if g, w := length, int32(0); g != w {
		t.Fatalf("%s: length mismatch\n Got: %v\nWant: %v", name, g, w)
	}
	if g, w := res, unsafe.Pointer(nil); g != w {
		t.Fatalf("%s: ptr mismatch\n Got: %v\nWant: %v", name, g, w)
	}
}

// verifyDataMessage verifies that the result contains a data message.
func verifyDataMessage(t *testing.T, name string, mem int64, code int32, id int64, length int32, res unsafe.Pointer) {
	if g, w := code, int32(0); g != w {
		t.Fatalf("%s: result mismatch\n Got: %v\nWant: %v", name, g, w)
	}
	if mem == int64(0) {
		t.Fatalf("%s: No memory identifier returned", name)
	}
	if g, w := id, int64(0); g != w {
		t.Fatalf("%s: ID mismatch\n Got: %v\nWant: %v", name, g, w)
	}
	if length == int32(0) {
		t.Fatalf("%s: zero length returned", name)
	}
	if res == unsafe.Pointer(nil) {
		t.Fatalf("%s: nil pointer returned", name)
	}
}

func countOpenMemoryPointers() (c int) {
	pinners.Range(func(key, value any) bool {
		c++
		return true
	})
	return
}

func setupMockServer(t *testing.T) (server *testutil.MockedSpannerInMemTestServer, teardown func()) {
	return setupMockServerWithDialect(t, databasepb.DatabaseDialect_GOOGLE_STANDARD_SQL)
}

func setupMockServerWithDialect(t *testing.T, dialect databasepb.DatabaseDialect) (server *testutil.MockedSpannerInMemTestServer, teardown func()) {
	server, _, serverTeardown := testutil.NewMockedSpannerInMemTestServer(t)
	server.SetupSelectDialectResult(dialect)
	return server, serverTeardown
}
