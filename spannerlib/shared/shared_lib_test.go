package main

import (
	"fmt"
	"reflect"
	"testing"
	"unsafe"

	"cloud.google.com/go/spanner/admin/database/apiv1/databasepb"
	"cloud.google.com/go/spanner/apiv1/spannerpb"
	"github.com/google/uuid"
	"github.com/googleapis/go-sql-spanner/testutil"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"
)

func TestCreateAndClosePool(t *testing.T) {
	server, teardown := setupMockServer(t, databasepb.DatabaseDialect_GOOGLE_STANDARD_SQL)
	defer teardown()

	project := "my-project"
	instance := "my-instance"
	database := "my-database"
	dsn := fmt.Sprintf("//%s/projects/%s/instances/%s/databases/%s;usePlainText=true", server.Address, project, instance, database)
	pinner, code, poolId, length, ptr := CreatePool(dsn)
	verifyEmptyIdMessage(t, "CreatePool", pinner, code, poolId, length, ptr)

	pinner, code, _, length, ptr = ClosePool(poolId)
	verifyEmptyMessage(t, "ClosePool", pinner, code, length, ptr)
}

func TestCreateAndCloseConnection(t *testing.T) {
	server, teardown := setupMockServer(t, databasepb.DatabaseDialect_GOOGLE_STANDARD_SQL)
	defer teardown()

	project := "my-project"
	instance := "my-instance"
	database := "my-database"
	dsn := fmt.Sprintf("//%s/projects/%s/instances/%s/databases/%s;usePlainText=true", server.Address, project, instance, database)
	_, code, poolId, _, _ := CreatePool(dsn)
	if g, w := code, int32(codes.OK); g != w {
		t.Fatalf("CreatePool returned non-OK code\n Got: %v\nWant: %d", g, w)
	}
	defer ClosePool(poolId)

	pinner, code, connId, length, ptr := CreateConnection(poolId)
	verifyEmptyIdMessage(t, "CreateConnection", pinner, code, connId, length, ptr)

	pinner, code, _, length, ptr = CloseConnection(poolId, connId)
	verifyEmptyMessage(t, "CloseConnection", pinner, code, length, ptr)
}

func TestApply(t *testing.T) {
	poolId, connId, server, teardown := setupTestConnection(t, databasepb.DatabaseDialect_GOOGLE_STANDARD_SQL)
	defer teardown()

	mutations := &spannerpb.BatchWriteRequest_MutationGroup{
		Mutations: []*spannerpb.Mutation{
			{Operation: &spannerpb.Mutation_Insert{
				Insert: &spannerpb.Mutation_Write{
					Table:   "my_table",
					Columns: []string{"col1", "col2", "col3", "col4"},
					Values: []*structpb.ListValue{
						{Values: []*structpb.Value{
							{Kind: &structpb.Value_StringValue{StringValue: "val1"}},
							{Kind: &structpb.Value_NumberValue{NumberValue: 3.14}},
							{Kind: &structpb.Value_NullValue{NullValue: structpb.NullValue_NULL_VALUE}},
							{Kind: &structpb.Value_BoolValue{BoolValue: true}},
						}},
						{Values: []*structpb.Value{
							{Kind: &structpb.Value_StringValue{StringValue: "val2"}},
							{Kind: &structpb.Value_NumberValue{NumberValue: 6.626}},
							{Kind: &structpb.Value_NullValue{NullValue: structpb.NullValue_NULL_VALUE}},
							{Kind: &structpb.Value_BoolValue{BoolValue: false}},
						}},
					},
				},
			}},
		},
	}
	mutationBytes, err := proto.Marshal(mutations)
	if err != nil {
		t.Fatal(err)
	}
	pinner, code, id, length, ptr := WriteMutations(poolId, connId, mutationBytes)
	verifyNonEmptyMessage(t, "WriteMutations", pinner, code, id, length, ptr)
	defer Release(pinner)

	responseBytes := reflect.SliceAt(reflect.TypeOf(byte(0)), ptr, int(length)).Bytes()
	response := &spannerpb.CommitResponse{}
	if err := proto.Unmarshal(responseBytes, response); err != nil {
		t.Fatal(err)
	}
	if response.CommitTimestamp == nil {
		t.Fatal("CommitTimestamp is nil")
	}

	requests := server.TestSpanner.DrainRequestsFromServer()
	beginRequests := testutil.RequestsOfType(requests, reflect.TypeOf(&spannerpb.BeginTransactionRequest{}))
	if g, w := len(beginRequests), 1; g != w {
		t.Fatalf("num begin requests mismatch\n Got: %v\nWant: %v", g, w)
	}
	commitRequests := testutil.RequestsOfType(requests, reflect.TypeOf(&spannerpb.CommitRequest{}))
	if g, w := len(commitRequests), 1; g != w {
		t.Fatalf("num commit requests mismatch\n Got: %v\nWant: %v", g, w)
	}
	commitRequest := commitRequests[0].(*spannerpb.CommitRequest)
	if g, w := len(commitRequest.Mutations), 1; g != w {
		t.Fatalf("num mutations mismatch\n Got: %v\nWant: %v", g, w)
	}
	if g, w := commitRequest.Mutations[0].GetInsert().GetTable(), "my_table"; g != w {
		t.Fatalf("insert table mismatch\n Got: %v\nWant: %v", g, w)
	}
	if g, w := len(commitRequest.Mutations[0].GetInsert().GetValues()), 2; g != w {
		t.Fatalf("num rows mismatch\n Got: %v\nWant: %v", g, w)
	}
}

func TestExecute(t *testing.T) {
	poolId, connId, server, teardown := setupTestConnection(t, databasepb.DatabaseDialect_GOOGLE_STANDARD_SQL)
	defer teardown()

	query := "select * from my_table"
	_ = server.TestSpanner.PutStatementResult(query, &testutil.StatementResult{
		Type:      testutil.StatementResultResultSet,
		ResultSet: generateResultSet(5),
	})

	request := &spannerpb.ExecuteSqlRequest{
		Sql: query,
	}
	requestBytes, err := proto.Marshal(request)
	if err != nil {
		t.Fatal(err)
	}
	pinner, code, rowsId, length, ptr := Execute(poolId, connId, requestBytes)
	verifyEmptyIdMessage(t, "Execute", pinner, code, rowsId, length, ptr)

	requests := server.TestSpanner.DrainRequestsFromServer()
	executeRequests := testutil.RequestsOfType(requests, reflect.TypeOf(&spannerpb.ExecuteSqlRequest{}))
	if g, w := len(executeRequests), 1; g != w {
		t.Fatalf("num execute requests mismatch\n Got: %v\nWant: %v", g, w)
	}
}

func verifyEmptyIdMessage(t *testing.T, name string, pinner int64, code int32, id int64, length int32, ptr unsafe.Pointer) {
	verifyEmptyMessage(t, name, pinner, code, length, ptr)
	if id <= int64(0) {
		t.Fatalf("%s returned zero or negative id\n Got: %v", name, id)
	}
}

func verifyEmptyMessage(t *testing.T, name string, pinner int64, code int32, length int32, ptr unsafe.Pointer) {
	if pinner != int64(0) && code != int32(0) {
		msg := unsafe.String((*byte)(ptr), length)
		t.Fatalf("%s returned error: %v", name, msg)
	}

	if g, w := pinner, int64(0); g != w {
		t.Fatalf("%s returned non-zero pinner\n Got: %v\nWant: %v", name, g, w)
	}
	if g, w := code, int32(codes.OK); g != w {
		t.Fatalf("%s returned non-OK code\n Got: %v\nWant: %d", name, g, w)
	}
	if g, w := length, int32(0); g != w {
		t.Fatalf("%s returned non-empty length\n Got: %v\nWant: %v", name, g, w)
	}
	if g, w := ptr, unsafe.Pointer(nil); g != w {
		t.Fatalf("%s returned non-nil pointer\n Got: %v\nWant: %v", name, g, w)
	}
}

func verifyNonEmptyMessage(t *testing.T, name string, pinner int64, code int32, id int64, length int32, ptr unsafe.Pointer) {
	if pinner == int64(0) {
		t.Fatalf("%s returned zero pinner", name)
	}
	if g, w := code, int32(codes.OK); g != w {
		t.Fatalf("%s returned non-OK code\n Got: %v\nWant: %d", name, g, w)
	}
	if g, w := id, int64(0); g != w {
		t.Fatalf("%s returned non-zero id\n Got: %v\nWant: %v", name, g, w)
	}
	if length == int32(0) {
		t.Fatalf("%s returned empty length", name)
	}
	if ptr == unsafe.Pointer(nil) {
		t.Fatalf("%s returned nil pointer", name)
	}
}

func generateResultSet(numRows int) *spannerpb.ResultSet {
	res := &spannerpb.ResultSet{
		Metadata: &spannerpb.ResultSetMetadata{
			RowType: &spannerpb.StructType{
				Fields: []*spannerpb.StructType_Field{
					{Type: &spannerpb.Type{Code: spannerpb.TypeCode_STRING}, Name: "col1"},
					{Type: &spannerpb.Type{Code: spannerpb.TypeCode_STRING}, Name: "col2"},
					{Type: &spannerpb.Type{Code: spannerpb.TypeCode_STRING}, Name: "col3"},
					{Type: &spannerpb.Type{Code: spannerpb.TypeCode_STRING}, Name: "col4"},
					{Type: &spannerpb.Type{Code: spannerpb.TypeCode_STRING}, Name: "col5"},
				},
			},
		},
	}
	rows := make([]*structpb.ListValue, 0, numRows)
	for i := 0; i < numRows; i++ {
		values := make([]*structpb.Value, 0, 5)
		for j := 0; j < 5; j++ {
			values = append(values, &structpb.Value{
				Kind: &structpb.Value_StringValue{StringValue: uuid.New().String()},
			})
		}
		rows = append(rows, &structpb.ListValue{
			Values: values,
		})
	}
	res.Rows = rows
	return res
}

func setupMockServer(t *testing.T, dialect databasepb.DatabaseDialect) (server *testutil.MockedSpannerInMemTestServer, teardown func()) {
	server, _, serverTeardown := testutil.NewMockedSpannerInMemTestServer(t)
	server.SetupSelectDialectResult(dialect)

	return server, serverTeardown
}

func setupTestConnection(t *testing.T, dialect databasepb.DatabaseDialect) (poolId int64, connId int64, server *testutil.MockedSpannerInMemTestServer, teardown func()) {
	server, serverTeardown := setupMockServer(t, databasepb.DatabaseDialect_GOOGLE_STANDARD_SQL)

	project := "my-project"
	instance := "my-instance"
	database := "my-database"
	dsn := fmt.Sprintf("//%s/projects/%s/instances/%s/databases/%s;usePlainText=true", server.Address, project, instance, database)
	_, code, poolId, _, _ := CreatePool(dsn)
	if code != int32(codes.OK) {
		t.Fatalf("CreatePool returned non-OK code\n Got: %v", code)
	}
	_, code, connId, _, _ = CreateConnection(poolId)
	if code != int32(codes.OK) {
		t.Fatalf("CreateConnection returned non-OK code\n Got: %v", code)
	}

	return poolId, connId, server, func() {
		CloseConnection(poolId, connId)
		ClosePool(poolId)
		serverTeardown()
	}
}
