// Copyright 2021 Google LLC
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

package testutil

import (
	"encoding/base64"
	"fmt"
	"net"
	"strconv"
	"testing"

	"cloud.google.com/go/spanner/admin/database/apiv1/databasepb"
	"cloud.google.com/go/spanner/admin/instance/apiv1/instancepb"
	"cloud.google.com/go/spanner/apiv1/spannerpb"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/structpb"
)

// SelectFooFromBar is a SELECT statement that is added to the mocked test
// server and will return a one-col-two-rows result set containing the INT64
// values 1 and 2.
const SelectFooFromBar = "SELECT FOO FROM BAR"

var selectFooFromBarResults = []int64{1, 2}

// SelectSingerIDAlbumIDAlbumTitleFromAlbums i a SELECT statement that is added
// to the mocked test server and will return a 3-cols-3-rows result set.
const SelectSingerIDAlbumIDAlbumTitleFromAlbums = "SELECT SingerId, AlbumId, AlbumTitle FROM Albums"

// SelectSingerIDAlbumIDAlbumTitleFromAlbumsRowCount is the number of rows
// returned by the SelectSingerIDAlbumIDAlbumTitleFromAlbums statement.
const SelectSingerIDAlbumIDAlbumTitleFromAlbumsRowCount int64 = 3

// SelectSingerIDAlbumIDAlbumTitleFromAlbumsColCount is the number of cols
// returned by the SelectSingerIDAlbumIDAlbumTitleFromAlbums statement.
const SelectSingerIDAlbumIDAlbumTitleFromAlbumsColCount int = 3

// UpdateBarSetFoo is an UPDATE	statement that is added to the mocked test
// server that will return an update count of 5.
const UpdateBarSetFoo = "UPDATE FOO SET BAR=1 WHERE BAZ=2"

// UpdateBarSetFooRowCount is the constant update count value returned by the
// statement defined in UpdateBarSetFoo.
const UpdateBarSetFooRowCount = 5

// UpdateSingersSetLastName is an UPDATE statement that is added to the mocked test
// server that will return an update count of 1.
const UpdateSingersSetLastName = "UPDATE Singers SET LastName='Test' WHERE SingerId=1"

// UpdateSingersSetLastNameRowCount is the constant update count value returned by the
// statement defined in UpdateSingersSetLastName.
const UpdateSingersSetLastNameRowCount = 1

// MockedSpannerInMemTestServer is an InMemSpannerServer with results for a
// number of SQL statements readily mocked.
type MockedSpannerInMemTestServer struct {
	TestSpanner       InMemSpannerServer
	TestInstanceAdmin InMemInstanceAdminServer
	TestDatabaseAdmin InMemDatabaseAdminServer
	server            *grpc.Server
	Address           string
}

// NewMockedSpannerInMemTestServer creates a MockedSpannerInMemTestServer at
// localhost with a random port and returns client options that can be used
// to connect to it.
func NewMockedSpannerInMemTestServer(t *testing.T) (mockedServer *MockedSpannerInMemTestServer, opts []option.ClientOption, teardown func()) {
	return NewMockedSpannerInMemTestServerWithAddr(t, "localhost:0")
}

// NewMockedSpannerInMemTestServerWithAddr creates a MockedSpannerInMemTestServer
// at a given listening address and returns client options that can be used
// to connect to it.
func NewMockedSpannerInMemTestServerWithAddr(t *testing.T, addr string) (mockedServer *MockedSpannerInMemTestServer, opts []option.ClientOption, teardown func()) {
	mockedServer = &MockedSpannerInMemTestServer{}
	opts = mockedServer.setupMockedServerWithAddr(t, addr)
	return mockedServer, opts, func() {
		mockedServer.TestSpanner.Stop()
		mockedServer.TestInstanceAdmin.Stop()
		mockedServer.TestDatabaseAdmin.Stop()
		mockedServer.server.Stop()
	}
}

func (s *MockedSpannerInMemTestServer) setupMockedServerWithAddr(t *testing.T, addr string) []option.ClientOption {
	s.TestSpanner = NewInMemSpannerServer()
	s.TestInstanceAdmin = NewInMemInstanceAdminServer()
	s.TestDatabaseAdmin = NewInMemDatabaseAdminServer()
	s.setupSelect1Result()
	s.setupFooResults()
	s.setupSingersResults()
	s.server = grpc.NewServer()
	spannerpb.RegisterSpannerServer(s.server, s.TestSpanner)
	instancepb.RegisterInstanceAdminServer(s.server, s.TestInstanceAdmin)
	databasepb.RegisterDatabaseAdminServer(s.server, s.TestDatabaseAdmin)

	lis, err := net.Listen("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	go s.server.Serve(lis)

	s.Address = lis.Addr().String()
	opts := []option.ClientOption{
		option.WithEndpoint(s.Address),
		option.WithGRPCDialOption(grpc.WithTransportCredentials(insecure.NewCredentials())),
		option.WithoutAuthentication(),
	}
	return opts
}

func (s *MockedSpannerInMemTestServer) setupSelect1Result() {
	result := &StatementResult{Type: StatementResultResultSet, ResultSet: CreateSelect1ResultSet()}
	s.TestSpanner.PutStatementResult("SELECT 1", result)
}

func (s *MockedSpannerInMemTestServer) setupFooResults() {
	resultSet := CreateSingleColumnResultSet(selectFooFromBarResults, "FOO")
	result := &StatementResult{Type: StatementResultResultSet, ResultSet: resultSet}
	s.TestSpanner.PutStatementResult(SelectFooFromBar, result)
	s.TestSpanner.PutStatementResult(UpdateBarSetFoo, &StatementResult{
		Type:        StatementResultUpdateCount,
		UpdateCount: UpdateBarSetFooRowCount,
	})
	s.TestSpanner.PutStatementResult(UpdateSingersSetLastName, &StatementResult{
		Type:        StatementResultUpdateCount,
		UpdateCount: UpdateSingersSetLastNameRowCount,
	})
}

func (s *MockedSpannerInMemTestServer) setupSingersResults() {
	metadata := createSingersMetadata()
	rows := make([]*structpb.ListValue, SelectSingerIDAlbumIDAlbumTitleFromAlbumsRowCount)
	var idx int64
	for idx = 0; idx < SelectSingerIDAlbumIDAlbumTitleFromAlbumsRowCount; idx++ {
		rows[idx] = createSingersRow(idx)
	}
	resultSet := &spannerpb.ResultSet{
		Metadata: metadata,
		Rows:     rows,
	}
	result := &StatementResult{Type: StatementResultResultSet, ResultSet: resultSet}
	s.TestSpanner.PutStatementResult(SelectSingerIDAlbumIDAlbumTitleFromAlbums, result)
}

// CreateSingleRowSingersResult creates a result set containing a single row of
// the SelectSingerIDAlbumIDAlbumTitleFromAlbums result set, or zero rows if
// the given rowNum is greater than the number of rows in the result set. This
// method can be used to mock results for different partitions of a
// BatchReadOnlyTransaction.
func (s *MockedSpannerInMemTestServer) CreateSingleRowSingersResult(rowNum int64) *StatementResult {
	metadata := createSingersMetadata()
	var returnedRows int
	if rowNum < SelectSingerIDAlbumIDAlbumTitleFromAlbumsRowCount {
		returnedRows = 1
	} else {
		returnedRows = 0
	}
	rows := make([]*structpb.ListValue, returnedRows)
	if returnedRows > 0 {
		rows[0] = createSingersRow(rowNum)
	}
	resultSet := &spannerpb.ResultSet{
		Metadata: metadata,
		Rows:     rows,
	}
	return &StatementResult{Type: StatementResultResultSet, ResultSet: resultSet}
}

func createSingersMetadata() *spannerpb.ResultSetMetadata {
	fields := make([]*spannerpb.StructType_Field, SelectSingerIDAlbumIDAlbumTitleFromAlbumsColCount)
	fields[0] = &spannerpb.StructType_Field{
		Name: "SingerId",
		Type: &spannerpb.Type{Code: spannerpb.TypeCode_INT64},
	}
	fields[1] = &spannerpb.StructType_Field{
		Name: "AlbumId",
		Type: &spannerpb.Type{Code: spannerpb.TypeCode_INT64},
	}
	fields[2] = &spannerpb.StructType_Field{
		Name: "AlbumTitle",
		Type: &spannerpb.Type{Code: spannerpb.TypeCode_STRING},
	}
	rowType := &spannerpb.StructType{
		Fields: fields,
	}
	return &spannerpb.ResultSetMetadata{
		RowType: rowType,
	}
}

func createSingersRow(idx int64) *structpb.ListValue {
	rowValue := make([]*structpb.Value, SelectSingerIDAlbumIDAlbumTitleFromAlbumsColCount)
	rowValue[0] = &structpb.Value{
		Kind: &structpb.Value_StringValue{StringValue: strconv.FormatInt(idx+1, 10)},
	}
	rowValue[1] = &structpb.Value{
		Kind: &structpb.Value_StringValue{StringValue: strconv.FormatInt(idx*10+idx, 10)},
	}
	rowValue[2] = &structpb.Value{
		Kind: &structpb.Value_StringValue{StringValue: fmt.Sprintf("Album title %d", idx)},
	}
	return &structpb.ListValue{
		Values: rowValue,
	}
}

func CreateResultSetWithAllTypes(nullValues bool) *spannerpb.ResultSet {
	index := 0
	fields := make([]*spannerpb.StructType_Field, 20)
	fields[index] = &spannerpb.StructType_Field{
		Name: "ColBool",
		Type: &spannerpb.Type{Code: spannerpb.TypeCode_BOOL},
	}
	index++
	fields[index] = &spannerpb.StructType_Field{
		Name: "ColString",
		Type: &spannerpb.Type{Code: spannerpb.TypeCode_STRING},
	}
	index++
	fields[index] = &spannerpb.StructType_Field{
		Name: "ColBytes",
		Type: &spannerpb.Type{Code: spannerpb.TypeCode_BYTES},
	}
	index++
	fields[index] = &spannerpb.StructType_Field{
		Name: "ColInt",
		Type: &spannerpb.Type{Code: spannerpb.TypeCode_INT64},
	}
	index++
	fields[index] = &spannerpb.StructType_Field{
		Name: "ColFloat32",
		Type: &spannerpb.Type{Code: spannerpb.TypeCode_FLOAT32},
	}
	index++
	fields[index] = &spannerpb.StructType_Field{
		Name: "ColFloat64",
		Type: &spannerpb.Type{Code: spannerpb.TypeCode_FLOAT64},
	}
	index++
	fields[index] = &spannerpb.StructType_Field{
		Name: "ColNumeric",
		Type: &spannerpb.Type{Code: spannerpb.TypeCode_NUMERIC},
	}
	index++
	fields[index] = &spannerpb.StructType_Field{
		Name: "ColDate",
		Type: &spannerpb.Type{Code: spannerpb.TypeCode_DATE},
	}
	index++
	fields[index] = &spannerpb.StructType_Field{
		Name: "ColTimestamp",
		Type: &spannerpb.Type{Code: spannerpb.TypeCode_TIMESTAMP},
	}
	index++
	fields[index] = &spannerpb.StructType_Field{
		Name: "ColJson",
		Type: &spannerpb.Type{Code: spannerpb.TypeCode_JSON},
	}
	index++
	fields[index] = &spannerpb.StructType_Field{
		Name: "ColBoolArray",
		Type: &spannerpb.Type{
			Code:             spannerpb.TypeCode_ARRAY,
			ArrayElementType: &spannerpb.Type{Code: spannerpb.TypeCode_BOOL},
		},
	}
	index++
	fields[index] = &spannerpb.StructType_Field{
		Name: "ColStringArray",
		Type: &spannerpb.Type{
			Code:             spannerpb.TypeCode_ARRAY,
			ArrayElementType: &spannerpb.Type{Code: spannerpb.TypeCode_STRING},
		},
	}
	index++
	fields[index] = &spannerpb.StructType_Field{
		Name: "ColBytesArray",
		Type: &spannerpb.Type{
			Code:             spannerpb.TypeCode_ARRAY,
			ArrayElementType: &spannerpb.Type{Code: spannerpb.TypeCode_BYTES},
		},
	}
	index++
	fields[index] = &spannerpb.StructType_Field{
		Name: "ColIntArray",
		Type: &spannerpb.Type{
			Code:             spannerpb.TypeCode_ARRAY,
			ArrayElementType: &spannerpb.Type{Code: spannerpb.TypeCode_INT64},
		},
	}
	index++
	fields[index] = &spannerpb.StructType_Field{
		Name: "ColFloat32Array",
		Type: &spannerpb.Type{
			Code:             spannerpb.TypeCode_ARRAY,
			ArrayElementType: &spannerpb.Type{Code: spannerpb.TypeCode_FLOAT32},
		},
	}
	index++
	fields[index] = &spannerpb.StructType_Field{
		Name: "ColFloat64Array",
		Type: &spannerpb.Type{
			Code:             spannerpb.TypeCode_ARRAY,
			ArrayElementType: &spannerpb.Type{Code: spannerpb.TypeCode_FLOAT64},
		},
	}
	index++
	fields[index] = &spannerpb.StructType_Field{
		Name: "ColNumericArray",
		Type: &spannerpb.Type{
			Code:             spannerpb.TypeCode_ARRAY,
			ArrayElementType: &spannerpb.Type{Code: spannerpb.TypeCode_NUMERIC},
		},
	}
	index++
	fields[index] = &spannerpb.StructType_Field{
		Name: "ColDateArray",
		Type: &spannerpb.Type{
			Code:             spannerpb.TypeCode_ARRAY,
			ArrayElementType: &spannerpb.Type{Code: spannerpb.TypeCode_DATE},
		},
	}
	index++
	fields[index] = &spannerpb.StructType_Field{
		Name: "ColTimestampArray",
		Type: &spannerpb.Type{
			Code:             spannerpb.TypeCode_ARRAY,
			ArrayElementType: &spannerpb.Type{Code: spannerpb.TypeCode_TIMESTAMP},
		},
	}
	index++
	fields[index] = &spannerpb.StructType_Field{
		Name: "ColJsonArray",
		Type: &spannerpb.Type{
			Code:             spannerpb.TypeCode_ARRAY,
			ArrayElementType: &spannerpb.Type{Code: spannerpb.TypeCode_JSON},
		},
	}
	rowType := &spannerpb.StructType{
		Fields: fields,
	}
	metadata := &spannerpb.ResultSetMetadata{
		RowType: rowType,
	}
	rows := make([]*structpb.ListValue, 1)
	rowValue := make([]*structpb.Value, len(fields))
	if nullValues {
		for i := range fields {
			rowValue[i] = &structpb.Value{Kind: &structpb.Value_NullValue{NullValue: structpb.NullValue_NULL_VALUE}}
		}
	} else {
		index = 0
		rowValue[index] = &structpb.Value{Kind: &structpb.Value_BoolValue{BoolValue: true}}
		index++
		rowValue[index] = &structpb.Value{Kind: &structpb.Value_StringValue{StringValue: "test"}}
		index++
		rowValue[index] = &structpb.Value{Kind: &structpb.Value_StringValue{StringValue: base64.StdEncoding.EncodeToString([]byte("testbytes"))}}
		index++
		rowValue[index] = &structpb.Value{Kind: &structpb.Value_StringValue{StringValue: "5"}}
		index++
		rowValue[index] = &structpb.Value{Kind: &structpb.Value_NumberValue{NumberValue: float64(float32(3.14))}}
		index++
		rowValue[index] = &structpb.Value{Kind: &structpb.Value_NumberValue{NumberValue: 3.14}}
		index++
		rowValue[index] = &structpb.Value{Kind: &structpb.Value_StringValue{StringValue: "6.626"}}
		index++
		rowValue[index] = &structpb.Value{Kind: &structpb.Value_StringValue{StringValue: "2021-07-21"}}
		index++
		rowValue[index] = &structpb.Value{Kind: &structpb.Value_StringValue{StringValue: "2021-07-21T21:07:59.339911800Z"}}
		index++
		rowValue[index] = &structpb.Value{Kind: &structpb.Value_StringValue{StringValue: `{"key": "value", "other-key": ["value1", "value2"]}`}}
		index++
		rowValue[index] = &structpb.Value{Kind: &structpb.Value_ListValue{
			ListValue: &structpb.ListValue{Values: []*structpb.Value{
				{Kind: &structpb.Value_BoolValue{BoolValue: true}},
				{Kind: &structpb.Value_NullValue{}},
				{Kind: &structpb.Value_BoolValue{BoolValue: false}},
			}},
		}}
		index++
		rowValue[index] = &structpb.Value{Kind: &structpb.Value_ListValue{
			ListValue: &structpb.ListValue{Values: []*structpb.Value{
				{Kind: &structpb.Value_StringValue{StringValue: "test1"}},
				{Kind: &structpb.Value_NullValue{}},
				{Kind: &structpb.Value_StringValue{StringValue: "test2"}},
			}},
		}}
		index++
		rowValue[index] = &structpb.Value{Kind: &structpb.Value_ListValue{
			ListValue: &structpb.ListValue{Values: []*structpb.Value{
				{Kind: &structpb.Value_StringValue{StringValue: base64.StdEncoding.EncodeToString([]byte("testbytes1"))}},
				{Kind: &structpb.Value_NullValue{}},
				{Kind: &structpb.Value_StringValue{StringValue: base64.StdEncoding.EncodeToString([]byte("testbytes2"))}},
			}},
		}}
		index++
		rowValue[index] = &structpb.Value{Kind: &structpb.Value_ListValue{
			ListValue: &structpb.ListValue{Values: []*structpb.Value{
				{Kind: &structpb.Value_StringValue{StringValue: "1"}},
				{Kind: &structpb.Value_NullValue{}},
				{Kind: &structpb.Value_StringValue{StringValue: "2"}},
			}},
		}}
		index++
		rowValue[index] = &structpb.Value{Kind: &structpb.Value_ListValue{
			ListValue: &structpb.ListValue{Values: []*structpb.Value{
				{Kind: &structpb.Value_NumberValue{NumberValue: float64(float32(3.14))}},
				{Kind: &structpb.Value_NullValue{}},
				{Kind: &structpb.Value_NumberValue{NumberValue: float64(float32(-99.99))}},
			}},
		}}
		index++
		rowValue[index] = &structpb.Value{Kind: &structpb.Value_ListValue{
			ListValue: &structpb.ListValue{Values: []*structpb.Value{
				{Kind: &structpb.Value_NumberValue{NumberValue: 6.626}},
				{Kind: &structpb.Value_NullValue{}},
				{Kind: &structpb.Value_NumberValue{NumberValue: 10.01}},
			}},
		}}
		index++
		rowValue[index] = &structpb.Value{Kind: &structpb.Value_ListValue{
			ListValue: &structpb.ListValue{Values: []*structpb.Value{
				{Kind: &structpb.Value_StringValue{StringValue: "3.14"}},
				{Kind: &structpb.Value_NullValue{}},
				{Kind: &structpb.Value_StringValue{StringValue: "10.01"}},
			}},
		}}
		index++
		rowValue[index] = &structpb.Value{Kind: &structpb.Value_ListValue{
			ListValue: &structpb.ListValue{Values: []*structpb.Value{
				{Kind: &structpb.Value_StringValue{StringValue: "2000-02-29"}},
				{Kind: &structpb.Value_NullValue{}},
				{Kind: &structpb.Value_StringValue{StringValue: "2021-07-27"}},
			}},
		}}
		index++
		rowValue[index] = &structpb.Value{Kind: &structpb.Value_ListValue{
			ListValue: &structpb.ListValue{Values: []*structpb.Value{
				{Kind: &structpb.Value_StringValue{StringValue: "2021-07-21T21:07:59.339911800Z"}},
				{Kind: &structpb.Value_NullValue{}},
				{Kind: &structpb.Value_StringValue{StringValue: "2021-07-27T21:07:59.339911800Z"}},
			}},
		}}
		index++
		rowValue[index] = &structpb.Value{Kind: &structpb.Value_ListValue{
			ListValue: &structpb.ListValue{Values: []*structpb.Value{
				{Kind: &structpb.Value_StringValue{StringValue: `{"key1": "value1", "other-key1": ["value1", "value2"]}`}},
				{Kind: &structpb.Value_NullValue{}},
				{Kind: &structpb.Value_StringValue{StringValue: `{"key2": "value2", "other-key2": ["value1", "value2"]}`}},
			}},
		}}
	}
	rows[0] = &structpb.ListValue{
		Values: rowValue,
	}
	return &spannerpb.ResultSet{
		Metadata: metadata,
		Rows:     rows,
	}
}

func CreateSelect1ResultSet() *spannerpb.ResultSet {
	return CreateSingleColumnResultSet([]int64{1}, "")
}

func CreateSingleColumnResultSet(values []int64, name string) *spannerpb.ResultSet {
	fields := make([]*spannerpb.StructType_Field, 1)
	fields[0] = &spannerpb.StructType_Field{
		Name: name,
		Type: &spannerpb.Type{Code: spannerpb.TypeCode_INT64},
	}
	rowType := &spannerpb.StructType{
		Fields: fields,
	}
	metadata := &spannerpb.ResultSetMetadata{
		RowType: rowType,
	}
	rows := make([]*structpb.ListValue, len(values))
	for i, v := range values {
		rowValue := make([]*structpb.Value, 1)
		rowValue[0] = &structpb.Value{
			Kind: &structpb.Value_StringValue{StringValue: fmt.Sprintf("%v", v)},
		}
		rows[i] = &structpb.ListValue{
			Values: rowValue,
		}
	}
	return &spannerpb.ResultSet{
		Metadata: metadata,
		Rows:     rows,
	}
}

func CreateTwoColumnResultSet(values [][2]int64, name [2]string) *spannerpb.ResultSet {
	fields := make([]*spannerpb.StructType_Field, 2)
	fields[0] = &spannerpb.StructType_Field{
		Name: name[0],
		Type: &spannerpb.Type{Code: spannerpb.TypeCode_INT64},
	}
	fields[1] = &spannerpb.StructType_Field{
		Name: name[1],
		Type: &spannerpb.Type{Code: spannerpb.TypeCode_INT64},
	}
	rowType := &spannerpb.StructType{
		Fields: fields,
	}
	metadata := &spannerpb.ResultSetMetadata{
		RowType: rowType,
	}
	rows := make([]*structpb.ListValue, len(values))
	for i, v := range values {
		rowValue := make([]*structpb.Value, 2)
		rowValue[0] = &structpb.Value{
			Kind: &structpb.Value_StringValue{StringValue: fmt.Sprintf("%v", v[0])},
		}
		rowValue[1] = &structpb.Value{
			Kind: &structpb.Value_StringValue{StringValue: fmt.Sprintf("%v", v[1])},
		}
		rows[i] = &structpb.ListValue{
			Values: rowValue,
		}
	}
	return &spannerpb.ResultSet{
		Metadata: metadata,
		Rows:     rows,
	}
}
