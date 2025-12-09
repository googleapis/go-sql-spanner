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
	crypto "crypto/rand"
	"encoding/base64"
	"fmt"
	"math"
	"math/rand"
	"net"
	"strconv"
	"testing"
	"time"

	"cloud.google.com/go/spanner/admin/database/apiv1/databasepb"
	"cloud.google.com/go/spanner/admin/instance/apiv1/instancepb"
	"cloud.google.com/go/spanner/apiv1/spannerpb"
	pb "cloud.google.com/go/spanner/testdata/protos"
	"github.com/google/uuid"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/known/structpb"
)

const selectDialect = "select option_value from information_schema.database_options where option_name='database_dialect'"

// SelectFooFromBar is a SELECT statement that is added to the mocked test
// server and will return a one-col-two-rows result set containing the INT64
// values 1 and 2.
const SelectFooFromBar = "SELECT FOO FROM BAR"
const selectFooFromBarRowCount int64 = 2
const selectFooFromBarColCount int = 1

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
	s.SetupSelectDialectResult(databasepb.DatabaseDialect_GOOGLE_STANDARD_SQL)
	s.setupSelect1Result()
	s.setupFooResults()
	s.setupSingersResults()
	var serverOpts []grpc.ServerOption
	// Set a max message size that is essentially no limit.
	serverOpts = append(serverOpts, grpc.MaxRecvMsgSize(math.MaxInt32))
	s.server = grpc.NewServer(serverOpts...)
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

func (s *MockedSpannerInMemTestServer) SetupSelectDialectResult(dialect databasepb.DatabaseDialect) {
	result := &StatementResult{Type: StatementResultResultSet, ResultSet: CreateSelectDialectResultSet(dialect)}
	s.TestSpanner.PutStatementResult(selectDialect, result)
}

func (s *MockedSpannerInMemTestServer) setupSelect1Result() {
	result := &StatementResult{Type: StatementResultResultSet, ResultSet: CreateSelect1ResultSet()}
	s.TestSpanner.PutStatementResult("SELECT 1", result)
}

func (s *MockedSpannerInMemTestServer) setupFooResults() {
	resultSet := CreateSingleColumnInt64ResultSet(selectFooFromBarResults, "FOO")
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

func CreateResultSetMetadataWithAllTypes() *spannerpb.ResultSetMetadata {
	index := 0
	fields := make([]*spannerpb.StructType_Field, 26)
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
		Name: "ColUuid",
		Type: &spannerpb.Type{Code: spannerpb.TypeCode_UUID},
	}
	index++
	fields[index] = &spannerpb.StructType_Field{
		Name: "ColProto",
		Type: &spannerpb.Type{Code: spannerpb.TypeCode_PROTO},
	}
	index++
	fields[index] = &spannerpb.StructType_Field{
		Name: "ColProtoEnum",
		Type: &spannerpb.Type{Code: spannerpb.TypeCode_ENUM},
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
	index++
	fields[index] = &spannerpb.StructType_Field{
		Name: "ColUuidArray",
		Type: &spannerpb.Type{
			Code:             spannerpb.TypeCode_ARRAY,
			ArrayElementType: &spannerpb.Type{Code: spannerpb.TypeCode_UUID},
		},
	}
	index++
	fields[index] = &spannerpb.StructType_Field{
		Name: "ColProtoArray",
		Type: &spannerpb.Type{
			Code:             spannerpb.TypeCode_ARRAY,
			ArrayElementType: &spannerpb.Type{Code: spannerpb.TypeCode_PROTO},
		},
	}
	index++
	fields[index] = &spannerpb.StructType_Field{
		Name: "ColProtoEnumArray",
		Type: &spannerpb.Type{
			Code:             spannerpb.TypeCode_ARRAY,
			ArrayElementType: &spannerpb.Type{Code: spannerpb.TypeCode_ENUM},
		},
	}
	rowType := &spannerpb.StructType{
		Fields: fields,
	}
	return &spannerpb.ResultSetMetadata{
		RowType: rowType,
	}
}

func CreateResultSetWithAllTypes(nullValues, nullValuesInArrays bool) *spannerpb.ResultSet {
	metadata := CreateResultSetMetadataWithAllTypes()
	fields := metadata.RowType.Fields
	rows := make([]*structpb.ListValue, 1)
	rowValue := make([]*structpb.Value, len(fields))
	if nullValues {
		for i := range fields {
			rowValue[i] = &structpb.Value{Kind: &structpb.Value_NullValue{NullValue: structpb.NullValue_NULL_VALUE}}
		}
	} else {
		singerEnumValue := pb.Genre_ROCK
		singerProtoMsg := pb.SingerInfo{
			SingerId:    proto.Int64(1),
			BirthDate:   proto.String("January"),
			Nationality: proto.String("Country1"),
			Genre:       &singerEnumValue,
		}

		singer2ProtoEnum := pb.Genre_FOLK
		singer2ProtoMsg := pb.SingerInfo{
			SingerId:    proto.Int64(2),
			BirthDate:   proto.String("February"),
			Nationality: proto.String("Country2"),
			Genre:       &singer2ProtoEnum,
		}

		index := 0
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
		rowValue[index] = &structpb.Value{Kind: &structpb.Value_StringValue{StringValue: `a4e71944-fe14-4047-9d0a-e68c281602e1`}}
		index++
		rowValue[index] = protoMessageProto(&singerProtoMsg)
		index++
		rowValue[index] = protoEnumProto(&singerEnumValue)
		index++
		rowValue[index] = &structpb.Value{Kind: &structpb.Value_ListValue{
			ListValue: &structpb.ListValue{Values: []*structpb.Value{
				{Kind: &structpb.Value_BoolValue{BoolValue: true}},
				nullValueOrAlt(nullValuesInArrays, &structpb.Value{Kind: &structpb.Value_BoolValue{BoolValue: true}}),
				{Kind: &structpb.Value_BoolValue{BoolValue: false}},
			}},
		}}
		index++
		rowValue[index] = &structpb.Value{Kind: &structpb.Value_ListValue{
			ListValue: &structpb.ListValue{Values: []*structpb.Value{
				{Kind: &structpb.Value_StringValue{StringValue: "test1"}},
				nullValueOrAlt(nullValuesInArrays, &structpb.Value{Kind: &structpb.Value_StringValue{StringValue: "alt"}}),
				{Kind: &structpb.Value_StringValue{StringValue: "test2"}},
			}},
		}}
		index++
		rowValue[index] = &structpb.Value{Kind: &structpb.Value_ListValue{
			ListValue: &structpb.ListValue{Values: []*structpb.Value{
				{Kind: &structpb.Value_StringValue{StringValue: base64.StdEncoding.EncodeToString([]byte("testbytes1"))}},
				nullValueOrAlt(nullValuesInArrays, &structpb.Value{Kind: &structpb.Value_StringValue{StringValue: base64.StdEncoding.EncodeToString([]byte("altbytes"))}}),
				{Kind: &structpb.Value_StringValue{StringValue: base64.StdEncoding.EncodeToString([]byte("testbytes2"))}},
			}},
		}}
		index++
		rowValue[index] = &structpb.Value{Kind: &structpb.Value_ListValue{
			ListValue: &structpb.ListValue{Values: []*structpb.Value{
				{Kind: &structpb.Value_StringValue{StringValue: "1"}},
				nullValueOrAlt(nullValuesInArrays, &structpb.Value{Kind: &structpb.Value_StringValue{StringValue: "0"}}),
				{Kind: &structpb.Value_StringValue{StringValue: "2"}},
			}},
		}}
		index++
		rowValue[index] = &structpb.Value{Kind: &structpb.Value_ListValue{
			ListValue: &structpb.ListValue{Values: []*structpb.Value{
				{Kind: &structpb.Value_NumberValue{NumberValue: float64(float32(3.14))}},
				nullValueOrAlt(nullValuesInArrays, &structpb.Value{Kind: &structpb.Value_NumberValue{NumberValue: float64(float32(0.0))}}),
				{Kind: &structpb.Value_NumberValue{NumberValue: float64(float32(-99.99))}},
			}},
		}}
		index++
		rowValue[index] = &structpb.Value{Kind: &structpb.Value_ListValue{
			ListValue: &structpb.ListValue{Values: []*structpb.Value{
				{Kind: &structpb.Value_NumberValue{NumberValue: 6.626}},
				nullValueOrAlt(nullValuesInArrays, &structpb.Value{Kind: &structpb.Value_NumberValue{NumberValue: 0.0}}),
				{Kind: &structpb.Value_NumberValue{NumberValue: 10.01}},
			}},
		}}
		index++
		rowValue[index] = &structpb.Value{Kind: &structpb.Value_ListValue{
			ListValue: &structpb.ListValue{Values: []*structpb.Value{
				{Kind: &structpb.Value_StringValue{StringValue: "3.14"}},
				nullValueOrAlt(nullValuesInArrays, &structpb.Value{Kind: &structpb.Value_StringValue{StringValue: "1.0"}}),
				{Kind: &structpb.Value_StringValue{StringValue: "10.01"}},
			}},
		}}
		index++
		rowValue[index] = &structpb.Value{Kind: &structpb.Value_ListValue{
			ListValue: &structpb.ListValue{Values: []*structpb.Value{
				{Kind: &structpb.Value_StringValue{StringValue: "2000-02-29"}},
				nullValueOrAlt(nullValuesInArrays, &structpb.Value{Kind: &structpb.Value_StringValue{StringValue: "2000-01-01"}}),
				{Kind: &structpb.Value_StringValue{StringValue: "2021-07-27"}},
			}},
		}}
		index++
		rowValue[index] = &structpb.Value{Kind: &structpb.Value_ListValue{
			ListValue: &structpb.ListValue{Values: []*structpb.Value{
				{Kind: &structpb.Value_StringValue{StringValue: "2021-07-21T21:07:59.339911800Z"}},
				nullValueOrAlt(nullValuesInArrays, &structpb.Value{Kind: &structpb.Value_StringValue{StringValue: "2000-01-01T00:00:00Z"}}),
				{Kind: &structpb.Value_StringValue{StringValue: "2021-07-27T21:07:59.339911800Z"}},
			}},
		}}
		index++
		rowValue[index] = &structpb.Value{Kind: &structpb.Value_ListValue{
			ListValue: &structpb.ListValue{Values: []*structpb.Value{
				{Kind: &structpb.Value_StringValue{StringValue: `{"key1": "value1", "other-key1": ["value1", "value2"]}`}},
				nullValueOrAlt(nullValuesInArrays, &structpb.Value{Kind: &structpb.Value_StringValue{StringValue: "{}"}}),
				{Kind: &structpb.Value_StringValue{StringValue: `{"key2": "value2", "other-key2": ["value1", "value2"]}`}},
			}},
		}}
		index++
		rowValue[index] = &structpb.Value{Kind: &structpb.Value_ListValue{
			ListValue: &structpb.ListValue{Values: []*structpb.Value{
				{Kind: &structpb.Value_StringValue{StringValue: `d0546638-6d51-4d7c-a4a9-9062204ee5bb`}},
				nullValueOrAlt(nullValuesInArrays, &structpb.Value{Kind: &structpb.Value_StringValue{StringValue: "00000000-0000-0000-0000-000000000000"}}),
				{Kind: &structpb.Value_StringValue{StringValue: `0dd0f9b7-05af-48e0-a5b1-35432a01c6bf`}},
			}},
		}}
		index++
		rowValue[index] = &structpb.Value{Kind: &structpb.Value_ListValue{
			ListValue: &structpb.ListValue{Values: []*structpb.Value{
				protoMessageProto(&singerProtoMsg),
				nullValueOrAlt(nullValuesInArrays, protoMessageProto(&singerProtoMsg)),
				protoMessageProto(&singer2ProtoMsg),
			}},
		}}
		index++
		rowValue[index] = &structpb.Value{Kind: &structpb.Value_ListValue{
			ListValue: &structpb.ListValue{Values: []*structpb.Value{
				protoEnumProto(&singerEnumValue),
				nullValueOrAlt(nullValuesInArrays, protoEnumProto(&singerEnumValue)),
				protoEnumProto(&singer2ProtoEnum),
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

func CreateRandomResultSet(numRows int) *spannerpb.ResultSet {
	metadata := CreateResultSetMetadataWithAllTypes()
	fields := metadata.RowType.Fields
	rows := make([]*structpb.ListValue, numRows)

	for i := 0; i < numRows; i++ {
		rowValue := make([]*structpb.Value, len(fields))
		for col := range fields {
			rowValue[col] = randomValue(fields[col].Type)
		}
		rows[i] = &structpb.ListValue{Values: rowValue}
	}
	return &spannerpb.ResultSet{
		Metadata: metadata,
		Rows:     rows,
	}
}

var nullValue *structpb.Value

func init() {
	nullValue = &structpb.Value{Kind: &structpb.Value_NullValue{NullValue: structpb.NullValue_NULL_VALUE}}
}

func randomValue(t *spannerpb.Type) *structpb.Value {
	if rand.Intn(10) == 5 {
		return nullValue
	}
	switch t.Code {
	case spannerpb.TypeCode_BOOL:
		return randomBoolValue()
	case spannerpb.TypeCode_BYTES:
		return randomBytesValue()
	case spannerpb.TypeCode_DATE:
		return randomDateValue()
	case spannerpb.TypeCode_FLOAT32:
		return randomFloat32Value()
	case spannerpb.TypeCode_FLOAT64:
		return randomFloat64Value()
	case spannerpb.TypeCode_INT64:
		return randomInt64Value()
	case spannerpb.TypeCode_JSON:
		return randomJsonValue()
	case spannerpb.TypeCode_NUMERIC:
		return randomNumericValue()
	case spannerpb.TypeCode_STRING:
		return randomStringValue()
	case spannerpb.TypeCode_TIMESTAMP:
		return randomTimestampValue()
	case spannerpb.TypeCode_UUID:
		return randomUuidValue()
	case spannerpb.TypeCode_ARRAY:
		numElements := rand.Intn(10)
		value := &structpb.Value{Kind: &structpb.Value_ListValue{ListValue: &structpb.ListValue{Values: make([]*structpb.Value, numElements)}}}
		for i := range numElements {
			value.GetListValue().Values[i] = randomValue(t.ArrayElementType)
		}
		return value
	}
	return nullValue
}

func randomBoolValue() *structpb.Value {
	return &structpb.Value{Kind: &structpb.Value_BoolValue{BoolValue: rand.Intn(2) == 1}}
}

func randomString() string {
	b := make([]byte, rand.Intn(1024))
	_, _ = crypto.Read(b)
	return base64.StdEncoding.EncodeToString(b)
}

func randomBytesValue() *structpb.Value {
	return &structpb.Value{Kind: &structpb.Value_StringValue{StringValue: randomString()}}
}

func randomDateValue() *structpb.Value {
	year := rand.Intn(2100) + 1
	month := rand.Intn(12) + 1
	day := rand.Intn(28) + 1
	return &structpb.Value{Kind: &structpb.Value_StringValue{StringValue: fmt.Sprintf("%04d-%02d-%02d", year, month, day)}}
}

func randomFloat32Value() *structpb.Value {
	return &structpb.Value{Kind: &structpb.Value_NumberValue{NumberValue: float64(rand.Float32())}}
}

func randomFloat64Value() *structpb.Value {
	return &structpb.Value{Kind: &structpb.Value_NumberValue{NumberValue: rand.Float64()}}
}

func randomJsonValue() *structpb.Value {
	return &structpb.Value{Kind: &structpb.Value_StringValue{StringValue: fmt.Sprintf(`{"key": "%s"}`, randomString())}}
}

func randomInt64Value() *structpb.Value {
	return &structpb.Value{Kind: &structpb.Value_StringValue{StringValue: fmt.Sprintf("%d", rand.Int63())}}
}

func randomNumericValue() *structpb.Value {
	return &structpb.Value{Kind: &structpb.Value_StringValue{StringValue: fmt.Sprintf("%d.%d", rand.Intn(10000000), rand.Intn(1000))}}
}

func randomStringValue() *structpb.Value {
	return randomBytesValue()
}

func randomTimestampValue() *structpb.Value {
	t := time.UnixMilli(time.Now().UnixMilli() + int64(rand.Intn(1000000)))
	return &structpb.Value{Kind: &structpb.Value_StringValue{StringValue: t.Format(time.RFC3339)}}
}

func randomUuidValue() *structpb.Value {
	return &structpb.Value{Kind: &structpb.Value_StringValue{StringValue: uuid.New().String()}}
}

func nullValueOrAlt(nullValue bool, alt *structpb.Value) *structpb.Value {
	if nullValue {
		return &structpb.Value{Kind: &structpb.Value_NullValue{}}
	}
	return alt
}

func protoMessageProto(m proto.Message) *structpb.Value {
	var b, _ = proto.Marshal(m)
	return &structpb.Value{Kind: &structpb.Value_StringValue{StringValue: base64.StdEncoding.EncodeToString(b)}}
}

func protoEnumProto(e protoreflect.Enum) *structpb.Value {
	return &structpb.Value{Kind: &structpb.Value_StringValue{StringValue: strconv.FormatInt(int64(e.Number()), 10)}}
}

func CreateSelectDialectResultSet(dialect databasepb.DatabaseDialect) *spannerpb.ResultSet {
	name := databasepb.DatabaseDialect_name[int32(dialect)]
	return CreateSingleColumnStringResultSet([]string{name}, "option_value")
}

func CreateSelect1ResultSet() *spannerpb.ResultSet {
	return CreateSingleColumnInt64ResultSet([]int64{1}, "")
}

func CreateSingleColumnInt64ResultSet(values []int64, name string) *spannerpb.ResultSet {
	return CreateSingleColumnResultSet(values, func(v int64) *structpb.Value {
		return &structpb.Value{
			Kind: &structpb.Value_StringValue{StringValue: fmt.Sprintf("%v", v)},
		}
	}, name, spannerpb.TypeCode_INT64)
}

func CreateSingleColumnStringResultSet(values []string, name string) *spannerpb.ResultSet {
	return CreateSingleColumnResultSet(values, func(v string) *structpb.Value {
		return &structpb.Value{
			Kind: &structpb.Value_StringValue{StringValue: v},
		}
	}, name, spannerpb.TypeCode_STRING)
}

func CreateSingleColumnProtoResultSet(values [][]byte, name string) *spannerpb.ResultSet {
	return CreateSingleColumnResultSet(values, func(v []byte) *structpb.Value {
		str := base64.StdEncoding.EncodeToString(v)
		return &structpb.Value{
			Kind: &structpb.Value_StringValue{StringValue: str},
		}
	}, name, spannerpb.TypeCode_PROTO)
}

func CreateSingleColumnResultSet[V any](values []V, converter func(V) *structpb.Value, name string, typeCode spannerpb.TypeCode) *spannerpb.ResultSet {
	fields := make([]*spannerpb.StructType_Field, 1)
	fields[0] = &spannerpb.StructType_Field{
		Name: name,
		Type: &spannerpb.Type{Code: typeCode},
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
		rowValue[0] = converter(v)
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
