package main

import (
	"context"
	cryptorand "crypto/rand"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"testing"
	"time"

	"cloud.google.com/go/longrunning/autogen/longrunningpb"
	"cloud.google.com/go/spanner"
	"cloud.google.com/go/spanner/admin/database/apiv1/databasepb"
	sppb "cloud.google.com/go/spanner/apiv1/spannerpb"
	"github.com/google/uuid"
	"github.com/googleapis/go-sql-spanner/testutil"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/structpb"
	pb "spannerlib/grpc-server/google/spannerlib/v1"
)

func TestCreatePool(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	server, teardown := setupMockSpannerServer(t)
	defer teardown()
	dsn := fmt.Sprintf("%s/projects/p/instances/i/databases/d?useplaintext=true", server.Address)

	client, cleanup := startTestSpannerLibServer(t)
	defer cleanup()

	pool, err := client.CreatePool(ctx, &pb.CreatePoolRequest{ConnectionString: dsn})
	if err != nil {
		t.Fatalf("failed to create pool: %v", err)
	}
	if pool.Id <= 0 {
		t.Fatalf("pool id should be greater than zero")
	}

	if _, err := client.ClosePool(ctx, pool); err != nil {
		t.Fatalf("failed to close pool: %v", err)
	}
}

func TestCreateConnection(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	server, teardown := setupMockSpannerServer(t)
	defer teardown()
	dsn := fmt.Sprintf("%s/projects/p/instances/i/databases/d?useplaintext=true", server.Address)

	client, cleanup := startTestSpannerLibServer(t)
	defer cleanup()

	pool, err := client.CreatePool(ctx, &pb.CreatePoolRequest{ConnectionString: dsn})
	if err != nil {
		t.Fatalf("failed to create pool: %v", err)
	}
	connection, err := client.CreateConnection(ctx, &pb.CreateConnectionRequest{Pool: pool})
	if err != nil {
		t.Fatalf("failed to create connection: %v", err)
	}
	if connection.Id <= 0 {
		t.Fatalf("connection id should be greater than zero")
	}

	if _, err := client.CloseConnection(ctx, connection); err != nil {
		t.Fatalf("failed to close connection: %v", err)
	}
	if _, err := client.ClosePool(ctx, pool); err != nil {
		t.Fatalf("failed to close pool: %v", err)
	}
}

func TestExecute(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	server, teardown := setupMockSpannerServer(t)
	defer teardown()
	dsn := fmt.Sprintf("%s/projects/p/instances/i/databases/d?useplaintext=true", server.Address)

	client, cleanup := startTestSpannerLibServer(t)
	defer cleanup()

	pool, err := client.CreatePool(ctx, &pb.CreatePoolRequest{ConnectionString: dsn})
	if err != nil {
		t.Fatalf("failed to create pool: %v", err)
	}
	connection, err := client.CreateConnection(ctx, &pb.CreateConnectionRequest{Pool: pool})
	if err != nil {
		t.Fatalf("failed to create connection: %v", err)
	}
	rows, err := client.Execute(ctx, &pb.ExecuteRequest{
		Connection:        connection,
		ExecuteSqlRequest: &sppb.ExecuteSqlRequest{Sql: testutil.SelectFooFromBar},
	})
	if err != nil {
		t.Fatalf("failed to execute: %v", err)
	}
	metadata, err := client.Metadata(ctx, rows)
	if err != nil {
		t.Fatalf("failed to get metadata: %v", err)
	}
	if g, w := len(metadata.RowType.Fields), 1; g != w {
		t.Fatalf("num fields mismatch\n Got: %d\nWant: %d", g, w)
	}

	numRows := 0
	for {
		row, err := client.Next(ctx, &pb.NextRequest{Rows: rows, NumRows: 1})
		if err != nil {
			t.Fatalf("failed to fetch next row: %v", err)
		}
		if row.Values == nil {
			break
		}
		if g, w := len(row.Values), 1; g != w {
			t.Fatalf("num values mismatch\n Got: %v\nWant: %v", g, w)
		}
		numRows++
		if g, w := row.Values[0].GetStringValue(), fmt.Sprintf("%d", numRows); g != w {
			t.Fatalf("value mismatch\n Got: %v\nWant: %v", g, w)
		}
	}
	if g, w := numRows, 2; g != w {
		t.Fatalf("num rows mismatch\n Got: %v\nWant: %v", g, w)
	}
	stats, err := client.ResultSetStats(ctx, rows)
	if err != nil {
		t.Fatalf("failed to get stats: %v", err)
	}
	if g, w := stats.GetRowCountExact(), int64(0); g != w {
		t.Fatalf("row count mismatch\n Got: %v\nWant: %v", g, w)
	}
	if _, err := client.CloseRows(ctx, rows); err != nil {
		t.Fatalf("failed to close rows: %v", err)
	}

	if _, err := client.ClosePool(ctx, pool); err != nil {
		t.Fatalf("failed to close pool: %v", err)
	}
}

func TestExecuteWithTimeout(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	server, teardown := setupMockSpannerServer(t)
	defer teardown()
	dsn := fmt.Sprintf("%s/projects/p/instances/i/databases/d?useplaintext=true", server.Address)

	client, cleanup := startTestSpannerLibServer(t)
	defer cleanup()

	pool, err := client.CreatePool(ctx, &pb.CreatePoolRequest{ConnectionString: dsn})
	if err != nil {
		t.Fatalf("failed to create pool: %v", err)
	}
	connection, err := client.CreateConnection(ctx, &pb.CreateConnectionRequest{Pool: pool})
	if err != nil {
		t.Fatalf("failed to create connection: %v", err)
	}

	server.TestSpanner.PutExecutionTime(testutil.MethodExecuteStreamingSql, testutil.SimulatedExecutionTime{MinimumExecutionTime: 2 * time.Millisecond})
	withTimeout, cancel := context.WithTimeout(ctx, time.Millisecond)
	defer cancel()
	_, err = client.Execute(withTimeout, &pb.ExecuteRequest{
		Connection:        connection,
		ExecuteSqlRequest: &sppb.ExecuteSqlRequest{Sql: testutil.SelectFooFromBar},
	})
	if g, w := status.Code(err), codes.DeadlineExceeded; g != w {
		t.Fatalf("error code mismatch\n Got: %v\nWant: %v", g, w)
	}

	if _, err := client.ClosePool(ctx, pool); err != nil {
		t.Fatalf("failed to close pool: %v", err)
	}
}

func TestExecuteStreaming(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	server, teardown := setupMockSpannerServer(t)
	defer teardown()
	dsn := fmt.Sprintf("%s/projects/p/instances/i/databases/d?useplaintext=true", server.Address)

	client, cleanup := startTestSpannerLibServer(t)
	defer cleanup()

	pool, err := client.CreatePool(ctx, &pb.CreatePoolRequest{ConnectionString: dsn})
	if err != nil {
		t.Fatalf("failed to create pool: %v", err)
	}
	connection, err := client.CreateConnection(ctx, &pb.CreateConnectionRequest{Pool: pool})
	if err != nil {
		t.Fatalf("failed to create connection: %v", err)
	}
	stream, err := client.ExecuteStreaming(ctx, &pb.ExecuteRequest{
		Connection:        connection,
		ExecuteSqlRequest: &sppb.ExecuteSqlRequest{Sql: testutil.SelectFooFromBar},
	})
	if err != nil {
		t.Fatalf("failed to execute: %v", err)
	}
	numRows := 0
	for {
		row, err := stream.Recv()
		if err != nil {
			t.Fatalf("failed to receive row: %v", err)
		}
		if len(row.Data) == 0 {
			break
		}
		if g, w := len(row.Data), 1; g != w {
			t.Fatalf("num rows mismatch\n Got: %v\nWant: %v", g, w)
		}
		if g, w := len(row.Data[0].Values), 1; g != w {
			t.Fatalf("num values mismatch\n Got: %v\nWant: %v", g, w)
		}
		numRows++
		if g, w := row.Data[0].Values[0].GetStringValue(), fmt.Sprintf("%d", numRows); g != w {
			t.Fatalf("value mismatch\n Got: %v\nWant: %v", g, w)
		}
	}
	if g, w := numRows, 2; g != w {
		t.Fatalf("num rows mismatch\n Got: %v\nWant: %v", g, w)
	}

	if _, err := client.ClosePool(ctx, pool); err != nil {
		t.Fatalf("failed to close pool: %v", err)
	}
}

func TestExecuteStreamingMultiQuery(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	server, teardown := setupMockSpannerServer(t)
	defer teardown()
	dsn := fmt.Sprintf("%s/projects/p/instances/i/databases/d?useplaintext=true", server.Address)

	client, cleanup := startTestSpannerLibServer(t)
	defer cleanup()

	query1 := "select c from table1"
	_ = server.TestSpanner.PutStatementResult(query1, &testutil.StatementResult{
		Type:      testutil.StatementResultResultSet,
		ResultSet: testutil.CreateSingleColumnInt64ResultSet([]int64{1, 2}, "c"),
	})
	query2 := "select c1, c2 from table2"
	_ = server.TestSpanner.PutStatementResult(query2, &testutil.StatementResult{
		Type:      testutil.StatementResultResultSet,
		ResultSet: testutil.CreateTwoColumnResultSet([][2]int64{{10, 10}, {11, 11}, {12, 12}}, [2]string{"c1", "c2"}),
	})
	query3 := "select c from table3"
	_ = server.TestSpanner.PutStatementResult(query3, &testutil.StatementResult{
		Type:      testutil.StatementResultResultSet,
		ResultSet: testutil.CreateSingleColumnInt64ResultSet([]int64{3, 4, 5, 6}, "c"),
	})

	pool, err := client.CreatePool(ctx, &pb.CreatePoolRequest{ConnectionString: dsn})
	if err != nil {
		t.Fatalf("failed to create pool: %v", err)
	}
	connection, err := client.CreateConnection(ctx, &pb.CreateConnectionRequest{Pool: pool})
	if err != nil {
		t.Fatalf("failed to create connection: %v", err)
	}
	stream, err := client.ExecuteStreaming(ctx, &pb.ExecuteRequest{
		Connection:        connection,
		ExecuteSqlRequest: &sppb.ExecuteSqlRequest{Sql: fmt.Sprintf("%s;%s;%s", query1, query2, query3)},
	})
	if err != nil {
		t.Fatalf("failed to execute: %v", err)
	}

	consume := func() ([][]string, bool, error) {
		result := make([][]string, 0)
		hasMore := false
		for {
			chunk, err := stream.Recv()
			if err != nil {
				return nil, false, err
			}
			if len(chunk.Data) == 0 {
				hasMore = chunk.HasMoreResults
				break
			}
			for _, row := range chunk.Data {
				rowValues := make([]string, len(row.Values))
				for i := range rowValues {
					rowValues[i] = row.Values[i].GetStringValue()
				}
				result = append(result, rowValues)
			}
		}
		return result, hasMore, nil
	}
	result1, hasMore, err := consume()
	if err != nil {
		t.Fatalf("failed to consume result 1: %v", err)
	}
	if !hasMore {
		t.Fatalf("missing result 2")
	}
	result2, hasMore, err := consume()
	if err != nil {
		t.Fatalf("failed to consume result 2: %v", err)
	}
	if !hasMore {
		t.Fatalf("missing result 3")
	}
	result3, hasMore, err := consume()
	if err != nil {
		t.Fatalf("failed to consume result 3: %v", err)
	}
	if hasMore {
		t.Fatalf("found unexpected results after result 3")
	}
	if g, w := result1, [][]string{{"1"}, {"2"}}; !reflect.DeepEqual(g, w) {
		t.Fatalf("result1 mismatch\n Got: %v\nWant: %v", g, w)
	}
	if g, w := result2, [][]string{{"10", "10"}, {"11", "11"}, {"12", "12"}}; !reflect.DeepEqual(g, w) {
		t.Fatalf("result2 mismatch\n Got: %v\nWant: %v", g, w)
	}
	if g, w := result3, [][]string{{"3"}, {"4"}, {"5"}, {"6"}}; !reflect.DeepEqual(g, w) {
		t.Fatalf("result3 mismatch\n Got: %v\nWant: %v", g, w)
	}

	if _, err := client.ClosePool(ctx, pool); err != nil {
		t.Fatalf("failed to close pool: %v", err)
	}
}

func TestExecuteStreamingMultiStatement(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	server, teardown := setupMockSpannerServer(t)
	defer teardown()
	dsn := fmt.Sprintf("%s/projects/p/instances/i/databases/d?useplaintext=true", server.Address)

	// Add a generic successful DDL response to the mock server.
	var expectedResponse = &emptypb.Empty{}
	anyMsg, _ := anypb.New(expectedResponse)
	server.TestDatabaseAdmin.SetResps([]proto.Message{
		&longrunningpb.Operation{
			Done:   true,
			Result: &longrunningpb.Operation_Response{Response: anyMsg},
			Name:   "test-operation",
		},
	})

	client, cleanup := startTestSpannerLibServer(t)
	defer cleanup()

	pool, err := client.CreatePool(ctx, &pb.CreatePoolRequest{ConnectionString: dsn})
	if err != nil {
		t.Fatalf("failed to create pool: %v", err)
	}
	connection, err := client.CreateConnection(ctx, &pb.CreateConnectionRequest{Pool: pool})
	if err != nil {
		t.Fatalf("failed to create connection: %v", err)
	}

	type expectedResults struct {
		numRows  int
		affected int64
	}
	type test struct {
		name               string
		sql                string
		numExecuteRequests int
		numBachDmlRequests int
		expectedResults    []expectedResults
	}

	for _, tt := range []test{
		{
			name:               "two queries",
			sql:                fmt.Sprintf("%s;%s", testutil.SelectFooFromBar, testutil.SelectFooFromBar),
			numExecuteRequests: 2,
			expectedResults: []expectedResults{
				{numRows: 2},
				{numRows: 2},
			},
		},
		{
			name:               "three queries",
			sql:                fmt.Sprintf("%s;%s;%s", testutil.SelectFooFromBar, testutil.SelectFooFromBar, testutil.SelectFooFromBar),
			numExecuteRequests: 3,
			expectedResults: []expectedResults{
				{numRows: 2},
				{numRows: 2},
				{numRows: 2},
			},
		},
		{
			name:               "two DML statements",
			sql:                fmt.Sprintf("%s;%s", testutil.UpdateBarSetFoo, testutil.UpdateBarSetFoo),
			numBachDmlRequests: 1,
			expectedResults: []expectedResults{
				{affected: testutil.UpdateBarSetFooRowCount},
				{affected: testutil.UpdateBarSetFooRowCount},
			},
		},
		{
			name:            "two DDL statements",
			sql:             "create table my_table (id int64 primary key, value varchar(max)); create index my_index on my_table (value);",
			expectedResults: []expectedResults{{}, {}},
		},
		{
			name:               "query then DML",
			sql:                fmt.Sprintf("%s;%s", testutil.SelectFooFromBar, testutil.UpdateBarSetFoo),
			numExecuteRequests: 2,
			expectedResults: []expectedResults{
				{numRows: 2},
				{affected: testutil.UpdateBarSetFooRowCount},
			},
		},
		{
			name:               "DML then query",
			sql:                fmt.Sprintf("%s;%s", testutil.UpdateBarSetFoo, testutil.SelectFooFromBar),
			numExecuteRequests: 2,
			expectedResults: []expectedResults{
				{affected: testutil.UpdateBarSetFooRowCount},
				{numRows: 2},
			},
		},
		{
			name:               "query then DDL",
			sql:                fmt.Sprintf("%s;%s", testutil.SelectFooFromBar, "create table my_table (id int64 primary key, value varchar(max));"),
			numExecuteRequests: 1,
			expectedResults: []expectedResults{
				{numRows: 2},
				{},
			},
		},
		{
			name:               "DDL then query",
			sql:                fmt.Sprintf("%s;%s", "create table my_table (id int64 primary key)", testutil.SelectFooFromBar),
			numExecuteRequests: 1,
			expectedResults: []expectedResults{
				{},
				{numRows: 2},
			},
		},
		{
			name:               "DML then DDL",
			sql:                fmt.Sprintf("%s;%s", testutil.UpdateBarSetFoo, "create table my_table (id int64 primary key, value varchar(max));"),
			numExecuteRequests: 1,
			expectedResults: []expectedResults{
				{affected: testutil.UpdateBarSetFooRowCount},
				{},
			},
		},
		{
			name:               "DDL then DML",
			sql:                fmt.Sprintf("%s;%s", "create table my_table (id int64 primary key)", testutil.UpdateBarSetFoo),
			numExecuteRequests: 1,
			expectedResults: []expectedResults{
				{},
				{affected: testutil.UpdateBarSetFooRowCount},
			},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			stream, err := client.ExecuteStreaming(ctx, &pb.ExecuteRequest{
				Connection:        connection,
				ExecuteSqlRequest: &sppb.ExecuteSqlRequest{Sql: tt.sql},
			})
			if err != nil {
				t.Fatalf("failed to execute: %v", err)
			}
			numResultSets := 1
			numRows := 0
			for {
				row, err := stream.Recv()
				if err != nil {
					t.Fatalf("failed to receive row: %v", err)
				}
				if len(row.Data) == 0 {
					if g, w := numRows, tt.expectedResults[numResultSets-1].numRows; g != w {
						t.Fatalf("row count mismatch\n Got: %d\nWant: %d", g, w)
					}
					if row.Stats == nil {
						t.Fatal("missing stats in end-of-result-set marker")
					}
					if g, w := row.Stats.GetRowCountExact(), tt.expectedResults[numResultSets-1].affected; g != w {
						t.Fatalf("update count mismatch\n Got: %d\nWant: %d", g, w)
					}
					if row.HasMoreResults {
						numRows = 0
						numResultSets++
						continue
					}
					break
				}
				if g, w := len(row.Data), 1; g != w {
					t.Fatalf("num rows mismatch\n Got: %v\nWant: %v", g, w)
				}
				if g, w := len(row.Data[0].Values), 1; g != w {
					t.Fatalf("num values mismatch\n Got: %v\nWant: %v", g, w)
				}
				numRows++
				if g, w := row.Data[0].Values[0].GetStringValue(), fmt.Sprintf("%d", numRows); g != w {
					t.Fatalf("value mismatch\n Got: %v\nWant: %v", g, w)
				}
			}
			if g, w := numResultSets, len(tt.expectedResults); g != w {
				t.Fatalf("num result sets mismatch\n Got: %v\nWant: %v", g, w)
			}
			requests := server.TestSpanner.DrainRequestsFromServer()
			executeRequests := testutil.RequestsOfType(requests, reflect.TypeOf(&sppb.ExecuteSqlRequest{}))
			batchDmlRequests := testutil.RequestsOfType(requests, reflect.TypeOf(&sppb.ExecuteBatchDmlRequest{}))
			if g, w := len(executeRequests), tt.numExecuteRequests; g != w {
				t.Fatalf("num ExecuteSql requests mismatch\n Got: %d\nWant: %d", g, w)
			}
			if g, w := len(batchDmlRequests), tt.numBachDmlRequests; g != w {
				t.Fatalf("num BatchDml requests mismatch\n Got: %d\nWant: %d", g, w)
			}
		})
	}

	if _, err := client.ClosePool(ctx, pool); err != nil {
		t.Fatalf("failed to close pool: %v", err)
	}
}

func TestLargeMessage(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	server, teardown := setupMockSpannerServer(t)
	defer teardown()
	dsn := fmt.Sprintf("%s/projects/p/instances/i/databases/d?useplaintext=true", server.Address)

	client, cleanup := startTestSpannerLibServer(t)
	defer cleanup()

	pool, err := client.CreatePool(ctx, &pb.CreatePoolRequest{ConnectionString: dsn})
	if err != nil {
		t.Fatalf("failed to create pool: %v", err)
	}
	connection, err := client.CreateConnection(ctx, &pb.CreateConnectionRequest{Pool: pool})
	if err != nil {
		t.Fatalf("failed to create connection: %v", err)
	}

	query := "insert into foo (value) values (@value)"
	b := make([]byte, 10_000_000)
	n, err := cryptorand.Read(b)
	if err != nil {
		t.Fatalf("failed to read value: %v", err)
	}
	if g, w := n, len(b); g != w {
		t.Fatalf("length mismatch\n Got: %v\nWant: %v", g, w)
	}
	value := base64.StdEncoding.EncodeToString(b)
	_ = server.TestSpanner.PutStatementResult(query, &testutil.StatementResult{
		Type:        testutil.StatementResultUpdateCount,
		UpdateCount: 1,
	})
	stream, err := client.ExecuteStreaming(ctx, &pb.ExecuteRequest{
		Connection: connection,
		ExecuteSqlRequest: &sppb.ExecuteSqlRequest{
			Sql: query,
			Params: &structpb.Struct{
				Fields: map[string]*structpb.Value{
					"value": {Kind: &structpb.Value_StringValue{StringValue: value}},
				},
			},
		},
	})
	if err != nil {
		t.Fatalf("failed to execute: %v", err)
	}
	numRows := 0
	for {
		row, err := stream.Recv()
		if err != nil {
			t.Fatalf("failed to receive row: %v", err)
		}
		if len(row.Data) == 0 {
			break
		}
		numRows++
	}
	if g, w := numRows, 0; g != w {
		t.Fatalf("num rows mismatch\n Got: %v\nWant: %v", g, w)
	}

	if _, err := client.ClosePool(ctx, pool); err != nil {
		t.Fatalf("failed to close pool: %v", err)
	}
}

func TestExecuteStreamingWithTimeout(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	server, teardown := setupMockSpannerServer(t)
	defer teardown()
	dsn := fmt.Sprintf("%s/projects/p/instances/i/databases/d?useplaintext=true", server.Address)

	client, cleanup := startTestSpannerLibServer(t)
	defer cleanup()

	pool, err := client.CreatePool(ctx, &pb.CreatePoolRequest{ConnectionString: dsn})
	if err != nil {
		t.Fatalf("failed to create pool: %v", err)
	}
	connection, err := client.CreateConnection(ctx, &pb.CreateConnectionRequest{Pool: pool})
	if err != nil {
		t.Fatalf("failed to create connection: %v", err)
	}

	server.TestSpanner.PutExecutionTime(testutil.MethodExecuteStreamingSql, testutil.SimulatedExecutionTime{MinimumExecutionTime: 2 * time.Millisecond})
	withTimeout, cancel := context.WithTimeout(ctx, time.Millisecond)
	defer cancel()
	stream, err := client.ExecuteStreaming(withTimeout, &pb.ExecuteRequest{
		Connection:        connection,
		ExecuteSqlRequest: &sppb.ExecuteSqlRequest{Sql: testutil.SelectFooFromBar},
	})
	// The timeout can happen here or while waiting for the first response.
	if err != nil {
		if g, w := spanner.ErrCode(err), codes.DeadlineExceeded; g != w {
			t.Fatalf("error code mismatch\n Got: %v\nWant: %v", g, w)
		}
	} else {
		_, err = stream.Recv()
		if g, w := spanner.ErrCode(err), codes.DeadlineExceeded; g != w {
			t.Fatalf("error code mismatch\n Got: %v\nWant: %v", g, w)
		}
	}

	if _, err := client.ClosePool(ctx, pool); err != nil {
		t.Fatalf("failed to close pool: %v", err)
	}
}

func TestExecuteStreamingClientSideStatement(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	server, teardown := setupMockSpannerServer(t)
	defer teardown()
	dsn := fmt.Sprintf("%s/projects/p/instances/i/databases/d?useplaintext=true", server.Address)

	client, cleanup := startTestSpannerLibServer(t)
	defer cleanup()

	pool, err := client.CreatePool(ctx, &pb.CreatePoolRequest{ConnectionString: dsn})
	if err != nil {
		t.Fatalf("failed to create pool: %v", err)
	}
	connection, err := client.CreateConnection(ctx, &pb.CreateConnectionRequest{Pool: pool})
	if err != nil {
		t.Fatalf("failed to create connection: %v", err)
	}
	stream, err := client.ExecuteStreaming(ctx, &pb.ExecuteRequest{
		Connection:        connection,
		ExecuteSqlRequest: &sppb.ExecuteSqlRequest{Sql: "begin"},
	})
	if err != nil {
		t.Fatalf("failed to execute: %v", err)
	}
	for {
		row, err := stream.Recv()
		if err != nil {
			t.Fatalf("failed to receive row: %v", err)
		}
		if len(row.Data) == 0 {
			break
		}
	}
	stream, err = client.ExecuteStreaming(context.Background(), &pb.ExecuteRequest{
		Connection:        connection,
		ExecuteSqlRequest: &sppb.ExecuteSqlRequest{Sql: "commit"},
	})
	if err != nil {
		t.Fatalf("failed to execute: %v", err)
	}
	for {
		row, err := stream.Recv()
		if err != nil {
			t.Fatalf("failed to receive row: %v", err)
		}
		if len(row.Data) == 0 {
			break
		}
	}

	if _, err := client.ClosePool(ctx, pool); err != nil {
		t.Fatalf("failed to close pool: %v", err)
	}
}

func TestExecuteStreamingCustomSql(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	server, teardown := setupMockSpannerServer(t)
	defer teardown()
	dsn := fmt.Sprintf("%s/projects/p/instances/i/databases/d?useplaintext=true", server.Address)

	client, cleanup := startTestSpannerLibServer(t)
	defer cleanup()

	pool, err := client.CreatePool(ctx, &pb.CreatePoolRequest{ConnectionString: dsn})
	if err != nil {
		t.Fatalf("failed to create pool: %v", err)
	}
	connection, err := client.CreateConnection(ctx, &pb.CreateConnectionRequest{Pool: pool})
	if err != nil {
		t.Fatalf("failed to create connection: %v", err)
	}

	stream, err := client.ExecuteStreaming(ctx, &pb.ExecuteRequest{
		Connection:        connection,
		ExecuteSqlRequest: &sppb.ExecuteSqlRequest{Sql: "begin"},
	})
	if err != nil {
		t.Fatalf("failed to execute: %v", err)
	}
	row, err := stream.Recv()
	if err != nil {
		t.Fatalf("failed to receive row: %v", err)
	}
	if g, w := len(row.Data), 0; g != w {
		t.Fatalf("row data length mismatch\n Got: %v\nWant: %v", g, w)
	}
	if _, err := stream.Recv(); !errors.Is(err, io.EOF) {
		t.Fatalf("expected io.EOF, got: %v", err)
	}

	stream, err = client.ExecuteStreaming(ctx, &pb.ExecuteRequest{
		Connection:        connection,
		ExecuteSqlRequest: &sppb.ExecuteSqlRequest{Sql: testutil.SelectFooFromBar},
	})
	if err != nil {
		t.Fatalf("failed to execute: %v", err)
	}
	numRows := 0
	for {
		row, err := stream.Recv()
		if err != nil {
			t.Fatalf("failed to receive row: %v", err)
		}
		if len(row.Data) == 0 {
			break
		}
		if g, w := len(row.Data), 1; g != w {
			t.Fatalf("num rows mismatch\n Got: %v\nWant: %v", g, w)
		}
		if g, w := len(row.Data[0].Values), 1; g != w {
			t.Fatalf("num values mismatch\n Got: %v\nWant: %v", g, w)
		}
		numRows++
	}
	if g, w := numRows, 2; g != w {
		t.Fatalf("num rows mismatch\n Got: %v\nWant: %v", g, w)
	}

	stream, err = client.ExecuteStreaming(ctx, &pb.ExecuteRequest{
		Connection:        connection,
		ExecuteSqlRequest: &sppb.ExecuteSqlRequest{Sql: "commit"},
	})
	if err != nil {
		t.Fatalf("failed to execute: %v", err)
	}
	row, err = stream.Recv()
	if err != nil {
		t.Fatalf("failed to receive row: %v", err)
	}
	if g, w := len(row.Data), 0; g != w {
		t.Fatalf("row data length mismatch\n Got: %v\nWant: %v", g, w)
	}
	if _, err := stream.Recv(); !errors.Is(err, io.EOF) {
		t.Fatalf("expected io.EOF, got: %v", err)
	}

	if _, err := client.ClosePool(ctx, pool); err != nil {
		t.Fatalf("failed to close pool: %v", err)
	}
}

func TestExecuteBatch(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	server, teardown := setupMockSpannerServer(t)
	defer teardown()
	dsn := fmt.Sprintf("%s/projects/p/instances/i/databases/d?useplaintext=true", server.Address)

	client, cleanup := startTestSpannerLibServer(t)
	defer cleanup()

	pool, err := client.CreatePool(ctx, &pb.CreatePoolRequest{ConnectionString: dsn})
	if err != nil {
		t.Fatalf("failed to create pool: %v", err)
	}
	connection, err := client.CreateConnection(ctx, &pb.CreateConnectionRequest{Pool: pool})
	if err != nil {
		t.Fatalf("failed to create connection: %v", err)
	}

	resp, err := client.ExecuteBatch(ctx, &pb.ExecuteBatchRequest{
		Connection: connection,
		ExecuteBatchDmlRequest: &sppb.ExecuteBatchDmlRequest{
			Statements: []*sppb.ExecuteBatchDmlRequest_Statement{
				{Sql: testutil.UpdateBarSetFoo},
				{Sql: testutil.UpdateBarSetFoo},
			},
		},
	})
	if err != nil {
		t.Fatalf("failed to execute batch: %v", err)
	}
	if g, w := len(resp.ResultSets), 2; g != w {
		t.Fatalf("num results mismatch\n Got: %v\nWant: %v", g, w)
	}

	if _, err := client.ClosePool(ctx, pool); err != nil {
		t.Fatalf("failed to close pool: %v", err)
	}
}

func TestTransaction(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	server, teardown := setupMockSpannerServer(t)
	defer teardown()
	dsn := fmt.Sprintf("%s/projects/p/instances/i/databases/d?useplaintext=true", server.Address)

	client, cleanup := startTestSpannerLibServer(t)
	defer cleanup()

	pool, err := client.CreatePool(ctx, &pb.CreatePoolRequest{ConnectionString: dsn})
	if err != nil {
		t.Fatalf("failed to create pool: %v", err)
	}
	connection, err := client.CreateConnection(ctx, &pb.CreateConnectionRequest{Pool: pool})
	if err != nil {
		t.Fatalf("failed to create connection: %v", err)
	}
	if _, err := client.Execute(ctx, &pb.ExecuteRequest{
		Connection:        connection,
		ExecuteSqlRequest: &sppb.ExecuteSqlRequest{Sql: "set transaction_tag='test_tag'"},
	}); err != nil {
		t.Fatalf("failed to set transaction_tag: %v", err)
	}
	if _, err := client.BeginTransaction(ctx, &pb.BeginTransactionRequest{
		Connection:         connection,
		TransactionOptions: &sppb.TransactionOptions{},
	}); err != nil {
		t.Fatalf("failed to begin transaction: %v", err)
	}
	rows, err := client.Execute(ctx, &pb.ExecuteRequest{
		Connection:        connection,
		ExecuteSqlRequest: &sppb.ExecuteSqlRequest{Sql: testutil.UpdateBarSetFoo},
	})
	if err != nil {
		t.Fatalf("failed to execute: %v", err)
	}
	row, err := client.Next(ctx, &pb.NextRequest{Rows: rows, NumRows: 1})
	if err != nil {
		t.Fatalf("failed to fetch next row: %v", err)
	}
	if row.Values != nil {
		t.Fatalf("row values should be nil: %v", row.Values)
	}
	stats, err := client.ResultSetStats(ctx, rows)
	if err != nil {
		t.Fatalf("failed to get stats: %v", err)
	}
	if g, w := stats.GetRowCountExact(), int64(testutil.UpdateBarSetFooRowCount); g != w {
		t.Fatalf("row count mismatch\n Got: %v\nWant: %v", g, w)
	}
	if _, err := client.CloseRows(ctx, rows); err != nil {
		t.Fatalf("failed to close rows: %v", err)
	}
	if _, err := client.Commit(ctx, connection); err != nil {
		t.Fatalf("failed to commit: %v", err)
	}

	if _, err := client.ClosePool(ctx, pool); err != nil {
		t.Fatalf("failed to close pool: %v", err)
	}

	requests := server.TestSpanner.DrainRequestsFromServer()
	executeRequests := testutil.RequestsOfType(requests, reflect.TypeOf(&sppb.ExecuteSqlRequest{}))
	if g, w := len(executeRequests), 1; g != w {
		t.Fatalf("num execute requests mismatch\n Got: %v\nWant: %v", g, w)
	}
	request := executeRequests[0].(*sppb.ExecuteSqlRequest)
	if g, w := request.RequestOptions.TransactionTag, "test_tag"; g != w {
		t.Fatalf("transaction tag mismatch\n Got: %v\nWant: %v", g, w)
	}
}

func TestRollback(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	server, teardown := setupMockSpannerServer(t)
	defer teardown()
	dsn := fmt.Sprintf("%s/projects/p/instances/i/databases/d?useplaintext=true", server.Address)

	client, cleanup := startTestSpannerLibServer(t)
	defer cleanup()

	pool, err := client.CreatePool(ctx, &pb.CreatePoolRequest{ConnectionString: dsn})
	if err != nil {
		t.Fatalf("failed to create pool: %v", err)
	}
	connection, err := client.CreateConnection(ctx, &pb.CreateConnectionRequest{Pool: pool})
	if err != nil {
		t.Fatalf("failed to create connection: %v", err)
	}
	if _, err := client.BeginTransaction(ctx, &pb.BeginTransactionRequest{
		Connection:         connection,
		TransactionOptions: &sppb.TransactionOptions{},
	}); err != nil {
		t.Fatalf("failed to begin transaction: %v", err)
	}
	rows, err := client.Execute(ctx, &pb.ExecuteRequest{
		Connection:        connection,
		ExecuteSqlRequest: &sppb.ExecuteSqlRequest{Sql: testutil.UpdateBarSetFoo},
	})
	if err != nil {
		t.Fatalf("failed to execute: %v", err)
	}
	if _, err := client.CloseRows(ctx, rows); err != nil {
		t.Fatalf("failed to close rows: %v", err)
	}
	if _, err := client.Rollback(ctx, connection); err != nil {
		t.Fatalf("failed to rollback: %v", err)
	}

	if _, err := client.ClosePool(ctx, pool); err != nil {
		t.Fatalf("failed to close pool: %v", err)
	}
}

func TestWriteMutations(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	server, teardown := setupMockSpannerServer(t)
	defer teardown()
	dsn := fmt.Sprintf("%s/projects/p/instances/i/databases/d?useplaintext=true", server.Address)

	client, cleanup := startTestSpannerLibServer(t)
	defer cleanup()

	pool, err := client.CreatePool(ctx, &pb.CreatePoolRequest{ConnectionString: dsn})
	if err != nil {
		t.Fatalf("failed to create pool: %v", err)
	}
	connection, err := client.CreateConnection(ctx, &pb.CreateConnectionRequest{Pool: pool})
	if err != nil {
		t.Fatalf("failed to create connection: %v", err)
	}
	resp, err := client.WriteMutations(ctx, &pb.WriteMutationsRequest{
		Connection: connection,
		Mutations: &sppb.BatchWriteRequest_MutationGroup{
			Mutations: []*sppb.Mutation{
				{Operation: &sppb.Mutation_Update{
					Update: &sppb.Mutation_Write{
						Table:   "my_table",
						Columns: []string{"id", "value"},
						Values: []*structpb.ListValue{
							{Values: []*structpb.Value{
								{Kind: &structpb.Value_StringValue{StringValue: "1"}},
								{Kind: &structpb.Value_StringValue{StringValue: "One"}},
							}},
							{Values: []*structpb.Value{
								{Kind: &structpb.Value_StringValue{StringValue: "2"}},
								{Kind: &structpb.Value_StringValue{StringValue: "Two"}},
							}},
						},
					},
				}},
			},
		},
	})
	if err != nil {
		t.Fatalf("failed to write mutations: %v", err)
	}
	if resp == nil {
		t.Fatalf("response should not be nil")
	}
	if resp.CommitTimestamp == nil {
		t.Fatalf("commit timestamp should not be nil")
	}

	if _, err := client.ClosePool(ctx, pool); err != nil {
		t.Fatalf("failed to close pool: %v", err)
	}
}

func startTestSpannerLibServer(t *testing.T) (client pb.SpannerLibClient, cleanup func()) {
	var tp string
	var name string
	var protocol string
	if runtime.GOOS == "windows" {
		tp = "tcp"
		name = "localhost:0"
		protocol = ""
	} else {
		tp = "unix"
		protocol = "unix://"
		name = filepath.Join(os.TempDir(), fmt.Sprintf("spannerlib-%s", uuid.NewString()))
	}
	lis, err := net.Listen(tp, name)
	if err != nil {
		t.Fatalf("failed to listen: %v\n", err)
	}
	addr := lis.Addr().String()
	grpcServer, err := createServer()
	if err != nil {
		t.Fatalf("failed to create server: %v\n", err)
	}
	go func() { _ = grpcServer.Serve(lis) }()

	conn, err := grpc.NewClient(
		fmt.Sprintf("%s%s", protocol, addr),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("failed to create client connection: %v", err)
	}
	client = pb.NewSpannerLibClient(conn)

	cleanup = func() {
		_ = conn.Close()
		grpcServer.GracefulStop()
		_ = os.Remove(name)
	}

	return
}

func setupMockSpannerServer(t *testing.T) (server *testutil.MockedSpannerInMemTestServer, teardown func()) {
	return setupMockSpannerServerWithDialect(t, databasepb.DatabaseDialect_GOOGLE_STANDARD_SQL)
}

func setupMockSpannerServerWithDialect(t *testing.T, dialect databasepb.DatabaseDialect) (server *testutil.MockedSpannerInMemTestServer, teardown func()) {
	server, _, serverTeardown := testutil.NewMockedSpannerInMemTestServer(t)
	server.SetupSelectDialectResult(dialect)
	return server, serverTeardown
}
