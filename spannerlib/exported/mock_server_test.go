package exported

import (
	"context"
	"database/sql"
	"fmt"
	"reflect"
	"testing"
	"time"

	"cloud.google.com/go/spanner"
	"cloud.google.com/go/spanner/admin/database/apiv1/databasepb"
	sppb "cloud.google.com/go/spanner/apiv1/spannerpb"
	"github.com/google/uuid"
	"github.com/googleapis/go-sql-spanner/testutil"
	"google.golang.org/api/option"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/structpb"
)

func TestSimpleQuery(t *testing.T) {
	t.Parallel()

	dsn, _, teardown := setupTestDBConnection(t)
	defer teardown()

	pool := CreatePool(dsn)
	conn := CreateConnection(pool.ObjectId)
	statement := sppb.ExecuteSqlRequest{
		Sql: testutil.SelectFooFromBar,
	}
	statementBytes, err := proto.Marshal(&statement)
	if err != nil {
		t.Fatalf("failed to marshal statement: %v", err)
	}
	results := Execute(pool.ObjectId, conn.ObjectId, statementBytes)
	metadata := Metadata(pool.ObjectId, conn.ObjectId, results.ObjectId)
	if metadata.Code != 0 {
		t.Fatalf("metadata.Code: %v", metadata.Code)
	}
	for {
		row := Next(pool.ObjectId, conn.ObjectId, results.ObjectId)
		if row.Length() == 0 {
			break
		}
		values := structpb.ListValue{}
		if err := proto.Unmarshal(row.Res, &values); err != nil {
			t.Fatalf("failed to unmarshal row: %v", err)
		}
	}
	stats := ResultSetStats(pool.ObjectId, conn.ObjectId, results.ObjectId)
	if stats.Code != 0 {
		t.Fatalf("stats.Code: %v", stats.Code)
	}
	CloseRows(pool.ObjectId, conn.ObjectId, results.ObjectId)
	CloseConnection(pool.ObjectId, conn.ObjectId)
	ClosePool(pool.ObjectId)
}

func generateLargeResultSet(numRows int) *sppb.ResultSet {
	res := &sppb.ResultSet{
		Metadata: &sppb.ResultSetMetadata{
			RowType: &sppb.StructType{
				Fields: []*sppb.StructType_Field{
					{Type: &sppb.Type{Code: sppb.TypeCode_STRING}, Name: "col1"},
					{Type: &sppb.Type{Code: sppb.TypeCode_STRING}, Name: "col2"},
					{Type: &sppb.Type{Code: sppb.TypeCode_STRING}, Name: "col3"},
					{Type: &sppb.Type{Code: sppb.TypeCode_STRING}, Name: "col4"},
					{Type: &sppb.Type{Code: sppb.TypeCode_STRING}, Name: "col5"},
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

func TestLargeQuery(t *testing.T) {
	t.Skip("only for manual testing")
	t.Parallel()

	dsn, server, teardown := setupTestDBConnection(t)
	defer teardown()

	_ = server.TestSpanner.PutStatementResult("select * from all_types", &testutil.StatementResult{
		Type:      testutil.StatementResultResultSet,
		ResultSet: generateLargeResultSet(1000000),
	})

	pool := CreatePool(dsn)
	conn := CreateConnection(pool.ObjectId)
	statement := sppb.ExecuteSqlRequest{
		Sql: "select * from all_types",
	}
	statementBytes, err := proto.Marshal(&statement)
	if err != nil {
		t.Fatalf("failed to marshal statement: %v", err)
	}

	startTime := time.Now()
	results := Execute(pool.ObjectId, conn.ObjectId, statementBytes)
	metadata := Metadata(pool.ObjectId, conn.ObjectId, results.ObjectId)
	if metadata.Code != 0 {
		t.Fatalf("metadata.Code: %v", metadata.Code)
	}
	for {
		row := Next(pool.ObjectId, conn.ObjectId, results.ObjectId)
		if row.Length() == 0 {
			break
		}
		values := structpb.ListValue{}
		if err := proto.Unmarshal(row.Res, &values); err != nil {
			t.Fatalf("failed to unmarshal row: %v", err)
		}
	}
	stats := ResultSetStats(pool.ObjectId, conn.ObjectId, results.ObjectId)
	if stats.Code != 0 {
		t.Fatalf("stats.Code: %v", stats.Code)
	}
	CloseRows(pool.ObjectId, conn.ObjectId, results.ObjectId)
	endTime := time.Now()

	fmt.Printf("Query took %v\n", endTime.Sub(startTime))

	CloseConnection(pool.ObjectId, conn.ObjectId)
	ClosePool(pool.ObjectId)
}

func TestQueryWithTimestampBound(t *testing.T) {
	t.Parallel()

	dsn, server, teardown := setupTestDBConnection(t)
	defer teardown()

	pool := CreatePool(dsn)
	conn := CreateConnection(pool.ObjectId)
	statement := sppb.ExecuteSqlRequest{
		Sql: testutil.SelectFooFromBar,
		Transaction: &sppb.TransactionSelector{
			Selector: &sppb.TransactionSelector_SingleUse{
				SingleUse: &sppb.TransactionOptions{
					Mode: &sppb.TransactionOptions_ReadOnly_{
						ReadOnly: &sppb.TransactionOptions_ReadOnly{
							TimestampBound: &sppb.TransactionOptions_ReadOnly_MaxStaleness{
								MaxStaleness: &durationpb.Duration{Seconds: 10},
							},
						},
					},
				},
			},
		},
	}
	statementBytes, err := proto.Marshal(&statement)
	if err != nil {
		t.Fatalf("failed to marshal statement: %v", err)
	}
	results := Execute(pool.ObjectId, conn.ObjectId, statementBytes)
	metadata := Metadata(pool.ObjectId, conn.ObjectId, results.ObjectId)
	if metadata.Code != 0 {
		t.Fatalf("metadata.Code: %v", metadata.Code)
	}
	for {
		row := Next(pool.ObjectId, conn.ObjectId, results.ObjectId)
		if row.Length() == 0 {
			break
		}
		values := structpb.ListValue{}
		if err := proto.Unmarshal(row.Res, &values); err != nil {
			t.Fatalf("failed to unmarshal row: %v", err)
		}
	}
	stats := ResultSetStats(pool.ObjectId, conn.ObjectId, results.ObjectId)
	if stats.Code != 0 {
		t.Fatalf("stats.Code: %v", stats.Code)
	}
	CloseRows(pool.ObjectId, conn.ObjectId, results.ObjectId)
	CloseConnection(pool.ObjectId, conn.ObjectId)
	ClosePool(pool.ObjectId)

	requests := drainRequestsFromServer(server.TestSpanner)
	sqlRequests := requestsOfType(requests, reflect.TypeOf(&sppb.ExecuteSqlRequest{}))
	if g, w := len(sqlRequests), 1; g != w {
		t.Fatalf("sql requests count mismatch\nGot: %v\nWant: %v", g, w)
	}
	req := sqlRequests[0].(*sppb.ExecuteSqlRequest)
	if req.Transaction == nil {
		t.Fatalf("missing transaction for ExecuteSqlRequest")
	}
	if req.Transaction.GetSingleUse() == nil {
		t.Fatalf("missing single use selector for ExecuteSqlRequest")
	}
	if req.Transaction.GetSingleUse().GetReadOnly() == nil {
		t.Fatalf("missing read-only option for ExecuteSqlRequest")
	}
	if req.Transaction.GetSingleUse().GetReadOnly().GetMaxStaleness() == nil {
		t.Fatalf("missing max staleness timestampbound for ExecuteSqlRequest")
	}
	if g, w := req.Transaction.GetSingleUse().GetReadOnly().GetMaxStaleness().GetSeconds(), int64(10); g != w {
		t.Fatalf("max staleness seconds mismatch\n Got: %v\nWant: %v", g, w)
	}
}

func TestDisableInternalRetries(t *testing.T) {
	t.Parallel()

	dsn, server, teardown := setupTestDBConnection(t)
	defer teardown()

	pool := CreatePool(dsn)
	defer ClosePool(pool.ObjectId)
	conn := CreateConnection(pool.ObjectId)
	defer CloseConnection(pool.ObjectId, conn.ObjectId)

	txOpts := &sppb.TransactionOptions{}
	txOptsBytes, _ := proto.Marshal(txOpts)
	tx := BeginTransaction(pool.ObjectId, conn.ObjectId, txOptsBytes)

	statement := sppb.ExecuteSqlRequest{
		Sql: "set retry_aborts_internally = false",
	}
	statementBytes, _ := proto.Marshal(&statement)
	results := Execute(pool.ObjectId, conn.ObjectId, statementBytes)
	if results.Code != 0 {
		t.Fatalf("failed to set retry_aborts_internally: %v", results.Code)
	}
	CloseRows(pool.ObjectId, conn.ObjectId, results.ObjectId)

	statement = sppb.ExecuteSqlRequest{
		Sql: testutil.UpdateBarSetFoo,
	}
	statementBytes, _ = proto.Marshal(&statement)
	results = Execute(pool.ObjectId, conn.ObjectId, statementBytes)
	if results.Code != 0 {
		t.Fatalf("failed to execute update: %v", results.Code)
	}
	CloseRows(pool.ObjectId, conn.ObjectId, results.ObjectId)

	server.TestSpanner.PutExecutionTime(testutil.MethodCommitTransaction, testutil.SimulatedExecutionTime{
		Errors: []error{status.Error(codes.Aborted, "Aborted")},
	})
	results = Commit(pool.ObjectId, conn.ObjectId, tx.ObjectId)
	if g, w := codes.Code(results.Code), codes.Aborted; g != w {
		t.Fatalf("commit status mismatch\n Got: %v\nWant: %v", g, w)
	}
	CloseRows(pool.ObjectId, conn.ObjectId, results.ObjectId)
}

func TestApply(t *testing.T) {
	t.Parallel()

	dsn, server, teardown := setupTestDBConnection(t)
	defer teardown()

	pool := CreatePool(dsn)
	defer ClosePool(pool.ObjectId)
	conn := CreateConnection(pool.ObjectId)
	defer CloseConnection(pool.ObjectId, conn.ObjectId)

	mutations := sppb.BatchWriteRequest_MutationGroup{
		Mutations: []*sppb.Mutation{
			{Operation: &sppb.Mutation_Insert{Insert: &sppb.Mutation_Write{
				Table:   "foo",
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
			}}},
		},
	}
	mutationsBytes, _ := proto.Marshal(&mutations)
	response := Apply(pool.ObjectId, conn.ObjectId, mutationsBytes)
	if response.Code != 0 {
		t.Fatalf("failed to apply mutations: %v", response.Code)
	}
	commitResponse := sppb.CommitResponse{}
	_ = proto.Unmarshal(response.Res, &commitResponse)
	if commitResponse.CommitTimestamp == nil {
		t.Fatal("commit timestamp missing")
	}

	requests := drainRequestsFromServer(server.TestSpanner)
	beginRequests := requestsOfType(requests, reflect.TypeOf(&sppb.BeginTransactionRequest{}))
	if g, w := len(beginRequests), 1; g != w {
		t.Fatalf("begin requests count mismatch\nGot: %v\nWant: %v", g, w)
	}
	req := beginRequests[0].(*sppb.BeginTransactionRequest)
	if req.Options == nil {
		t.Fatalf("missing tx opts")
	}
	if req.Options.GetReadWrite() == nil {
		t.Fatalf("missing tx read write")
	}
	commitRequests := requestsOfType(requests, reflect.TypeOf(&sppb.CommitRequest{}))
	if g, w := len(commitRequests), 1; g != w {
		t.Fatalf("commit requests count mismatch\nGot: %v\nWant: %v", g, w)
	}
	commitReq := commitRequests[0].(*sppb.CommitRequest)
	if g, w := len(commitReq.Mutations), 1; g != w {
		t.Fatalf("mutation count mismatch\n Got: %v\nWant: %v", g, w)
	}
	if g, w := len(commitReq.Mutations[0].GetInsert().Values), 2; g != w {
		t.Fatalf("mutation values count mismatch\n Got: %v\nWant: %v", g, w)
	}
}

func TestBufferWrite(t *testing.T) {
	t.Parallel()

	dsn, server, teardown := setupTestDBConnection(t)
	defer teardown()

	pool := CreatePool(dsn)
	defer ClosePool(pool.ObjectId)
	conn := CreateConnection(pool.ObjectId)
	defer CloseConnection(pool.ObjectId, conn.ObjectId)

	txOpts := &sppb.TransactionOptions{}
	txOptsBytes, _ := proto.Marshal(txOpts)
	tx := BeginTransaction(pool.ObjectId, conn.ObjectId, txOptsBytes)

	mutations := sppb.BatchWriteRequest_MutationGroup{
		Mutations: []*sppb.Mutation{
			{Operation: &sppb.Mutation_Insert{Insert: &sppb.Mutation_Write{
				Table:   "foo",
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
			}}},
		},
	}
	mutationsBytes, _ := proto.Marshal(&mutations)
	response := BufferWrite(pool.ObjectId, conn.ObjectId, tx.ObjectId, mutationsBytes)
	if response.Code != 0 {
		t.Fatalf("failed to apply mutations: %v", response.Code)
	}
	if response.Length() > 0 {
		t.Fatal("response length mismatch")
	}

	requests := drainRequestsFromServer(server.TestSpanner)
	// There should not be any BeginTransaction requests yet, as we use inlined-begin.
	beginRequests := requestsOfType(requests, reflect.TypeOf(&sppb.BeginTransactionRequest{}))
	if g, w := len(beginRequests), 0; g != w {
		t.Fatalf("begin requests count mismatch\nGot: %v\nWant: %v", g, w)
	}
	// There should not be any commit requests yet.
	commitRequests := requestsOfType(requests, reflect.TypeOf(&sppb.CommitRequest{}))
	if g, w := len(commitRequests), 0; g != w {
		t.Fatalf("commit requests count mismatch\nGot: %v\nWant: %v", g, w)
	}

	// Commit the transaction with the mutation.
	res := Commit(pool.ObjectId, conn.ObjectId, tx.ObjectId)
	if res.Code != 0 {
		t.Fatalf("failed to commit: %v", res.Code)
	}

	// Verify that we have a commit request on the server.
	requests = drainRequestsFromServer(server.TestSpanner)
	beginRequests = requestsOfType(requests, reflect.TypeOf(&sppb.BeginTransactionRequest{}))
	if g, w := len(beginRequests), 1; g != w {
		t.Fatalf("begin requests count mismatch\nGot: %v\nWant: %v", g, w)
	}
	req := beginRequests[0].(*sppb.BeginTransactionRequest)
	if req.Options == nil {
		t.Fatalf("missing tx opts")
	}
	if req.Options.GetReadWrite() == nil {
		t.Fatalf("missing tx read write")
	}
	commitRequests = requestsOfType(requests, reflect.TypeOf(&sppb.CommitRequest{}))
	if g, w := len(commitRequests), 1; g != w {
		t.Fatalf("commit requests count mismatch\nGot: %v\nWant: %v", g, w)
	}
	commitReq := commitRequests[0].(*sppb.CommitRequest)
	if g, w := len(commitReq.Mutations), 1; g != w {
		t.Fatalf("mutation count mismatch\n Got: %v\nWant: %v", g, w)
	}
	if g, w := len(commitReq.Mutations[0].GetInsert().Values), 2; g != w {
		t.Fatalf("mutation values count mismatch\n Got: %v\nWant: %v", g, w)
	}

}

func TestBufferWrite_RetryAborted(t *testing.T) {
	t.Parallel()

	dsn, server, teardown := setupTestDBConnection(t)
	defer teardown()

	pool := CreatePool(dsn)
	defer ClosePool(pool.ObjectId)
	conn := CreateConnection(pool.ObjectId)
	defer CloseConnection(pool.ObjectId, conn.ObjectId)

	txOpts := &sppb.TransactionOptions{}
	txOptsBytes, _ := proto.Marshal(txOpts)
	tx := BeginTransaction(pool.ObjectId, conn.ObjectId, txOptsBytes)

	mutations := sppb.BatchWriteRequest_MutationGroup{
		Mutations: []*sppb.Mutation{
			{Operation: &sppb.Mutation_Insert{Insert: &sppb.Mutation_Write{
				Table:   "foo",
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
			}}},
		},
	}
	mutationsBytes, _ := proto.Marshal(&mutations)
	response := BufferWrite(pool.ObjectId, conn.ObjectId, tx.ObjectId, mutationsBytes)
	if response.Code != 0 {
		t.Fatalf("failed to apply mutations: %v", response.Code)
	}
	if response.Length() > 0 {
		t.Fatal("response length mismatch")
	}

	requests := drainRequestsFromServer(server.TestSpanner)
	// There should not be any commit requests yet.
	commitRequests := requestsOfType(requests, reflect.TypeOf(&sppb.CommitRequest{}))
	if g, w := len(commitRequests), 0; g != w {
		t.Fatalf("commit requests count mismatch\nGot: %v\nWant: %v", g, w)
	}

	// Instruct the mock server to abort the transaction.
	server.TestSpanner.PutExecutionTime(testutil.MethodCommitTransaction, testutil.SimulatedExecutionTime{
		Errors: []error{status.Error(codes.Aborted, "Aborted")},
	})

	// Commit the transaction with the mutation.
	res := Commit(pool.ObjectId, conn.ObjectId, tx.ObjectId)
	if res.Code != 0 {
		t.Fatalf("failed to commit: %v", res.Code)
	}

	// Verify that we have both begin and commit requests on the server.
	requests = drainRequestsFromServer(server.TestSpanner)
	beginRequests := requestsOfType(requests, reflect.TypeOf(&sppb.BeginTransactionRequest{}))
	if g, w := len(beginRequests), 2; g != w {
		t.Fatalf("begin requests count mismatch\nGot: %v\nWant: %v", g, w)
	}
	for _, beginReq := range beginRequests {
		req := beginReq.(*sppb.BeginTransactionRequest)
		if req.Options == nil {
			t.Fatalf("missing tx opts")
		}
		if req.Options.GetReadWrite() == nil {
			t.Fatalf("missing tx read write")
		}
	}
	commitRequests = requestsOfType(requests, reflect.TypeOf(&sppb.CommitRequest{}))
	if g, w := len(commitRequests), 2; g != w {
		t.Fatalf("commit requests count mismatch\nGot: %v\nWant: %v", g, w)
	}
	for _, req := range commitRequests {
		commitReq := req.(*sppb.CommitRequest)
		if g, w := len(commitReq.Mutations), 1; g != w {
			t.Fatalf("mutation count mismatch\n Got: %v\nWant: %v", g, w)
		}
		if g, w := len(commitReq.Mutations[0].GetInsert().Values), 2; g != w {
			t.Fatalf("mutation values count mismatch\n Got: %v\nWant: %v", g, w)
		}
	}
}

func setupTestDBConnection(t *testing.T) (dsn string, server *testutil.MockedSpannerInMemTestServer, teardown func()) {
	return setupTestDBConnectionWithParams(t, "")
}

func setupTestDBConnectionWithDialect(t *testing.T, dialect databasepb.DatabaseDialect) (dsn string, server *testutil.MockedSpannerInMemTestServer, teardown func()) {
	return setupTestDBConnectionWithParamsAndDialect(t, "", dialect)
}

func setupTestDBConnectionWithParams(t *testing.T, params string) (dsn string, server *testutil.MockedSpannerInMemTestServer, teardown func()) {
	return setupTestDBConnectionWithParamsAndDialect(t, params, databasepb.DatabaseDialect_GOOGLE_STANDARD_SQL)
}

func setupMockedTestServerWithDialect(t *testing.T, dialect databasepb.DatabaseDialect) (server *testutil.MockedSpannerInMemTestServer, client *spanner.Client, teardown func()) {
	return setupMockedTestServerWithConfigAndClientOptionsAndDialect(t, spanner.ClientConfig{}, []option.ClientOption{}, dialect)
}

func setupTestDBConnectionWithParamsAndDialect(t *testing.T, params string, dialect databasepb.DatabaseDialect) (dsn string, server *testutil.MockedSpannerInMemTestServer, teardown func()) {
	server, _, serverTeardown := setupMockedTestServerWithDialect(t, dialect)
	dsn = fmt.Sprintf("%s/projects/p/instances/i/databases/d?useplaintext=true;%s", server.Address, params)
	db, err := sql.Open("spanner", dsn)
	if err != nil {
		serverTeardown()
		t.Fatal(err)
	}
	return dsn, server, func() {
		_ = db.Close()
		serverTeardown()
	}
}

func setupMockedTestServerWithConfigAndClientOptionsAndDialect(t *testing.T, config spanner.ClientConfig, clientOptions []option.ClientOption, dialect databasepb.DatabaseDialect) (server *testutil.MockedSpannerInMemTestServer, client *spanner.Client, teardown func()) {
	server, opts, serverTeardown := testutil.NewMockedSpannerInMemTestServer(t)
	server.SetupSelectDialectResult(dialect)

	opts = append(opts, clientOptions...)
	ctx := context.Background()
	formattedDatabase := fmt.Sprintf("projects/%s/instances/%s/databases/%s", "[PROJECT]", "[INSTANCE]", "[DATABASE]")
	config.DisableNativeMetrics = true
	client, err := spanner.NewClientWithConfig(ctx, formattedDatabase, config, opts...)
	if err != nil {
		t.Fatal(err)
	}
	return server, client, func() {
		client.Close()
		serverTeardown()
	}
}

func requestsOfType(requests []interface{}, t reflect.Type) []interface{} {
	res := make([]interface{}, 0)
	for _, req := range requests {
		if reflect.TypeOf(req) == t {
			res = append(res, req)
		}
	}
	return res
}

func drainRequestsFromServer(server testutil.InMemSpannerServer) []interface{} {
	var reqs []interface{}
loop:
	for {
		select {
		case req := <-server.ReceivedRequests():
			reqs = append(reqs, req)
		default:
			break loop
		}
	}
	return reqs
}
