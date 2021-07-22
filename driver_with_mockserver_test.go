// Copyright 2021 Google Inc. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package spannerdriver

import (
	"cloud.google.com/go/civil"
	"cloud.google.com/go/spanner"
	"cloud.google.com/go/spanner/testutil"
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/base64"
	"fmt"
	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes"
	emptypb "github.com/golang/protobuf/ptypes/empty"
	proto3 "github.com/golang/protobuf/ptypes/struct"
	"github.com/google/go-cmp/cmp"
	"google.golang.org/api/option"
	longrunningpb "google.golang.org/genproto/googleapis/longrunning"
	sppb "google.golang.org/genproto/googleapis/spanner/v1"
	"google.golang.org/grpc/codes"
	gstatus "google.golang.org/grpc/status"
	"math/big"
	"reflect"
	"testing"
	"time"
)

func TestPing(t *testing.T) {
	t.Parallel()

	db, _, teardown := setupTestDbConnection(t)
	defer teardown()
	if err := db.Ping(); err != nil {
		t.Fatalf("unexpected error for ping: %v", err)
	}
}

func TestPing_Fails(t *testing.T) {
	t.Parallel()

	db, server, teardown := setupTestDbConnection(t)
	defer teardown()
	s := gstatus.Newf(codes.PermissionDenied, "Permission denied for database")
	server.TestSpanner.PutStatementResult("SELECT 1", &testutil.StatementResult{Err: s.Err()})
	if g, w := db.Ping(), driver.ErrBadConn; g != w {
		t.Fatalf("ping error mismatch\nGot: %v\nWant: %v", g, w)
	}
}

func TestSimpleQuery(t *testing.T) {
	t.Parallel()

	db, server, teardown := setupTestDbConnection(t)
	defer teardown()
	rows, err := db.Query(testutil.SelectFooFromBar)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()

	for want := int64(1); rows.Next(); want++ {
		cols, err := rows.Columns()
		if err != nil {
			t.Fatal(err)
		}
		if !cmp.Equal(cols, []string{"FOO"}) {
			t.Fatalf("cols mismatch\nGot: %v\nWant: %v", cols, []string{"FOO"})
		}
		var got int64
		err = rows.Scan(&got)
		if err != nil {
			t.Fatal(err)
		}
		if got != want {
			t.Fatalf("value mismatch\nGot: %v\nWant: %v", got, want)
		}
	}
	if rows.Err() != nil {
		t.Fatal(rows.Err())
	}
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
	if !req.Transaction.GetSingleUse().GetReadOnly().GetStrong() {
		t.Fatalf("missing strong timestampbound for ExecuteSqlRequest")
	}
}

func TestSimpleReadOnlyTransaction(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	db, server, teardown := setupTestDbConnection(t)
	defer teardown()
	tx, err := db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		t.Fatal(err)
	}
	rows, err := tx.Query(testutil.SelectFooFromBar)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()

	for want := int64(1); rows.Next(); want++ {
		cols, err := rows.Columns()
		if err != nil {
			t.Fatal(err)
		}
		if !cmp.Equal(cols, []string{"FOO"}) {
			t.Fatalf("cols mismatch\nGot: %v\nWant: %v", cols, []string{"FOO"})
		}
		var got int64
		err = rows.Scan(&got)
		if err != nil {
			t.Fatal(err)
		}
		if got != want {
			t.Fatalf("value mismatch\nGot: %v\nWant: %v", got, want)
		}
	}
	if rows.Err() != nil {
		t.Fatal(rows.Err())
	}
	err = tx.Commit()
	if err != nil {
		t.Fatal(err)
	}

	requests := drainRequestsFromServer(server.TestSpanner)
	sqlRequests := requestsOfType(requests, reflect.TypeOf(&sppb.ExecuteSqlRequest{}))
	if g, w := len(sqlRequests), 1; g != w {
		t.Fatalf("ExecuteSqlRequests count mismatch\nGot: %v\nWant: %v", g, w)
	}
	req := sqlRequests[0].(*sppb.ExecuteSqlRequest)
	if req.Transaction == nil {
		t.Fatalf("missing transaction for ExecuteSqlRequest")
	}
	if req.Transaction.GetId() == nil {
		t.Fatalf("missing id selector for ExecuteSqlRequest")
	}
	// Read-only transactions are not really committed on Cloud Spanner, so
	// there should be no commit request on the server.
	commitRequests := requestsOfType(requests, reflect.TypeOf(&sppb.CommitRequest{}))
	if g, w := len(commitRequests), 0; g != w {
		t.Fatalf("commit requests count mismatch\nGot: %v\nWant: %v", g, w)
	}
	beginReadOnlyRequests := filterBeginReadOnlyRequests(requestsOfType(requests, reflect.TypeOf(&sppb.BeginTransactionRequest{})))
	if g, w := len(beginReadOnlyRequests), 1; g != w {
		t.Fatalf("begin requests count mismatch\nGot: %v\nWant: %v", g, w)
	}
}

func TestSimpleReadWriteTransaction(t *testing.T) {
	t.Parallel()

	db, server, teardown := setupTestDbConnection(t)
	defer teardown()
	tx, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	rows, err := tx.Query(testutil.SelectFooFromBar)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()

	for want := int64(1); rows.Next(); want++ {
		cols, err := rows.Columns()
		if err != nil {
			t.Fatal(err)
		}
		if !cmp.Equal(cols, []string{"FOO"}) {
			t.Fatalf("cols mismatch\nGot: %v\nWant: %v", cols, []string{"FOO"})
		}
		var got int64
		err = rows.Scan(&got)
		if err != nil {
			t.Fatal(err)
		}
		if got != want {
			t.Fatalf("value mismatch\nGot: %v\nWant: %v", got, want)
		}
	}
	if rows.Err() != nil {
		t.Fatal(rows.Err())
	}
	err = tx.Commit()
	if err != nil {
		t.Fatal(err)
	}

	requests := drainRequestsFromServer(server.TestSpanner)
	sqlRequests := requestsOfType(requests, reflect.TypeOf(&sppb.ExecuteSqlRequest{}))
	if g, w := len(sqlRequests), 1; g != w {
		t.Fatalf("ExecuteSqlRequests count mismatch\nGot: %v\nWant: %v", g, w)
	}
	req := sqlRequests[0].(*sppb.ExecuteSqlRequest)
	if req.Transaction == nil {
		t.Fatalf("missing transaction for ExecuteSqlRequest")
	}
	if req.Transaction.GetId() == nil {
		t.Fatalf("missing id selector for ExecuteSqlRequest")
	}
	commitRequests := requestsOfType(requests, reflect.TypeOf(&sppb.CommitRequest{}))
	if g, w := len(commitRequests), 1; g != w {
		t.Fatalf("commit requests count mismatch\nGot: %v\nWant: %v", g, w)
	}
	commitReq := commitRequests[0].(*sppb.CommitRequest)
	if c, e := commitReq.GetTransactionId(), req.Transaction.GetId(); !cmp.Equal(c, e) {
		t.Fatalf("transaction id mismatch\nCommit: %c\nExecute: %v", c, e)
	}
}

func TestPreparedQuery(t *testing.T) {
	t.Parallel()

	db, server, teardown := setupTestDbConnection(t)
	defer teardown()
	server.TestSpanner.PutStatementResult(
		"SELECT * FROM Test WHERE Id=@id",
		&testutil.StatementResult{
			Type:      testutil.StatementResultResultSet,
			ResultSet: testutil.CreateSelect1ResultSet(),
		},
	)

	stmt, err := db.Prepare("SELECT * FROM Test WHERE Id=@id")
	if err != nil {
		t.Fatal(err)
	}
	defer stmt.Close()
	rows, err := stmt.Query(1)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()

	for want := int64(1); rows.Next(); want++ {
		var got int64
		err = rows.Scan(&got)
		if err != nil {
			t.Fatal(err)
		}
		if got != want {
			t.Fatalf("value mismatch\nGot: %v\nWant: %v", got, want)
		}
	}
	if rows.Err() != nil {
		t.Fatal(rows.Err())
	}
	requests := drainRequestsFromServer(server.TestSpanner)
	sqlRequests := requestsOfType(requests, reflect.TypeOf(&sppb.ExecuteSqlRequest{}))
	if g, w := len(sqlRequests), 1; g != w {
		t.Fatalf("sql requests count mismatch\nGot: %v\nWant: %v", g, w)
	}
	req := sqlRequests[0].(*sppb.ExecuteSqlRequest)
	if g, w := len(req.ParamTypes), 1; g != w {
		t.Fatalf("param types length mismatch\nGot: %v\nWant: %v", g, w)
	}
	if pt, ok := req.ParamTypes["id"]; ok {
		if g, w := pt.Code, sppb.TypeCode_INT64; g != w {
			t.Fatalf("param type mismatch\nGot: %v\nWant: %v", g, w)
		}
	} else {
		t.Fatalf("no param type found for @id")
	}
	if g, w := len(req.Params.Fields), 1; g != w {
		t.Fatalf("params length mismatch\nGot: %v\nWant: %v", g, w)
	}
	if val, ok := req.Params.Fields["id"]; ok {
		if g, w := val.GetStringValue(), "1"; g != w {
			t.Fatalf("param value mismatch\nGot: %v\nWant: %v", g, w)
		}
	} else {
		t.Fatalf("no value found for param @id")
	}
}

func TestQueryWithAllTypes(t *testing.T) {
	t.Parallel()

	db, server, teardown := setupTestDbConnection(t)
	defer teardown()
	sql := `SELECT *
             FROM Test
             WHERE ColBool=@bool 
             AND   ColString=@string
             AND   ColBytes=@bytes
             AND   ColInt=@int64
             AND   ColFloat=@float64
             AND   ColNumeric=@numeric
             AND   ColDate=@date
             AND   ColTimestamp=@timestamp`
	server.TestSpanner.PutStatementResult(
		sql,
		&testutil.StatementResult{
			Type:      testutil.StatementResultResultSet,
			ResultSet: testutil.CreateResultSetWithAllTypes(),
		},
	)

	stmt, err := db.Prepare(sql)
	if err != nil {
		t.Fatal(err)
	}
	defer stmt.Close()
	ts, _ := time.Parse(time.RFC3339Nano, "2021-07-22T10:26:17.123Z")
	rows, err := stmt.QueryContext(
		context.Background(),
		true,
		"test",
		[]byte("testbytes"),
		int64(5),
		3.14,
		numeric("6.626"),
		civil.Date{Year: 2021, Month: 7, Day: 21},
		ts)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()

	for rows.Next() {
		var b bool
		var s string
		var bt []byte
		var i int64
		var f float64
		var r big.Rat
		var d time.Time
		var ts time.Time
		err = rows.Scan(&b, &s, &bt, &i, &f, &r, &d, &ts)
		if err != nil {
			t.Fatal(err)
		}
		if g, w := b, true; g != w {
			t.Errorf("row value mismatch for bool\nGot: %v\nWant: %v", g, w)
		}
		if g, w := s, "test"; g != w {
			t.Errorf("row value mismatch for string\nGot: %v\nWant: %v", g, w)
		}
		if g, w := bt, []byte("testbytes"); !cmp.Equal(g, w) {
			t.Errorf("row value mismatch for bytes\nGot: %v\nWant: %v", g, w)
		}
		if g, w := i, int64(5); g != w {
			t.Errorf("row value mismatch for int64\nGot: %v\nWant: %v", g, w)
		}
		if g, w := f, 3.14; g != w {
			t.Errorf("row value mismatch for float64\nGot: %v\nWant: %v", g, w)
		}
		if g, w := r, numeric("6.626"); g.Cmp(w) != 0 {
			t.Errorf("row value mismatch for numeric\nGot: %v\nWant: %v", g, w)
		}
		// 2021-07-21
		if g, w := d, time.Date(2021, 7, 21, 0, 0, 0, 0, time.UTC); g != w {
			t.Errorf("row value mismatch for date\nGot: %v\nWant: %v", g, w)
		}
		// 2021-07-21T21:07:59.339911800Z
		if g, w := ts, time.Date(2021, 7, 21, 21, 7, 59, 339911800, time.UTC); g != w {
			t.Errorf("row value mismatch for timestamp\nGot: %v\nWant: %v", g, w)
		}
	}
	if rows.Err() != nil {
		t.Fatal(rows.Err())
	}
	requests := drainRequestsFromServer(server.TestSpanner)
	sqlRequests := requestsOfType(requests, reflect.TypeOf(&sppb.ExecuteSqlRequest{}))
	if g, w := len(sqlRequests), 1; g != w {
		t.Fatalf("sql requests count mismatch\nGot: %v\nWant: %v", g, w)
	}
	req := sqlRequests[0].(*sppb.ExecuteSqlRequest)
	if g, w := len(req.ParamTypes), 8; g != w {
		t.Fatalf("param types length mismatch\nGot: %v\nWant: %v", g, w)
	}
	if g, w := len(req.Params.Fields), 8; g != w {
		t.Fatalf("params length mismatch\nGot: %v\nWant: %v", g, w)
	}
	wantParams := []struct {
		name  string
		code  sppb.TypeCode
		value interface{}
	}{
		{
			name:  "bool",
			code:  sppb.TypeCode_BOOL,
			value: true,
		},
		{
			name:  "string",
			code:  sppb.TypeCode_STRING,
			value: "test",
		},
		{
			name:  "bytes",
			code:  sppb.TypeCode_BYTES,
			value: base64.StdEncoding.EncodeToString([]byte("testbytes")),
		},
		{
			name:  "int64",
			code:  sppb.TypeCode_INT64,
			value: "5",
		},
		{
			name:  "float64",
			code:  sppb.TypeCode_FLOAT64,
			value: 3.14,
		},
		{
			name:  "numeric",
			code:  sppb.TypeCode_NUMERIC,
			value: "6.626000000",
		},
		{
			name:  "date",
			code:  sppb.TypeCode_DATE,
			value: "2021-07-21",
		},
		{
			name:  "timestamp",
			code:  sppb.TypeCode_TIMESTAMP,
			value: "2021-07-22T10:26:17.123Z",
		},
	}
	for _, wantParam := range wantParams {
		if pt, ok := req.ParamTypes[wantParam.name]; ok {
			if g, w := pt.Code, wantParam.code; g != w {
				t.Errorf("param type mismatch\nGot: %v\nWant: %v", g, w)
			}
		} else {
			t.Errorf("no param type found for @%s", wantParam.name)
		}
		if val, ok := req.Params.Fields[wantParam.name]; ok {
			var g interface{}
			switch wantParam.code {
			case sppb.TypeCode_BOOL:
				g = val.GetBoolValue()
			case sppb.TypeCode_FLOAT64:
				g = val.GetNumberValue()
			default:
				g = val.GetStringValue()
			}
			if g != wantParam.value {
				t.Errorf("param value mismatch\nGot: %v\nWant: %v", g, wantParam.value)
			}
		} else {
			t.Errorf("no value found for param @%s", wantParam.name)
		}
	}
}

func TestQueryWithNullParameters(t *testing.T) {
	t.Parallel()

	db, server, teardown := setupTestDbConnection(t)
	defer teardown()
	sql := `SELECT *
             FROM Test
             WHERE ColBool=@bool 
             AND   ColString=@string
             AND   ColBytes=@bytes
             AND   ColInt=@int64
             AND   ColFloat=@float64
             AND   ColNumeric=@numeric
             AND   ColDate=@date
             AND   ColTimestamp=@timestamp`
	server.TestSpanner.PutStatementResult(
		sql,
		&testutil.StatementResult{
			Type:      testutil.StatementResultResultSet,
			ResultSet: testutil.CreateResultSetWithAllTypes(),
		},
	)

	stmt, err := db.Prepare(sql)
	if err != nil {
		t.Fatal(err)
	}
	defer stmt.Close()
	rows, err := stmt.QueryContext(
		context.Background(),
		nil, // bool
		nil, // string
		nil, // bytes
		nil, // int64
		nil, // float64
		nil, // numeric
		nil, // date
		nil, // timestamp
	)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()

	for rows.Next() {
		var b bool
		var s string
		var bt []byte
		var i int64
		var f float64
		var r big.Rat
		var d time.Time
		var ts time.Time
		err = rows.Scan(&b, &s, &bt, &i, &f, &r, &d, &ts)
		if err != nil {
			t.Fatal(err)
		}
		if g, w := b, true; g != w {
			t.Errorf("row value mismatch for bool\nGot: %v\nWant: %v", g, w)
		}
		if g, w := s, "test"; g != w {
			t.Errorf("row value mismatch for string\nGot: %v\nWant: %v", g, w)
		}
		if g, w := bt, []byte("testbytes"); !cmp.Equal(g, w) {
			t.Errorf("row value mismatch for bytes\nGot: %v\nWant: %v", g, w)
		}
		if g, w := i, int64(5); g != w {
			t.Errorf("row value mismatch for int64\nGot: %v\nWant: %v", g, w)
		}
		if g, w := f, 3.14; g != w {
			t.Errorf("row value mismatch for float64\nGot: %v\nWant: %v", g, w)
		}
		if g, w := r, numeric("6.626"); g.Cmp(w) != 0 {
			t.Errorf("row value mismatch for numeric\nGot: %v\nWant: %v", g, w)
		}
		// 2021-07-21
		if g, w := d, time.Date(2021, 7, 21, 0, 0, 0, 0, time.UTC); g != w {
			t.Errorf("row value mismatch for date\nGot: %v\nWant: %v", g, w)
		}
		// 2021-07-21T21:07:59.339911800Z
		if g, w := ts, time.Date(2021, 7, 21, 21, 7, 59, 339911800, time.UTC); g != w {
			t.Errorf("row value mismatch for timestamp\nGot: %v\nWant: %v", g, w)
		}
	}
	if rows.Err() != nil {
		t.Fatal(rows.Err())
	}
	requests := drainRequestsFromServer(server.TestSpanner)
	sqlRequests := requestsOfType(requests, reflect.TypeOf(&sppb.ExecuteSqlRequest{}))
	if g, w := len(sqlRequests), 1; g != w {
		t.Fatalf("sql requests count mismatch\nGot: %v\nWant: %v", g, w)
	}
	req := sqlRequests[0].(*sppb.ExecuteSqlRequest)
	// The param types map should be empty, as we are only sending nil params.
	if g, w := len(req.ParamTypes), 0; g != w {
		t.Fatalf("param types length mismatch\nGot: %v\nWant: %v", g, w)
	}
	if g, w := len(req.Params.Fields), 8; g != w {
		t.Fatalf("params length mismatch\nGot: %v\nWant: %v", g, w)
	}
	for _, param := range req.Params.Fields {
		if _, ok := param.GetKind().(*proto3.Value_NullValue); !ok {
			t.Errorf("param value mismatch\nGot: %v\nWant: %v", param.GetKind(), proto3.Value_NullValue{})
		}
	}
}

func TestDmlInAutocommit(t *testing.T) {
	t.Parallel()

	db, server, teardown := setupTestDbConnection(t)
	defer teardown()
	res, err := db.ExecContext(context.Background(), testutil.UpdateBarSetFoo)
	if err != nil {
		t.Fatal(err)
	}
	affected, err := res.RowsAffected()
	if err != nil {
		t.Fatal(err)
	}
	if g, w := affected, int64(testutil.UpdateBarSetFooRowCount); g != w {
		t.Fatalf("row count mismatch\nGot: %v\nWant: %v", g, w)
	}
	requests := drainRequestsFromServer(server.TestSpanner)
	sqlRequests := requestsOfType(requests, reflect.TypeOf(&sppb.ExecuteSqlRequest{}))
	if g, w := len(sqlRequests), 1; g != w {
		t.Fatalf("ExecuteSqlRequests count mismatch\nGot: %v\nWant: %v", g, w)
	}
	// The DML statement should use a transaction even though no explicit
	// transaction was created.
	req := sqlRequests[0].(*sppb.ExecuteSqlRequest)
	if req.Transaction == nil {
		t.Fatalf("missing transaction for ExecuteSqlRequest")
	}
	if req.Transaction.GetId() == nil {
		t.Fatalf("missing id selector for ExecuteSqlRequest")
	}
	commitRequests := requestsOfType(requests, reflect.TypeOf(&sppb.CommitRequest{}))
	if g, w := len(commitRequests), 1; g != w {
		t.Fatalf("commit requests count mismatch\nGot: %v\nWant: %v", g, w)
	}
	commitReq := commitRequests[0].(*sppb.CommitRequest)
	if c, e := commitReq.GetTransactionId(), req.Transaction.GetId(); !cmp.Equal(c, e) {
		t.Fatalf("transaction id mismatch\nCommit: %c\nExecute: %v", c, e)
	}
}

func TestDdlInAutocommit(t *testing.T) {
	t.Parallel()

	db, server, teardown := setupTestDbConnection(t)
	defer teardown()

	var expectedResponse *emptypb.Empty = &emptypb.Empty{}
	any, _ := ptypes.MarshalAny(expectedResponse)
	server.TestDatabaseAdmin.SetResps([]proto.Message{
		&longrunningpb.Operation{
			Done:   true,
			Result: &longrunningpb.Operation_Response{Response: any},
			Name:   "test-operation",
		},
	})
	res, err := db.ExecContext(context.Background(), "CREATE TABLE Singers (SingerId INT64, FirstName STRING(100), LastName STRING(100)) PRIMARY KEY (SingerId)")
	if err != nil {
		t.Fatal(err)
	}
	affected, err := res.RowsAffected()
	if err != nil {
		t.Fatal(err)
	}
	if affected != 0 {
		t.Fatalf("affected rows count mismatch\nGot: %v\nWant: %v", affected, 0)
	}
}

func numeric(v string) *big.Rat {
	res, _ := big.NewRat(1, 1).SetString(v)
	return res
}

func setupTestDbConnection(t *testing.T) (db *sql.DB, server *testutil.MockedSpannerInMemTestServer, teardown func()) {
	server, _, serverTeardown := setupMockedTestServer(t)
	db, err := sql.Open(
		"spanner",
		fmt.Sprintf("%s/projects/p/instances/i/databases/d?useplaintext=true", server.Address))
	if err != nil {
		serverTeardown()
		t.Fatal(err)
	}
	return db, server, func() {
		db.Close()
		serverTeardown()
	}
}

func setupMockedTestServer(t *testing.T) (server *testutil.MockedSpannerInMemTestServer, client *spanner.Client, teardown func()) {
	return setupMockedTestServerWithConfig(t, spanner.ClientConfig{})
}

func setupMockedTestServerWithConfig(t *testing.T, config spanner.ClientConfig) (server *testutil.MockedSpannerInMemTestServer, client *spanner.Client, teardown func()) {
	return setupMockedTestServerWithConfigAndClientOptions(t, config, []option.ClientOption{})
}

func setupMockedTestServerWithConfigAndClientOptions(t *testing.T, config spanner.ClientConfig, clientOptions []option.ClientOption) (server *testutil.MockedSpannerInMemTestServer, client *spanner.Client, teardown func()) {
	server, opts, serverTeardown := testutil.NewMockedSpannerInMemTestServer(t)
	opts = append(opts, clientOptions...)
	ctx := context.Background()
	formattedDatabase := fmt.Sprintf("projects/%s/instances/%s/databases/%s", "[PROJECT]", "[INSTANCE]", "[DATABASE]")
	client, err := spanner.NewClientWithConfig(ctx, formattedDatabase, config, opts...)
	if err != nil {
		t.Fatal(err)
	}
	return server, client, func() {
		client.Close()
		serverTeardown()
	}
}

func filterBeginReadOnlyRequests(requests []interface{}) []*sppb.BeginTransactionRequest {
	res := make([]*sppb.BeginTransactionRequest, 0)
	for _, r := range requests {
		if req, ok := r.(*sppb.BeginTransactionRequest); ok {
			if req.Options != nil && req.Options.GetReadOnly() != nil {
				res = append(res, req)
			}
		}
	}
	return res
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
