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

package spannerdriver

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"math/big"
	"math/rand"
	"reflect"
	"sync"
	"testing"
	"time"

	"cloud.google.com/go/civil"
	"cloud.google.com/go/longrunning/autogen/longrunningpb"
	"cloud.google.com/go/spanner"
	"cloud.google.com/go/spanner/admin/database/apiv1/databasepb"
	sppb "cloud.google.com/go/spanner/apiv1/spannerpb"
	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/googleapis/go-sql-spanner/testutil"
	"google.golang.org/api/option"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/emptypb"

	"google.golang.org/grpc/codes"
	gstatus "google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/structpb"
)

func TestPingContext(t *testing.T) {
	t.Parallel()

	db, _, teardown := setupTestDBConnection(t)
	defer teardown()
	if err := db.PingContext(context.Background()); err != nil {
		t.Fatalf("unexpected error for ping: %v", err)
	}
}

func TestPingContext_Fails(t *testing.T) {
	t.Parallel()

	db, server, teardown := setupTestDBConnection(t)
	defer teardown()
	s := gstatus.Newf(codes.PermissionDenied, "Permission denied for database")
	_ = server.TestSpanner.PutStatementResult("SELECT 1", &testutil.StatementResult{Err: s.Err()})
	if g, w := db.PingContext(context.Background()), driver.ErrBadConn; g != w {
		t.Fatalf("ping error mismatch\nGot: %v\nWant: %v", g, w)
	}
}

func TestSimpleQuery(t *testing.T) {
	t.Parallel()

	db, server, teardown := setupTestDBConnection(t)
	defer teardown()
	rows, err := db.QueryContext(context.Background(), testutil.SelectFooFromBar)
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

func TestConcurrentScanAndClose(t *testing.T) {
	t.Parallel()

	db, _, teardown := setupTestDBConnection(t)
	defer teardown()
	rows, err := db.QueryContext(context.Background(), testutil.SelectFooFromBar)
	if err != nil {
		t.Fatal(err)
	}

	// Only fetch the first row of the query to make sure that the rows are not auto-closed
	// when the end of the stream is reached.
	rows.Next()
	var got int64
	err = rows.Scan(&got)
	if err != nil {
		t.Fatal(err)
	}

	// Close both the database and the rows (connection) in parallel.
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		_ = db.Close()
	}()
	go func() {
		defer wg.Done()
		_ = rows.Close()
	}()
	wg.Wait()
}

func TestSingleQueryWithTimestampBound(t *testing.T) {
	t.Parallel()

	db, server, teardown := setupTestDBConnection(t)
	defer teardown()
	ctx := context.Background()
	conn, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := conn.ExecContext(ctx, "SET READ_ONLY_STALENESS = 'MAX_STALENESS 10s'"); err != nil {
		t.Fatalf("Set read-only staleness: %v", err)
	}
	rows, err := conn.QueryContext(context.Background(), testutil.SelectFooFromBar)
	if err != nil {
		t.Fatal(err)
	}

	for rows.Next() {
	}
	if rows.Err() != nil {
		t.Fatal(rows.Err())
	}
	_ = rows.Close()
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
		t.Fatalf("missing max_staleness timestampbound for ExecuteSqlRequest")
	}

	// Close the connection and execute a new query. This should use a strong read.
	_ = conn.Close()
	rows, err = db.QueryContext(context.Background(), testutil.SelectFooFromBar)
	if err != nil {
		t.Fatal(err)
	}

	for rows.Next() {
	}
	if rows.Err() != nil {
		t.Fatal(rows.Err())
	}
	_ = rows.Close()
	requests = drainRequestsFromServer(server.TestSpanner)
	sqlRequests = requestsOfType(requests, reflect.TypeOf(&sppb.ExecuteSqlRequest{}))
	if g, w := len(sqlRequests), 1; g != w {
		t.Fatalf("sql requests count mismatch\nGot: %v\nWant: %v", g, w)
	}
	req = sqlRequests[0].(*sppb.ExecuteSqlRequest)
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
	db, server, teardown := setupTestDBConnection(t)
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

func TestReadOnlyTransactionWithStaleness(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	db, server, teardown := setupTestDBConnection(t)
	defer teardown()

	conn, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := conn.ExecContext(ctx, "SET READ_ONLY_STALENESS = 'EXACT_STALENESS 10s'"); err != nil {
		t.Fatalf("Set read-only staleness: %v", err)
	}
	tx, err := conn.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		t.Fatal(err)
	}
	rows, err := tx.Query(testutil.SelectFooFromBar)
	if err != nil {
		t.Fatal(err)
	}

	for rows.Next() {
	}
	if rows.Err() != nil {
		t.Fatal(rows.Err())
	}
	_ = rows.Close()
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}

	requests := drainRequestsFromServer(server.TestSpanner)
	beginReadOnlyRequests := filterBeginReadOnlyRequests(requestsOfType(requests, reflect.TypeOf(&sppb.BeginTransactionRequest{})))
	if g, w := len(beginReadOnlyRequests), 1; g != w {
		t.Fatalf("begin requests count mismatch\nGot: %v\nWant: %v", g, w)
	}
	beginReq := beginReadOnlyRequests[0]
	if beginReq.GetOptions().GetReadOnly().GetExactStaleness() == nil {
		t.Fatalf("missing exact_staleness option on BeginTransaction request")
	}
}

func TestSimpleReadWriteTransaction(t *testing.T) {
	t.Parallel()

	db, server, teardown := setupTestDBConnection(t)
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

	db, server, teardown := setupTestDBConnection(t)
	defer teardown()
	_ = server.TestSpanner.PutStatementResult(
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

	db, server, teardown := setupTestDBConnection(t)
	defer teardown()
	query := `SELECT *
             FROM Test
             WHERE ColBool=@bool 
             AND   ColString=@string
             AND   ColBytes=@bytes
             AND   ColInt=@int64
             AND   ColFloat32=@float32
             AND   ColFloat64=@float64
             AND   ColNumeric=@numeric
             AND   ColDate=@date
             AND   ColTimestamp=@timestamp
             AND   ColJson=@json
             AND   ColBoolArray=@boolArray
             AND   ColStringArray=@stringArray
             AND   ColBytesArray=@bytesArray
             AND   ColIntArray=@int64Array
             AND   ColFloat32Array=@float32Array
             AND   ColFloat64Array=@float64Array
             AND   ColNumericArray=@numericArray
             AND   ColDateArray=@dateArray
             AND   ColTimestampArray=@timestampArray
             AND   ColJsonArray=@jsonArray`
	_ = server.TestSpanner.PutStatementResult(
		query,
		&testutil.StatementResult{
			Type:      testutil.StatementResultResultSet,
			ResultSet: testutil.CreateResultSetWithAllTypes(false),
		},
	)

	stmt, err := db.Prepare(query)
	if err != nil {
		t.Fatal(err)
	}
	defer stmt.Close()
	ts, _ := time.Parse(time.RFC3339Nano, "2021-07-22T10:26:17.123Z")
	ts1, _ := time.Parse(time.RFC3339Nano, "2021-07-21T21:07:59.339911800Z")
	ts2, _ := time.Parse(time.RFC3339Nano, "2021-07-27T21:07:59.339911800Z")
	rows, err := stmt.QueryContext(
		context.Background(),
		true,
		"test",
		[]byte("testbytes"),
		uint(5),
		float32(3.14),
		3.14,
		numeric("6.626"),
		civil.Date{Year: 2021, Month: 7, Day: 21},
		ts,
		nullJson(true, `{"key":"value","other-key":["value1","value2"]}`),
		[]spanner.NullBool{{Valid: true, Bool: true}, {}, {Valid: true, Bool: false}},
		[]spanner.NullString{{Valid: true, StringVal: "test1"}, {}, {Valid: true, StringVal: "test2"}},
		[][]byte{[]byte("testbytes1"), nil, []byte("testbytes2")},
		[]spanner.NullInt64{{Valid: true, Int64: 1}, {}, {Valid: true, Int64: 2}},
		[]spanner.NullFloat32{{Valid: true, Float32: 3.14}, {}, {Valid: true, Float32: -99.99}},
		[]spanner.NullFloat64{{Valid: true, Float64: 6.626}, {}, {Valid: true, Float64: 10.01}},
		[]spanner.NullNumeric{nullNumeric(true, "3.14"), {}, nullNumeric(true, "10.01")},
		[]spanner.NullDate{{Valid: true, Date: civil.Date{Year: 2000, Month: 2, Day: 29}}, {}, {Valid: true, Date: civil.Date{Year: 2021, Month: 7, Day: 27}}},
		[]spanner.NullTime{{Valid: true, Time: ts1}, {}, {Valid: true, Time: ts2}},
		[]spanner.NullJSON{
			nullJson(true, `{"key1": "value1", "other-key1": ["value1", "value2"]}`),
			nullJson(false, ""),
			nullJson(true, `{"key2": "value2", "other-key2": ["value1", "value2"]}`),
		},
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
		var f32 float32
		var f float64
		var r big.Rat
		var d civil.Date
		var ts time.Time
		var j spanner.NullJSON
		var bArray []spanner.NullBool
		var sArray []spanner.NullString
		var btArray [][]byte
		var iArray []spanner.NullInt64
		var f32Array []spanner.NullFloat32
		var fArray []spanner.NullFloat64
		var rArray []spanner.NullNumeric
		var dArray []spanner.NullDate
		var tsArray []spanner.NullTime
		var jArray []spanner.NullJSON
		err = rows.Scan(&b, &s, &bt, &i, &f32, &f, &r, &d, &ts, &j, &bArray, &sArray, &btArray, &iArray, &f32Array, &fArray, &rArray, &dArray, &tsArray, &jArray)
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
		if g, w := f32, float32(3.14); g != w {
			t.Errorf("row value mismatch for float32\nGot: %v\nWant: %v", g, w)
		}
		if g, w := f, 3.14; g != w {
			t.Errorf("row value mismatch for float64\nGot: %v\nWant: %v", g, w)
		}
		if g, w := r, numeric("6.626"); g.Cmp(&w) != 0 {
			t.Errorf("row value mismatch for numeric\nGot: %v\nWant: %v", g, w)
		}
		if g, w := d, date("2021-07-21"); !cmp.Equal(g, w) {
			t.Errorf("row value mismatch for date\nGot: %v\nWant: %v", g, w)
		}
		if g, w := ts, time.Date(2021, 7, 21, 21, 7, 59, 339911800, time.UTC); g != w {
			t.Errorf("row value mismatch for timestamp\nGot: %v\nWant: %v", g, w)
		}
		if !runsOnEmulator() {
			if g, w := j, nullJson(true, `{"key":"value","other-key":["value1","value2"]}`); !cmp.Equal(g, w) {
				t.Errorf("row value mismatch for json\nGot: %v\nWant: %v", g, w)
			}
		}
		if g, w := bArray, []spanner.NullBool{{Valid: true, Bool: true}, {}, {Valid: true, Bool: false}}; !cmp.Equal(g, w) {
			t.Errorf("row value mismatch for bool array\nGot: %v\nWant: %v", g, w)
		}
		if g, w := sArray, []spanner.NullString{{Valid: true, StringVal: "test1"}, {}, {Valid: true, StringVal: "test2"}}; !cmp.Equal(g, w) {
			t.Errorf("row value mismatch for string array\nGot: %v\nWant: %v", g, w)
		}
		if g, w := btArray, [][]byte{[]byte("testbytes1"), nil, []byte("testbytes2")}; !cmp.Equal(g, w) {
			t.Errorf("row value mismatch for bytes array\nGot: %v\nWant: %v", g, w)
		}
		if g, w := iArray, []spanner.NullInt64{{Valid: true, Int64: 1}, {}, {Valid: true, Int64: 2}}; !cmp.Equal(g, w) {
			t.Errorf("row value mismatch for int array\nGot: %v\nWant: %v", g, w)
		}
		if g, w := f32Array, []spanner.NullFloat32{{Valid: true, Float32: 3.14}, {}, {Valid: true, Float32: -99.99}}; !cmp.Equal(g, w) {
			t.Errorf("row value mismatch for float32 array\nGot: %v\nWant: %v", g, w)
		}
		if g, w := fArray, []spanner.NullFloat64{{Valid: true, Float64: 6.626}, {}, {Valid: true, Float64: 10.01}}; !cmp.Equal(g, w) {
			t.Errorf("row value mismatch for float64 array\nGot: %v\nWant: %v", g, w)
		}
		if g, w := rArray, []spanner.NullNumeric{nullNumeric(true, "3.14"), {}, nullNumeric(true, "10.01")}; !cmp.Equal(g, w, cmp.AllowUnexported(big.Rat{}, big.Int{})) {
			t.Errorf("row value mismatch for numeric array\nGot: %v\nWant: %v", g, w)
		}
		if g, w := dArray, []spanner.NullDate{{Valid: true, Date: civil.Date{Year: 2000, Month: 2, Day: 29}}, {}, {Valid: true, Date: civil.Date{Year: 2021, Month: 7, Day: 27}}}; !cmp.Equal(g, w) {
			t.Errorf("row value mismatch for date array\nGot: %v\nWant: %v", g, w)
		}
		if g, w := tsArray, []spanner.NullTime{{Valid: true, Time: ts1}, {}, {Valid: true, Time: ts2}}; !cmp.Equal(g, w) {
			t.Errorf("row value mismatch for timestamp array\nGot: %v\nWant: %v", g, w)
		}
		if !runsOnEmulator() {
			if g, w := jArray, []spanner.NullJSON{
				nullJson(true, `{"key1": "value1", "other-key1": ["value1", "value2"]}`),
				nullJson(false, ""),
				nullJson(true, `{"key2": "value2", "other-key2": ["value1", "value2"]}`),
			}; !cmp.Equal(g, w) {
				t.Errorf("row value mismatch for json array\nGot: %v\nWant: %v", g, w)
			}
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
	if g, w := len(req.ParamTypes), 20; g != w {
		t.Fatalf("param types length mismatch\nGot: %v\nWant: %v", g, w)
	}
	if g, w := len(req.Params.Fields), 20; g != w {
		t.Fatalf("params length mismatch\nGot: %v\nWant: %v", g, w)
	}
	wantParams := []struct {
		name  string
		code  sppb.TypeCode
		array bool
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
			name:  "float32",
			code:  sppb.TypeCode_FLOAT32,
			value: float64(float32(3.14)),
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
		{
			name:  "json",
			code:  sppb.TypeCode_JSON,
			value: `{"key":"value","other-key":["value1","value2"]}`,
		},
		{
			name:  "boolArray",
			code:  sppb.TypeCode_BOOL,
			array: true,
			value: &structpb.ListValue{Values: []*structpb.Value{
				{Kind: &structpb.Value_BoolValue{BoolValue: true}},
				{Kind: &structpb.Value_NullValue{}},
				{Kind: &structpb.Value_BoolValue{BoolValue: false}},
			}},
		},
		{
			name:  "stringArray",
			code:  sppb.TypeCode_STRING,
			array: true,
			value: &structpb.ListValue{Values: []*structpb.Value{
				{Kind: &structpb.Value_StringValue{StringValue: "test1"}},
				{Kind: &structpb.Value_NullValue{}},
				{Kind: &structpb.Value_StringValue{StringValue: "test2"}},
			}},
		},
		{
			name:  "bytesArray",
			code:  sppb.TypeCode_BYTES,
			array: true,
			value: &structpb.ListValue{Values: []*structpb.Value{
				{Kind: &structpb.Value_StringValue{StringValue: base64.StdEncoding.EncodeToString([]byte("testbytes1"))}},
				{Kind: &structpb.Value_NullValue{}},
				{Kind: &structpb.Value_StringValue{StringValue: base64.StdEncoding.EncodeToString([]byte("testbytes2"))}},
			}},
		},
		{
			name:  "int64Array",
			code:  sppb.TypeCode_INT64,
			array: true,
			value: &structpb.ListValue{Values: []*structpb.Value{
				{Kind: &structpb.Value_StringValue{StringValue: "1"}},
				{Kind: &structpb.Value_NullValue{}},
				{Kind: &structpb.Value_StringValue{StringValue: "2"}},
			}},
		},
		{
			name:  "float32Array",
			code:  sppb.TypeCode_FLOAT32,
			array: true,
			value: &structpb.ListValue{Values: []*structpb.Value{
				{Kind: &structpb.Value_NumberValue{NumberValue: float64(float32(3.14))}},
				{Kind: &structpb.Value_NullValue{}},
				{Kind: &structpb.Value_NumberValue{NumberValue: float64(float32(-99.99))}},
			}},
		},
		{
			name:  "float64Array",
			code:  sppb.TypeCode_FLOAT64,
			array: true,
			value: &structpb.ListValue{Values: []*structpb.Value{
				{Kind: &structpb.Value_NumberValue{NumberValue: 6.626}},
				{Kind: &structpb.Value_NullValue{}},
				{Kind: &structpb.Value_NumberValue{NumberValue: 10.01}},
			}},
		},
		{
			name:  "numericArray",
			code:  sppb.TypeCode_NUMERIC,
			array: true,
			value: &structpb.ListValue{Values: []*structpb.Value{
				{Kind: &structpb.Value_StringValue{StringValue: "3.140000000"}},
				{Kind: &structpb.Value_NullValue{}},
				{Kind: &structpb.Value_StringValue{StringValue: "10.010000000"}},
			}},
		},
		{
			name:  "dateArray",
			code:  sppb.TypeCode_DATE,
			array: true,
			value: &structpb.ListValue{Values: []*structpb.Value{
				{Kind: &structpb.Value_StringValue{StringValue: "2000-02-29"}},
				{Kind: &structpb.Value_NullValue{}},
				{Kind: &structpb.Value_StringValue{StringValue: "2021-07-27"}},
			}},
		},
		{
			name:  "timestampArray",
			code:  sppb.TypeCode_TIMESTAMP,
			array: true,
			value: &structpb.ListValue{Values: []*structpb.Value{
				{Kind: &structpb.Value_StringValue{StringValue: "2021-07-21T21:07:59.3399118Z"}},
				{Kind: &structpb.Value_NullValue{}},
				{Kind: &structpb.Value_StringValue{StringValue: "2021-07-27T21:07:59.3399118Z"}},
			}},
		},
		{
			name:  "jsonArray",
			code:  sppb.TypeCode_JSON,
			array: true,
			value: &structpb.ListValue{Values: []*structpb.Value{
				{Kind: &structpb.Value_StringValue{StringValue: `{"key1":"value1","other-key1":["value1","value2"]}`}},
				{Kind: &structpb.Value_NullValue{}},
				{Kind: &structpb.Value_StringValue{StringValue: `{"key2":"value2","other-key2":["value1","value2"]}`}},
			}},
		},
	}
	for _, wantParam := range wantParams {
		if pt, ok := req.ParamTypes[wantParam.name]; ok {
			if wantParam.array {
				if g, w := pt.Code, sppb.TypeCode_ARRAY; g != w {
					t.Errorf("param type mismatch\nGot: %v\nWant: %v", g, w)
				}
				if g, w := pt.ArrayElementType.Code, wantParam.code; g != w {
					t.Errorf("param array element type mismatch\nGot: %v\nWant: %v", g, w)
				}
			} else {
				if g, w := pt.Code, wantParam.code; g != w {
					t.Errorf("param type mismatch\nGot: %v\nWant: %v", g, w)
				}
			}
		} else {
			t.Errorf("no param type found for @%s", wantParam.name)
		}
		if val, ok := req.Params.Fields[wantParam.name]; ok {
			var g interface{}
			if wantParam.array {
				g = val.GetListValue()
			} else {
				switch wantParam.code {
				case sppb.TypeCode_BOOL:
					g = val.GetBoolValue()
				case sppb.TypeCode_FLOAT32:
					g = val.GetNumberValue()
				case sppb.TypeCode_FLOAT64:
					g = val.GetNumberValue()
				default:
					g = val.GetStringValue()
				}
			}
			if wantParam.array {
				if !cmp.Equal(g, wantParam.value, cmpopts.IgnoreUnexported(structpb.ListValue{}, structpb.Value{})) {
					t.Errorf("array param value mismatch\nGot:  %v\nWant: %v", g, wantParam.value)
				}
			} else {
				if g != wantParam.value {
					t.Errorf("param value mismatch\nGot: %v\nWant: %v", g, wantParam.value)
				}
			}
		} else {
			t.Errorf("no value found for param @%s", wantParam.name)
		}
	}
}

func TestQueryWithNullParameters(t *testing.T) {
	t.Parallel()

	db, server, teardown := setupTestDBConnection(t)
	defer teardown()
	query := `SELECT *
             FROM Test
             WHERE ColBool=@bool 
             AND   ColString=@string
             AND   ColBytes=@bytes
             AND   ColInt=@int64
             AND   ColFloat32=@float32
             AND   ColFloat64=@float64
             AND   ColNumeric=@numeric
             AND   ColDate=@date
             AND   ColTimestamp=@timestamp
             AND   ColJson=@json
             AND   ColBoolArray=@boolArray
             AND   ColStringArray=@stringArray
             AND   ColBytesArray=@bytesArray
             AND   ColIntArray=@int64Array
             AND   ColFloat32Array=@float32Array
             AND   ColFloat64Array=@float64Array
             AND   ColNumericArray=@numericArray
             AND   ColDateArray=@dateArray
             AND   ColTimestampArray=@timestampArray
             AND   ColJsonArray=@jsonArray`
	_ = server.TestSpanner.PutStatementResult(
		query,
		&testutil.StatementResult{
			Type:      testutil.StatementResultResultSet,
			ResultSet: testutil.CreateResultSetWithAllTypes(true),
		},
	)

	stmt, err := db.Prepare(query)
	if err != nil {
		t.Fatal(err)
	}
	defer stmt.Close()
	for _, p := range []struct {
		typed  int
		values []interface{}
	}{
		{
			typed: 0,
			values: []interface{}{
				nil, // bool
				nil, // string
				nil, // bytes
				nil, // int64
				nil, // float32
				nil, // float64
				nil, // numeric
				nil, // date
				nil, // timestamp
				nil, // json
				nil, // bool array
				nil, // string array
				nil, // bytes array
				nil, // int64 array
				nil, // float32 array
				nil, // float64 array
				nil, // numeric array
				nil, // date array
				nil, // timestamp array
				nil, // json array
			}},
		{
			typed: 9,
			values: []interface{}{
				spanner.NullBool{},
				spanner.NullString{},
				nil, // bytes
				spanner.NullInt64{},
				spanner.NullFloat32{},
				spanner.NullFloat64{},
				spanner.NullNumeric{},
				spanner.NullDate{},
				spanner.NullTime{},
				spanner.NullJSON{},
				nil, // bool array
				nil, // string array
				nil, // bytes array
				nil, // int64 array
				nil, // float32 array
				nil, // float64 array
				nil, // numeric array
				nil, // date array
				nil, // timestamp array
				nil, // json array
			}},
	} {
		rows, err := stmt.QueryContext(context.Background(), p.values...)
		if err != nil {
			t.Fatal(err)
		}
		defer rows.Close()

		for rows.Next() {
			var b sql.NullBool
			var s sql.NullString
			var bt []byte
			var i sql.NullInt64
			var f32 spanner.NullFloat32 // There's no equivalent sql type.
			var f sql.NullFloat64
			var r spanner.NullNumeric // There's no equivalent sql type.
			var d spanner.NullDate    // There's no equivalent sql type.
			var ts sql.NullTime
			var j spanner.NullJSON // There's no equivalent sql type.
			var bArray []spanner.NullBool
			var sArray []spanner.NullString
			var btArray [][]byte
			var iArray []spanner.NullInt64
			var f32Array []spanner.NullFloat32
			var fArray []spanner.NullFloat64
			var rArray []spanner.NullNumeric
			var dArray []spanner.NullDate
			var tsArray []spanner.NullTime
			var jArray []spanner.NullJSON
			err = rows.Scan(&b, &s, &bt, &i, &f32, &f, &r, &d, &ts, &j, &bArray, &sArray, &btArray, &iArray, &f32Array, &fArray, &rArray, &dArray, &tsArray, &jArray)
			if err != nil {
				t.Fatal(err)
			}
			if b.Valid {
				t.Errorf("row value mismatch for bool\nGot: %v\nWant: %v", b, spanner.NullBool{})
			}
			if s.Valid {
				t.Errorf("row value mismatch for string\nGot: %v\nWant: %v", s, spanner.NullString{})
			}
			if bt != nil {
				t.Errorf("row value mismatch for bytes\nGot: %v\nWant: %v", bt, nil)
			}
			if i.Valid {
				t.Errorf("row value mismatch for int64\nGot: %v\nWant: %v", i, spanner.NullInt64{})
			}
			if f32.Valid {
				t.Errorf("row value mismatch for float32\nGot: %v\nWant: %v", f, spanner.NullFloat32{})
			}
			if f.Valid {
				t.Errorf("row value mismatch for float64\nGot: %v\nWant: %v", f, spanner.NullFloat64{})
			}
			if r.Valid {
				t.Errorf("row value mismatch for numeric\nGot: %v\nWant: %v", r, spanner.NullNumeric{})
			}
			if d.Valid {
				t.Errorf("row value mismatch for date\nGot: %v\nWant: %v", d, spanner.NullDate{})
			}
			if ts.Valid {
				t.Errorf("row value mismatch for timestamp\nGot: %v\nWant: %v", ts, spanner.NullTime{})
			}
			if j.Valid {
				t.Errorf("row value mismatch for json\nGot: %v\nWant: %v", j, spanner.NullJSON{})
			}
			if bArray != nil {
				t.Errorf("row value mismatch for bool array\nGot: %v\nWant: %v", bArray, nil)
			}
			if sArray != nil {
				t.Errorf("row value mismatch for string array\nGot: %v\nWant: %v", sArray, nil)
			}
			if btArray != nil {
				t.Errorf("row value mismatch for bytes array array\nGot: %v\nWant: %v", btArray, nil)
			}
			if iArray != nil {
				t.Errorf("row value mismatch for int64 array\nGot: %v\nWant: %v", iArray, nil)
			}
			if f32Array != nil {
				t.Errorf("row value mismatch for float32 array\nGot: %v\nWant: %v", f32Array, nil)
			}
			if fArray != nil {
				t.Errorf("row value mismatch for float64 array\nGot: %v\nWant: %v", fArray, nil)
			}
			if rArray != nil {
				t.Errorf("row value mismatch for numeric array\nGot: %v\nWant: %v", rArray, nil)
			}
			if dArray != nil {
				t.Errorf("row value mismatch for date array\nGot: %v\nWant: %v", dArray, nil)
			}
			if tsArray != nil {
				t.Errorf("row value mismatch for timestamp array\nGot: %v\nWant: %v", tsArray, nil)
			}
			if jArray != nil {
				t.Errorf("row value mismatch for json array\nGot: %v\nWant: %v", jArray, nil)
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
		// The param types map should be empty when we are only sending untyped nil params.
		if g, w := len(req.ParamTypes), p.typed; g != w {
			t.Fatalf("param types length mismatch\nGot: %v\nWant: %v", g, w)
		}
		if g, w := len(req.Params.Fields), 20; g != w {
			t.Fatalf("params length mismatch\nGot: %v\nWant: %v", g, w)
		}
		for _, param := range req.Params.Fields {
			if _, ok := param.GetKind().(*structpb.Value_NullValue); !ok {
				t.Errorf("param value mismatch\nGot: %v\nWant: %v", param.GetKind(), structpb.Value_NullValue{})
			}
		}
	}
}

func TestDmlInAutocommit(t *testing.T) {
	t.Parallel()

	db, server, teardown := setupTestDBConnection(t)
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
	if _, ok := req.Transaction.Selector.(*sppb.TransactionSelector_Begin); !ok {
		t.Fatalf("unsupported transaction type %T", req.Transaction.Selector)
	}
	commitRequests := requestsOfType(requests, reflect.TypeOf(&sppb.CommitRequest{}))
	if g, w := len(commitRequests), 1; g != w {
		t.Fatalf("commit requests count mismatch\nGot: %v\nWant: %v", g, w)
	}
	commitReq := commitRequests[0].(*sppb.CommitRequest)
	if commitReq.GetTransactionId() == nil {
		t.Fatalf("missing id selector for CommitRequest")
	}
}

func TestQueryWithDuplicateNamedParameter(t *testing.T) {
	t.Parallel()

	db, server, teardown := setupTestDBConnection(t)
	defer teardown()

	s := "insert into users (id, name) values (@name, @name)"
	_ = server.TestSpanner.PutStatementResult(s, &testutil.StatementResult{
		Type:        testutil.StatementResultUpdateCount,
		UpdateCount: 1,
	})
	_, err := db.Exec(s, sql.Named("name", "foo"), sql.Named("name", "bar"))
	if err != nil {
		t.Fatal(err)
	}
	// Verify that 'bar' is used for both instances of the parameter @name.
	requests := drainRequestsFromServer(server.TestSpanner)
	sqlRequests := requestsOfType(requests, reflect.TypeOf(&sppb.ExecuteSqlRequest{}))
	if len(sqlRequests) != 1 {
		t.Fatalf("sql requests count mismatch\nGot: %v\nWant: %v", len(sqlRequests), 1)
	}
	req := sqlRequests[0].(*sppb.ExecuteSqlRequest)
	if g, w := len(req.Params.Fields), 1; g != w {
		t.Fatalf("params count mismatch\n Got: %v\nWant: %v", g, w)
	}
	if g, w := req.Params.Fields["name"].GetStringValue(), "bar"; g != w {
		t.Fatalf("param value mismatch\n Got: %v\nWant: %v", g, w)
	}
}

func TestQueryWithReusedNamedParameter(t *testing.T) {
	t.Parallel()

	db, server, teardown := setupTestDBConnection(t)
	defer teardown()

	s := "insert into users (id, name) values (@name, @name)"
	_ = server.TestSpanner.PutStatementResult(s, &testutil.StatementResult{
		Type:        testutil.StatementResultUpdateCount,
		UpdateCount: 1,
	})
	_, err := db.Exec(s, sql.Named("name", "foo"))
	if err != nil {
		t.Fatal(err)
	}
	// Verify that 'foo' is used for both instances of the parameter @name.
	requests := drainRequestsFromServer(server.TestSpanner)
	sqlRequests := requestsOfType(requests, reflect.TypeOf(&sppb.ExecuteSqlRequest{}))
	if len(sqlRequests) != 1 {
		t.Fatalf("sql requests count mismatch\nGot: %v\nWant: %v", len(sqlRequests), 1)
	}
	req := sqlRequests[0].(*sppb.ExecuteSqlRequest)
	if g, w := len(req.Params.Fields), 1; g != w {
		t.Fatalf("params count mismatch\n Got: %v\nWant: %v", g, w)
	}
	if g, w := req.Params.Fields["name"].GetStringValue(), "foo"; g != w {
		t.Fatalf("param value mismatch\n Got: %v\nWant: %v", g, w)
	}
}

func TestQueryWithReusedPositionalParameter(t *testing.T) {
	t.Parallel()

	db, server, teardown := setupTestDBConnection(t)
	defer teardown()

	s := "insert into users (id, name) values (@name, @name)"
	_ = server.TestSpanner.PutStatementResult(s, &testutil.StatementResult{
		Type:        testutil.StatementResultUpdateCount,
		UpdateCount: 1,
	})
	_, err := db.Exec(s, "foo", "bar")
	if err != nil {
		t.Fatal(err)
	}
	// Verify that 'bar' is used for both instances of the parameter @name.
	requests := drainRequestsFromServer(server.TestSpanner)
	sqlRequests := requestsOfType(requests, reflect.TypeOf(&sppb.ExecuteSqlRequest{}))
	if len(sqlRequests) != 1 {
		t.Fatalf("sql requests count mismatch\nGot: %v\nWant: %v", len(sqlRequests), 1)
	}
	req := sqlRequests[0].(*sppb.ExecuteSqlRequest)
	if g, w := len(req.Params.Fields), 1; g != w {
		t.Fatalf("params count mismatch\n Got: %v\nWant: %v", g, w)
	}
	if g, w := req.Params.Fields["name"].GetStringValue(), "bar"; g != w {
		t.Fatalf("param value mismatch\n Got: %v\nWant: %v", g, w)
	}
}

func TestQueryWithMissingPositionalParameter(t *testing.T) {
	t.Parallel()

	db, server, teardown := setupTestDBConnection(t)
	defer teardown()

	s := "insert into users (id, name) values (@name, @name)"
	_ = server.TestSpanner.PutStatementResult(s, &testutil.StatementResult{
		Type:        testutil.StatementResultUpdateCount,
		UpdateCount: 1,
	})
	_, err := db.Exec(s, "foo")
	if err != nil {
		t.Fatal(err)
	}
	// Verify that 'foo' is used for the parameter @name.
	requests := drainRequestsFromServer(server.TestSpanner)
	sqlRequests := requestsOfType(requests, reflect.TypeOf(&sppb.ExecuteSqlRequest{}))
	if len(sqlRequests) != 1 {
		t.Fatalf("sql requests count mismatch\nGot: %v\nWant: %v", len(sqlRequests), 1)
	}
	req := sqlRequests[0].(*sppb.ExecuteSqlRequest)
	if g, w := len(req.Params.Fields), 1; g != w {
		t.Fatalf("params count mismatch\n Got: %v\nWant: %v", g, w)
	}
	if g, w := req.Params.Fields["name"].GetStringValue(), "foo"; g != w {
		t.Fatalf("param value mismatch\n Got: %v\nWant: %v", g, w)
	}
}

func TestDdlInAutocommit(t *testing.T) {
	t.Parallel()

	db, server, teardown := setupTestDBConnection(t)
	defer teardown()

	var expectedResponse = &emptypb.Empty{}
	anyMsg, _ := anypb.New(expectedResponse)
	server.TestDatabaseAdmin.SetResps([]proto.Message{
		&longrunningpb.Operation{
			Done:   true,
			Result: &longrunningpb.Operation_Response{Response: anyMsg},
			Name:   "test-operation",
		},
	})
	query := "CREATE TABLE Singers (SingerId INT64, FirstName STRING(100), LastName STRING(100)) PRIMARY KEY (SingerId)"
	_, err := db.ExecContext(context.Background(), query)
	if err != nil {
		t.Fatal(err)
	}
	requests := server.TestDatabaseAdmin.Reqs()
	if g, w := len(requests), 1; g != w {
		t.Fatalf("requests count mismatch\nGot: %v\nWant: %v", g, w)
	}
	if req, ok := requests[0].(*databasepb.UpdateDatabaseDdlRequest); ok {
		if g, w := len(req.Statements), 1; g != w {
			t.Fatalf("statement count mismatch\nGot: %v\nWant: %v", g, w)
		}
		if g, w := req.Statements[0], query; g != w {
			t.Fatalf("statement mismatch\nGot: %v\nWant: %v", g, w)
		}
	} else {
		t.Fatalf("request type mismatch, got %v", requests[0])
	}
}

func TestDdlInTransaction(t *testing.T) {
	t.Parallel()

	db, server, teardown := setupTestDBConnection(t)
	defer teardown()

	query := "CREATE TABLE Singers (SingerId INT64, FirstName STRING(100), LastName STRING(100)) PRIMARY KEY (SingerId)"
	tx, err := db.BeginTx(context.Background(), &sql.TxOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.ExecContext(context.Background(), query); spanner.ErrCode(err) != codes.FailedPrecondition {
		t.Fatalf("error mismatch\nGot:  %v\nWant: %v", spanner.ErrCode(err), codes.FailedPrecondition)
	}
	requests := server.TestDatabaseAdmin.Reqs()
	if g, w := len(requests), 0; g != w {
		t.Fatalf("requests count mismatch\nGot: %v\nWant: %v", g, w)
	}
}

func TestBegin(t *testing.T) {
	t.Parallel()

	db, _, teardown := setupTestDBConnection(t)
	defer teardown()

	// Ensure that the old Begin method works.
	_, err := db.Begin()
	if err != nil {
		t.Fatalf("Begin failed: %v", err)
	}
}

func TestQuery(t *testing.T) {
	t.Parallel()

	db, _, teardown := setupTestDBConnection(t)
	defer teardown()

	// Ensure that the old Query method works.
	rows, err := db.Query(testutil.SelectFooFromBar)
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}
	defer rows.Close()
}

func TestExec(t *testing.T) {
	t.Parallel()

	db, _, teardown := setupTestDBConnection(t)
	defer teardown()

	// Ensure that the old Exec method works.
	_, err := db.Exec(testutil.UpdateBarSetFoo)
	if err != nil {
		t.Fatalf("Exec failed: %v", err)
	}
}

func TestPrepare(t *testing.T) {
	t.Parallel()

	db, _, teardown := setupTestDBConnection(t)
	defer teardown()

	// Ensure that the old Prepare method works.
	_, err := db.Prepare(testutil.SelectFooFromBar)
	if err != nil {
		t.Fatalf("Prepare failed: %v", err)
	}
}

func TestApplyMutations(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	db, server, teardown := setupTestDBConnection(t)
	defer teardown()

	conn, err := db.Conn(ctx)
	if err != nil {
		t.Fatalf("failed to get connection: %v", err)
	}
	var commitTimestamp time.Time
	if err := conn.Raw(func(driverConn interface{}) error {
		spannerConn, ok := driverConn.(SpannerConn)
		if !ok {
			return fmt.Errorf("unexpected driver connection %v, expected SpannerConn", driverConn)
		}
		commitTimestamp, err = spannerConn.Apply(ctx, []*spanner.Mutation{
			spanner.Insert("Accounts", []string{"AccountId", "Nickname", "Balance"}, []interface{}{int64(1), "Foo", int64(50)}),
			spanner.Insert("Accounts", []string{"AccountId", "Nickname", "Balance"}, []interface{}{int64(2), "Bar", int64(1)}),
		})
		return err
	}); err != nil {
		t.Fatalf("failed to apply mutations: %v", err)
	}
	if commitTimestamp.Equal(time.Time{}) {
		t.Fatal("no commit timestamp returned")
	}

	// Even though the Apply method is used outside a transaction, the connection will internally start a read/write
	// transaction for the mutations.
	requests := drainRequestsFromServer(server.TestSpanner)
	commitRequests := requestsOfType(requests, reflect.TypeOf(&sppb.CommitRequest{}))
	if g, w := len(commitRequests), 1; g != w {
		t.Fatalf("commit requests count mismatch\nGot: %v\nWant: %v", g, w)
	}
	commitRequest := commitRequests[0].(*sppb.CommitRequest)
	if g, w := len(commitRequest.Mutations), 2; g != w {
		t.Fatalf("mutation count mismatch\nGot: %v\nWant: %v", g, w)
	}
}

func TestApplyMutationsFailure(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	db, _, teardown := setupTestDBConnection(t)
	defer teardown()

	con, err := db.Conn(ctx)
	if err != nil {
		t.Fatalf("failed to get connection: %v", err)
	}
	_, err = con.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		t.Fatalf("failed to begin transaction: %v", err)
	}
	if g, w := spanner.ErrCode(con.Raw(func(driverConn interface{}) error {
		spannerConn, ok := driverConn.(SpannerConn)
		if !ok {
			return fmt.Errorf("unexpected driver connection %v, expected SpannerConn", driverConn)
		}
		_, err = spannerConn.Apply(ctx, []*spanner.Mutation{
			spanner.Insert("Accounts", []string{"AccountId", "Nickname", "Balance"}, []interface{}{int64(1), "Foo", int64(50)}),
			spanner.Insert("Accounts", []string{"AccountId", "Nickname", "Balance"}, []interface{}{int64(2), "Bar", int64(1)}),
		})
		return err
	})), codes.FailedPrecondition; g != w {
		t.Fatalf("error code mismatch for Apply during transaction\nGot:  %v\nWant: %v", g, w)
	}
}

func TestBufferWriteMutations(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	db, server, teardown := setupTestDBConnection(t)
	defer teardown()

	con, err := db.Conn(ctx)
	if err != nil {
		t.Fatalf("failed to get connection: %v", err)
	}
	tx, err := con.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		t.Fatalf("failed to begin transaction: %v", err)
	}
	if err := con.Raw(func(driverConn interface{}) error {
		spannerConn, ok := driverConn.(SpannerConn)
		if !ok {
			return fmt.Errorf("unexpected driver connection %v, expected SpannerConn", driverConn)
		}
		return spannerConn.BufferWrite([]*spanner.Mutation{
			spanner.Insert("Accounts", []string{"AccountId", "Nickname", "Balance"}, []interface{}{int64(1), "Foo", int64(50)}),
			spanner.Insert("Accounts", []string{"AccountId", "Nickname", "Balance"}, []interface{}{int64(2), "Bar", int64(1)}),
		})
	}); err != nil {
		t.Fatalf("failed to buffer mutations: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("failed to commit transaction: %v", err)
	}

	requests := drainRequestsFromServer(server.TestSpanner)
	commitRequests := requestsOfType(requests, reflect.TypeOf(&sppb.CommitRequest{}))
	if g, w := len(commitRequests), 1; g != w {
		t.Fatalf("commit requests count mismatch\nGot: %v\nWant: %v", g, w)
	}
	commitRequest := commitRequests[0].(*sppb.CommitRequest)
	if g, w := len(commitRequest.Mutations), 2; g != w {
		t.Fatalf("mutation count mismatch\nGot: %v\nWant: %v", g, w)
	}
}

func TestBufferWriteMutationsFails(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	db, _, teardown := setupTestDBConnection(t)
	defer teardown()

	con, err := db.Conn(ctx)
	if err != nil {
		t.Fatalf("failed to get connection: %v", err)
	}
	if g, w := spanner.ErrCode(con.Raw(func(driverConn interface{}) error {
		spannerConn, ok := driverConn.(SpannerConn)
		if !ok {
			return fmt.Errorf("unexpected driver connection %v, expected SpannerConn", driverConn)
		}
		return spannerConn.BufferWrite([]*spanner.Mutation{
			spanner.Insert("Accounts", []string{"AccountId", "Nickname", "Balance"}, []interface{}{int64(1), "Foo", int64(50)}),
			spanner.Insert("Accounts", []string{"AccountId", "Nickname", "Balance"}, []interface{}{int64(2), "Bar", int64(1)}),
		})
	})), codes.FailedPrecondition; g != w {
		t.Fatalf("error code mismatch for BufferWrite outside transaction\nGot:  %v\nWant: %v", g, w)
	}
}

func TestPing(t *testing.T) {
	t.Parallel()

	db, _, teardown := setupTestDBConnection(t)
	defer teardown()

	// Ensure that the old Ping method works.
	err := db.Ping()
	if err != nil {
		t.Fatalf("Ping failed: %v", err)
	}
}

func TestDdlBatch(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	db, server, teardown := setupTestDBConnection(t)
	defer teardown()

	var expectedResponse = &emptypb.Empty{}
	anyMsg, _ := anypb.New(expectedResponse)
	server.TestDatabaseAdmin.SetResps([]proto.Message{
		&longrunningpb.Operation{
			Done:   true,
			Result: &longrunningpb.Operation_Response{Response: anyMsg},
			Name:   "test-operation",
		},
	})

	statements := []string{"CREATE TABLE FOO", "CREATE TABLE BAR"}
	conn, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	if _, err = conn.ExecContext(ctx, "START BATCH DDL"); err != nil {
		t.Fatalf("failed to start DDL batch: %v", err)
	}
	for _, stmt := range statements {
		if _, err = conn.ExecContext(ctx, stmt); err != nil {
			t.Fatalf("failed to execute statement in DDL batch: %v", err)
		}
	}
	if _, err = conn.ExecContext(ctx, "RUN BATCH"); err != nil {
		t.Fatalf("failed to run DDL batch: %v", err)
	}

	requests := server.TestDatabaseAdmin.Reqs()
	if g, w := len(requests), 1; g != w {
		t.Fatalf("requests count mismatch\nGot: %v\nWant: %v", g, w)
	}
	if req, ok := requests[0].(*databasepb.UpdateDatabaseDdlRequest); ok {
		if g, w := len(req.Statements), len(statements); g != w {
			t.Fatalf("statement count mismatch\nGot: %v\nWant: %v", g, w)
		}
		for i, stmt := range statements {
			if g, w := req.Statements[i], stmt; g != w {
				t.Fatalf("statement mismatch\nGot: %v\nWant: %v", g, w)
			}
		}
	} else {
		t.Fatalf("request type mismatch, got %v", requests[0])
	}
}

func TestAbortDdlBatch(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	db, server, teardown := setupTestDBConnection(t)
	defer teardown()

	statements := []string{"CREATE TABLE FOO", "CREATE TABLE BAR"}
	c, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	if _, err = c.ExecContext(ctx, "START BATCH DDL"); err != nil {
		t.Fatalf("failed to start DDL batch: %v", err)
	}
	for _, stmt := range statements {
		if _, err = c.ExecContext(ctx, stmt); err != nil {
			t.Fatalf("failed to execute statement in DDL batch: %v", err)
		}
	}
	// Check that the statements have been batched.
	_ = c.Raw(func(driverConn interface{}) error {
		conn := driverConn.(*conn)
		if conn.batch == nil {
			t.Fatalf("missing batch on connection")
		}
		if g, w := len(conn.batch.statements), 2; g != w {
			t.Fatalf("batch length mismatch\nGot: %v\nWant: %v", g, w)
		}
		return nil
	})

	if _, err = c.ExecContext(ctx, "ABORT BATCH"); err != nil {
		t.Fatalf("failed to abort DDL batch: %v", err)
	}

	requests := server.TestDatabaseAdmin.Reqs()
	if g, w := len(requests), 0; g != w {
		t.Fatalf("requests count mismatch\nGot: %v\nWant: %v", g, w)
	}

	_ = c.Raw(func(driverConn interface{}) error {
		spannerConn := driverConn.(SpannerConn)
		if spannerConn.InDDLBatch() {
			t.Fatalf("connection still has an active DDL batch")
		}
		return nil
	})
}

func TestShowAndSetVariableRetryAbortsInternally(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	db, server, teardown := setupTestDBConnection(t)
	defer teardown()

	c, err := db.Conn(ctx)
	if err != nil {
		t.Fatalf("failed to obtain a connection: %v", err)
	}
	defer c.Close()

	for _, tc := range []struct {
		expected bool
		set      bool
	}{
		{expected: true, set: false},
		{expected: false, set: true},
		{expected: true, set: true},
	} {
		// Get the current value.
		rows, err := c.QueryContext(ctx, "SHOW VARIABLE RETRY_ABORTS_INTERNALLY")
		if err != nil {
			t.Fatalf("failed to execute get variable retry_aborts_internally: %v", err)
		}
		defer rows.Close()
		for rows.Next() {
			var retry bool
			if err := rows.Scan(&retry); err != nil {
				t.Fatalf("failed to scan value for retry_aborts_internally: %v", err)
			}
			if g, w := retry, tc.expected; g != w {
				t.Fatalf("retry_aborts_internally mismatch\nGot: %v\nWant: %v", g, w)
			}
		}
		if err := rows.Err(); err != nil {
			t.Fatalf("failed to iterate over result for get variable retry_aborts_internally: %v", err)
		}

		// Check that the behavior matches the setting.
		tx, _ := c.BeginTx(ctx, nil)
		server.TestSpanner.PutExecutionTime(testutil.MethodCommitTransaction, testutil.SimulatedExecutionTime{
			Errors: []error{gstatus.Error(codes.Aborted, "Aborted")},
		})
		err = tx.Commit()
		if tc.expected && err != nil {
			t.Fatalf("unexpected error for commit: %v", err)
		} else if !tc.expected && spanner.ErrCode(err) != codes.Aborted {
			t.Fatalf("error code mismatch\nGot: %v\nWant: %v", spanner.ErrCode(err), codes.Aborted)
		}

		// Set a new value for the variable.
		if _, err := c.ExecContext(ctx, fmt.Sprintf("SET RETRY_ABORTS_INTERNALLY = %v", tc.set)); err != nil {
			t.Fatalf("failed to set value for retry_aborts_internally: %v", err)
		}
	}

	// Verify that the value cannot be set during a transaction.
	tx, _ := c.BeginTx(ctx, nil)
	defer tx.Rollback()
	_, err = c.ExecContext(ctx, "SET RETRY_ABORTS_INTERNALLY = TRUE")
	if g, w := spanner.ErrCode(err), codes.FailedPrecondition; g != w {
		t.Fatalf("error code mismatch for setting retry_aborts_internally during a transaction\nGot: %v\nWant: %v", g, w)
	}
}

func TestPartitionedDml(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	db, server, teardown := setupTestDBConnection(t)
	defer teardown()

	c, err := db.Conn(ctx)
	if err != nil {
		t.Fatalf("failed to obtain a connection: %v", err)
	}
	defer c.Close()

	if _, err := c.ExecContext(ctx, "set autocommit_dml_mode = 'Partitioned_Non_Atomic'"); err != nil {
		t.Fatalf("could not set autocommit dml mode: %v", err)
	}

	_ = server.TestSpanner.PutStatementResult("DELETE FROM Foo WHERE TRUE", &testutil.StatementResult{
		Type:        testutil.StatementResultUpdateCount,
		UpdateCount: 200,
	})
	// The following statement should be executed using PDML instead of DML.
	res, err := c.ExecContext(ctx, "DELETE FROM Foo WHERE TRUE")
	if err != nil {
		t.Fatalf("could not execute DML statement: %v", err)
	}
	affected, _ := res.RowsAffected()
	if affected != 200 {
		t.Fatalf("affected rows mismatch\nGot: %v\nWant: %v", affected, 200)
	}
	requests := drainRequestsFromServer(server.TestSpanner)
	beginRequests := requestsOfType(requests, reflect.TypeOf(&sppb.BeginTransactionRequest{}))
	var beginPdml *sppb.BeginTransactionRequest
	for _, req := range beginRequests {
		if req.(*sppb.BeginTransactionRequest).Options.GetPartitionedDml() != nil {
			beginPdml = req.(*sppb.BeginTransactionRequest)
			break
		}
	}
	if beginPdml == nil {
		t.Fatal("no begin request for Partitioned DML found")
	}
	sqlRequests := requestsOfType(requests, reflect.TypeOf(&sppb.ExecuteSqlRequest{}))
	if len(sqlRequests) != 1 {
		t.Fatalf("sql requests count mismatch\nGot: %v\nWant: %v", len(sqlRequests), 1)
	}
	req := sqlRequests[0].(*sppb.ExecuteSqlRequest)
	if req.Transaction == nil || req.Transaction.GetId() == nil {
		t.Fatal("missing transaction id for sql request")
	}
	if !server.TestSpanner.IsPartitionedDmlTransaction(req.Transaction.GetId()) {
		t.Fatalf("sql request did not use a PDML transaction")
	}
}

func TestAutocommitBatchDml(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	db, server, teardown := setupTestDBConnection(t)
	defer teardown()

	c, err := db.Conn(ctx)
	if err != nil {
		t.Fatalf("failed to obtain a connection: %v", err)
	}
	defer c.Close()

	if _, err := c.ExecContext(ctx, "START BATCH DML"); err != nil {
		t.Fatalf("could not start a DML batch: %v", err)
	}
	_ = server.TestSpanner.PutStatementResult("INSERT INTO Foo (Id, Val) VALUES (1, 'One')", &testutil.StatementResult{
		Type:        testutil.StatementResultUpdateCount,
		UpdateCount: 1,
	})
	_ = server.TestSpanner.PutStatementResult("INSERT INTO Foo (Id, Val) VALUES (2, 'Two')", &testutil.StatementResult{
		Type:        testutil.StatementResultUpdateCount,
		UpdateCount: 1,
	})

	// The following statements should be batched locally and only sent to Spanner once
	// 'RUN BATCH' is executed.
	res, err := c.ExecContext(ctx, "INSERT INTO Foo (Id, Val) VALUES (1, 'One')")
	if err != nil {
		t.Fatalf("could not execute DML statement: %v", err)
	}
	affected, _ := res.RowsAffected()
	if affected != 0 {
		t.Fatalf("affected rows mismatch\nGot: %v\nWant: %v", affected, 0)
	}
	res, err = c.ExecContext(ctx, "INSERT INTO Foo (Id, Val) VALUES (2, 'Two')")
	if err != nil {
		t.Fatalf("could not execute DML statement: %v", err)
	}
	affected, _ = res.RowsAffected()
	if affected != 0 {
		t.Fatalf("affected rows mismatch\nGot: %v\nWant: %v", affected, 0)
	}

	// There should be no ExecuteSqlRequest statements on the server.
	requests := drainRequestsFromServer(server.TestSpanner)
	sqlRequests := requestsOfType(requests, reflect.TypeOf(&sppb.ExecuteSqlRequest{}))
	if len(sqlRequests) != 0 {
		t.Fatalf("sql requests count mismatch\nGot: %v\nWant: %v", len(sqlRequests), 0)
	}
	batchRequests := requestsOfType(requests, reflect.TypeOf(&sppb.ExecuteBatchDmlRequest{}))
	if len(batchRequests) != 0 {
		t.Fatalf("BatchDML requests count mismatch\nGot: %v\nWant: %v", len(batchRequests), 0)
	}

	// Execute a RUN BATCH statement. This should trigger a BatchDML request followed by a Commit request.
	res, err = c.ExecContext(ctx, "RUN BATCH")
	if err != nil {
		t.Fatalf("failed to execute RUN BATCH: %v", err)
	}
	affected, err = res.RowsAffected()
	if err != nil {
		t.Fatalf("could not get rows affected from batch: %v", err)
	}
	if affected != 2 {
		t.Fatalf("affected rows mismatch\nGot: %v\nWant: %v", affected, 2)
	}

	requests = drainRequestsFromServer(server.TestSpanner)
	// There should still be no ExecuteSqlRequests on the server.
	sqlRequests = requestsOfType(requests, reflect.TypeOf(&sppb.ExecuteSqlRequest{}))
	if len(sqlRequests) != 0 {
		t.Fatalf("sql requests count mismatch\nGot: %v\nWant: %v", len(sqlRequests), 0)
	}
	batchRequests = requestsOfType(requests, reflect.TypeOf(&sppb.ExecuteBatchDmlRequest{}))
	if len(batchRequests) != 1 {
		t.Fatalf("BatchDML requests count mismatch\nGot: %v\nWant: %v", len(batchRequests), 1)
	}
	// The transaction should also have been committed.
	commitRequests := requestsOfType(requests, reflect.TypeOf(&sppb.CommitRequest{}))
	if len(commitRequests) != 1 {
		t.Fatalf("Commit requests count mismatch\nGot: %v\nWant: %v", len(commitRequests), 1)
	}
}

func TestTransactionBatchDml(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	db, server, teardown := setupTestDBConnection(t)
	defer teardown()

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("failed to start transaction: %v", err)
	}

	if _, err := tx.ExecContext(ctx, "START BATCH DML"); err != nil {
		t.Fatalf("could not start a DML batch: %v", err)
	}
	_ = server.TestSpanner.PutStatementResult("INSERT INTO Foo (Id, Val) VALUES (1, 'One')", &testutil.StatementResult{
		Type:        testutil.StatementResultUpdateCount,
		UpdateCount: 1,
	})
	_ = server.TestSpanner.PutStatementResult("INSERT INTO Foo (Id, Val) VALUES (2, 'Two')", &testutil.StatementResult{
		Type:        testutil.StatementResultUpdateCount,
		UpdateCount: 1,
	})

	// The following statements should be batched locally and only sent to Spanner once
	// 'RUN BATCH' is executed.
	res, err := tx.ExecContext(ctx, "INSERT INTO Foo (Id, Val) VALUES (1, 'One')")
	if err != nil {
		t.Fatalf("could not execute DML statement: %v", err)
	}
	affected, _ := res.RowsAffected()
	if affected != 0 {
		t.Fatalf("affected rows mismatch\nGot: %v\nWant: %v", affected, 0)
	}
	res, err = tx.ExecContext(ctx, "INSERT INTO Foo (Id, Val) VALUES (2, 'Two')")
	if err != nil {
		t.Fatalf("could not execute DML statement: %v", err)
	}
	affected, _ = res.RowsAffected()
	if affected != 0 {
		t.Fatalf("affected rows mismatch\nGot: %v\nWant: %v", affected, 0)
	}

	// There should be no ExecuteSqlRequest statements on the server.
	requests := drainRequestsFromServer(server.TestSpanner)
	sqlRequests := requestsOfType(requests, reflect.TypeOf(&sppb.ExecuteSqlRequest{}))
	if len(sqlRequests) != 0 {
		t.Fatalf("sql requests count mismatch\nGot: %v\nWant: %v", len(sqlRequests), 0)
	}
	batchRequests := requestsOfType(requests, reflect.TypeOf(&sppb.ExecuteBatchDmlRequest{}))
	if len(batchRequests) != 0 {
		t.Fatalf("BatchDML requests count mismatch\nGot: %v\nWant: %v", len(batchRequests), 0)
	}

	// Execute a RUN BATCH statement. This should trigger a BatchDML request.
	res, err = tx.ExecContext(ctx, "RUN BATCH")
	if err != nil {
		t.Fatalf("failed to execute RUN BATCH: %v", err)
	}
	affected, err = res.RowsAffected()
	if err != nil {
		t.Fatalf("could not get rows affected from batch: %v", err)
	}
	if affected != 2 {
		t.Fatalf("affected rows mismatch\nGot: %v\nWant: %v", affected, 2)
	}

	requests = drainRequestsFromServer(server.TestSpanner)
	// There should still be no ExecuteSqlRequests on the server.
	sqlRequests = requestsOfType(requests, reflect.TypeOf(&sppb.ExecuteSqlRequest{}))
	if len(sqlRequests) != 0 {
		t.Fatalf("sql requests count mismatch\nGot: %v\nWant: %v", len(sqlRequests), 0)
	}
	batchRequests = requestsOfType(requests, reflect.TypeOf(&sppb.ExecuteBatchDmlRequest{}))
	if len(batchRequests) != 1 {
		t.Fatalf("BatchDML requests count mismatch\nGot: %v\nWant: %v", len(batchRequests), 1)
	}
	// The transaction should still be active.
	commitRequests := requestsOfType(requests, reflect.TypeOf(&sppb.CommitRequest{}))
	if len(commitRequests) != 0 {
		t.Fatalf("Commit requests count mismatch\nGot: %v\nWant: %v", len(commitRequests), 0)
	}

	// Executing another DML statement on the same transaction now that the batch has been
	// executed should cause the statement to be sent to Spanner.
	_ = server.TestSpanner.PutStatementResult("INSERT INTO Foo (Id, Val) VALUES (3, 'Three')", &testutil.StatementResult{
		Type:        testutil.StatementResultUpdateCount,
		UpdateCount: 1,
	})
	if _, err := tx.ExecContext(ctx, "INSERT INTO Foo (Id, Val) VALUES (3, 'Three')"); err != nil {
		t.Fatalf("failed to execute DML statement after batch: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("failed to commit transaction after batch: %v", err)
	}

	requests = drainRequestsFromServer(server.TestSpanner)
	// There should now be one ExecuteSqlRequests on the server.
	sqlRequests = requestsOfType(requests, reflect.TypeOf(&sppb.ExecuteSqlRequest{}))
	if len(sqlRequests) != 1 {
		t.Fatalf("sql requests count mismatch\nGot: %v\nWant: %v", len(sqlRequests), 1)
	}
	// There should be no new Batch DML requests.
	batchRequests = requestsOfType(requests, reflect.TypeOf(&sppb.ExecuteBatchDmlRequest{}))
	if len(batchRequests) != 0 {
		t.Fatalf("BatchDML requests count mismatch\nGot: %v\nWant: %v", len(batchRequests), 0)
	}
	// The transaction should now be committed.
	commitRequests = requestsOfType(requests, reflect.TypeOf(&sppb.CommitRequest{}))
	if len(commitRequests) != 1 {
		t.Fatalf("Commit requests count mismatch\nGot: %v\nWant: %v", len(commitRequests), 1)
	}
}

func TestCommitTimestamp(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	db, _, teardown := setupTestDBConnection(t)
	defer teardown()

	conn, err := db.Conn(ctx)
	defer func() {
		if err := conn.Close(); err != nil {
			t.Fatal(err)
		}
	}()
	if err != nil {
		t.Fatalf("failed to get a connection: %v", err)
	}
	tx, err := conn.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("failed to start transaction: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit failed: %v", err)
	}
	// Get the commit timestamp from the connection.
	// We do this in a simple loop to verify that we can get it multiple times.
	for i := 0; i < 2; i++ {
		var ts time.Time
		if err := conn.Raw(func(driverConn interface{}) error {
			ts, err = driverConn.(SpannerConn).CommitTimestamp()
			return err
		}); err != nil {
			t.Fatalf("failed to get commit timestamp: %v", err)
		}
		if cmp.Equal(time.Time{}, ts) {
			t.Fatalf("got zero commit timestamp: %v", ts)
		}
	}
}

func TestCommitTimestampAutocommit(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	db, _, teardown := setupTestDBConnection(t)
	defer teardown()

	conn, err := db.Conn(ctx)
	defer func() {
		if err := conn.Close(); err != nil {
			t.Fatal(err)
		}
	}()
	if err != nil {
		t.Fatalf("failed to get a connection: %v", err)
	}
	if _, err := conn.ExecContext(ctx, testutil.UpdateBarSetFoo); err != nil {
		t.Fatalf("failed to execute update: %v", err)
	}
	// Get the commit timestamp from the connection.
	// We do this in a simple loop to verify that we can get it multiple times.
	for i := 0; i < 2; i++ {
		var ts time.Time
		if err := conn.Raw(func(driverConn interface{}) error {
			ts, err = driverConn.(SpannerConn).CommitTimestamp()
			return err
		}); err != nil {
			t.Fatalf("failed to get commit timestamp: %v", err)
		}
		if cmp.Equal(time.Time{}, ts) {
			t.Fatalf("got zero commit timestamp: %v", ts)
		}
	}
}

func TestCommitTimestampFailsAfterRollback(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	db, _, teardown := setupTestDBConnection(t)
	defer teardown()

	conn, err := db.Conn(ctx)
	defer func() {
		if err := conn.Close(); err != nil {
			t.Fatal(err)
		}
	}()
	if err != nil {
		t.Fatalf("failed to get a connection: %v", err)
	}
	tx, err := conn.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("failed to start transaction: %v", err)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatalf("rollback failed: %v", err)
	}
	// Try to get the commit timestamp from the connection.
	err = conn.Raw(func(driverConn interface{}) error {
		_, err = driverConn.(SpannerConn).CommitTimestamp()
		return err
	})
	if g, w := spanner.ErrCode(err), codes.FailedPrecondition; g != w {
		t.Fatalf("get commit timestamp error code mismatch\n Got: %v\nWant: %v", g, w)
	}
}

func TestCommitTimestampFailsAfterAutocommitQuery(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	db, _, teardown := setupTestDBConnection(t)
	defer teardown()

	conn, err := db.Conn(ctx)
	defer func() {
		if err := conn.Close(); err != nil {
			t.Fatal(err)
		}
	}()
	if err != nil {
		t.Fatalf("failed to get a connection: %v", err)
	}
	var v string
	if err := conn.QueryRowContext(ctx, testutil.SelectFooFromBar).Scan(&v); err != nil {
		t.Fatalf("failed to execute query: %v", err)
	}
	// Try to get the commit timestamp from the connection. This should not be possible as a query in autocommit mode
	// will not return a commit timestamp.
	err = conn.Raw(func(driverConn interface{}) error {
		_, err = driverConn.(SpannerConn).CommitTimestamp()
		return err
	})
	if g, w := spanner.ErrCode(err), codes.FailedPrecondition; g != w {
		t.Fatalf("get commit timestamp error code mismatch\n Got: %v\nWant: %v", g, w)
	}
}

func TestShowVariableCommitTimestamp(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	db, _, teardown := setupTestDBConnection(t)
	defer teardown()

	conn, err := db.Conn(ctx)
	defer func() {
		if err := conn.Close(); err != nil {
			t.Fatal(err)
		}
	}()
	if err != nil {
		t.Fatalf("failed to get a connection: %v", err)
	}
	tx, err := conn.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("failed to start transaction: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit failed: %v", err)
	}
	// Get the commit timestamp from the connection using a custom SQL statement.
	// We do this in a simple loop to verify that we can get it multiple times.
	for i := 0; i < 2; i++ {
		var ts time.Time
		if err := conn.QueryRowContext(ctx, "SHOW VARIABLE COMMIT_TIMESTAMP").Scan(&ts); err != nil {
			t.Fatalf("failed to get commit timestamp: %v", err)
		}
		if cmp.Equal(time.Time{}, ts) {
			t.Fatalf("got zero commit timestamp: %v", ts)
		}
	}
}

func TestMinSessions(t *testing.T) {
	t.Parallel()

	minSessions := int32(10)
	ctx := context.Background()
	db, server, teardown := setupTestDBConnectionWithParams(t, fmt.Sprintf("minSessions=%v", minSessions))
	defer teardown()

	conn, err := db.Conn(ctx)
	if err != nil {
		t.Fatalf("failed to get a connection: %v", err)
	}
	var res int64
	if err := conn.QueryRowContext(ctx, "SELECT 1").Scan(&res); err != nil {
		t.Fatalf("failed to execute query on connection: %v", err)
	}
	// Wait until all sessions have been created.
	waitFor(t, func() error {
		created := int32(server.TestSpanner.TotalSessionsCreated())
		if created != minSessions {
			return fmt.Errorf("num open sessions mismatch\n Got: %d\nWant: %d", created, minSessions)
		}
		return nil
	})
	_ = conn.Close()
	_ = db.Close()

	// Verify that the connector created 10 sessions on the server.
	reqs := drainRequestsFromServer(server.TestSpanner)
	createReqs := requestsOfType(reqs, reflect.TypeOf(&sppb.BatchCreateSessionsRequest{}))
	numCreated := int32(0)
	for _, req := range createReqs {
		numCreated += req.(*sppb.BatchCreateSessionsRequest).SessionCount
	}
	if g, w := numCreated, minSessions; g != w {
		t.Errorf("session creation count mismatch\n Got: %v\nWant: %v", g, w)
	}
}

func TestMaxSessions(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	db, server, teardown := setupTestDBConnectionWithParams(t, "minSessions=0;maxSessions=2")
	defer teardown()

	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			conn, err := db.Conn(ctx)
			if err != nil {
				t.Errorf("failed to get a connection: %v", err)
			}
			var res int64
			if err := conn.QueryRowContext(ctx, "SELECT 1").Scan(&res); err != nil {
				t.Errorf("failed to execute query on connection: %v", err)
			}
			_ = conn.Close()
		}()
	}
	wg.Wait()

	// Verify that the connector only created 2 sessions on the server.
	reqs := drainRequestsFromServer(server.TestSpanner)
	createReqs := requestsOfType(reqs, reflect.TypeOf(&sppb.BatchCreateSessionsRequest{}))
	numCreated := int32(0)
	for _, req := range createReqs {
		numCreated += req.(*sppb.BatchCreateSessionsRequest).SessionCount
	}
	if g, w := numCreated, int32(2); g != w {
		t.Errorf("session creation count mismatch\n Got: %v\nWant: %v", g, w)
	}
}

func TestClientReuse(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	db, server, teardown := setupTestDBConnectionWithParams(t, "minSessions=2")
	defer teardown()

	// Repeatedly get a connection and close it using the same DB instance. These
	// connections should all share the same Spanner client, and only initialized
	// one session pool.
	for i := 0; i < 5; i++ {
		conn, err := db.Conn(ctx)
		if err != nil {
			t.Fatalf("failed to get a connection: %v", err)
		}
		var res int64
		if err := conn.QueryRowContext(ctx, "SELECT 1").Scan(&res); err != nil {
			t.Fatalf("failed to execute query on connection: %v", err)
		}
		_ = conn.Close()
	}
	// Verify that the connector only created 2 sessions on the server.
	reqs := drainRequestsFromServer(server.TestSpanner)
	createReqs := requestsOfType(reqs, reflect.TypeOf(&sppb.BatchCreateSessionsRequest{}))
	numCreated := int32(0)
	for _, req := range createReqs {
		numCreated += req.(*sppb.BatchCreateSessionsRequest).SessionCount
	}
	if g, w := numCreated, int32(2); g != w {
		t.Errorf("session creation count mismatch\n Got: %v\nWant: %v", g, w)
	}

	// Now close the DB instance and create a new DB connection.
	// This should cause the first Spanner client to be closed and
	// a new one to be opened.
	_ = db.Close()

	db, err := sql.Open(
		"spanner",
		fmt.Sprintf("%s/projects/p/instances/i/databases/d?useplaintext=true;minSessions=2", server.Address))
	if err != nil {
		t.Fatalf("failed to open new DB instance: %v", err)
	}
	var res int64
	if err := db.QueryRowContext(ctx, "SELECT 1").Scan(&res); err != nil {
		t.Fatalf("failed to execute query on db: %v", err)
	}
	reqs = drainRequestsFromServer(server.TestSpanner)
	createReqs = requestsOfType(reqs, reflect.TypeOf(&sppb.BatchCreateSessionsRequest{}))
	numCreated = int32(0)
	for _, req := range createReqs {
		numCreated += req.(*sppb.BatchCreateSessionsRequest).SessionCount
	}
	if g, w := numCreated, int32(2); g != w {
		t.Errorf("session creation count mismatch\n Got: %v\nWant: %v", g, w)
	}
}

func TestStressClientReuse(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	_, server, teardown := setupTestDBConnection(t)
	defer teardown()

	rng := rand.New(rand.NewSource(time.Now().UnixNano()))

	numSessions := 10
	numClients := 5
	numParallel := 50
	var wg sync.WaitGroup
	for clientIndex := 0; clientIndex < numClients; clientIndex++ {
		// Open a DB using a dsn that contains a meaningless number. This will ensure that
		// the underlying client will be different from the other connections that use a
		// different number.
		db, err := sql.Open("spanner",
			fmt.Sprintf("%s/projects/p/instances/i/databases/d?useplaintext=true;minSessions=%v;maxSessions=%v;randomNumber=%v", server.Address, numSessions, numSessions, clientIndex))
		if err != nil {
			t.Fatalf("failed to open DB: %v", err)
		}
		// Execute random operations in parallel on the database.
		for i := 0; i < numParallel; i++ {
			doUpdate := rng.Int()%2 == 0

			wg.Add(1)
			go func() {
				defer wg.Done()
				conn, err := db.Conn(ctx)
				if err != nil {
					t.Errorf("failed to get a connection: %v", err)
				}
				if doUpdate {
					if _, err := conn.ExecContext(ctx, testutil.UpdateBarSetFoo); err != nil {
						t.Errorf("failed to execute update on connection: %v", err)
					}
				} else {
					var res int64
					if err := conn.QueryRowContext(ctx, "SELECT 1").Scan(&res); err != nil {
						t.Errorf("failed to execute query on connection: %v", err)
					}
				}
				_ = conn.Close()
			}()
		}
	}
	wg.Wait()

	// Verify that each unique connection string created numSessions (10) sessions on the server.
	reqs := drainRequestsFromServer(server.TestSpanner)
	createReqs := requestsOfType(reqs, reflect.TypeOf(&sppb.BatchCreateSessionsRequest{}))
	numCreated := int32(0)
	for _, req := range createReqs {
		numCreated += req.(*sppb.BatchCreateSessionsRequest).SessionCount
	}
	if g, w := numCreated, int32(numSessions*numClients); g != w {
		t.Errorf("session creation count mismatch\n Got: %v\nWant: %v", g, w)
	}
	sqlReqs := requestsOfType(reqs, reflect.TypeOf(&sppb.ExecuteSqlRequest{}))
	if g, w := len(sqlReqs), numClients*numParallel; g != w {
		t.Errorf("ExecuteSql request count mismatch\n Got: %v\nWant: %v", g, w)
	}
}

func TestExcludeTxnFromChangeStreams_AutoCommitUpdate(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	db, server, teardown := setupTestDBConnection(t)
	defer teardown()

	conn, err := db.Conn(ctx)
	defer func() {
		if err := conn.Close(); err != nil {
			t.Fatal(err)
		}
	}()
	if err != nil {
		t.Fatalf("failed to get a connection: %v", err)
	}

	var exclude bool
	if err := conn.QueryRowContext(ctx, "SHOW VARIABLE EXCLUDE_TXN_FROM_CHANGE_STREAMS").Scan(&exclude); err != nil {
		t.Fatalf("failed to get exclude setting: %v", err)
	}
	if g, w := exclude, false; g != w {
		t.Fatalf("exclude_txn_from_change_streams mismatch\n Got: %v\nWant: %v", g, w)
	}
	if _, err := conn.ExecContext(ctx, "set exclude_txn_from_change_streams = true"); err != nil {
		t.Fatal(err)
	}
	if _, err = conn.ExecContext(ctx, testutil.UpdateBarSetFoo); err != nil {
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
	if _, ok := req.Transaction.Selector.(*sppb.TransactionSelector_Begin); !ok {
		t.Fatalf("unsupported transaction type %T", req.Transaction.Selector)
	}
	begin := req.Transaction.Selector.(*sppb.TransactionSelector_Begin)
	if !begin.Begin.ExcludeTxnFromChangeStreams {
		t.Fatalf("missing ExcludeTxnFromChangeStreams option on BeginTransaction option")
	}
}

func TestExcludeTxnFromChangeStreams_AutoCommitBatchDml(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	db, server, teardown := setupTestDBConnection(t)
	defer teardown()

	conn, err := db.Conn(ctx)
	defer func() {
		if err := conn.Close(); err != nil {
			t.Fatal(err)
		}
	}()
	if err != nil {
		t.Fatalf("failed to get a connection: %v", err)
	}

	_, _ = conn.ExecContext(ctx, "set exclude_txn_from_change_streams = true")
	_, _ = conn.ExecContext(ctx, "start batch dml")
	_, _ = conn.ExecContext(ctx, testutil.UpdateBarSetFoo)
	_, _ = conn.ExecContext(ctx, "run batch")
	requests := drainRequestsFromServer(server.TestSpanner)
	batchRequests := requestsOfType(requests, reflect.TypeOf(&sppb.ExecuteBatchDmlRequest{}))
	if g, w := len(batchRequests), 1; g != w {
		t.Fatalf("ExecuteBatchDmlRequest count mismatch\nGot: %v\nWant: %v", g, w)
	}
	req := batchRequests[0].(*sppb.ExecuteBatchDmlRequest)
	if req.Transaction == nil {
		t.Fatalf("missing transaction for ExecuteBatchDmlRequest")
	}
	if _, ok := req.Transaction.Selector.(*sppb.TransactionSelector_Begin); !ok {
		t.Fatalf("unsupported transaction type %T", req.Transaction.Selector)
	}
	begin := req.Transaction.Selector.(*sppb.TransactionSelector_Begin)
	if !begin.Begin.ExcludeTxnFromChangeStreams {
		t.Fatalf("missing ExcludeTxnFromChangeStreams option on BeginTransaction option")
	}
}

func TestExcludeTxnFromChangeStreams_PartitionedDml(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	db, server, teardown := setupTestDBConnection(t)
	defer teardown()

	conn, err := db.Conn(ctx)
	defer func() {
		if err := conn.Close(); err != nil {
			t.Fatal(err)
		}
	}()
	if err != nil {
		t.Fatalf("failed to get a connection: %v", err)
	}

	conn.ExecContext(ctx, "set exclude_txn_from_change_streams = true")
	conn.ExecContext(ctx, "set autocommit_dml_mode = 'partitioned_non_atomic'")
	conn.ExecContext(ctx, testutil.UpdateBarSetFoo)
	requests := drainRequestsFromServer(server.TestSpanner)
	beginRequests := requestsOfType(requests, reflect.TypeOf(&sppb.BeginTransactionRequest{}))
	if g, w := len(beginRequests), 1; g != w {
		t.Fatalf("BeginTransactionRequest count mismatch\nGot: %v\nWant: %v", g, w)
	}
	req := beginRequests[0].(*sppb.BeginTransactionRequest)
	if !req.Options.ExcludeTxnFromChangeStreams {
		t.Fatalf("missing ExcludeTxnFromChangeStreams option on BeginTransaction option")
	}
}

func TestExcludeTxnFromChangeStreams_Transaction(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	db, server, teardown := setupTestDBConnection(t)
	defer teardown()

	conn, err := db.Conn(ctx)
	defer func() {
		if err := conn.Close(); err != nil {
			t.Fatal(err)
		}
	}()
	if err != nil {
		t.Fatalf("failed to get a connection: %v", err)
	}

	var exclude bool
	if err := conn.QueryRowContext(ctx, "SHOW VARIABLE EXCLUDE_TXN_FROM_CHANGE_STREAMS").Scan(&exclude); err != nil {
		t.Fatalf("failed to get exclude setting: %v", err)
	}
	if g, w := exclude, false; g != w {
		t.Fatalf("exclude_txn_from_change_streams mismatch\n Got: %v\nWant: %v", g, w)
	}
	_, _ = conn.ExecContext(ctx, "set exclude_txn_from_change_streams = true")
	tx, err := conn.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		t.Fatal(err)
	}
	_, _ = conn.ExecContext(ctx, testutil.UpdateBarSetFoo)
	_ = tx.Commit()

	requests := drainRequestsFromServer(server.TestSpanner)
	beginRequests := requestsOfType(requests, reflect.TypeOf(&sppb.BeginTransactionRequest{}))
	if g, w := len(beginRequests), 1; g != w {
		t.Fatalf("BeginTransactionRequest count mismatch\nGot: %v\nWant: %v", g, w)
	}
	req := beginRequests[0].(*sppb.BeginTransactionRequest)
	if !req.Options.ExcludeTxnFromChangeStreams {
		t.Fatalf("missing ExcludeTxnFromChangeStreams option on BeginTransaction option")
	}

	// Verify that the flag is reset after the transaction.
	if err := conn.QueryRowContext(ctx, "SHOW VARIABLE EXCLUDE_TXN_FROM_CHANGE_STREAMS").Scan(&exclude); err != nil {
		t.Fatalf("failed to get exclude setting: %v", err)
	}
	if g, w := exclude, false; g != w {
		t.Fatalf("exclude_txn_from_change_streams mismatch\n Got: %v\nWant: %v", g, w)
	}
}

func TestMaxIdleConnectionsNonZero(t *testing.T) {
	t.Parallel()

	// Set MinSessions=1, so we can use the number of BatchCreateSessions requests as an indication
	// of the number of clients that was created.
	db, server, teardown := setupTestDBConnectionWithParams(t, "MinSessions=1")
	defer teardown()

	db.SetMaxIdleConns(2)
	for i := 0; i < 2; i++ {
		openAndCloseConn(t, db)
	}

	// Verify that only one client was created.
	// This happens because we have a non-zero value for the number of idle connections.
	requests := drainRequestsFromServer(server.TestSpanner)
	batchRequests := requestsOfType(requests, reflect.TypeOf(&sppb.BatchCreateSessionsRequest{}))
	if g, w := len(batchRequests), 1; g != w {
		t.Fatalf("BatchCreateSessions requests count mismatch\n Got: %v\nWant: %v", g, w)
	}
}

func TestMaxIdleConnectionsZero(t *testing.T) {
	t.Parallel()

	// Set MinSessions=1, so we can use the number of BatchCreateSessions requests as an indication
	// of the number of clients that was created.
	db, server, teardown := setupTestDBConnectionWithParams(t, "MinSessions=1")
	defer teardown()

	db.SetMaxIdleConns(0)
	for i := 0; i < 2; i++ {
		openAndCloseConn(t, db)
	}

	// Verify that two clients were created and closed.
	// This should happen because we do not keep any idle connections open.
	requests := drainRequestsFromServer(server.TestSpanner)
	batchRequests := requestsOfType(requests, reflect.TypeOf(&sppb.BatchCreateSessionsRequest{}))
	if g, w := len(batchRequests), 2; g != w {
		t.Fatalf("BatchCreateSessions requests count mismatch\n Got: %v\nWant: %v", g, w)
	}
}

func openAndCloseConn(t *testing.T, db *sql.DB) {
	ctx := context.Background()
	conn, err := db.Conn(ctx)
	if err != nil {
		t.Fatalf("failed to get a connection: %v", err)
	}
	defer func() {
		err = conn.Close()
		if err != nil {
			t.Fatalf("failed to close connection: %v", err)
		}
	}()

	var result int64
	if err := conn.QueryRowContext(ctx, "SELECT 1").Scan(&result); err != nil {
		t.Fatalf("failed to select: %v", err)
	}
	if result != 1 {
		t.Fatalf("expected 1 got %v", result)
	}
}

func TestCannotReuseClosedConnector(t *testing.T) {
	// Note: This test cannot be parallel, as it inspects the size of the shared
	// map of connectors in the driver. There is no guarantee how many connectors
	// will be open when the test is running, if there are also other tests running
	// in parallel.

	db, _, teardown := setupTestDBConnection(t)
	defer teardown()

	ctx := context.Background()
	conn, err := db.Conn(ctx)
	if err != nil {
		t.Fatalf("failed to get a connection: %v", err)
	}
	_ = conn.Close()
	connectors := db.Driver().(*Driver).connectors
	if g, w := len(connectors), 1; g != w {
		t.Fatal("underlying connector has not been created")
	}
	var connector *connector
	for _, v := range connectors {
		connector = v
	}
	if connector.closed {
		t.Fatal("connector is closed")
	}

	if err := db.Close(); err != nil {
		t.Fatalf("failed to close connector: %v", err)
	}
	_, err = db.Conn(ctx)
	if err == nil {
		t.Fatal("missing error for getting a connection from a closed connector")
	}
	if g, w := err.Error(), "sql: database is closed"; g != w {
		t.Fatalf("error mismatch for getting a connection from a closed connector\n Got: %v\nWant: %v", g, w)
	}
	// Verify that the underlying connector also has been closed.
	if g, w := len(connectors), 0; g != w {
		t.Fatal("underlying connector has not been closed")
	}
	if !connector.closed {
		t.Fatal("connector is not closed")
	}
}

func TestRunTransaction(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	db, server, teardown := setupTestDBConnection(t)
	defer teardown()

	err := RunTransaction(ctx, db, nil, func(ctx context.Context, tx *sql.Tx) error {
		rows, err := tx.Query(testutil.SelectFooFromBar)
		if err != nil {
			return err
		}
		defer rows.Close()
		// Verify that internal retries are disabled during RunTransaction
		row := tx.QueryRow("show variable retry_aborts_internally")
		var retry bool
		if err := row.Scan(&retry); err != nil {
			return err
		}
		if retry {
			return fmt.Errorf("internal retries should be disabled during RunTransaction")
		}

		for want := int64(1); rows.Next(); want++ {
			cols, err := rows.Columns()
			if err != nil {
				return err
			}
			if !cmp.Equal(cols, []string{"FOO"}) {
				return fmt.Errorf("cols mismatch\nGot: %v\nWant: %v", cols, []string{"FOO"})
			}
			var got int64
			err = rows.Scan(&got)
			if err != nil {
				return err
			}
			if got != want {
				return fmt.Errorf("value mismatch\nGot: %v\nWant: %v", got, want)
			}
		}
		if err := rows.Err(); err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	// Verify that internal retries are enabled again after RunTransaction
	row := db.QueryRow("show variable retry_aborts_internally")
	var retry bool
	if err := row.Scan(&retry); err != nil {
		t.Fatal(err)
	}
	if !retry {
		t.Fatal("internal retries should be enabled after RunTransaction")
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

func TestRunTransactionCommitAborted(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	db, server, teardown := setupTestDBConnection(t)
	defer teardown()

	attempts := 0
	err := RunTransaction(ctx, db, nil, func(ctx context.Context, tx *sql.Tx) error {
		attempts++
		rows, err := tx.Query(testutil.SelectFooFromBar)
		if err != nil {
			return err
		}
		defer rows.Close()

		for want := int64(1); rows.Next(); want++ {
			cols, err := rows.Columns()
			if err != nil {
				return err
			}
			if !cmp.Equal(cols, []string{"FOO"}) {
				return fmt.Errorf("cols mismatch\nGot: %v\nWant: %v", cols, []string{"FOO"})
			}
			var got int64
			err = rows.Scan(&got)
			if err != nil {
				return err
			}
			if got != want {
				return fmt.Errorf("value mismatch\nGot: %v\nWant: %v", got, want)
			}
		}
		if err := rows.Err(); err != nil {
			return err
		}
		// Instruct the mock server to abort the transaction.
		if attempts == 1 {
			server.TestSpanner.PutExecutionTime(testutil.MethodCommitTransaction, testutil.SimulatedExecutionTime{
				Errors: []error{gstatus.Error(codes.Aborted, "Aborted")},
			})
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	requests := drainRequestsFromServer(server.TestSpanner)
	sqlRequests := requestsOfType(requests, reflect.TypeOf(&sppb.ExecuteSqlRequest{}))
	// There should be two requests, as the transaction is aborted and retried.
	if g, w := len(sqlRequests), 2; g != w {
		t.Fatalf("ExecuteSqlRequests count mismatch\nGot: %v\nWant: %v", g, w)
	}
	commitRequests := requestsOfType(requests, reflect.TypeOf(&sppb.CommitRequest{}))
	if g, w := len(commitRequests), 2; g != w {
		t.Fatalf("commit requests count mismatch\nGot: %v\nWant: %v", g, w)
	}
	for i := 0; i < 2; i++ {
		req := sqlRequests[i].(*sppb.ExecuteSqlRequest)
		if req.Transaction == nil {
			t.Fatalf("missing transaction for ExecuteSqlRequest")
		}
		if req.Transaction.GetId() == nil {
			t.Fatalf("missing id selector for ExecuteSqlRequest")
		}
		commitReq := commitRequests[i].(*sppb.CommitRequest)
		if c, e := commitReq.GetTransactionId(), req.Transaction.GetId(); !cmp.Equal(c, e) {
			t.Fatalf("transaction id mismatch\nCommit: %c\nExecute: %v", c, e)
		}
	}
}

func TestRunTransactionQueryAborted(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	db, server, teardown := setupTestDBConnection(t)
	defer teardown()

	attempts := 0
	err := RunTransaction(ctx, db, nil, func(ctx context.Context, tx *sql.Tx) error {
		attempts++
		// Instruct the mock server to abort the transaction.
		if attempts == 1 {
			server.TestSpanner.PutExecutionTime(testutil.MethodExecuteStreamingSql, testutil.SimulatedExecutionTime{
				Errors: []error{gstatus.Error(codes.Aborted, "Aborted")},
			})
		}
		rows, err := tx.Query(testutil.SelectFooFromBar)
		if err != nil {
			return err
		}
		defer rows.Close()

		for want := int64(1); rows.Next(); want++ {
			cols, err := rows.Columns()
			if err != nil {
				return err
			}
			if !cmp.Equal(cols, []string{"FOO"}) {
				return fmt.Errorf("cols mismatch\nGot: %v\nWant: %v", cols, []string{"FOO"})
			}
			var got int64
			err = rows.Scan(&got)
			if err != nil {
				return err
			}
			if got != want {
				return fmt.Errorf("value mismatch\nGot: %v\nWant: %v", got, want)
			}
		}
		if err := rows.Err(); err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	requests := drainRequestsFromServer(server.TestSpanner)
	sqlRequests := requestsOfType(requests, reflect.TypeOf(&sppb.ExecuteSqlRequest{}))
	// There should be two ExecuteSql requests, as the transaction is aborted and retried.
	if g, w := len(sqlRequests), 2; g != w {
		t.Fatalf("ExecuteSqlRequests count mismatch\nGot: %v\nWant: %v", g, w)
	}
	commitRequests := requestsOfType(requests, reflect.TypeOf(&sppb.CommitRequest{}))
	// There should be only 1 CommitRequest, as the transaction is aborted before
	// the first commit attempt.
	if g, w := len(commitRequests), 1; g != w {
		t.Fatalf("commit requests count mismatch\nGot: %v\nWant: %v", g, w)
	}
	req := sqlRequests[1].(*sppb.ExecuteSqlRequest)
	if req.Transaction == nil {
		t.Fatalf("missing transaction for ExecuteSqlRequest")
	}
	if req.Transaction.GetId() == nil {
		t.Fatalf("missing id selector for ExecuteSqlRequest")
	}
	commitReq := commitRequests[0].(*sppb.CommitRequest)
	if c, e := commitReq.GetTransactionId(), req.Transaction.GetId(); !cmp.Equal(c, e) {
		t.Fatalf("transaction id mismatch\nCommit: %c\nExecute: %v", c, e)
	}
}

func TestRunTransactionQueryError(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	db, server, teardown := setupTestDBConnection(t)
	defer teardown()

	err := RunTransaction(ctx, db, nil, func(ctx context.Context, tx *sql.Tx) error {
		server.TestSpanner.PutExecutionTime(testutil.MethodExecuteStreamingSql, testutil.SimulatedExecutionTime{
			Errors: []error{gstatus.Error(codes.NotFound, "Table not found")},
		})
		rows, err := tx.Query(testutil.SelectFooFromBar)
		if err != nil {
			return err
		}
		defer rows.Close()

		for want := int64(1); rows.Next(); want++ {
			cols, err := rows.Columns()
			if err != nil {
				return err
			}
			if !cmp.Equal(cols, []string{"FOO"}) {
				return fmt.Errorf("cols mismatch\nGot: %v\nWant: %v", cols, []string{"FOO"})
			}
			var got int64
			err = rows.Scan(&got)
			if err != nil {
				return err
			}
			if got != want {
				return fmt.Errorf("value mismatch\nGot: %v\nWant: %v", got, want)
			}
		}
		if err := rows.Err(); err != nil {
			return err
		}
		return nil
	})
	if err == nil {
		t.Fatal("missing transaction error")
	}
	if g, w := spanner.ErrCode(err), codes.NotFound; g != w {
		t.Fatalf("error code mismatch\n Got: %v\nWant: %v", g, w)
	}

	requests := drainRequestsFromServer(server.TestSpanner)
	sqlRequests := requestsOfType(requests, reflect.TypeOf(&sppb.ExecuteSqlRequest{}))
	if g, w := len(sqlRequests), 1; g != w {
		t.Fatalf("ExecuteSqlRequests count mismatch\nGot: %v\nWant: %v", g, w)
	}
	commitRequests := requestsOfType(requests, reflect.TypeOf(&sppb.CommitRequest{}))
	// There should be no CommitRequest, as the transaction failed
	if g, w := len(commitRequests), 0; g != w {
		t.Fatalf("commit requests count mismatch\nGot: %v\nWant: %v", g, w)
	}
	// There should be a RollbackRequest, as the transaction failed.
	rollbackRequests := requestsOfType(requests, reflect.TypeOf(&sppb.RollbackRequest{}))
	if g, w := len(rollbackRequests), 1; g != w {
		t.Fatalf("rollback requests count mismatch\nGot: %v\nWant: %v", g, w)
	}
}

func TestRunTransactionCommitError(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	db, server, teardown := setupTestDBConnection(t)
	defer teardown()

	err := RunTransaction(ctx, db, nil, func(ctx context.Context, tx *sql.Tx) error {
		rows, err := tx.Query(testutil.SelectFooFromBar)
		if err != nil {
			return err
		}
		defer rows.Close()

		for want := int64(1); rows.Next(); want++ {
			cols, err := rows.Columns()
			if err != nil {
				return err
			}
			if !cmp.Equal(cols, []string{"FOO"}) {
				return fmt.Errorf("cols mismatch\nGot: %v\nWant: %v", cols, []string{"FOO"})
			}
			var got int64
			err = rows.Scan(&got)
			if err != nil {
				return err
			}
			if got != want {
				return fmt.Errorf("value mismatch\nGot: %v\nWant: %v", got, want)
			}
		}
		if err := rows.Err(); err != nil {
			return err
		}
		// Add an error for the Commit RPC. This will make the transaction fail,
		// as the commit fails.
		server.TestSpanner.PutExecutionTime(testutil.MethodCommitTransaction, testutil.SimulatedExecutionTime{
			Errors: []error{gstatus.Error(codes.FailedPrecondition, "Unique key constraint violation")},
		})
		return nil
	})
	if err == nil {
		t.Fatal("missing transaction error")
	}
	if g, w := spanner.ErrCode(err), codes.FailedPrecondition; g != w {
		t.Fatalf("error code mismatch\n Got: %v\nWant: %v", g, w)
	}

	requests := drainRequestsFromServer(server.TestSpanner)
	sqlRequests := requestsOfType(requests, reflect.TypeOf(&sppb.ExecuteSqlRequest{}))
	if g, w := len(sqlRequests), 1; g != w {
		t.Fatalf("ExecuteSqlRequests count mismatch\nGot: %v\nWant: %v", g, w)
	}
	commitRequests := requestsOfType(requests, reflect.TypeOf(&sppb.CommitRequest{}))
	// There should be no CommitRequest, as the transaction failed
	if g, w := len(commitRequests), 1; g != w {
		t.Fatalf("commit requests count mismatch\nGot: %v\nWant: %v", g, w)
	}
	// A Rollback request should normally not be necessary, as the Commit RPC
	// already closed the transaction. However, the Spanner client also sends
	// a RollbackRequest if a Commit fails.
	// TODO: Revisit once the client library has been checked whether it is really
	//       necessary to send a Rollback after a failed Commit.
	rollbackRequests := requestsOfType(requests, reflect.TypeOf(&sppb.RollbackRequest{}))
	if g, w := len(rollbackRequests), 1; g != w {
		t.Fatalf("rollback requests count mismatch\nGot: %v\nWant: %v", g, w)
	}
}

func TestTransactionWithLevelDisableRetryAborts(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	db, server, teardown := setupTestDBConnection(t)
	defer teardown()

	tx, err := db.BeginTx(ctx, &sql.TxOptions{Isolation: WithDisableRetryAborts(sql.LevelSerializable)})
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
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	// Simulate that the transaction was aborted.
	server.TestSpanner.PutExecutionTime(testutil.MethodCommitTransaction, testutil.SimulatedExecutionTime{
		Errors: []error{gstatus.Error(codes.Aborted, "Aborted")},
	})
	// Committing the transaction should fail, as we have disabled internal retries.
	err = tx.Commit()
	if err == nil {
		t.Fatal("missing aborted error after commit")
	}
	code := spanner.ErrCode(err)
	if w, g := code, codes.Aborted; w != g {
		t.Fatalf("error code mismatch\n Got: %v\nWant: %v", g, w)
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

func numeric(v string) big.Rat {
	res, _ := big.NewRat(1, 1).SetString(v)
	return *res
}

func nullNumeric(valid bool, v string) spanner.NullNumeric {
	if !valid {
		return spanner.NullNumeric{}
	}
	return spanner.NullNumeric{Valid: true, Numeric: numeric(v)}
}

func date(v string) civil.Date {
	res, _ := civil.ParseDate(v)
	return res
}

func nullJson(valid bool, v string) spanner.NullJSON {
	if !valid {
		return spanner.NullJSON{}
	}
	var m map[string]interface{}
	_ = json.Unmarshal([]byte(v), &m)
	return spanner.NullJSON{Valid: true, Value: m}
}

func setupTestDBConnection(t *testing.T) (db *sql.DB, server *testutil.MockedSpannerInMemTestServer, teardown func()) {
	return setupTestDBConnectionWithParams(t, "")
}

func setupTestDBConnectionWithParams(t *testing.T, params string) (db *sql.DB, server *testutil.MockedSpannerInMemTestServer, teardown func()) {
	server, _, serverTeardown := setupMockedTestServer(t)
	db, err := sql.Open(
		"spanner",
		fmt.Sprintf("%s/projects/p/instances/i/databases/d?useplaintext=true;%s", server.Address, params))
	if err != nil {
		serverTeardown()
		t.Fatal(err)
	}
	return db, server, func() {
		_ = db.Close()
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

func waitFor(t *testing.T, assert func() error) {
	t.Helper()
	timeout := 5 * time.Second
	ta := time.After(timeout)

	for {
		select {
		case <-ta:
			if err := assert(); err != nil {
				t.Fatalf("after %v waiting, got %v", timeout, err)
			}
			return
		default:
		}

		if err := assert(); err != nil {
			// Fail. Let's pause and retry.
			time.Sleep(time.Millisecond)
			continue
		}

		return
	}
}
