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

package spannerdriver

import (
	"context"
	"encoding/base64"
	"reflect"
	"strings"
	"testing"

	"cloud.google.com/go/spanner/apiv1/spannerpb"
	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/googleapis/go-sql-spanner/testdata/protos/concertspb"
	"github.com/googleapis/go-sql-spanner/testutil"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"
)

func TestPrepareQuery(t *testing.T) {
	t.Parallel()

	db, server, teardown := setupTestDBConnection(t)
	defer teardown()
	ctx := context.Background()

	query := "select value from test where id=?"
	if err := server.TestSpanner.PutStatementResult(
		strings.Replace(query, "?", "@p1", 1),
		&testutil.StatementResult{
			Type:      testutil.StatementResultResultSet,
			ResultSet: testutil.CreateSelect1ResultSet(),
		},
	); err != nil {
		t.Fatal(err)
	}

	stmt, err := db.PrepareContext(ctx, query)
	if err != nil {
		t.Fatalf("failed to prepare query: %v", err)
	}

	rows, err := stmt.QueryContext(ctx, 1)
	if err != nil {
		t.Fatalf("failed to execute query: %v", err)
	}
	defer silentClose(rows)

	var values []int64
	var value int64
	for rows.Next() {
		if err := rows.Scan(&value); err != nil {
			t.Fatalf("failed to scan row value: %v", err)
		}
		values = append(values, value)
	}
	if g, w := values, []int64{1}; !cmp.Equal(g, w) {
		t.Fatalf("returned values mismatch\n Got: %v\nWant: %v", g, w)
	}
}

func TestPrepareDml(t *testing.T) {
	t.Parallel()

	db, server, teardown := setupTestDBConnection(t)
	defer teardown()
	ctx := context.Background()

	query := "insert into test (id, value) values (?, ?)"
	if err := server.TestSpanner.PutStatementResult(
		"insert into test (id, value) values (@p1, @p2)",
		&testutil.StatementResult{
			Type:        testutil.StatementResultUpdateCount,
			UpdateCount: 1,
		},
	); err != nil {
		t.Fatal(err)
	}

	stmt, err := db.PrepareContext(ctx, query)
	if err != nil {
		t.Fatalf("failed to prepare dml: %v", err)
	}

	res, err := stmt.ExecContext(ctx, 1, "One")
	if err != nil {
		t.Fatalf("failed to execute dml: %v", err)
	}
	if rowsAffected, err := res.RowsAffected(); err != nil {
		t.Fatalf("failed to get rows affected: %v", err)
	} else if g, w := rowsAffected, int64(1); g != w {
		t.Fatalf("rows affected mismatch\n Got: %v\nWant: %v", g, w)
	}
}

func TestPrepareWithValuerScanner(t *testing.T) {
	t.Parallel()

	db, server, teardown := setupTestDBConnection(t)
	defer teardown()
	ctx := context.Background()

	items := []*concertspb.Item{concertspb.CreateItem("test-product", 2)}
	shippingAddress := concertspb.CreateAddress("Test Street 1", "Bentheim", "NDS", "DE")
	ticketOrder := concertspb.CreateTicketOrder("123", 1, shippingAddress, items)
	bytes, err := proto.Marshal(ticketOrder)
	if err != nil {
		t.Fatalf("failed to marshal proto: %v", err)
	}
	query := "select TicketOrder from test where TicketOrder=?"
	if err := server.TestSpanner.PutStatementResult(
		strings.Replace(query, "?", "@p1", 1),
		&testutil.StatementResult{
			Type:      testutil.StatementResultResultSet,
			ResultSet: testutil.CreateSingleColumnProtoResultSet([][]byte{bytes}, "TicketOrder"),
		},
	); err != nil {
		t.Fatal(err)
	}

	stmt, err := db.PrepareContext(ctx, query)
	if err != nil {
		t.Fatalf("failed to prepare query: %v", err)
	}

	rows, err := stmt.QueryContext(ctx, ticketOrder)
	if err != nil {
		t.Fatalf("failed to execute query: %v", err)
	}
	defer silentClose(rows)

	var value concertspb.TicketOrder
	if rows.Next() {
		if err := rows.Scan(&value); err != nil {
			t.Fatalf("failed to scan row value: %v", err)
		}
		if g, w := &value, ticketOrder; !cmp.Equal(g, w, cmpopts.IgnoreUnexported(concertspb.TicketOrder{}, concertspb.Address{}, concertspb.Item{})) {
			t.Fatalf("value mismatch\n Got: %v\nWant: %v", g, w)
		}
		if rows.Next() {
			t.Fatal("found more than one row")
		}
	} else {
		t.Fatal("no rows found")
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("failed to read rows: %v", err)
	}
	if err := rows.Close(); err != nil {
		t.Fatalf("failed to close rows: %v", err)
	}

	requests := server.TestSpanner.DrainRequestsFromServer()
	executeRequests := testutil.RequestsOfType(requests, reflect.TypeOf(&spannerpb.ExecuteSqlRequest{}))
	if g, w := len(executeRequests), 1; g != w {
		t.Fatalf("number of execute requests mismatch\n Got: %v\nWant: %v", g, w)
	}
	params := executeRequests[0].(*spannerpb.ExecuteSqlRequest).Params
	if g, w := len(params.Fields), 1; g != w {
		t.Fatalf("params length mismatch\n Got: %v\nWant: %v", g, w)
	}
	wantParamValue := &structpb.Value{
		Kind: &structpb.Value_StringValue{StringValue: base64.StdEncoding.EncodeToString(bytes)},
	}
	if g, w := params.Fields["p1"], wantParamValue; !cmp.Equal(g, w, cmpopts.IgnoreUnexported(structpb.Value{})) {
		t.Fatalf("param value mismatch\n Got: %v\nWant: %v", g, w)
	}
}
