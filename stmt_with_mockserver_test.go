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
	"database/sql"
	"encoding/base64"
	"reflect"
	"strings"
	"testing"
	"time"

	"cloud.google.com/go/spanner"
	"cloud.google.com/go/spanner/admin/database/apiv1/databasepb"
	"cloud.google.com/go/spanner/apiv1/spannerpb"
	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/googleapis/go-sql-spanner/testdata/protos/concertspb"
	"github.com/googleapis/go-sql-spanner/testutil"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
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

func TestPrepareClientSideStatement(t *testing.T) {
	t.Parallel()

	db, server, teardown := setupTestDBConnection(t)
	defer teardown()
	ctx := context.Background()

	c, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer silentClose(c)

	setStmt, err := c.PrepareContext(ctx, "set autocommit_dml_mode='partitioned_non_atomic'")
	if err != nil {
		t.Fatalf("failed to prepare statement: %v", err)
	}
	defer silentClose(setStmt)

	_, err = setStmt.ExecContext(ctx)
	if err != nil {
		t.Fatalf("failed to execute statement: %v", err)
	}

	showStmt, err := c.PrepareContext(ctx, "show variable autocommit_dml_mode")
	if err != nil {
		t.Fatalf("failed to prepare statement: %v", err)
	}
	defer silentClose(showStmt)
	r, err := showStmt.QueryContext(ctx, ExecOptions{DirectExecuteQuery: true})
	if err != nil {
		t.Fatalf("failed to execute statement as a query: %v", err)
	}
	defer silentClose(r)
	for r.Next() {
		var v string
		if err := r.Scan(&v); err != nil {
			t.Fatalf("failed to scan row value: %v", err)
		}
		if g, w := v, "Partitioned_Non_Atomic"; g != w {
			t.Fatalf("row value mismatch\n Got: %v\nWant: %v", g, w)
		}
	}

	requests := server.TestSpanner.DrainRequestsFromServer()
	executeRequests := testutil.RequestsOfType(requests, reflect.TypeOf(&spannerpb.ExecuteSqlRequest{}))
	if g, w := len(executeRequests), 0; g != w {
		t.Fatalf("num execute request mismatch\n Got: %v\nWant: %v", g, w)
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

func TestStatementTimeout(t *testing.T) {
	t.Parallel()

	db, server, teardown := setupTestDBConnectionWithParams(t, "statement_timeout=1ms")
	defer teardown()
	ctx := context.Background()

	// The database/sql driver uses ExecuteStreamingSql for queries and ExecuteSql for DMLs.
	server.TestSpanner.PutExecutionTime(testutil.MethodExecuteStreamingSql, testutil.SimulatedExecutionTime{MinimumExecutionTime: 50 * time.Millisecond})
	server.TestSpanner.PutExecutionTime(testutil.MethodExecuteSql, testutil.SimulatedExecutionTime{MinimumExecutionTime: 50 * time.Millisecond})

	_, err := db.ExecContext(ctx, testutil.UpdateBarSetFoo)
	if g, w := spanner.ErrCode(err), codes.DeadlineExceeded; g != w {
		t.Fatalf("error code mismatch\n Got: %v\nWant: %v", g, w)
	}
	if !strings.Contains(err.Error(), "requestID =") {
		t.Fatalf("missing requestID in error: %v", err)
	}
	_, err = db.QueryContext(ctx, testutil.SelectFooFromBar, ExecOptions{DirectExecuteQuery: true})
	if g, w := spanner.ErrCode(err), codes.DeadlineExceeded; g != w {
		t.Fatalf("error code mismatch\n Got: %v\nWant: %v", g, w)
	}
	if !strings.Contains(err.Error(), "requestID =") {
		t.Fatalf("missing requestID in error: %v", err)
	}

	// Get a connection and remove the timeout and verify that the statements work.
	c, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.ExecContext(ctx, "set statement_timeout = null"); err != nil {
		t.Fatalf("failed to remove statement_timeout: %v", err)
	}
	_, err = c.ExecContext(ctx, testutil.UpdateBarSetFoo)
	if g, w := spanner.ErrCode(err), codes.OK; g != w {
		t.Fatalf("error code mismatch\n Got: %v\nWant: %v", g, w)
	}
	r, err := c.QueryContext(ctx, testutil.SelectFooFromBar, ExecOptions{DirectExecuteQuery: true})
	if r != nil {
		_ = r.Close()
	}
	if g, w := spanner.ErrCode(err), codes.OK; g != w {
		t.Fatalf("error code mismatch\n Got: %v\nWant: %v", g, w)
	}

	// Add the timeout again on the connection and verify that the statements time out again.
	if _, err := c.ExecContext(ctx, "set statement_timeout = 1ms"); err != nil {
		t.Fatalf("failed to set statement_timeout: %v", err)
	}
	_, err = c.ExecContext(ctx, testutil.UpdateBarSetFoo)
	if g, w := spanner.ErrCode(err), codes.DeadlineExceeded; g != w {
		t.Fatalf("error code mismatch\n Got: %v\nWant: %v", g, w)
	}
	if !strings.Contains(err.Error(), "requestID =") {
		t.Fatalf("missing requestID in error: %v", err)
	}
	_, err = c.QueryContext(ctx, testutil.SelectFooFromBar, ExecOptions{DirectExecuteQuery: true})
	if g, w := spanner.ErrCode(err), codes.DeadlineExceeded; g != w {
		t.Fatalf("error code mismatch\n Got: %v\nWant: %v", g, w)
	}
	if !strings.Contains(err.Error(), "requestID =") {
		t.Fatalf("missing requestID in error: %v", err)
	}

	// Set a longer timeout and verify that executing a query and iterating its results works.
	if _, err := c.ExecContext(ctx, "set statement_timeout = 1s"); err != nil {
		t.Fatalf("failed to set statement_timeout: %v", err)
	}
	r, err = c.QueryContext(ctx, testutil.SelectFooFromBar)
	if err != nil {
		t.Fatal(err)
	}
	for r.Next() {
		var val int64
		if err := r.Scan(&val); err != nil {
			t.Fatal(err)
		}
	}
	if err := r.Close(); err != nil {
		t.Fatal(err)
	}

	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestStatementScopedPropertyValues(t *testing.T) {
	t.Parallel()

	db, server, teardown := setupTestDBConnection(t)
	defer teardown()
	ctx := context.Background()

	_, err := db.ExecContext(ctx, testutil.UpdateBarSetFoo, &ExecOptions{PropertyValues: []PropertyValue{
		CreatePropertyValue("statement_tag", "my_update_tag"),
		CreatePropertyValue("rpc_priority", "low"),
	}})
	if err != nil {
		t.Fatal(err)
	}

	it, err := db.QueryContext(ctx, testutil.SelectFooFromBar, ExecOptions{DirectExecuteQuery: true, PropertyValues: []PropertyValue{
		CreatePropertyValue("statement_tag", "my_query_tag"),
		CreatePropertyValue("rpc_priority", "medium"),
	}})
	if err != nil {
		t.Fatal(err)
	}
	_ = it.Close()

	requests := server.TestSpanner.DrainRequestsFromServer()
	executeRequests := testutil.RequestsOfType(requests, reflect.TypeOf(&spannerpb.ExecuteSqlRequest{}))
	if g, w := len(executeRequests), 2; g != w {
		t.Fatalf("number of execute requests mismatch\n Got: %v\nWant: %v", g, w)
	}

	update := executeRequests[0].(*spannerpb.ExecuteSqlRequest)
	if g, w := update.RequestOptions.RequestTag, "my_update_tag"; g != w {
		t.Fatalf("request tag mismatch\n Got: %v\nWant: %v", g, w)
	}
	if g, w := update.RequestOptions.Priority, spannerpb.RequestOptions_PRIORITY_LOW; g != w {
		t.Fatalf("request priority mismatch\n Got: %v\nWant: %v", g, w)
	}

	query := executeRequests[1].(*spannerpb.ExecuteSqlRequest)
	if g, w := query.RequestOptions.RequestTag, "my_query_tag"; g != w {
		t.Fatalf("request tag mismatch\n Got: %v\nWant: %v", g, w)
	}
	if g, w := query.RequestOptions.Priority, spannerpb.RequestOptions_PRIORITY_MEDIUM; g != w {
		t.Fatalf("request priority mismatch\n Got: %v\nWant: %v", g, w)
	}
}

func TestMissingNamedParam(t *testing.T) {
	t.Parallel()
	db, server, teardown := setupTestDBConnection(t)
	defer teardown()
	ctx := context.Background()

	query := "insert into my_table (id, value) values (@id, @value)"
	_ = server.TestSpanner.PutStatementResult(query, &testutil.StatementResult{
		Type:        testutil.StatementResultUpdateCount,
		UpdateCount: 1,
	})

	// Note: The name of the second query parameter is wrong.
	_, err := db.ExecContext(ctx, query, sql.Named("id", "1"), sql.Named("my_value", "One"))
	if err == nil {
		t.Fatal("missing expected error")
	}
	if g, w := err.Error(), "spanner: code = \"InvalidArgument\", desc = \"missing value for query parameter @value\""; g != w {
		t.Fatalf("error message mismatch\n Got: %v\nWant: %v", g, w)
	}
}

type paramTest struct {
	name           string
	input          string
	params         []any
	wantSQL        string
	wantParams     map[string]string
	wantPrepareErr string
	wantErr        string
	wantExecErr    []string
}

func (p paramTest) wantErrOrExecErr(index int) string {
	if p.wantErr != "" {
		return p.wantErr
	}
	if p.wantExecErr == nil || index >= len(p.wantExecErr) {
		return ""
	}
	return p.wantExecErr[index]
}

func TestPositionalParametersWithGoogleSQL(t *testing.T) {
	t.Parallel()
	db, server, teardown := setupTestDBConnection(t)
	defer teardown()

	for _, test := range []paramTest{
		{
			name:       "simple",
			params:     []any{1, "One"},
			input:      "insert into my_table (id, value) values (?, ?)",
			wantSQL:    "insert into my_table (id, value) values (@p1, @p2)",
			wantParams: map[string]string{"p1": "1", "p2": "One"},
		},
		{
			name:    "many",
			params:  []any{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20},
			input:   "insert into my_table (id, value) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
			wantSQL: "insert into my_table (id, value) values (@p1, @p2, @p3, @p4, @p5, @p6, @p7, @p8, @p9, @p10, @p11, @p12, @p13, @p14, @p15, @p16, @p17, @p18, @p19, @p20)",
			wantParams: map[string]string{
				"p1": "1", "p2": "2", "p3": "3", "p4": "4", "p5": "5", "p6": "6", "p7": "7", "p8": "8", "p9": "9", "p10": "10",
				"p11": "11", "p12": "12", "p13": "13", "p14": "14", "p15": "15", "p16": "16", "p17": "17", "p18": "18", "p19": "19", "p20": "20",
			},
		},
		{
			name:       "param_at_end_of_statement",
			params:     []any{1, "One", "random-value"},
			input:      "insert into my_table (id, value) values (?, ?) returning ?",
			wantSQL:    "insert into my_table (id, value) values (@p1, @p2) returning @p3",
			wantParams: map[string]string{"p1": "1", "p2": "One", "p3": "random-value"},
		},
		{
			name:    "missing_param",
			params:  []any{"One"},
			input:   "insert into my_table (id, value) values (?, ?)",
			wantSQL: "insert into my_table (id, value) values (@p1, @p2)",
			wantErr: "spanner: code = \"InvalidArgument\", desc = \"missing value for query parameter @p2\"",
		},
		{
			name:       "too_many_values",
			params:     []any{1, "One", "Not used"},
			input:      "insert into my_table (id, value) values (?, ?)",
			wantSQL:    "insert into my_table (id, value) values (@p1, @p2)",
			wantParams: map[string]string{"p1": "1", "p2": "One"},
			// Using too many parameter values with a prepared statement will trigger an error
			// from the database/sql package.
			wantExecErr: []string{"", "sql: expected 2 arguments, got 3"},
		},
		{
			name:    "mixed_named_and_positional",
			params:  []any{sql.Named("id", 1), "One"},
			input:   "insert into my_table (id, value) values (@id, ?)",
			wantErr: "spanner: code = \"InvalidArgument\", desc = \"statement must only use one parameter style (named or positional): insert into my_table (id, value) values (@id, ?)\"",
		},
		{
			name:    "mixed_positional_and_named",
			params:  []any{1, sql.Named("value", "One")},
			input:   "insert into my_table (id, value) values (?, @value)",
			wantErr: "spanner: code = \"InvalidArgument\", desc = \"statement must only use one parameter style (named or positional): insert into my_table (id, value) values (?, @value)\"",
		},
		{
			name:       "mixed_positional_and_pg_style",
			params:     []any{1},
			input:      "insert into my_table (id, value) values (?, $1)",
			wantSQL:    "insert into my_table (id, value) values (@p1, $1)",
			wantParams: map[string]string{"p1": "1"},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			executeParamTest(t, test, server, db)
		})
	}
}

func TestNamedParametersWithGoogleSQL(t *testing.T) {
	t.Parallel()
	db, server, teardown := setupTestDBConnection(t)
	defer teardown()

	for _, test := range []paramTest{
		{
			name:       "simple",
			params:     []any{sql.Named("id", 1), sql.Named("value", "One")},
			input:      "insert into my_table (id, value) values (@id, @value)",
			wantParams: map[string]string{"id": "1", "value": "One"},
		},
		{
			name: "many",
			params: []any{
				sql.Named("val1", 1),
				sql.Named("val2", 2),
				sql.Named("val3", 3),
				sql.Named("val4", 4),
				sql.Named("val5", 5),
				sql.Named("val6", 6),
				sql.Named("val7", 7),
				sql.Named("val8", 8),
				sql.Named("val9", 9),
				sql.Named("val10", 10),
				sql.Named("val11", 11),
				sql.Named("val12", 12),
				sql.Named("val13", 13),
				sql.Named("val14", 14),
				sql.Named("val15", 15),
				sql.Named("val16", 16),
				sql.Named("val17", 17),
				sql.Named("val18", 18),
				sql.Named("val19", 19),
				sql.Named("val20", 20),
			},
			input: "insert into my_table (id, value) values (@val1, @val2, @val3, @val4, @val5, @val6, @val7, @val8, @val9, @val10, @val11, @val12, @val13, @val14, @val15, @val16, @val17, @val18, @val19, @val20)",
			wantParams: map[string]string{
				"val1": "1", "val2": "2", "val3": "3", "val4": "4", "val5": "5", "val6": "6", "val7": "7", "val8": "8", "val9": "9", "val10": "10",
				"val11": "11", "val12": "12", "val13": "13", "val14": "14", "val15": "15", "val16": "16", "val17": "17", "val18": "18", "val19": "19", "val20": "20",
			},
		},
		{
			name:       "reuse_named_param",
			params:     []any{sql.Named("id", 1), sql.Named("value", "One")},
			input:      "insert into my_table (id, initial_value, current_value, generation) values (@id, @value, @value, @id)",
			wantParams: map[string]string{"id": "1", "value": "One"},
		},
		{
			name:       "case_insensitive",
			params:     []any{sql.Named("ID", 1), sql.Named("Value", "One")},
			input:      "insert into my_table (id, value) values (@id, @value)",
			wantParams: map[string]string{"id": "1", "value": "One"},
		},
		{
			name:       "case_insensitive_2",
			params:     []any{sql.Named("id", 1), sql.Named("value", "One")},
			input:      "insert into my_table (id, value) values (@ID, @VALUE)",
			wantParams: map[string]string{"ID": "1", "VALUE": "One"},
		},
		{
			name:       "case_insensitive_exact_match_priority",
			params:     []any{sql.Named("id", 1), sql.Named("ID", 2)},
			input:      "insert into my_table (id, value) values (@id, @ID)",
			wantParams: map[string]string{"id": "1", "ID": "2"},
		},
		{
			name:       "input_out_of_order",
			params:     []any{sql.Named("value", "One"), sql.Named("id", 1)},
			input:      "insert into my_table (id, value) values (@id, @value)",
			wantParams: map[string]string{"id": "1", "value": "One"},
		},
		{
			name:       "param_at_end_of_statement",
			params:     []any{sql.Named("id", 1), sql.Named("value", "One"), sql.Named("key", "random-value")},
			input:      "insert into my_table (id, value) values (@id, @value) returning @key",
			wantParams: map[string]string{"id": "1", "value": "One", "key": "random-value"},
		},
		{
			name:    "missing_id",
			params:  []any{sql.Named("value", "One")},
			input:   "insert into my_table (id, value) values (@id, @value)",
			wantErr: "spanner: code = \"InvalidArgument\", desc = \"missing value for query parameter @id\"",
		},
		{
			name:    "missing_value",
			params:  []any{sql.Named("id", 1)},
			input:   "insert into my_table (id, value) values (@id, @value)",
			wantErr: "spanner: code = \"InvalidArgument\", desc = \"missing value for query parameter @value\"",
		},
		{
			name:       "too_many_values",
			params:     []any{sql.Named("id", 1), sql.Named("value", "One"), sql.Named("key", "random-value")},
			input:      "insert into my_table (id, value) values (@id, @value)",
			wantParams: map[string]string{"id": "1", "value": "One", "key": "random-value"},
			// Using too many parameter values with a prepared statement will trigger an error
			// from the database/sql package.
			wantExecErr: []string{"", "sql: expected 2 arguments, got 3"},
		},
		{
			name:    "mixed_named_and_positional",
			params:  []any{sql.Named("id", 1), "One"},
			input:   "insert into my_table (id, value) values (@id, ?)",
			wantErr: "spanner: code = \"InvalidArgument\", desc = \"statement must only use one parameter style (named or positional): insert into my_table (id, value) values (@id, ?)\"",
		},
		{
			name:    "mixed_positional_and_named",
			params:  []any{1, sql.Named("value", "One")},
			input:   "insert into my_table (id, value) values (?, @value)",
			wantErr: "spanner: code = \"InvalidArgument\", desc = \"statement must only use one parameter style (named or positional): insert into my_table (id, value) values (?, @value)\"",
		},
		{
			name:       "mixed_named_and_pg_style",
			params:     []any{sql.Named("id", 1)},
			input:      "insert into my_table (id, value) values (@id, $1)",
			wantParams: map[string]string{"id": "1"},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			executeParamTest(t, test, server, db)
		})
	}
}

func TestIndexedParametersWithPG(t *testing.T) {
	t.Parallel()
	db, server, teardown := setupTestDBConnectionWithParamsAndDialect(t, "", databasepb.DatabaseDialect_POSTGRESQL)
	defer teardown()

	for _, test := range []paramTest{
		{
			name:       "simple",
			params:     []any{1, "One"},
			input:      "insert into my_table (id, value) values ($1, $2)",
			wantParams: map[string]string{"p1": "1", "p2": "One"},
		},
		{
			name:   "many",
			params: []any{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20},
			input:  "insert into my_table (id, value) values ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20)",
			wantParams: map[string]string{
				"p1": "1", "p2": "2", "p3": "3", "p4": "4", "p5": "5", "p6": "6", "p7": "7", "p8": "8", "p9": "9", "p10": "10",
				"p11": "11", "p12": "12", "p13": "13", "p14": "14", "p15": "15", "p16": "16", "p17": "17", "p18": "18", "p19": "19", "p20": "20",
			},
		},
		{
			name:       "reuse_param",
			params:     []any{1, "One"},
			input:      "insert into my_table (id, initial_value, current_value, generation) values ($1, $2, $2, $1)",
			wantParams: map[string]string{"p1": "1", "p2": "One"},
		},
		{
			name:       "input_out_of_order",
			params:     []any{"One", 1},
			input:      "insert into my_table (id, value) values ($2, $1)",
			wantParams: map[string]string{"p2": "One", "p1": "1"},
		},
		{
			name:       "input_out_of_order_named",
			params:     []any{sql.Named("p1", "One"), sql.Named("p2", 1)},
			input:      "insert into my_table (id, value) values ($2, $1)",
			wantParams: map[string]string{"p1": "One", "p2": "1"},
		},
		{
			name:       "param_at_end_of_statement",
			params:     []any{1, "One", "random-value"},
			input:      "insert into my_table (id, value) values ($1, $2) returning $3",
			wantParams: map[string]string{"p1": "1", "p2": "One", "p3": "random-value"},
		},
		{
			name:    "missing_param",
			params:  []any{1},
			input:   "insert into my_table (id, value) values ($1, $2)",
			wantErr: "spanner: code = \"InvalidArgument\", desc = \"missing value for query parameter @p2\"",
		},
		{
			name:        "too_many_values",
			params:      []any{1, "One", "Not used"},
			input:       "insert into my_table (id, value) values ($1, $2)",
			wantParams:  map[string]string{"p1": "1", "p2": "One"},
			wantExecErr: []string{"", "sql: expected 2 arguments, got 3"},
		},
		{
			name:    "missing_id",
			params:  []any{sql.Named("p2", "One")},
			input:   "insert into my_table (id, value) values ($1, $2)",
			wantErr: "spanner: code = \"InvalidArgument\", desc = \"missing value for query parameter @p1\"",
		},
		{
			name:    "missing_value",
			params:  []any{sql.Named("p1", 1)},
			input:   "insert into my_table (id, value) values ($1, $2)",
			wantErr: "spanner: code = \"InvalidArgument\", desc = \"missing value for query parameter @p2\"",
		},
		{
			name:    "mixed_indexed_and_positional",
			params:  []any{1, "One"},
			input:   "insert into my_table (id, value) values ($1, ?)",
			wantErr: "spanner: code = \"InvalidArgument\", desc = \"statement must only use one parameter style (named or positional): insert into my_table (id, value) values ($1, ?)\"",
		},
		{
			name:    "mixed_positional_and_indexed",
			params:  []any{1, "One"},
			input:   "insert into my_table (id, value) values (?, $1)",
			wantErr: "spanner: code = \"InvalidArgument\", desc = \"statement must only use one parameter style (named or positional): insert into my_table (id, value) values (?, $1)\"",
		},
		{
			name:    "mixed_named_and_indexed",
			params:  []any{sql.Named("id", 1), "One"},
			input:   "insert into my_table (id, value) values (@id, $1)",
			wantErr: "spanner: code = \"InvalidArgument\", desc = \"statement must only use one parameter style (named or positional): insert into my_table (id, value) values (@id, $1)\"",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			executeParamTest(t, test, server, db)
		})
	}
}

func TestNamedParametersWithPG(t *testing.T) {
	t.Parallel()
	db, server, teardown := setupTestDBConnectionWithParamsAndDialect(t, "", databasepb.DatabaseDialect_POSTGRESQL)
	defer teardown()

	for _, test := range []paramTest{
		{
			name:       "simple",
			params:     []any{sql.Named("id", 1), sql.Named("value", "One")},
			input:      "insert into my_table (id, value) values (@id, @value)",
			wantSQL:    "insert into my_table (id, value) values ($1, $2)",
			wantParams: map[string]string{"p1": "1", "p2": "One"},
		},
		{
			name: "many",
			params: []any{
				sql.Named("val1", 1),
				sql.Named("val2", 2),
				sql.Named("val3", 3),
				sql.Named("val4", 4),
				sql.Named("val5", 5),
				sql.Named("val6", 6),
				sql.Named("val7", 7),
				sql.Named("val8", 8),
				sql.Named("val9", 9),
				sql.Named("val10", 10),
				sql.Named("val11", 11),
				sql.Named("val12", 12),
				sql.Named("val13", 13),
				sql.Named("val14", 14),
				sql.Named("val15", 15),
				sql.Named("val16", 16),
				sql.Named("val17", 17),
				sql.Named("val18", 18),
				sql.Named("val19", 19),
				sql.Named("val20", 20),
			},
			input:   "insert into my_table (id, value) values (@val1, @val2, @val3, @val4, @val5, @val6, @val7, @val8, @val9, @val10, @val11, @val12, @val13, @val14, @val15, @val16, @val17, @val18, @val19, @val20)",
			wantSQL: "insert into my_table (id, value) values ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20)",
			wantParams: map[string]string{
				"p1": "1", "p2": "2", "p3": "3", "p4": "4", "p5": "5", "p6": "6", "p7": "7", "p8": "8", "p9": "9", "p10": "10",
				"p11": "11", "p12": "12", "p13": "13", "p14": "14", "p15": "15", "p16": "16", "p17": "17", "p18": "18", "p19": "19", "p20": "20",
			},
		},
		{
			name:       "reuse_named_param",
			params:     []any{sql.Named("id", 1), sql.Named("value", "One")},
			input:      "insert into my_table (id, initial_value, current_value, generation) values (@id, @value, @value, @id)",
			wantSQL:    "insert into my_table (id, initial_value, current_value, generation) values ($1, $2, $2, $1)",
			wantParams: map[string]string{"p1": "1", "p2": "One"},
		},
		{
			name:       "case_insensitive",
			params:     []any{sql.Named("ID", 1), sql.Named("Value", "One")},
			input:      "insert into my_table (id, value) values (@id, @value)",
			wantSQL:    "insert into my_table (id, value) values ($1, $2)",
			wantParams: map[string]string{"p1": "1", "p2": "One"},
		},
		{
			name:       "case_insensitive_2",
			params:     []any{sql.Named("id", 1), sql.Named("value", "One")},
			input:      "insert into my_table (id, value) values (@ID, @VALUE)",
			wantSQL:    "insert into my_table (id, value) values ($1, $2)",
			wantParams: map[string]string{"p1": "1", "p2": "One"},
		},
		{
			name:       "input_out_of_order",
			params:     []any{sql.Named("value", "One"), sql.Named("id", 1)},
			input:      "insert into my_table (id, value) values (@id, @value)",
			wantSQL:    "insert into my_table (id, value) values ($1, $2)",
			wantParams: map[string]string{"p1": "1", "p2": "One"},
		},
		{
			name:       "param_at_end_of_statement",
			params:     []any{sql.Named("id", 1), sql.Named("value", "One"), sql.Named("key", "random-value")},
			input:      "insert into my_table (id, value) values (@id, @value) returning @key",
			wantSQL:    "insert into my_table (id, value) values ($1, $2) returning $3",
			wantParams: map[string]string{"p1": "1", "p2": "One", "p3": "random-value"},
		},
		{
			name:    "missing_id",
			params:  []any{sql.Named("value", "One")},
			input:   "insert into my_table (id, value) values (@id, @value)",
			wantSQL: "insert into my_table (id, value) values ($1, $2)",
			wantErr: "spanner: code = \"InvalidArgument\", desc = \"missing value for query parameter @id\"",
		},
		{
			name:    "missing_value",
			params:  []any{sql.Named("id", 1)},
			input:   "insert into my_table (id, value) values (@id, @value)",
			wantErr: "spanner: code = \"InvalidArgument\", desc = \"missing value for query parameter @value\"",
		},
		{
			name:    "too_many_values",
			params:  []any{sql.Named("id", 1), sql.Named("value", "One"), sql.Named("key", "random-value")},
			input:   "insert into my_table (id, value) values (@id, @value)",
			wantSQL: "insert into my_table (id, value) values ($1, $2)",
			// Parameter values that are not matched to anything in the SQL string are not
			// translated to a PostgreSQL-style parameter name.
			wantParams:  map[string]string{"p1": "1", "p2": "One", "key": "random-value"},
			wantExecErr: []string{"", "sql: expected 2 arguments, got 3"},
		},
		{
			name:    "mixed_named_and_positional",
			params:  []any{sql.Named("id", 1), "One"},
			input:   "insert into my_table (id, value) values (@id, ?)",
			wantErr: "spanner: code = \"InvalidArgument\", desc = \"statement must only use one parameter style (named or positional): insert into my_table (id, value) values (@id, ?)\"",
		},
		{
			name:    "mixed_positional_and_named",
			params:  []any{1, sql.Named("value", "One")},
			input:   "insert into my_table (id, value) values (?, @value)",
			wantErr: "spanner: code = \"InvalidArgument\", desc = \"statement must only use one parameter style (named or positional): insert into my_table (id, value) values (?, @value)\"",
		},
		{
			name:    "mixed_named_and_indexed",
			params:  []any{sql.Named("id", 1), "One"},
			input:   "insert into my_table (id, value) values (@id, $1)",
			wantErr: "spanner: code = \"InvalidArgument\", desc = \"statement must only use one parameter style (named or positional): insert into my_table (id, value) values (@id, $1)\"",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			executeParamTest(t, test, server, db)
		})
	}
}

func TestPositionalParametersWithPG(t *testing.T) {
	t.Parallel()
	db, server, teardown := setupTestDBConnectionWithParamsAndDialect(t, "", databasepb.DatabaseDialect_POSTGRESQL)
	defer teardown()

	for _, test := range []paramTest{
		{
			name:       "simple",
			params:     []any{1, "One"},
			input:      "insert into my_table (id, value) values (?, ?)",
			wantSQL:    "insert into my_table (id, value) values ($1, $2)",
			wantParams: map[string]string{"p1": "1", "p2": "One"},
		},
		{
			name:    "many",
			params:  []any{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20},
			input:   "insert into my_table (id, value) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
			wantSQL: "insert into my_table (id, value) values ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20)",
			wantParams: map[string]string{
				"p1": "1", "p2": "2", "p3": "3", "p4": "4", "p5": "5", "p6": "6", "p7": "7", "p8": "8", "p9": "9", "p10": "10",
				"p11": "11", "p12": "12", "p13": "13", "p14": "14", "p15": "15", "p16": "16", "p17": "17", "p18": "18", "p19": "19", "p20": "20",
			},
		},
		{
			name:       "param_at_end_of_statement",
			params:     []any{1, "One", "random-value"},
			input:      "insert into my_table (id, value) values (?, ?) returning ?",
			wantSQL:    "insert into my_table (id, value) values ($1, $2) returning $3",
			wantParams: map[string]string{"p1": "1", "p2": "One", "p3": "random-value"},
		},
		{
			name:    "missing_param",
			params:  []any{"One"},
			input:   "insert into my_table (id, value) values (?, ?)",
			wantSQL: "insert into my_table (id, value) values ($1, $2)",
			wantErr: "spanner: code = \"InvalidArgument\", desc = \"missing value for query parameter @p2\"",
		},
		{
			name:        "too_many_values",
			params:      []any{1, "One", "Not used"},
			input:       "insert into my_table (id, value) values (?, ?)",
			wantSQL:     "insert into my_table (id, value) values ($1, $2)",
			wantParams:  map[string]string{"p1": "1", "p2": "One"},
			wantExecErr: []string{"", "sql: expected 2 arguments, got 3"},
		},
		{
			name:    "mixed_named_and_positional",
			params:  []any{sql.Named("id", 1), "One"},
			input:   "insert into my_table (id, value) values (@id, ?)",
			wantErr: "spanner: code = \"InvalidArgument\", desc = \"statement must only use one parameter style (named or positional): insert into my_table (id, value) values (@id, ?)\"",
		},
		{
			name:    "mixed_positional_and_named",
			params:  []any{1, sql.Named("value", "One")},
			input:   "insert into my_table (id, value) values (?, @value)",
			wantErr: "spanner: code = \"InvalidArgument\", desc = \"statement must only use one parameter style (named or positional): insert into my_table (id, value) values (?, @value)\"",
		},
		{
			name:    "mixed_positional_and_indexed",
			params:  []any{1, "One"},
			input:   "insert into my_table (id, value) values (?, $1)",
			wantErr: "spanner: code = \"InvalidArgument\", desc = \"statement must only use one parameter style (named or positional): insert into my_table (id, value) values (?, $1)\"",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			executeParamTest(t, test, server, db)
		})
	}
}

func matchExpectedErr(got, want string) bool {
	if got == want {
		return true
	}
	return strings.HasSuffix(got, want) && strings.HasPrefix(got, "[SQLSTATE ")
}

func executeParamTest(t *testing.T, test paramTest, server *testutil.MockedSpannerInMemTestServer, db *sql.DB) {
	ctx := context.Background()
	query := test.wantSQL
	if query == "" {
		query = test.input
	}
	_ = server.TestSpanner.PutStatementResult(query, &testutil.StatementResult{
		Type:        testutil.StatementResultUpdateCount,
		UpdateCount: 1,
	})

	stmt, err := db.PrepareContext(ctx, test.input)
	if err != nil {
		if test.wantErr == "" {
			t.Fatal(err)
		} else {
			if g, w := err.Error(), test.wantErr; !matchExpectedErr(g, w) {
				t.Fatalf("error mismatch\nGot:  %v\nWant: %v", g, w)
			}
		}
	} else if test.wantPrepareErr != "" {
		t.Fatalf("missing expected error: %v", test.wantPrepareErr)
	}

	var testExecFuncs []func() error
	if stmt == nil {
		testExecFuncs = []func() error{func() error {
			_, err := db.ExecContext(ctx, test.input, test.params...)
			return err
		}}
	} else {
		// Execute the statement both as the previously prepared statement,
		// and directly using the sql.DB instance.
		testExecFuncs = []func() error{func() error {
			_, err := db.ExecContext(ctx, test.input, test.params...)
			return err
		}, func() error {
			_, err = stmt.ExecContext(ctx, test.params...)
			return err
		}}
	}
	for i, testExecFunc := range testExecFuncs {
		err := testExecFunc()
		wantErr := test.wantErrOrExecErr(i)
		if err != nil {
			if wantErr == "" {
				t.Fatal(err)
			} else {
				if g, w := err.Error(), wantErr; !matchExpectedErr(g, w) {
					t.Fatalf("error mismatch\nGot:  %v\nWant: %v", g, w)
				}
				return
			}
		} else if wantErr != "" {
			t.Fatalf("missing expected error: %v", wantErr)
		}

		requests := server.TestSpanner.DrainRequestsFromServer()
		executeRequests := testutil.RequestsOfType(requests, reflect.TypeOf(&spannerpb.ExecuteSqlRequest{}))
		if g, w := len(executeRequests), 1; g != w {
			t.Fatalf("number of execute requests mismatch\n Got: %v\nWant: %v", g, w)
		}
		request := executeRequests[0].(*spannerpb.ExecuteSqlRequest)
		if g, w := len(request.Params.Fields), len(test.wantParams); g != w {
			t.Fatalf("number of parameters mismatch\n Got: %v\nWant: %v", g, w)
		}
		for k, v := range test.wantParams {
			if g, w := request.Params.Fields[k].GetStringValue(), v; g != w {
				t.Fatalf("%s: parameter value mismatch\n Got: %v\nWant: %v", k, g, w)
			}
		}
	}
}

func TestPrepareStmtGORMSharedOptionsBug(t *testing.T) {
	t.Parallel()

	db, server, teardown := setupTestDBConnection(t)
	defer teardown()
	ctx := context.Background()

	// Mock the update query.
	query := "update test set value = ? where id = ?"
	if err := server.TestSpanner.PutStatementResult(
		"update test set value = @p1 where id = @p2",
		&testutil.StatementResult{
			Type:        testutil.StatementResultUpdateCount,
			UpdateCount: 1,
		},
	); err != nil {
		t.Fatal(err)
	}

	// 1. Prepare statement.
	stmt, err := db.PrepareContext(ctx, query)
	if err != nil {
		t.Fatalf("failed to prepare query: %v", err)
	}
	defer stmt.Close()

	// 2. Execute statement outside transaction (auto-commit).
	_, err = stmt.ExecContext(ctx, "val1", int64(1))
	if err != nil {
		t.Fatalf("failed auto-commit execution: %v", err)
	}

	// 3. Start explicit transaction.
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("failed to begin transaction: %v", err)
	}

	// 4. Execute the cached statement inside transaction using tx.Stmt().
	txStmt := tx.Stmt(stmt)
	_, err = txStmt.ExecContext(ctx, "val2", int64(1))
	if err != nil {
		_ = tx.Rollback()
		t.Fatalf("failed in-transaction statement execution: %v", err)
	}

	// 5. Execute another statement in the same transaction.
	_, err = tx.ExecContext(ctx, "update test set value = ? where id = ?", "val3", int64(2))
	if err != nil {
		_ = tx.Rollback()
		t.Fatalf("failed second in-transaction execution: %v", err)
	}

	// 6. Commit transaction.
	if err := tx.Commit(); err != nil {
		t.Fatalf("failed to commit: %v", err)
	}

	// 7. Verify mock server requests.
	requests := server.TestSpanner.DrainRequestsFromServer()
	executeRequests := testutil.RequestsOfType(requests, reflect.TypeOf(&spannerpb.ExecuteSqlRequest{}))
	if g, w := len(executeRequests), 3; g != w {
		t.Fatalf("number of execute requests mismatch\n Got: %v\nWant: %v", g, w)
	}

	req1 := executeRequests[0].(*spannerpb.ExecuteSqlRequest) // auto-commit outside transaction
	req2 := executeRequests[1].(*spannerpb.ExecuteSqlRequest) // first DML in transaction (prepared statement)
	req3 := executeRequests[2].(*spannerpb.ExecuteSqlRequest) // second DML in transaction

	if !req1.LastStatement {
		t.Error("Expected LastStatement=true for auto-commit execution, got false")
	}
	if req2.LastStatement {
		t.Error("Expected LastStatement=false for first in-transaction statement, got true")
	}
	if req3.LastStatement {
		t.Error("Expected LastStatement=false for second in-transaction statement, got true")
	}
}

func TestQueryMismatchedParameterName(t *testing.T) {
	t.Parallel()

	db, _, teardown := setupTestDBConnection(t)
	defer teardown()
	ctx := context.Background()

	// SELECT with @my_id parameter but passing named parameter "id" instead
	_, err := db.QueryContext(ctx, "select value from test where id=@my_id", sql.Named("id", 123))
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if !strings.Contains(err.Error(), "missing value for query parameter @my_id") {
		t.Fatalf("expected missing value error, got: %v", err)
	}
}

func TestPreparedStatement_ExecOptionsNotSticky(t *testing.T) {
	t.Parallel()

	db, server, teardown := setupTestDBConnection(t)
	defer teardown()
	ctx := context.Background()

	conn, err := db.Conn(ctx)
	if err != nil {
		t.Fatalf("failed to open connection: %v", err)
	}
	defer silentClose(conn)

	if _, err := conn.ExecContext(ctx, "SET statement_tag = 'prepare_tag'"); err != nil {
		t.Fatalf("SET statement_tag failed: %v", err)
	}
	queryStmt, err := conn.PrepareContext(ctx, testutil.SelectFooFromBar)
	if err != nil {
		t.Fatalf("PrepareContext(SELECT) failed: %v", err)
	}
	defer silentClose(queryStmt)

	for _, callOpts := range []*ExecOptions{
		nil,
		{QueryOptions: spanner.QueryOptions{RequestTag: "override_tag"}},
		nil,
	} {
		var rows *sql.Rows
		if callOpts == nil {
			rows, err = queryStmt.QueryContext(ctx)
		} else {
			rows, err = queryStmt.QueryContext(ctx, *callOpts)
		}
		if err != nil {
			t.Fatalf("queryStmt.QueryContext failed: %v", err)
		}
		for rows.Next() {
		}
		if err := rows.Err(); err != nil {
			_ = rows.Close()
			t.Fatalf("rows iteration failed: %v", err)
		}
		_ = rows.Close()
	}

	dmlStmt, err := conn.PrepareContext(ctx, testutil.UpdateBarSetFoo)
	if err != nil {
		t.Fatalf("PrepareContext(UPDATE) failed: %v", err)
	}
	defer silentClose(dmlStmt)

	if _, err := dmlStmt.ExecContext(ctx, &ExecOptions{QueryOptions: spanner.QueryOptions{RequestTag: "dml_override_tag"}}); err != nil {
		t.Fatalf("dmlStmt.ExecContext with ExecOptions failed: %v", err)
	}
	if _, err := dmlStmt.ExecContext(ctx); err != nil {
		t.Fatalf("dmlStmt.ExecContext without ExecOptions failed: %v", err)
	}

	requests := server.TestSpanner.DrainRequestsFromServer()
	executeRequests := testutil.RequestsOfType(requests, reflect.TypeOf(&spannerpb.ExecuteSqlRequest{}))
	if g, w := len(executeRequests), 5; g != w {
		t.Fatalf("number of execute requests mismatch\nGot:  %v\nWant: %v", g, w)
	}

	wantTags := []string{
		"prepare_tag",
		"override_tag",
		"prepare_tag",
		"dml_override_tag",
		"",
	}
	for i, wantTag := range wantTags {
		req := executeRequests[i].(*spannerpb.ExecuteSqlRequest)
		if g, w := req.RequestOptions.GetRequestTag(), wantTag; g != w {
			t.Errorf("request[%d] tag mismatch\nGot:  %q\nWant: %q", i, g, w)
		}
	}
}

func TestPreparedStatement_ExecOptions_ParameterizedPositions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		queryArgs func(opts ExecOptions) []any
		dmlArgs   func(opts ExecOptions) []any
	}{
		{
			name:      "ExecOptions before parameters",
			queryArgs: func(opts ExecOptions) []any { return []any{opts, int64(10), "foo"} },
			dmlArgs:   func(opts ExecOptions) []any { return []any{opts, "bar", int64(20)} },
		},
		{
			name:      "ExecOptions between parameters",
			queryArgs: func(opts ExecOptions) []any { return []any{int64(10), &opts, "foo"} },
			dmlArgs:   func(opts ExecOptions) []any { return []any{"bar", &opts, int64(20)} },
		},
		{
			name:      "ExecOptions after parameters",
			queryArgs: func(opts ExecOptions) []any { return []any{int64(10), "foo", opts} },
			dmlArgs:   func(opts ExecOptions) []any { return []any{"bar", int64(20), opts} },
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			db, server, teardown := setupTestDBConnection(t)
			defer teardown()
			ctx := context.Background()

			if err := server.TestSpanner.PutStatementResult(
				"SELECT value FROM test WHERE id=@p1 AND name=@p2",
				&testutil.StatementResult{
					Type:      testutil.StatementResultResultSet,
					ResultSet: testutil.CreateSelect1ResultSet(),
				},
			); err != nil {
				t.Fatalf("PutStatementResult(SELECT) failed: %v", err)
			}
			if err := server.TestSpanner.PutStatementResult(
				"UPDATE test SET value=@p1 WHERE id=@p2",
				&testutil.StatementResult{
					Type:        testutil.StatementResultUpdateCount,
					UpdateCount: 1,
				},
			); err != nil {
				t.Fatalf("PutStatementResult(UPDATE) failed: %v", err)
			}

			conn, err := db.Conn(ctx)
			if err != nil {
				t.Fatalf("failed to open connection: %v", err)
			}
			defer silentClose(conn)

			if _, err := conn.ExecContext(ctx, "SET statement_tag = 'prepare_tag'"); err != nil {
				t.Fatalf("SET statement_tag failed: %v", err)
			}
			queryStmt, err := conn.PrepareContext(ctx, "SELECT value FROM test WHERE id=? AND name=?")
			if err != nil {
				t.Fatalf("PrepareContext(SELECT) failed: %v", err)
			}
			defer silentClose(queryStmt)

			runSelect := func(args ...any) {
				rows, queryErr := queryStmt.QueryContext(ctx, args...)
				if queryErr != nil {
					t.Fatalf("queryStmt.QueryContext failed: %v", queryErr)
				}
				if !rows.Next() {
					_ = rows.Close()
					t.Fatalf("expected row from queryStmt, got none (err=%v)", rows.Err())
				}
				_ = rows.Close()
			}

			runSelect(int64(10), "foo")
			runSelect(tc.queryArgs(ExecOptions{QueryOptions: spanner.QueryOptions{RequestTag: "override_tag"}})...)
			runSelect(int64(10), "foo")

			dmlStmt, err := conn.PrepareContext(ctx, "UPDATE test SET value=? WHERE id=?")
			if err != nil {
				t.Fatalf("PrepareContext(UPDATE) failed: %v", err)
			}
			defer silentClose(dmlStmt)

			if _, err := dmlStmt.ExecContext(ctx, tc.dmlArgs(ExecOptions{QueryOptions: spanner.QueryOptions{RequestTag: "dml_override_tag"}})...); err != nil {
				t.Fatalf("dmlStmt.ExecContext(with ExecOptions) failed: %v", err)
			}
			if _, err := dmlStmt.ExecContext(ctx, "bar", int64(20)); err != nil {
				t.Fatalf("dmlStmt.ExecContext(without ExecOptions) failed: %v", err)
			}

			requests := server.TestSpanner.DrainRequestsFromServer()
			executeRequests := testutil.RequestsOfType(requests, reflect.TypeOf(&spannerpb.ExecuteSqlRequest{}))
			if g, w := len(executeRequests), 5; g != w {
				t.Fatalf("number of execute requests mismatch\nGot:  %v\nWant: %v", g, w)
			}

			wantTags := []string{
				"prepare_tag",
				"override_tag",
				"prepare_tag",
				"dml_override_tag",
				"",
			}
			for i, wantTag := range wantTags {
				req := executeRequests[i].(*spannerpb.ExecuteSqlRequest)
				if g, w := req.RequestOptions.GetRequestTag(), wantTag; g != w {
					t.Errorf("request[%d] tag mismatch\nGot:  %q\nWant: %q", i, g, w)
				}
			}

			// Verify SQL parameters arrived intact on both override and follow-up calls.
			overrideSelectReq := executeRequests[1].(*spannerpb.ExecuteSqlRequest)
			if g, w := overrideSelectReq.Params.Fields["p1"].GetStringValue(), "10"; g != w {
				t.Errorf("override select p1 mismatch\nGot:  %q\nWant: %q", g, w)
			}
			if g, w := overrideSelectReq.Params.Fields["p2"].GetStringValue(), "foo"; g != w {
				t.Errorf("override select p2 mismatch\nGot:  %q\nWant: %q", g, w)
			}
			overrideDMLReq := executeRequests[3].(*spannerpb.ExecuteSqlRequest)
			if g, w := overrideDMLReq.Params.Fields["p1"].GetStringValue(), "bar"; g != w {
				t.Errorf("override DML p1 mismatch\nGot:  %q\nWant: %q", g, w)
			}
			if g, w := overrideDMLReq.Params.Fields["p2"].GetStringValue(), "20"; g != w {
				t.Errorf("override DML p2 mismatch\nGot:  %q\nWant: %q", g, w)
			}
		})
	}
}

func TestPreparedStatement_ExecOptions_ErrorAndFailedCheckNamedValueCleanup(t *testing.T) {
	t.Parallel()

	db, server, teardown := setupTestDBConnection(t)
	defer teardown()
	ctx := context.Background()

	conn, err := db.Conn(ctx)
	if err != nil {
		t.Fatalf("failed to open connection: %v", err)
	}
	defer silentClose(conn)

	if _, err := conn.ExecContext(ctx, "SET statement_tag = 'prepare_tag'"); err != nil {
		t.Fatalf("SET statement_tag failed: %v", err)
	}
	queryStmt, err := conn.PrepareContext(ctx, testutil.SelectFooFromBar)
	if err != nil {
		t.Fatalf("PrepareContext(SELECT) failed: %v", err)
	}
	defer silentClose(queryStmt)

	// 1a. CheckNamedValue failure after ExecOptions must not leak tempExecOptions into the next call.
	failedRows, err := queryStmt.QueryContext(ctx, ExecOptions{QueryOptions: spanner.QueryOptions{RequestTag: "leaked_check_tag"}}, make(chan int))
	if failedRows != nil {
		_ = failedRows.Close()
	}
	if err == nil {
		t.Fatalf("expected CheckNamedValue error for unsupported argument, got nil")
	}

	// 1b. Excess argument mismatch after ExecOptions must not leak tempExecOptions into the next call.
	excessRows, err := queryStmt.QueryContext(ctx, ExecOptions{QueryOptions: spanner.QueryOptions{RequestTag: "leaked_excess_tag"}}, int64(1))
	if excessRows != nil {
		_ = excessRows.Close()
	}
	if err == nil {
		t.Fatalf("expected argument count error for excess argument, got nil")
	}

	// 1c. Excess argument before ExecOptions must not leak tempExecOptions into the next call.
	excessBeforeRows, err := queryStmt.QueryContext(ctx, int64(1), ExecOptions{QueryOptions: spanner.QueryOptions{RequestTag: "leaked_excess_before_tag"}})
	if excessBeforeRows != nil {
		_ = excessBeforeRows.Close()
	}
	if err == nil {
		t.Fatalf("expected argument count error for excess argument before ExecOptions, got nil")
	}

	// 2. Typed-nil *ExecOptions (including after an ExecOptions argument) must clear prior ExecOptions without panicking.
	var nilOpts *ExecOptions
	rows, err := queryStmt.QueryContext(ctx, ExecOptions{QueryOptions: spanner.QueryOptions{RequestTag: "overwritten_by_nil_tag"}}, nilOpts)
	if err != nil {
		t.Fatalf("QueryContext with ExecOptions followed by typed-nil *ExecOptions failed: %v", err)
	}
	if !rows.Next() {
		_ = rows.Close()
		t.Fatalf("expected row after typed-nil *ExecOptions, got none (err=%v)", rows.Err())
	}
	_ = rows.Close()

	// 3a. Server RPC error with DirectExecuteQuery=true and ExecOptions must not leak ExecOptions into the next call.
	server.TestSpanner.PutExecutionTime(testutil.MethodExecuteStreamingSql, testutil.SimulatedExecutionTime{
		Errors: []error{status.Error(codes.InvalidArgument, "simulated query failure")},
	})
	errRows, err := queryStmt.QueryContext(ctx, ExecOptions{
		DirectExecuteQuery: true,
		QueryOptions:       spanner.QueryOptions{RequestTag: "rpc_error_query_tag"},
	})
	if errRows != nil {
		_ = errRows.Close()
	}
	if g, w := status.Code(err), codes.InvalidArgument; g != w {
		t.Fatalf("QueryContext simulated error code mismatch\nGot:  %v\nWant: %v", g, w)
	}

	cleanRows, err := queryStmt.QueryContext(ctx)
	if err != nil {
		t.Fatalf("clean QueryContext after RPC error failed: %v", err)
	}
	if !cleanRows.Next() {
		_ = cleanRows.Close()
		t.Fatalf("expected row on clean QueryContext, got none (err=%v)", cleanRows.Err())
	}
	_ = cleanRows.Close()

	// 3b. Lazy execution (DirectExecuteQuery=false) RPC failure on rows.Next() must not leak ExecOptions into the next call.
	server.TestSpanner.PutExecutionTime(testutil.MethodExecuteStreamingSql, testutil.SimulatedExecutionTime{
		Errors: []error{status.Error(codes.InvalidArgument, "simulated lazy query failure")},
	})
	lazyErrRows, err := queryStmt.QueryContext(ctx, ExecOptions{
		QueryOptions: spanner.QueryOptions{RequestTag: "lazy_rpc_error_query_tag"},
	})
	if err != nil {
		t.Fatalf("lazy QueryContext returned unexpected immediate error: %v", err)
	}
	if lazyErrRows.Next() {
		_ = lazyErrRows.Close()
		t.Fatalf("expected lazy rows.Next() to fail")
	}
	if g, w := status.Code(lazyErrRows.Err()), codes.InvalidArgument; g != w {
		_ = lazyErrRows.Close()
		t.Fatalf("lazy rows.Err() code mismatch\nGot:  %v\nWant: %v", g, w)
	}
	_ = lazyErrRows.Close()

	cleanAfterLazyRows, err := queryStmt.QueryContext(ctx)
	if err != nil {
		t.Fatalf("clean QueryContext after lazy RPC error failed: %v", err)
	}
	if !cleanAfterLazyRows.Next() {
		_ = cleanAfterLazyRows.Close()
		t.Fatalf("expected row on clean QueryContext after lazy error, got none (err=%v)", cleanAfterLazyRows.Err())
	}
	_ = cleanAfterLazyRows.Close()

	// 4. Server RPC error with ExecOptions on ExecContext must not leak into the next ExecContext call.
	dmlStmt, err := conn.PrepareContext(ctx, testutil.UpdateBarSetFoo)
	if err != nil {
		t.Fatalf("PrepareContext(UPDATE) failed: %v", err)
	}
	defer silentClose(dmlStmt)

	server.TestSpanner.PutExecutionTime(testutil.MethodExecuteStreamingSql, testutil.SimulatedExecutionTime{
		Errors: []error{status.Error(codes.InvalidArgument, "simulated dml failure")},
	})
	_, err = dmlStmt.ExecContext(ctx, ExecOptions{QueryOptions: spanner.QueryOptions{RequestTag: "rpc_error_dml_tag"}})
	if g, w := status.Code(err), codes.InvalidArgument; g != w {
		t.Fatalf("ExecContext simulated error code mismatch\nGot:  %v\nWant: %v", g, w)
	}
	if _, err := dmlStmt.ExecContext(ctx); err != nil {
		t.Fatalf("clean ExecContext after RPC error failed: %v", err)
	}

	requests := server.TestSpanner.DrainRequestsFromServer()
	executeRequests := testutil.RequestsOfType(requests, reflect.TypeOf(&spannerpb.ExecuteSqlRequest{}))
	if g, w := len(executeRequests), 7; g != w {
		t.Fatalf("number of execute requests mismatch\nGot:  %v\nWant: %v", g, w)
	}
	wantTags := []string{
		"prepare_tag",
		"rpc_error_query_tag",
		"prepare_tag",
		"lazy_rpc_error_query_tag",
		"prepare_tag",
		"rpc_error_dml_tag",
		"",
	}
	for i, wantTag := range wantTags {
		req := executeRequests[i].(*spannerpb.ExecuteSqlRequest)
		if g, w := req.RequestOptions.GetRequestTag(), wantTag; g != w {
			t.Errorf("request[%d] tag mismatch\nGot:  %q\nWant: %q", i, g, w)
		}
	}
}

func TestPreparedStatement_ExecOptions_DirectedReadNotMutatedByRWTransaction(t *testing.T) {
	t.Parallel()

	db, server, teardown := setupTestDBConnection(t)
	defer teardown()
	ctx := context.Background()

	conn, err := db.Conn(ctx)
	if err != nil {
		t.Fatalf("failed to open connection: %v", err)
	}
	defer silentClose(conn)

	directedRead := &spannerpb.DirectedReadOptions{
		Replicas: &spannerpb.DirectedReadOptions_IncludeReplicas_{
			IncludeReplicas: &spannerpb.DirectedReadOptions_IncludeReplicas{
				ReplicaSelections: []*spannerpb.DirectedReadOptions_ReplicaSelection{
					{Location: "us-east1", Type: spannerpb.DirectedReadOptions_ReplicaSelection_READ_ONLY},
				},
			},
		},
	}
	if err := conn.Raw(func(driverConn any) error {
		sc := driverConn.(SpannerConn)
		return sc.SetDirectedReadOptions(directedRead)
	}); err != nil {
		t.Fatalf("SetDirectedReadOptions failed: %v", err)
	}

	stmt, err := conn.PrepareContext(ctx, testutil.SelectFooFromBar)
	if err != nil {
		t.Fatalf("PrepareContext failed: %v", err)
	}
	defer silentClose(stmt)

	// Execute once inside a read-write transaction (which clears DirectedReadOptions on that call's options).
	tx, err := conn.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("BeginTx failed: %v", err)
	}
	txStmt := tx.Stmt(stmt)
	txRows, err := txStmt.QueryContext(ctx)
	if err != nil {
		_ = tx.Rollback()
		t.Fatalf("txStmt.QueryContext failed: %v", err)
	}
	if !txRows.Next() {
		_ = txRows.Close()
		_ = tx.Rollback()
		t.Fatalf("expected row from txStmt, got none (err=%v)", txRows.Err())
	}
	_ = txRows.Close()
	if err := tx.Commit(); err != nil {
		t.Fatalf("tx.Commit failed: %v", err)
	}

	// Subsequent single-use execution on the same prepared statement must still preserve prepare-time DirectedReadOptions.
	rows, err := stmt.QueryContext(ctx)
	if err != nil {
		t.Fatalf("stmt.QueryContext failed: %v", err)
	}
	if !rows.Next() {
		_ = rows.Close()
		t.Fatalf("expected row from stmt, got none (err=%v)", rows.Err())
	}
	_ = rows.Close()

	requests := server.TestSpanner.DrainRequestsFromServer()
	executeRequests := testutil.RequestsOfType(requests, reflect.TypeOf(&spannerpb.ExecuteSqlRequest{}))
	if g, w := len(executeRequests), 2; g != w {
		t.Fatalf("number of execute requests mismatch\nGot:  %v\nWant: %v", g, w)
	}
	rwReq := executeRequests[0].(*spannerpb.ExecuteSqlRequest)
	if rwReq.DirectedReadOptions != nil {
		t.Errorf("expected nil DirectedReadOptions in RW transaction, got %v", rwReq.DirectedReadOptions)
	}
	roReq := executeRequests[1].(*spannerpb.ExecuteSqlRequest)
	if diff := cmp.Diff(directedRead, roReq.DirectedReadOptions, cmpopts.IgnoreUnexported(spannerpb.DirectedReadOptions{}, spannerpb.DirectedReadOptions_IncludeReplicas{}, spannerpb.DirectedReadOptions_ReplicaSelection{})); diff != "" {
		t.Errorf("DirectedReadOptions mismatch on follow-up execution (-want +got):\n%s", diff)
	}
}
