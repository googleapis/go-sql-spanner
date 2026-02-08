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

	// The database/sql driver uses ExecuteStreamingSql for all statements.
	server.TestSpanner.PutExecutionTime(testutil.MethodExecuteStreamingSql, testutil.SimulatedExecutionTime{MinimumExecutionTime: 50 * time.Millisecond})

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
			if g, w := err.Error(), test.wantErr; g != w {
				t.Fatalf("error mismatch\n Got: %v\nWant: %v", g, w)
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
				if g, w := err.Error(), wantErr; g != w {
					t.Fatalf("error mismatch\n Got: %v\nWant: %v", g, w)
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
