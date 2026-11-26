// Copyright 2026 Google LLC
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
	"fmt"
	"reflect"
	"testing"

	"cloud.google.com/go/spanner"
	"cloud.google.com/go/spanner/admin/database/apiv1/databasepb"
	sppb "cloud.google.com/go/spanner/apiv1/spannerpb"
	"github.com/googleapis/go-sql-spanner/testutil"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/structpb"
)

func TestQueryMultipleSkipEmptyFragments(t *testing.T) {
	tests := []struct {
		name       string
		sql        string
		statements []string
		dialect    databasepb.DatabaseDialect
	}{
		{name: "ordinary", sql: "SELECT 1;SELECT 2", statements: []string{"SELECT 1", "SELECT 2"}},
		{name: "empty middle", sql: "SELECT 1;;SELECT 2", statements: []string{"SELECT 1", "SELECT 2"}},
		{name: "single statement with empty fragments", sql: ";SELECT 1;;", statements: []string{"SELECT 1"}},
		{name: "comments between delimiters", sql: "SELECT 1;-- line ; comment\n/* block ; comment */;SELECT 2", statements: []string{"SELECT 1", "SELECT 2"}},
		{name: "leading and trailing empty fragments", sql: ";;SELECT 1; \t ;SELECT 2;;", statements: []string{"SELECT 1", "SELECT 2"}},
		{name: "comments attached to statements", sql: "SELECT /* keep ; */ 1;/* attached ; */SELECT 2", statements: []string{"SELECT /* keep ; */ 1", "/* attached ; */SELECT 2"}},
		{name: "quoted semicolon", sql: "SELECT ';' AS literal;SELECT 2", statements: []string{"SELECT ';' AS literal", "SELECT 2"}},
		{name: "GoogleSQL hash comment", sql: "SELECT 1;# comment ;\n;SELECT 2", statements: []string{"SELECT 1", "SELECT 2"}, dialect: databasepb.DatabaseDialect_GOOGLE_STANDARD_SQL},
		{name: "PostgreSQL nested comment", sql: "SELECT 1;/* outer /* inner ; */ outer */;SELECT 2", statements: []string{"SELECT 1", "SELECT 2"}, dialect: databasepb.DatabaseDialect_POSTGRESQL},
	}
	for _, dialect := range []databasepb.DatabaseDialect{databasepb.DatabaseDialect_GOOGLE_STANDARD_SQL, databasepb.DatabaseDialect_POSTGRESQL} {
		for _, tc := range tests {
			if tc.dialect != databasepb.DatabaseDialect_DATABASE_DIALECT_UNSPECIFIED && tc.dialect != dialect {
				continue
			}
			t.Run(fmt.Sprintf("%s/%s", dialect, tc.name), func(t *testing.T) {
				db, server, teardown := setupTestDBConnectionWithDialect(t, dialect)
				defer teardown()
				for i, statement := range tc.statements {
					if err := server.TestSpanner.PutStatementResult(statement, testInt64Result(i+1)); err != nil {
						t.Fatal(err)
					}
				}
				rows, err := db.QueryContext(context.Background(), tc.sql)
				if err != nil {
					t.Fatal(err)
				}
				defer silentClose(rows)
				var values []int64
				for {
					for rows.Next() {
						var value int64
						if err := rows.Scan(&value); err != nil {
							t.Fatal(err)
						}
						values = append(values, value)
					}
					if err := rows.Err(); err != nil {
						t.Fatalf("rows error after values %v: %v", values, err)
					}
					if !rows.NextResultSet() {
						if err := rows.Err(); err != nil {
							t.Fatalf("next result set after values %v: %v", values, err)
						}
						break
					}
				}
				wantValues := make([]int64, len(tc.statements))
				for i := range wantValues {
					wantValues[i] = int64(i + 1)
				}
				if !reflect.DeepEqual(values, wantValues) {
					t.Fatalf("values = %v, want %v", values, wantValues)
				}
				requests := testutil.RequestsOfType(server.TestSpanner.DrainRequestsFromServer(), reflect.TypeOf(&sppb.ExecuteSqlRequest{}))
				var executed []string
				for _, request := range requests {
					executed = append(executed, request.(*sppb.ExecuteSqlRequest).GetSql())
				}
				if !reflect.DeepEqual(executed, tc.statements) {
					t.Fatalf("executed SQL = %q, want %q", executed, tc.statements)
				}
				t.Logf("results=%v; backend SQL=%q", values, executed)
			})
		}
	}
}

func TestQueryMultipleEmptyFragmentDoesNotHideInvalidSQL(t *testing.T) {
	db, server, teardown := setupTestDBConnection(t)
	defer teardown()
	if err := server.TestSpanner.PutStatementResult("SELECT 1", testInt64Result(1)); err != nil {
		t.Fatal(err)
	}
	if err := server.TestSpanner.PutStatementResult("NOT A STATEMENT", &testutil.StatementResult{
		Type: testutil.StatementResultError,
		Err:  status.Error(codes.InvalidArgument, "invalid SQL"),
	}); err != nil {
		t.Fatal(err)
	}
	if err := server.TestSpanner.PutStatementResult("SELECT 2", testInt64Result(2)); err != nil {
		t.Fatal(err)
	}
	rows, err := db.QueryContext(context.Background(), "SELECT 1;;NOT A STATEMENT;SELECT 2")
	if err != nil {
		t.Fatal(err)
	}
	defer silentClose(rows)
	if !rows.Next() {
		t.Fatalf("missing first row: %v", rows.Err())
	}
	var value int64
	if err := rows.Scan(&value); err != nil || value != 1 {
		t.Fatalf("first row = %d, error = %v", value, err)
	}
	if rows.NextResultSet() {
		t.Fatal("expected the invalid statement to stop execution")
	}
	if got := spanner.ErrCode(rows.Err()); got != codes.InvalidArgument {
		t.Fatalf("error code = %v, want InvalidArgument; error: %v", got, rows.Err())
	}
	requests := testutil.RequestsOfType(server.TestSpanner.DrainRequestsFromServer(), reflect.TypeOf(&sppb.ExecuteSqlRequest{}))
	var executed []string
	for _, request := range requests {
		executed = append(executed, request.(*sppb.ExecuteSqlRequest).GetSql())
	}
	if want := []string{"SELECT 1", "NOT A STATEMENT"}; !reflect.DeepEqual(executed, want) {
		t.Fatalf("executed SQL = %q, want %q", executed, want)
	}
	t.Logf("first result=%d; backend SQL=%q; error=%v", value, executed, rows.Err())
}

func testInt64Result(value int) *testutil.StatementResult {
	return &testutil.StatementResult{
		Type: testutil.StatementResultResultSet,
		ResultSet: &sppb.ResultSet{
			Metadata: &sppb.ResultSetMetadata{RowType: &sppb.StructType{Fields: []*sppb.StructType_Field{{
				Name: "value", Type: &sppb.Type{Code: sppb.TypeCode_INT64},
			}}}},
			Rows: []*structpb.ListValue{{Values: []*structpb.Value{structpb.NewStringValue(fmt.Sprint(value))}}},
		},
	}
}

func TestQueryEmptyInputKeepsExistingBehavior(t *testing.T) {
	for _, tc := range []struct {
		name, sql, backendSQL string
	}{
		{name: "empty", sql: "", backendSQL: ""},
		{name: "comment only", sql: "-- comment only", backendSQL: "-- comment only"},
		{name: "only delimiters", sql: ";;", backendSQL: ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db, server, teardown := setupTestDBConnection(t)
			defer teardown()
			if err := server.TestSpanner.PutStatementResult(tc.backendSQL, &testutil.StatementResult{
				Type: testutil.StatementResultError,
				Err:  status.Error(codes.InvalidArgument, "empty input"),
			}); err != nil {
				t.Fatal(err)
			}
			rows, err := db.QueryContext(context.Background(), tc.sql)
			if rows != nil {
				defer silentClose(rows)
				for rows.Next() {
				}
				if err == nil {
					err = rows.Err()
				}
			}
			if got := spanner.ErrCode(err); got != codes.InvalidArgument {
				t.Fatalf("error code = %v, want InvalidArgument; error: %v", got, err)
			}
			requests := testutil.RequestsOfType(server.TestSpanner.DrainRequestsFromServer(), reflect.TypeOf(&sppb.ExecuteSqlRequest{}))
			if len(requests) != 1 || requests[0].(*sppb.ExecuteSqlRequest).GetSql() != tc.backendSQL {
				t.Fatalf("unexpected requests for %q: %v", tc.sql, requests)
			}
			t.Logf("input=%q; backend SQL=%q; error=%v", tc.sql, tc.backendSQL, err)
		})
	}
}
