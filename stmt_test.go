// Copyright 2024 Google LLC
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
	"database/sql/driver"
	"reflect"
	"strings"
	"testing"

	"cloud.google.com/go/spanner"
	"cloud.google.com/go/spanner/admin/database/apiv1/databasepb"
	"cloud.google.com/go/spanner/apiv1/spannerpb"
	"github.com/googleapis/go-sql-spanner/connectionstate"
	"github.com/googleapis/go-sql-spanner/parser"
	"google.golang.org/protobuf/types/known/structpb"
)

func TestPrepareSpannerStmt(t *testing.T) {
	state := createInitialConnectionState(connectionstate.TypeNonTransactional, nil)
	p, err := parser.NewStatementParser(databasepb.DatabaseDialect_GOOGLE_STANDARD_SQL, 1000)
	if err != nil {
		t.Fatal(err)
	}

	t.Run("QueryParameterNameMatchesExactly", func(t *testing.T) {
		stmt, err := prepareSpannerStmt(state, p, "SELECT * FROM Singers WHERE SingerId = @id", []driver.NamedValue{
			{Name: "id", Value: int64(1)},
		})
		if err != nil {
			t.Fatalf("Unexpected error for matching parameter name: %v", err)
		}
		if got, want := stmt.Params["id"], int64(1); got != want {
			t.Errorf("Params[\"id\"] = %v, want %v", got, want)
		}
	})

	t.Run("QueryParameterNameMismatch", func(t *testing.T) {
		_, err := prepareSpannerStmt(state, p, "SELECT * FROM Singers WHERE SingerId = @singer_id", []driver.NamedValue{
			{Name: "id", Value: int64(1)},
		})
		if err == nil {
			t.Fatal("Expected error for mismatched parameter name, got nil")
		}
		if !strings.Contains(err.Error(), "missing value for query parameter @singer_id") {
			t.Fatalf("Expected 'missing value for query parameter @singer_id' error, got %v", err)
		}
	})

	t.Run("PostgreSQLBooleanStringConversion", func(t *testing.T) {
		pgParser, err := parser.NewStatementParser(databasepb.DatabaseDialect_POSTGRESQL, 1000)
		if err != nil {
			t.Fatal(err)
		}
		tests := []struct {
			name  string
			input string
			want  bool
		}{
			// True inputs
			{"p1_t", "t", true},
			{"p1_tr", "tr", true},
			{"p1_tru", "tru", true},
			{"p1_true", "true", true},
			{"p1_y", "y", true},
			{"p1_ye", "ye", true},
			{"p1_yes", "yes", true},
			{"p1_on", "on", true},
			{"p1_1", "1", true},
			// True case-insensitivity & whitespace
			{"p1_True_mixed", "TrUe", true},
			{"p1_yes_whitespace", "  yes  ", true},
			{"p1_ON_caps", "ON", true},

			// False inputs
			{"p2_f", "f", false},
			{"p2_fa", "fa", false},
			{"p2_fal", "fal", false},
			{"p2_fals", "fals", false},
			{"p2_false", "false", false},
			{"p2_n", "n", false},
			{"p2_no", "no", false},
			{"p2_of", "of", false},
			{"p2_off", "off", false},
			{"p2_0", "0", false},
			// False case-insensitivity & whitespace
			{"p2_False_mixed", "FaLsE", false},
			{"p2_no_whitespace", "  No  ", false},
			{"p2_OFF_caps", "OFF", false},
		}
		for _, tc := range tests {
			t.Run(tc.name, func(t *testing.T) {
				gcv := spanner.GenericColumnValue{
					Type:  &spannerpb.Type{Code: spannerpb.TypeCode_BOOL},
					Value: structpb.NewStringValue(tc.input),
				}
				stmt, err := prepareSpannerStmt(state, pgParser, "SELECT * FROM users WHERE active = $1", []driver.NamedValue{
					{Name: "p1", Value: gcv},
				})
				if err != nil {
					t.Fatalf("Unexpected error: %v", err)
				}
				res, ok := stmt.Params["p1"].(spanner.GenericColumnValue)
				if !ok {
					t.Fatalf("Expected spanner.GenericColumnValue, got %T", stmt.Params["p1"])
				}
				bv, ok := res.Value.Kind.(*structpb.Value_BoolValue)
				if !ok {
					t.Fatalf("Expected BoolValue kind, got %T", res.Value.Kind)
				}
				if g, w := bv.BoolValue, tc.want; g != w {
					t.Errorf("bool value mismatch\nGot:  %v\nWant: %v", g, w)
				}
			})
		}
	})

	t.Run("PostgreSQLBooleanStringNoConversionForInvalid", func(t *testing.T) {
		pgParser, err := parser.NewStatementParser(databasepb.DatabaseDialect_POSTGRESQL, 1000)
		if err != nil {
			t.Fatal(err)
		}
		invalidInputs := []string{"o", "other", "on_invalid", "true_invalid", "f_invalid", "no_invalid"}
		for _, input := range invalidInputs {
			t.Run(input, func(t *testing.T) {
				gcv := spanner.GenericColumnValue{
					Type:  &spannerpb.Type{Code: spannerpb.TypeCode_BOOL},
					Value: structpb.NewStringValue(input),
				}
				stmt, err := prepareSpannerStmt(state, pgParser, "SELECT * FROM users WHERE active = $1", []driver.NamedValue{
					{Name: "p1", Value: gcv},
				})
				if err != nil {
					t.Fatalf("Unexpected error: %v", err)
				}
				res, ok := stmt.Params["p1"].(spanner.GenericColumnValue)
				if !ok {
					t.Fatalf("Expected spanner.GenericColumnValue, got %T", stmt.Params["p1"])
				}
				sv, ok := res.Value.Kind.(*structpb.Value_StringValue)
				if !ok {
					t.Fatalf("Expected StringValue kind, got %T", res.Value.Kind)
				}
				if g, w := sv.StringValue, input; g != w {
					t.Errorf("string value mismatch\nGot:  %v\nWant: %v", g, w)
				}
			})
		}
	})

	t.Run("GoogleSQLBooleanStringNoConversion", func(t *testing.T) {
		gcv := spanner.GenericColumnValue{
			Type:  &spannerpb.Type{Code: spannerpb.TypeCode_BOOL},
			Value: structpb.NewStringValue("t"),
		}
		stmt, err := prepareSpannerStmt(state, p, "SELECT * FROM users WHERE active = @p1", []driver.NamedValue{
			{Name: "p1", Value: gcv},
		})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		res, ok := stmt.Params["p1"].(spanner.GenericColumnValue)
		if !ok {
			t.Fatalf("Expected spanner.GenericColumnValue, got %T", stmt.Params["p1"])
		}
		sv, ok := res.Value.Kind.(*structpb.Value_StringValue)
		if !ok {
			t.Fatalf("Expected StringValue kind, got %T", res.Value.Kind)
		}
		if g, w := sv.StringValue, "t"; g != w {
			t.Errorf("string value mismatch\nGot:  %v\nWant: %v", g, w)
		}
	})
}

func TestConvertParam(t *testing.T) {
	check := func(in, want driver.Value) {
		t.Helper()
		got := convertParam(in, false)
		if !reflect.DeepEqual(got, want) {
			t.Errorf("in:%#v want:%#v got:%#v", in, want, got)
		}
	}

	check(uint(197), int64(197))
	check(pointerTo(uint(197)), pointerTo(int64(197)))
	check((*uint)(nil), (*int64)(nil))

	check([]uint{197}, []int64{197})
	check(pointerTo([]uint{197}), []int64{197})
	check([]*uint{pointerTo(uint(197))}, []*int64{pointerTo(int64(197))})
	check(([]*uint)(nil), ([]*int64)(nil))
	check((*[]uint)(nil), ([]int64)(nil))

	check([]int{197}, []int64{197})
	check([]*int{pointerTo(int(197))}, []*int64{pointerTo(int64(197))})
	check(pointerTo([]int{197}), []int64{197})
	check(([]*int)(nil), ([]*int64)(nil))
	check((*[]int)(nil), ([]int64)(nil))

	check(uint64(197), int64(197))
	check(pointerTo(uint64(197)), pointerTo(int64(197)))
	check((*uint64)(nil), (*int64)(nil))

	check([]uint64{197}, []int64{197})
	check(pointerTo([]uint64{197}), []int64{197})
	check([]*uint64{pointerTo(uint64(197))}, []*int64{pointerTo(int64(197))})
	check(([]*uint64)(nil), ([]*int64)(nil))
	check((*[]uint64)(nil), ([]int64)(nil))
}

func pointerTo[T any](v T) *T { return &v }
