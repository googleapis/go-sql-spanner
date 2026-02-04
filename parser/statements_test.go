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

package parser

import (
	"fmt"
	"reflect"
	"strings"
	"testing"

	"cloud.google.com/go/spanner/admin/database/apiv1/databasepb"
)

func TestParseShowStatement(t *testing.T) {
	t.Parallel()

	parser, err := NewStatementParser(databasepb.DatabaseDialect_GOOGLE_STANDARD_SQL, 1000)
	if err != nil {
		t.Fatal(err)
	}
	type test struct {
		input   string
		want    ParsedShowStatement
		wantErr bool
	}
	tests := []test{
		{
			input:   "show my_property",
			wantErr: parser.Dialect == databasepb.DatabaseDialect_GOOGLE_STANDARD_SQL,
			want: ParsedShowStatement{
				query:      "show my_property",
				Identifier: Identifier{Parts: []string{"my_property"}},
			},
		},
		{
			input: "show variable my_property",
			want: ParsedShowStatement{
				query:      "show variable my_property",
				Identifier: Identifier{Parts: []string{"my_property"}},
			},
		},
		{
			input: "SHOW variable my_extension.my_property",
			want: ParsedShowStatement{
				query:      "SHOW variable my_extension.my_property",
				Identifier: Identifier{Parts: []string{"my_extension", "my_property"}},
			},
		},
		{
			input: "show variable my_extension. my_property",
			want: ParsedShowStatement{
				query:      "show variable my_extension. my_property",
				Identifier: Identifier{Parts: []string{"my_extension", "my_property"}},
			},
		},
		{
			input: "show     variable            my_extension   .   my_property",
			want: ParsedShowStatement{
				query:      "show     variable            my_extension   .   my_property",
				Identifier: Identifier{Parts: []string{"my_extension", "my_property"}},
			},
		},
		{
			input: "show variable   /*comment*/\n my_extension  .  my_property   \n",
			want: ParsedShowStatement{
				query:      "show variable   /*comment*/\n my_extension  .  my_property   \n",
				Identifier: Identifier{Parts: []string{"my_extension", "my_property"}},
			},
		},
		{
			// Extra tokens after the statement are not allowed.
			input:   "show variable my_property foo",
			wantErr: true,
		},
		{
			// Extra tokens after the statement are not allowed.
			input:   "show variable my_property/",
			wantErr: true,
		},
		{
			input:   "show vraible my_property",
			wantErr: true,
		},
		{
			// Garbled comment.
			input:   "show variable /*should have been a comment* my_property",
			wantErr: true,
		},
	}
	keyword := "SHOW"
	for _, test := range tests {
		t.Run(test.input, func(t *testing.T) {
			stmt, err := parseStatement(parser, keyword, test.input)
			if test.wantErr {
				if err == nil {
					t.Fatalf("parseStatement(%q) should have failed", test.input)
				}
			} else {
				if err != nil {
					t.Fatal(err)
				}
				showStmt, ok := stmt.(*ParsedShowStatement)
				if !ok {
					t.Fatalf("parseStatement(%q) should have returned a *ParsedShowStatement", test.input)
				}
				if !reflect.DeepEqual(*showStmt, test.want) {
					t.Errorf("parseStatement(%q) = %v, want %v", test.input, *showStmt, test.want)
				}
			}
		})
	}
}

func TestParseSetStatement(t *testing.T) {
	t.Parallel()

	type test struct {
		input   string
		want    ParsedSetStatement
		onlyPg  bool
		wantErr bool
	}
	tests := []test{
		{
			input: "set my_property = 'foo'",
			want: ParsedSetStatement{
				query:       "set my_property = 'foo'",
				Identifiers: []Identifier{{Parts: []string{"my_property"}}},
				Literals:    []Literal{{Value: "foo"}},
			},
		},
		{
			input: "set local my_property = 'foo'",
			want: ParsedSetStatement{
				query:       "set local my_property = 'foo'",
				Identifiers: []Identifier{{Parts: []string{"my_property"}}},
				Literals:    []Literal{{Value: "foo"}},
				IsLocal:     true,
			},
		},
		{
			input: "set my_property = 1",
			want: ParsedSetStatement{
				query:       "set my_property = 1",
				Identifiers: []Identifier{{Parts: []string{"my_property"}}},
				Literals:    []Literal{{Value: "1"}},
			},
		},
		{
			input: "set my_property = true",
			want: ParsedSetStatement{
				query:       "set my_property = true",
				Identifiers: []Identifier{{Parts: []string{"my_property"}}},
				Literals:    []Literal{{Value: "true"}},
			},
		},
		{
			input: "set \n -- comment \n my_property /* yet more comments */ = \ntrue/*comment*/  ",
			want: ParsedSetStatement{
				query:       "set \n -- comment \n my_property /* yet more comments */ = \ntrue/*comment*/  ",
				Identifiers: []Identifier{{Parts: []string{"my_property"}}},
				Literals:    []Literal{{Value: "true"}},
			},
		},
		{
			input: "set \n -- comment \n a.b /* yet more comments */ =\n/*comment*/'value'/*comment*/  ",
			want: ParsedSetStatement{
				query:       "set \n -- comment \n a.b /* yet more comments */ =\n/*comment*/'value'/*comment*/  ",
				Identifiers: []Identifier{{Parts: []string{"a", "b"}}},
				Literals:    []Literal{{Value: "value"}},
			},
		},
		{
			input: "set transaction isolation level serializable",
			want: ParsedSetStatement{
				query:         "set transaction isolation level serializable",
				Identifiers:   []Identifier{{Parts: []string{"isolation_level"}}},
				Literals:      []Literal{{Value: "serializable"}},
				IsLocal:       true,
				IsTransaction: true,
			},
		},
		{
			input:   "set transaction isolation serializable",
			wantErr: true,
		},
		{
			input:   "set transaction isolation level serializable foo",
			wantErr: true,
		},
		{
			input:   "set isolation level serializable",
			wantErr: true,
		},
		{
			input:   "set transaction isolation level serialisable",
			wantErr: true,
		},
		{
			input: "set transaction isolation level repeatable read",
			want: ParsedSetStatement{
				query:         "set transaction isolation level repeatable read",
				Identifiers:   []Identifier{{Parts: []string{"isolation_level"}}},
				Literals:      []Literal{{Value: "repeatable_read"}},
				IsLocal:       true,
				IsTransaction: true,
			},
		},
		{
			input:   "set transaction isolation level repeatable",
			wantErr: true,
		},
		{
			input:   "set transaction isolation level read",
			wantErr: true,
		},
		{
			input:   "set transaction isolation level repeatable read serializable",
			wantErr: true,
		},
		{
			input:   "set transaction isolation level serializable repeatable read",
			wantErr: true,
		},
		{
			input: "set transaction read write",
			want: ParsedSetStatement{
				query:         "set transaction read write",
				Identifiers:   []Identifier{{Parts: []string{"transaction_read_only"}}},
				Literals:      []Literal{{Value: "false"}},
				IsLocal:       true,
				IsTransaction: true,
			},
		},
		{
			input: "set transaction read write isolation level serializable",
			want: ParsedSetStatement{
				query: "set transaction read write isolation level serializable",
				Identifiers: []Identifier{
					{Parts: []string{"transaction_read_only"}},
					{Parts: []string{"isolation_level"}},
				},
				Literals: []Literal{
					{Value: "false"},
					{Value: "serializable"},
				},
				IsLocal:       true,
				IsTransaction: true,
			},
		},
		{
			input: "set transaction read only, isolation level repeatable read",
			want: ParsedSetStatement{
				query: "set transaction read only, isolation level repeatable read",
				Identifiers: []Identifier{
					{Parts: []string{"transaction_read_only"}},
					{Parts: []string{"isolation_level"}},
				},
				Literals: []Literal{
					{Value: "true"},
					{Value: "repeatable_read"},
				},
				IsLocal:       true,
				IsTransaction: true,
			},
		},
		{
			onlyPg: true,
			input:  "set transaction read only, isolation level repeatable read not deferrable",
			want: ParsedSetStatement{
				query: "set transaction read only, isolation level repeatable read not deferrable",
				Identifiers: []Identifier{
					{Parts: []string{"transaction_read_only"}},
					{Parts: []string{"isolation_level"}},
					{Parts: []string{"transaction_deferrable"}},
				},
				Literals: []Literal{
					{Value: "true"},
					{Value: "repeatable_read"},
					{Value: "false"},
				},
				IsLocal:       true,
				IsTransaction: true,
			},
		},
		{
			input: "set transaction read only",
			want: ParsedSetStatement{
				query:         "set transaction read only",
				Identifiers:   []Identifier{{Parts: []string{"transaction_read_only"}}},
				Literals:      []Literal{{Value: "true"}},
				IsLocal:       true,
				IsTransaction: true,
			},
		},
		{
			input:   "set transaction read",
			wantErr: true,
		},
		{
			input:   "set transaction write",
			wantErr: true,
		},
		{
			input:   "set transaction read only write",
			wantErr: true,
		},
		{
			input:   "set transaction write only",
			wantErr: true,
		},
		{
			onlyPg: true,
			input:  "set transaction deferrable",
			want: ParsedSetStatement{
				query:         "set transaction deferrable",
				Identifiers:   []Identifier{{Parts: []string{"transaction_deferrable"}}},
				Literals:      []Literal{{Value: "true"}},
				IsLocal:       true,
				IsTransaction: true,
			},
		},
		{
			onlyPg: true,
			input:  "set transaction not deferrable",
			want: ParsedSetStatement{
				query:         "set transaction not deferrable",
				Identifiers:   []Identifier{{Parts: []string{"transaction_deferrable"}}},
				Literals:      []Literal{{Value: "false"}},
				IsLocal:       true,
				IsTransaction: true,
			},
		},
		{
			input:   "set my_property =",
			wantErr: true,
		},
		{
			input:   "set my_property 'my-value'",
			wantErr: true,
		},
		{
			input:   "set my_property = my-value'",
			wantErr: true,
		},
		{
			input:   "set my_property = 'my-value",
			wantErr: true,
		},
		{
			input:   "set my_property = 'my-value' foo",
			wantErr: true,
		},
		{
			input:   "set my_property local = 'my-value'",
			wantErr: true,
		},
	}
	for _, dialect := range []databasepb.DatabaseDialect{databasepb.DatabaseDialect_GOOGLE_STANDARD_SQL, databasepb.DatabaseDialect_POSTGRESQL} {
		parser, err := NewStatementParser(dialect, 1000)
		if err != nil {
			t.Fatal(err)
		}
		keyword := "SET"
		for _, test := range tests {
			t.Run(fmt.Sprintf("%s %s", dialect, test.input), func(t *testing.T) {
				stmt, err := parseStatement(parser, keyword, test.input)
				if test.wantErr || (test.onlyPg && dialect == databasepb.DatabaseDialect_GOOGLE_STANDARD_SQL) {
					if err == nil {
						t.Fatalf("parseStatement(%q) should have failed", test.input)
					}
				} else {
					if err != nil {
						t.Fatal(err)
					}
					showStmt, ok := stmt.(*ParsedSetStatement)
					if !ok {
						t.Fatalf("parseStatement(%q) should have returned a *parsedSetStatement", test.input)
					}
					if !reflect.DeepEqual(*showStmt, test.want) {
						t.Errorf("parseStatement(%q) = %v, want %v", test.input, *showStmt, test.want)
					}
				}
			})
		}
	}
}

func TestParseBeginStatementGoogleSQL(t *testing.T) {
	t.Parallel()

	type test struct {
		input   string
		want    ParsedBeginStatement
		wantErr bool
	}
	tests := []test{
		{
			input: "begin",
			want: ParsedBeginStatement{
				query: "begin",
			},
		},
		{
			input: "begin transaction",
			want: ParsedBeginStatement{
				query: "begin transaction",
			},
		},
		{
			input:   "start",
			wantErr: true,
		},
		{
			input:   "begin work",
			wantErr: true,
		},
		{
			input:   "begin transaction foo",
			wantErr: true,
		},
		{
			input: "begin read only",
			want: ParsedBeginStatement{
				query:       "begin read only",
				Identifiers: []Identifier{{Parts: []string{"transaction_read_only"}}},
				Literals:    []Literal{{Value: "true"}},
			},
		},
		{
			input: "begin read write",
			want: ParsedBeginStatement{
				query:       "begin read write",
				Identifiers: []Identifier{{Parts: []string{"transaction_read_only"}}},
				Literals:    []Literal{{Value: "false"}},
			},
		},
		{
			input: "begin transaction isolation level serializable",
			want: ParsedBeginStatement{
				query:       "begin transaction isolation level serializable",
				Identifiers: []Identifier{{Parts: []string{"isolation_level"}}},
				Literals:    []Literal{{Value: "serializable"}},
			},
		},
		{
			input: "begin transaction isolation level repeatable read, read write",
			want: ParsedBeginStatement{
				query:       "begin transaction isolation level repeatable read, read write",
				Identifiers: []Identifier{{Parts: []string{"isolation_level"}}, {Parts: []string{"transaction_read_only"}}},
				Literals:    []Literal{{Value: "repeatable_read"}, {Value: "false"}},
			},
		},
	}
	parser, err := NewStatementParser(databasepb.DatabaseDialect_GOOGLE_STANDARD_SQL, 1000)
	if err != nil {
		t.Fatal(err)
	}
	for _, test := range tests {
		t.Run(test.input, func(t *testing.T) {
			sp := &simpleParser{sql: []byte(test.input), statementParser: parser}
			keyword := strings.ToUpper(sp.readKeyword())
			stmt, err := parseStatement(parser, keyword, test.input)
			if test.wantErr {
				if err == nil {
					t.Fatalf("parseStatement(%q) should have failed", test.input)
				}
			} else {
				if err != nil {
					t.Fatal(err)
				}
				showStmt, ok := stmt.(*ParsedBeginStatement)
				if !ok {
					t.Fatalf("parseStatement(%q) should have returned a *parsedBeginStatement", test.input)
				}
				if !reflect.DeepEqual(*showStmt, test.want) {
					t.Errorf("parseStatement(%q) mismatch\n Got: %v\nWant: %v", test.input, *showStmt, test.want)
				}
			}
		})
	}
}

func TestParseBeginStatementPostgreSQL(t *testing.T) {
	t.Parallel()

	type test struct {
		input   string
		want    ParsedBeginStatement
		wantErr bool
	}
	tests := []test{
		{
			input: "begin",
			want: ParsedBeginStatement{
				query: "begin",
			},
		},
		{
			input: "begin transaction",
			want: ParsedBeginStatement{
				query: "begin transaction",
			},
		},
		{
			input: "start",
			want: ParsedBeginStatement{
				query: "start",
			},
		},
		{
			input: "start transaction",
			want: ParsedBeginStatement{
				query: "start transaction",
			},
		},
		{
			input: "begin work",
			want: ParsedBeginStatement{
				query: "begin work",
			},
		},
		{
			input: "start work",
			want: ParsedBeginStatement{
				query: "start work",
			},
		},
		{
			input: "start work read only",
			want: ParsedBeginStatement{
				query:       "start work read only",
				Identifiers: []Identifier{{Parts: []string{"transaction_read_only"}}},
				Literals:    []Literal{{Value: "true"}},
			},
		},
		{
			input: "begin read write",
			want: ParsedBeginStatement{
				query:       "begin read write",
				Identifiers: []Identifier{{Parts: []string{"transaction_read_only"}}},
				Literals:    []Literal{{Value: "false"}},
			},
		},
		{
			input: "begin read write, isolation level repeatable read",
			want: ParsedBeginStatement{
				query:       "begin read write, isolation level repeatable read",
				Identifiers: []Identifier{{Parts: []string{"transaction_read_only"}}, {Parts: []string{"isolation_level"}}},
				Literals:    []Literal{{Value: "false"}, {Value: "repeatable_read"}},
			},
		},
		{
			// Note that it is possible to set multiple conflicting transaction options in one statement.
			// This statement for example sets the transaction to both read/write and read-only.
			// The last option will take precedence, as these options are essentially the same as executing the
			// following statements sequentially after the BEGIN TRANSACTION statement:
			// SET TRANSACTION READ WRITE
			// SET TRANSACTION ISOLATION LEVEL REPEATABLE READ
			// SET TRANSACTION READ ONLY
			// SET TRANSACTION DEFERRABLE
			input: "begin transaction \nread write,\nisolation level repeatable read\nread only\ndeferrable",
			want: ParsedBeginStatement{
				query: "begin transaction \nread write,\nisolation level repeatable read\nread only\ndeferrable",
				Identifiers: []Identifier{
					{Parts: []string{"transaction_read_only"}},
					{Parts: []string{"isolation_level"}},
					{Parts: []string{"transaction_read_only"}},
					{Parts: []string{"transaction_deferrable"}},
				},
				Literals: []Literal{
					{Value: "false"},
					{Value: "repeatable_read"},
					{Value: "true"},
					{Value: "true"},
				},
			},
		},
		{
			input:   "start foo",
			wantErr: true,
		},
		{
			input:   "start work transaction",
			wantErr: true,
		},
		{
			input:   "begin transaction work",
			wantErr: true,
		},
	}
	parser, err := NewStatementParser(databasepb.DatabaseDialect_POSTGRESQL, 1000)
	if err != nil {
		t.Fatal(err)
	}
	for _, test := range tests {
		t.Run(test.input, func(t *testing.T) {
			sp := &simpleParser{sql: []byte(test.input), statementParser: parser}
			keyword := strings.ToUpper(sp.readKeyword())
			stmt, err := parseStatement(parser, keyword, test.input)
			if test.wantErr {
				if err == nil {
					t.Fatalf("parseStatement(%q) should have failed", test.input)
				}
			} else {
				if err != nil {
					t.Fatal(err)
				}
				showStmt, ok := stmt.(*ParsedBeginStatement)
				if !ok {
					t.Fatalf("parseStatement(%q) should have returned a *parsedBeginStatement", test.input)
				}
				if !reflect.DeepEqual(*showStmt, test.want) {
					t.Errorf("parseStatement(%q) mismatch\n Got: %v\nWant: %v", test.input, *showStmt, test.want)
				}
			}
		})
	}
}

func TestParseRunPartitionedQuery(t *testing.T) {
	t.Parallel()

	type test struct {
		input   string
		want    ParsedRunPartitionedQueryStatement
		wantErr bool
	}
	tests := []test{
		{
			input: "run partitioned query select * from my_table",
			want: ParsedRunPartitionedQueryStatement{
				Statement: " select * from my_table",
				query:     "run partitioned query select * from my_table",
			},
		},
		{
			input: "run partitioned query\nselect * from my_table",
			want: ParsedRunPartitionedQueryStatement{
				Statement: "\nselect * from my_table",
				query:     "run partitioned query\nselect * from my_table",
			},
		},
		{
			input: "run partitioned query\n--comment\nselect * from my_table",
			want: ParsedRunPartitionedQueryStatement{
				Statement: "\n--comment\nselect * from my_table",
				query:     "run partitioned query\n--comment\nselect * from my_table",
			},
		},
		{
			input: "run --comment\n partitioned /* comment */ query select * from my_table",
			want: ParsedRunPartitionedQueryStatement{
				Statement: " select * from my_table",
				query:     "run --comment\n partitioned /* comment */ query select * from my_table",
			},
		},
		{
			input:   "run partitioned query",
			wantErr: true,
		},
		{
			input:   "run partitioned query /* comment */",
			wantErr: true,
		},
		{
			input:   "run partitioned query -- comment\n",
			wantErr: true,
		},
		{
			input:   "run partitioned select * from my_table",
			wantErr: true,
		},
	}
	parser, err := NewStatementParser(databasepb.DatabaseDialect_GOOGLE_STANDARD_SQL, 1000)
	if err != nil {
		t.Fatal(err)
	}
	for _, test := range tests {
		t.Run(test.input, func(t *testing.T) {
			sp := &simpleParser{sql: []byte(test.input), statementParser: parser}
			keyword := strings.ToUpper(sp.readKeyword())
			stmt, err := parseStatement(parser, keyword, test.input)
			if test.wantErr {
				if err == nil {
					t.Fatalf("parseStatement(%q) should have failed", test.input)
				}
			} else {
				if err != nil {
					t.Fatal(err)
				}
				runStmt, ok := stmt.(*ParsedRunPartitionedQueryStatement)
				if !ok {
					t.Fatalf("parseStatement(%q) should have returned a *parsedRunPartitionedQueryStatement", test.input)
				}
				if !reflect.DeepEqual(*runStmt, test.want) {
					t.Errorf("parseStatement(%q) mismatch\n Got: %v\nWant: %v", test.input, *runStmt, test.want)
				}
			}
		})
	}
}
