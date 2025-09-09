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
		wantErr bool
	}
	tests := []test{
		{
			input: "set my_property = 'foo'",
			want: ParsedSetStatement{
				query:      "set my_property = 'foo'",
				Identifier: Identifier{Parts: []string{"my_property"}},
				Literal:    Literal{Value: "foo"},
			},
		},
		{
			input: "set local my_property = 'foo'",
			want: ParsedSetStatement{
				query:      "set local my_property = 'foo'",
				Identifier: Identifier{Parts: []string{"my_property"}},
				Literal:    Literal{Value: "foo"},
				IsLocal:    true,
			},
		},
		{
			input: "set my_property = 1",
			want: ParsedSetStatement{
				query:      "set my_property = 1",
				Identifier: Identifier{Parts: []string{"my_property"}},
				Literal:    Literal{Value: "1"},
			},
		},
		{
			input: "set my_property = true",
			want: ParsedSetStatement{
				query:      "set my_property = true",
				Identifier: Identifier{Parts: []string{"my_property"}},
				Literal:    Literal{Value: "true"},
			},
		},
		{
			input: "set \n -- comment \n my_property /* yet more comments */ = \ntrue/*comment*/  ",
			want: ParsedSetStatement{
				query:      "set \n -- comment \n my_property /* yet more comments */ = \ntrue/*comment*/  ",
				Identifier: Identifier{Parts: []string{"my_property"}},
				Literal:    Literal{Value: "true"},
			},
		},
		{
			input: "set \n -- comment \n a.b /* yet more comments */ =\n/*comment*/'value'/*comment*/  ",
			want: ParsedSetStatement{
				query:      "set \n -- comment \n a.b /* yet more comments */ =\n/*comment*/'value'/*comment*/  ",
				Identifier: Identifier{Parts: []string{"a", "b"}},
				Literal:    Literal{Value: "value"},
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
	parser, err := NewStatementParser(databasepb.DatabaseDialect_GOOGLE_STANDARD_SQL, 1000)
	if err != nil {
		t.Fatal(err)
	}
	keyword := "SET"
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
					t.Errorf("parseStatement(%q) = %v, want %v", test.input, *showStmt, test.want)
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
					t.Errorf("parseStatement(%q) = %v, want %v", test.input, *showStmt, test.want)
				}
			}
		})
	}
}
