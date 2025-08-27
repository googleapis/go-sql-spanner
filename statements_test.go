package spannerdriver

import (
	"reflect"
	"testing"

	"cloud.google.com/go/spanner/admin/database/apiv1/databasepb"
)

func TestParseShowStatement(t *testing.T) {
	t.Parallel()

	parser, err := newStatementParser(databasepb.DatabaseDialect_GOOGLE_STANDARD_SQL, 1000)
	if err != nil {
		t.Fatal(err)
	}
	type test struct {
		input   string
		want    parsedShowStatement
		wantErr bool
	}
	tests := []test{
		{
			input:   "show my_property",
			wantErr: parser.dialect == databasepb.DatabaseDialect_GOOGLE_STANDARD_SQL,
			want: parsedShowStatement{
				query:      "show my_property",
				identifier: identifier{parts: []string{"my_property"}},
			},
		},
		{
			input: "show variable my_property",
			want: parsedShowStatement{
				query:      "show variable my_property",
				identifier: identifier{parts: []string{"my_property"}},
			},
		},
		{
			input: "SHOW variable my_extension.my_property",
			want: parsedShowStatement{
				query:      "SHOW variable my_extension.my_property",
				identifier: identifier{parts: []string{"my_extension", "my_property"}},
			},
		},
		{
			input: "show variable my_extension. my_property",
			want: parsedShowStatement{
				query:      "show variable my_extension. my_property",
				identifier: identifier{parts: []string{"my_extension", "my_property"}},
			},
		},
		{
			input: "show     variable            my_extension   .   my_property",
			want: parsedShowStatement{
				query:      "show     variable            my_extension   .   my_property",
				identifier: identifier{parts: []string{"my_extension", "my_property"}},
			},
		},
		{
			input: "show variable   /*comment*/\n my_extension  .  my_property   \n",
			want: parsedShowStatement{
				query:      "show variable   /*comment*/\n my_extension  .  my_property   \n",
				identifier: identifier{parts: []string{"my_extension", "my_property"}},
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
				showStmt, ok := stmt.(*parsedShowStatement)
				if !ok {
					t.Fatalf("parseStatement(%q) should have returned a *parsedShowStatement", test.input)
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
		want    parsedSetStatement
		wantErr bool
	}
	tests := []test{
		{
			input: "set my_property = 'foo'",
			want: parsedSetStatement{
				query:      "set my_property = 'foo'",
				identifier: identifier{parts: []string{"my_property"}},
				literal:    literal{value: "foo"},
			},
		},
		{
			input: "set local my_property = 'foo'",
			want: parsedSetStatement{
				query:      "set local my_property = 'foo'",
				identifier: identifier{parts: []string{"my_property"}},
				literal:    literal{value: "foo"},
				isLocal:    true,
			},
		},
		{
			input: "set my_property = 1",
			want: parsedSetStatement{
				query:      "set my_property = 1",
				identifier: identifier{parts: []string{"my_property"}},
				literal:    literal{value: "1"},
			},
		},
		{
			input: "set my_property = true",
			want: parsedSetStatement{
				query:      "set my_property = true",
				identifier: identifier{parts: []string{"my_property"}},
				literal:    literal{value: "true"},
			},
		},
		{
			input: "set \n -- comment \n my_property /* yet more comments */ = \ntrue/*comment*/  ",
			want: parsedSetStatement{
				query:      "set \n -- comment \n my_property /* yet more comments */ = \ntrue/*comment*/  ",
				identifier: identifier{parts: []string{"my_property"}},
				literal:    literal{value: "true"},
			},
		},
		{
			input: "set \n -- comment \n a.b /* yet more comments */ =\n/*comment*/'value'/*comment*/  ",
			want: parsedSetStatement{
				query:      "set \n -- comment \n a.b /* yet more comments */ =\n/*comment*/'value'/*comment*/  ",
				identifier: identifier{parts: []string{"a", "b"}},
				literal:    literal{value: "value"},
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
	parser, err := newStatementParser(databasepb.DatabaseDialect_GOOGLE_STANDARD_SQL, 1000)
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
				showStmt, ok := stmt.(*parsedSetStatement)
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
