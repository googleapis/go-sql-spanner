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
	"testing"

	"cloud.google.com/go/spanner"
	"github.com/google/go-cmp/cmp"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestRemoveCommentsAndTrim(t *testing.T) {
	tests := []struct {
		input   string
		want    string
		wantErr bool
	}{
		{
			input: ``,
			want:  ``,
		},
		{
			input: `SELECT 1;`,
			want:  `SELECT 1`,
		},
		{
			input: `-- This is a single line comment
SELECT 1;`,
			want: `SELECT 1`,
		},
		{
			input: `# This is a single line comment
SELECT 1;`,
			want: `SELECT 1`,
		},
		{
			input: `/* This is a multi line comment on one line */
SELECT 1;`,
			want: `SELECT 1`,
		},
		{
			input: `/* This
is
a
multiline
comment
*/
SELECT 1;`,
			want: `SELECT 1`,
		},
		{
			input: `/* This
* is
* a
* multiline
* comment
*/
SELECT 1;`,
			want: `SELECT 1`,
		},
		{
			input: `/** This is a javadoc style comment on one line */
SELECT 1;`,
			want: `SELECT 1`,
		},
		{
			input: `/** This
is
a
javadoc
style
comment
on
multiple
lines
*/
SELECT 1;`,
			want: `SELECT 1`,
		},
		{
			input: `/** This
* is
* a
* javadoc
* style
* comment
* on
* multiple
* lines
*/
SELECT 1;`,
			want: `SELECT 1`,
		},
		{
			input: `-- First comment
SELECT--second comment
1`,
			want: `SELECT
1`,
		},
		{
			input: `# First comment
SELECT#second comment
1`,
			want: `SELECT
1`,
		},
		{
			input: `-- First comment
SELECT--second comment
1--third comment`,
			want: `SELECT
1`,
		},
		{
			input: `# First comment
SELECT#second comment
1#third comment`,
			want: `SELECT
1`,
		},
		{
			input: `/* First comment */
SELECT/* second comment */
1`,
			want: `SELECT
1`,
		},
		{
			input: `/* First comment */
SELECT/* second comment */
1/* third comment */`,
			want: `SELECT
1`,
		},
		{
			input: `SELECT "TEST -- This is not a comment"`,
			want:  `SELECT "TEST -- This is not a comment"`,
		},
		{
			input: `-- This is a comment
SELECT "TEST -- This is not a comment"`,
			want: `SELECT "TEST -- This is not a comment"`,
		},
		{
			input: `-- This is a comment
SELECT "TEST -- This is not a comment" -- This is a comment`,
			want: `SELECT "TEST -- This is not a comment"`,
		},
		{
			input: `SELECT "TEST # This is not a comment"`,
			want:  `SELECT "TEST # This is not a comment"`,
		},
		{
			input: `# This is a comment
SELECT "TEST # This is not a comment"`,
			want: `SELECT "TEST # This is not a comment"`,
		},
		{
			input: `# This is a comment
SELECT "TEST # This is not a comment" # This is a comment`,
			want: `SELECT "TEST # This is not a comment"`,
		},
		{
			input: `SELECT "TEST /* This is not a comment */"`,
			want:  `SELECT "TEST /* This is not a comment */"`,
		},
		{
			input: `/* This is a comment */
SELECT "TEST /* This is not a comment */"`,
			want: `SELECT "TEST /* This is not a comment */"`,
		},
		{
			input: `/* This is a comment */
SELECT "TEST /* This is not a comment */" /* This is a comment */`,
			want: `SELECT "TEST /* This is not a comment */"`,
		},
		{
			input: `SELECT 'TEST -- This is not a comment'`,
			want:  `SELECT 'TEST -- This is not a comment'`,
		},
		{
			input: `-- This is a comment
SELECT 'TEST -- This is not a comment'`,
			want: `SELECT 'TEST -- This is not a comment'`,
		},
		{
			input: `-- This is a comment
SELECT 'TEST -- This is not a comment' -- This is a comment`,
			want: `SELECT 'TEST -- This is not a comment'`,
		},
		{
			input: `SELECT 'TEST # This is not a comment'`,
			want:  `SELECT 'TEST # This is not a comment'`,
		},
		{
			input: `# This is a comment
SELECT 'TEST # This is not a comment'`,
			want: `SELECT 'TEST # This is not a comment'`,
		},
		{
			input: `# This is a comment
SELECT 'TEST # This is not a comment' # This is a comment`,
			want: `SELECT 'TEST # This is not a comment'`,
		},
		{
			input: `SELECT 'TEST /* This is not a comment */'`,
			want:  `SELECT 'TEST /* This is not a comment */'`,
		},
		{
			input: `/* This is a comment */
SELECT 'TEST /* This is not a comment */'`,
			want: `SELECT 'TEST /* This is not a comment */'`,
		},
		{
			input: `/* This is a comment */
SELECT 'TEST /* This is not a comment */' /* This is a comment */`,
			want: `SELECT 'TEST /* This is not a comment */'`,
		},
		{
			input: `SELECT '''TEST
-- This is not a comment
'''`,
			want: `SELECT '''TEST
-- This is not a comment
'''`,
		},
		{
			input: ` -- This is a comment
SELECT '''TEST
-- This is not a comment
''' -- This is a comment`,
			want: `SELECT '''TEST
-- This is not a comment
'''`,
		},
		{
			input: `SELECT '''TEST
# This is not a comment
'''`,
			want: `SELECT '''TEST
# This is not a comment
'''`,
		},
		{
			input: ` # This is a comment
SELECT '''TEST
# This is not a comment
''' # This is a comment`,
			want: `SELECT '''TEST
# This is not a comment
'''`,
		},
		{
			input: `SELECT '''TEST
/* This is not a comment */
'''`,
			want: `SELECT '''TEST
/* This is not a comment */
'''`,
		},
		{
			input: ` /* This is a comment */
SELECT '''TEST
/* This is not a comment */
''' /* This is a comment */`,
			want: `SELECT '''TEST
/* This is not a comment */
'''`,
		},
		{
			input: `SELECT """TEST
-- This is not a comment
"""`,
			want: `SELECT """TEST
-- This is not a comment
"""`,
		},
		{
			input: ` -- This is a comment
SELECT """TEST
-- This is not a comment
""" -- This is a comment`,
			want: `SELECT """TEST
-- This is not a comment
"""`,
		},
		{
			input: `SELECT """TEST
# This is not a comment
"""`,
			want: `SELECT """TEST
# This is not a comment
"""`,
		},
		{
			input: ` # This is a comment
SELECT """TEST
# This is not a comment
""" # This is a comment`,
			want: `SELECT """TEST
# This is not a comment
"""`,
		},
		{
			input: `SELECT """TEST
/* This is not a comment */
"""`,
			want: `SELECT """TEST
/* This is not a comment */
"""`,
		},
		{
			input: ` /* This is a comment */
SELECT """TEST
/* This is not a comment */
""" /* This is a comment */`,
			want: `SELECT """TEST
/* This is not a comment */
"""`,
		},
		{
			input: `/* This is a comment /* this is still a comment */
SELECT 1`,
			want: `SELECT 1`,
		},
		{
			input: `/** This is a javadoc style comment /* this is still a comment */
SELECT 1`,
			want: `SELECT 1`,
		},
		{
			input: `/** This is a javadoc style comment /** this is still a comment */
SELECT 1`,
			want: `SELECT 1`,
		},
		{
			input: `/** This is a javadoc style comment /** this is still a comment **/
SELECT 1`,
			want: `SELECT 1`,
		},
	}
	for _, tc := range tests {
		got, err := removeCommentsAndTrim(tc.input)
		if err != nil && !tc.wantErr {
			t.Error(err)
			continue
		}
		if tc.wantErr {
			t.Errorf("missing expected error for %q", tc.input)
			continue
		}
		if got != tc.want {
			t.Errorf("removeCommentsAndTrim result mismatch\nGot: %q\nWant: %q", got, tc.want)
		}
	}
}

func TestRemoveStatementHint(t *testing.T) {
	tests := []struct {
		input string
		want  string
	}{
		{
			input: `@{JOIN_METHOD=HASH_JOIN} SELECT * FROM PersonsTable`,
			want:  `SELECT * FROM PersonsTable`,
		},
		{
			input: `@ {JOIN_METHOD=HASH_JOIN} SELECT * FROM PersonsTable`,
			want:  `SELECT * FROM PersonsTable`,
		},
		{
			input: `@{ JOIN_METHOD=HASH_JOIN} SELECT * FROM PersonsTable`,
			want:  `SELECT * FROM PersonsTable`,
		},
		{
			input: `@{JOIN_METHOD=HASH_JOIN } SELECT * FROM PersonsTable`,
			want:  `SELECT * FROM PersonsTable`,
		},
		{
			input: `@{JOIN_METHOD=HASH_JOIN}
SELECT * FROM PersonsTable`,
			want: `SELECT * FROM PersonsTable`,
		},
		{
			input: `@{
JOIN_METHOD =  HASH_JOIN   	}
	 SELECT * FROM PersonsTable`,
			want: `SELECT * FROM PersonsTable`,
		},
		{
			input: `@{JOIN_METHOD=HASH_JOIN}
-- Single line comment
SELECT * FROM PersonsTable`,
			want: `SELECT * FROM PersonsTable`,
		},
		{
			input: `@{JOIN_METHOD=HASH_JOIN}
/* Multi line comment
with more comments
*/SELECT * FROM PersonsTable`,
			want: `SELECT * FROM PersonsTable`,
		},
		{
			input: `@{JOIN_METHOD=HASH_JOIN} WITH subQ1 AS (SELECT SchoolID FROM Roster),
     subQ2 AS (SELECT OpponentID FROM PlayerStats)
SELECT * FROM subQ1
UNION ALL
SELECT * FROM subQ2`,
			want: `WITH subQ1 AS (SELECT SchoolID FROM Roster),
     subQ2 AS (SELECT OpponentID FROM PlayerStats)
SELECT * FROM subQ1
UNION ALL
SELECT * FROM subQ2`,
		},
		// Multiple query hints.
		{
			input: `@{FORCE_INDEX=index_name} @{JOIN_METHOD=HASH_JOIN} SELECT * FROM tbl`,
			want:  `SELECT * FROM tbl`,
		},
		{
			input: `@{FORCE_INDEX=index_name}
@{JOIN_METHOD=HASH_JOIN}
SELECT SchoolID FROM Roster`,
			want: `SELECT SchoolID FROM Roster`,
		},
		// Invalid query hints.
		{
			input: `@{JOIN_METHOD=HASH_JOIN SELECT * FROM PersonsTable`,
			want:  `@{JOIN_METHOD=HASH_JOIN SELECT * FROM PersonsTable`,
		},
		{
			input: `@JOIN_METHOD=HASH_JOIN} SELECT * FROM PersonsTable`,
			want:  `@JOIN_METHOD=HASH_JOIN} SELECT * FROM PersonsTable`,
		},
		{
			input: `@JOIN_METHOD=HASH_JOIN SELECT * FROM PersonsTable`,
			want:  `@JOIN_METHOD=HASH_JOIN SELECT * FROM PersonsTable`,
		},
	}
	for _, tc := range tests {
		sql, err := removeCommentsAndTrim(tc.input)
		if err != nil {
			t.Fatal(err)
		}
		got := removeStatementHint(sql)
		if got != tc.want {
			t.Errorf("removeStatementHint result mismatch\nGot: %q\nWant: %q", got, tc.want)
		}
	}
}

func TestConvertPositionalParametersToNamedParameters(t *testing.T) {
	tests := []struct {
		input   string
		want    string
		wantErr bool
		err     error
	}{
		{
			input: `SELECT * FROM PersonsTable WHERE id=?`,
			want:  `SELECT * FROM PersonsTable WHERE id=@p1`,
		},
		{
			input: `?'?test?\"?test?\"?'?`,
			want:  `@p1'?test?\"?test?\"?'@p2`,
		},
		{
			input: `?'?it\\'?s'?`,
			want:  `@p1'?it\\'?s'@p2`,
		},
		{
			input: `?'?it\\\"?s'?`,
			want:  `@p1'?it\\\"?s'@p2`,
		},
		{
			input: "?`?it\\\\`?s`?",
			want:  "@p1`?it\\\\`?s`@p2",
		},
		{
			input: "select 1, ?, 'test?test', \"test?test\", foo.* from `foo` where col1=? and col2='test' and col3=? and col4='?' and col5=\"?\" and col6='?''?''?'",
			want:  "select 1, @p1, 'test?test', \"test?test\", foo.* from `foo` where col1=@p2 and col2='test' and col3=@p3 and col4='?' and col5=\"?\" and col6='?''?''?'",
		},
		{
			input: "select * from foo where name=? and col2 like ? and col3 > ?",
			want:  "select * from foo where name=@p1 and col2 like @p2 and col3 > @p3",
		},
		{
			input: "select * from foo where id between ? and ?",
			want:  "select * from foo where id between @p1 and @p2",
		},
		{
			input: "select * from foo limit ? offset ?",
			want:  "select * from foo limit @p1 offset @p2",
		},
		{
			input: "select * from foo where col1=? and col2 like ? and col3 > ? and col4 < ? and col5 != ? and col6 not in (?, ?, ?) and col7 in (?, ?, ?) and col8 between ? and ?",
			want:  "select * from foo where col1=@p1 and col2 like @p2 and col3 > @p3 and col4 < @p4 and col5 != @p5 and col6 not in (@p6, @p7, @p8) and col7 in (@p9, @p10, @p11) and col8 between @p12 and @p13",
		},
		{
			input: "?\"?it\\\"?s\"?",
			want:  "@p1\"?it\\\"?s\"@p2",
		},
		{
			input:   "?'?it\\'?s \n ?it\\'?s'?",
			wantErr: true,
			err:     spanner.ToSpannerError(status.Errorf(codes.InvalidArgument, "statement contains an unclosed literal: %s", "?'?it\\'?s \n ?it\\'?s'?")),
		},
		{
			input:   "?'?it\\'?s \n ?it\\'?s?",
			wantErr: true,
			err:     spanner.ToSpannerError(status.Errorf(codes.InvalidArgument, "statement contains an unclosed literal: %s", "?'?it\\'?s \n ?it\\'?s?")),
		},
		{
			input:   "?'''?it\\'?s \n ?it\\'?s'?",
			wantErr: true,
			err:     spanner.ToSpannerError(status.Errorf(codes.InvalidArgument, "statement contains an unclosed literal: %s", "?'''?it\\'?s \n ?it\\'?s'?")),
		},
	}
	for _, tc := range tests {
		got, err := convertPositionalParametersToNamedParameters('?', tc.input)
		if err != nil && !tc.wantErr {
			t.Error(err)
			continue
		}
		if tc.wantErr {
			if err == nil {
				t.Errorf("convertPositionalParametersToNamedParameters error mismatch\nGot: %v\nWant: %v", err, tc.wantErr)
			}
			if !cmp.Equal(err.Error(), tc.err.Error()) {
				t.Errorf("convertPositionalParametersToNamedParameters error mismatch\nGot: %s\nWant: %s", err, tc.err)
			}
			continue
		}
		if !cmp.Equal(got, tc.want) {
			t.Errorf("convertPositionalParametersToNamedParameters result mismatch\nGot: %s\nWant: %s", got, tc.want)
		}
	}
}

func TestFindParams(t *testing.T) {
	tests := []struct {
		input   string
		want    []string
		wantErr bool
	}{
		{
			input: `SELECT * FROM PersonsTable WHERE id=@id`,
			want:  []string{"id"},
		},
		{
			input: `SELECT * FROM PersonsTable WHERE id=@id AND name=@name`,
			want:  []string{"id", "name"},
		},
		{
			input: `SELECT * FROM PersonsTable WHERE Name like @name AND Email='test@test.com'`,
			want:  []string{"name"},
		},
		{
			input: `SELECT * FROM """strange
 @table
""" WHERE Name like @name AND Email='test@test.com'`,
			want: []string{"name"},
		},
		{
			input: `@{JOIN_METHOD=HASH_JOIN} SELECT * FROM PersonsTable WHERE Name like @name AND Email='test@test.com'`,
			want:  []string{"name"},
		},
		{
			input: "INSERT INTO Foo (Col1, Col2, Col3) VALUES (@param1, @param2, @param3)",
			want:  []string{"param1", "param2", "param3"},
		},
		{
			input: "SELECT * FROM PersonsTable@{FORCE_INDEX=`my_index`} WHERE id=@id AND name=@name",
			want:  []string{"id", "name"},
		},
		{
			input: "SELECT * FROM PersonsTable @{FORCE_INDEX=my_index} WHERE id=@id AND name=@name",
			want:  []string{"id", "name"},
		},
	}
	for _, tc := range tests {
		sql, err := removeCommentsAndTrim(tc.input)
		if err != nil {
			t.Fatal(err)
		}
		got, err := parseNamedParameters(removeStatementHint(sql))
		if err != nil && !tc.wantErr {
			t.Error(err)
			continue
		}
		if tc.wantErr {
			t.Errorf("missing expected error for %q", tc.input)
			continue
		}
		if !cmp.Equal(got, tc.want) {
			t.Errorf("parseNamedParameters result mismatch\nGot: %s\nWant: %s", got, tc.want)
		}
	}
}

// note: isDDL function does not check validity of statement
// just that the statement begins with a DDL instruction.
// Other checking performed by database.
func TestIsDdl(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  bool
	}{
		{
			name: "valid create",
			input: `CREATE TABLE Valid (
				A   STRING(1024)
			)	 PRIMARY KEY (A)`,
			want: true,
		},
		{
			name: "leading spaces",
			input: `    CREATE TABLE Valid (
				A   STRING(1024)
			)	 PRIMARY KEY (A)`,
			want: true,
		},
		{
			name: "leading newlines",
			input: `


			CREATE TABLE Valid (
				A   STRING(1024)
			)	 PRIMARY KEY (A)`,
			want: true,
		},
		{
			name: "leading tabs",
			input: `		CREATE TABLE Valid (
				A   STRING(1024)
			)	 PRIMARY KEY (A)`,
			want: true,
		},
		{
			name: "leading whitespace, miscellaneous",
			input: `
							 
			 CREATE TABLE Valid (
				A   STRING(1024)
			)	 PRIMARY KEY (A)`,
			want: true,
		},
		{
			name: "lower case",
			input: `create table Valid (
				A   STRING(1024)
			)	 PRIMARY KEY (A)`,
			want: true,
		},
		{
			name: "mixed case, leading whitespace",
			input: ` 
			 cREAte taBLE Valid (
				A   STRING(1024)
			)	 PRIMARY KEY (A)`,
			want: true,
		},
		{
			name:  "insert (not ddl)",
			input: `INSERT INTO Valid`,
			want:  false,
		},
		{
			name:  "delete (not ddl)",
			input: `DELETE FROM Valid`,
			want:  false,
		},
		{
			name:  "update (not ddl)",
			input: `UPDATE Valid`,
			want:  false,
		},
		{
			name:  "drop",
			input: `DROP TABLE Valid`,
			want:  true,
		},
		{
			name:  "alter",
			input: `alter TABLE Valid`,
			want:  true,
		},
		{
			name:  "typo (ccreate)",
			input: `cCREATE TABLE Valid`,
			want:  false,
		},
		{
			name:  "typo (reate)",
			input: `REATE TABLE Valid`,
			want:  false,
		},
		{
			name:  "typo (rx ceate)",
			input: `x CREATE TABLE Valid`,
			want:  false,
		},
		{
			name:  "leading int",
			input: `0CREATE TABLE Valid`,
			want:  false,
		},
		{
			name: "leading single line comment",
			input: `-- Create the Valid table
            CREATE TABLE Valid (
				A STRING(1024)
			) PRIMARY KEY (A)`,
			want: true,
		},
		{
			name: "leading single line comment using hash",
			input: `# Create the Valid table
            CREATE TABLE Valid (
				A STRING(1024)
			) PRIMARY KEY (A)`,
			want: true,
		},
		{
			name: "leading multi line comment",
			input: `/* Create the Valid table.
            This comment will be stripped before the statement is parsed. */
            CREATE TABLE Valid (
				A STRING(1024)
			) PRIMARY KEY (A)`,
			want: true,
		},
	}

	for _, tc := range tests {
		got, err := isDDL(tc.input)
		if err != nil {
			t.Error(err)
		}
		if got != tc.want {
			t.Errorf("isDDL test failed, %s: wanted %t got %t.", tc.name, tc.want, got)
		}
	}
}

func TestParseClientSideStatement(t *testing.T) {
	tests := []struct {
		name       string
		input      string
		want       string
		wantParams string
		exec       bool
		query      bool
	}{
		{
			name:  "Start DDL batch",
			input: "START BATCH DDL",
			want:  "START BATCH DDL",
			exec:  true,
		},
		{
			name:  "Start DDL batch using line feeds",
			input: "START\nBATCH\nDDL",
			want:  "START BATCH DDL",
			exec:  true,
		},
		{
			name:  "Start DDL batch lower case",
			input: "start batch ddl",
			want:  "START BATCH DDL",
			exec:  true,
		},
		{
			name:  "Start DDL batch with extra spaces",
			input: "\tSTART  BATCH\n\nDDL",
			want:  "START BATCH DDL",
			exec:  true,
		},
		{
			name:  "Start DML batch",
			input: "START BATCH DML",
			want:  "START BATCH DML",
			exec:  true,
		},
		{
			name:  "Run batch",
			input: "run batch",
			want:  "RUN BATCH",
			exec:  true,
		},
		{
			name:  "Abort batch",
			input: "abort batch",
			want:  "ABORT BATCH",
			exec:  true,
		},
		{
			name:  "Show variable Retry_Aborts_Internally",
			input: "show variable retry_aborts_internally",
			want:  "SHOW VARIABLE RETRY_ABORTS_INTERNALLY",
			query: true,
		},
		{
			name:       "SET Retry_Aborts_Internally",
			input:      "set retry_aborts_internally = false",
			want:       "SET RETRY_ABORTS_INTERNALLY = TRUE|FALSE",
			wantParams: "false",
			exec:       true,
		},
	}

	for _, tc := range tests {
		statement, err := parseClientSideStatement(&conn{}, tc.input)
		if err != nil {
			t.Fatalf("failed to parse statement %s: %v", tc.name, err)
		}
		if tc.exec && statement.execContext == nil {
			t.Errorf("execContext missing for %q", tc.input)
		}
		if tc.query && statement.queryContext == nil {
			t.Errorf("queryContext missing for %q", tc.input)
		}

		var got string
		if statement != nil {
			got = statement.Name
		}
		if got != tc.want {
			t.Errorf("parseClientSideStatement test failed: %s\nGot: %s\nWant: %s.", tc.name, got, tc.want)
		}
		if tc.wantParams != "" {
			if g, w := statement.params, tc.wantParams; g != w {
				t.Errorf("params mismatch for %s\nGot: %v\nWant: %v", tc.name, g, w)
			}
		}
	}
}

func TestRemoveCommentsAndTrim_Errors(t *testing.T) {
	_, err := removeCommentsAndTrim("SELECT 'Hello World FROM SomeTable")
	if g, w := spanner.ErrCode(err), codes.InvalidArgument; g != w {
		t.Errorf("error code mismatch\nGot: %v\nWant: %v\n", g, w)
	}
	_, err = removeCommentsAndTrim("SELECT 'Hello World\nFROM SomeTable")
	if g, w := spanner.ErrCode(err), codes.InvalidArgument; g != w {
		t.Errorf("error code mismatch\nGot: %v\nWant: %v\n", g, w)
	}
}

func TestFindParams_Errors(t *testing.T) {
	_, err := findParams("SELECT 'Hello World FROM SomeTable WHERE id=@id")
	if g, w := spanner.ErrCode(err), codes.InvalidArgument; g != w {
		t.Errorf("error code mismatch\nGot: %v\nWant: %v\n", g, w)
	}
	_, err = findParams("SELECT 'Hello World\nFROM SomeTable WHERE id=@id")
	if g, w := spanner.ErrCode(err), codes.InvalidArgument; g != w {
		t.Errorf("error code mismatch\nGot: %v\nWant: %v\n", g, w)
	}
}
