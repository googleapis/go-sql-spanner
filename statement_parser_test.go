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
	"fmt"
	"testing"

	"cloud.google.com/go/spanner"
	"cloud.google.com/go/spanner/admin/database/apiv1/databasepb"
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
	parser, err := newStatementParser(databasepb.DatabaseDialect_GOOGLE_STANDARD_SQL, 1000)
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range tests {
		got, err := parser.removeCommentsAndTrim(tc.input)
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
			want:  ` SELECT * FROM PersonsTable`,
		},
		{
			input: `@{JOIN_METHOD=HASH_JOIN} FROM Produce |> WHERE item != 'bananas'`,
			want:  ` FROM Produce |> WHERE item != 'bananas'`,
		},
		{
			input: `@{JOIN_METHOD=HASH_JOIN} GRAPH FinGraph MATCH (n) RETURN LABELS(n) AS label, n.id`,
			want:  ` GRAPH FinGraph MATCH (n) RETURN LABELS(n) AS label, n.id`,
		},
		{
			input: `@ {JOIN_METHOD=HASH_JOIN} SELECT * FROM PersonsTable`,
			want:  ` SELECT * FROM PersonsTable`,
		},
		{
			input: `@{ JOIN_METHOD=HASH_JOIN} SELECT * FROM PersonsTable`,
			want:  ` SELECT * FROM PersonsTable`,
		},
		{
			input: `@{JOIN_METHOD=HASH_JOIN } SELECT * FROM PersonsTable`,
			want:  ` SELECT * FROM PersonsTable`,
		},
		{
			input: `@{JOIN_METHOD=HASH_JOIN}
SELECT * FROM PersonsTable1`,
			want: `
SELECT * FROM PersonsTable1`,
		},
		{
			input: `@{
JOIN_METHOD =  HASH_JOIN   	}
	 SELECT * FROM PersonsTable2`,
			want: `
	 SELECT * FROM PersonsTable2`,
		},
		{
			input: `@{JOIN_METHOD=HASH_JOIN}
-- Single line comment
SELECT * FROM PersonsTable3`,
			want: `
-- Single line comment
SELECT * FROM PersonsTable3`,
		},
		{
			input: `@{JOIN_METHOD=HASH_JOIN}
/* Multi line comment
with more comments
*/SELECT * FROM PersonsTable4`,
			want: `
/* Multi line comment
with more comments
*/SELECT * FROM PersonsTable4`,
		},
		{
			input: `@{JOIN_METHOD=HASH_JOIN} WITH subQ1 AS (SELECT SchoolID FROM Roster),
     subQ2 AS (SELECT OpponentID FROM PlayerStats)
SELECT * FROM subQ1
UNION ALL
SELECT * FROM subQ2`,
			want: ` WITH subQ1 AS (SELECT SchoolID FROM Roster),
     subQ2 AS (SELECT OpponentID FROM PlayerStats)
SELECT * FROM subQ1
UNION ALL
SELECT * FROM subQ2`,
		},
		// Multiple query hints.
		{
			input: `@{FORCE_INDEX=index_name, JOIN_METHOD=HASH_JOIN} SELECT * FROM tbl`,
			want:  ` SELECT * FROM tbl`,
		},
		{
			input: `@{FORCE_INDEX=index_name,
JOIN_METHOD=HASH_JOIN}
SELECT SchoolID FROM Roster`,
			want: `
SELECT SchoolID FROM Roster`,
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
		{
			input: "@{FORCE_INDEX=index_name}\xb0\xb0\xb0\x80SELECT",
			want:  "\xb0\xb0\xb0\x80SELECT",
		},
	}
	parser, err := newStatementParser(databasepb.DatabaseDialect_GOOGLE_STANDARD_SQL, 1000)
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range tests {
		got := parser.removeStatementHint(tc.input)
		if got != tc.want {
			t.Errorf("removeStatementHint result mismatch\nGot: %q\nWant: %q", got, tc.want)
		}
	}
}

func FuzzRemoveCommentsAndTrim(f *testing.F) {
	for _, sample := range fuzzQuerySamples {
		f.Add(sample)
	}

	parser, err := newStatementParser(databasepb.DatabaseDialect_GOOGLE_STANDARD_SQL, 1000)
	if err != nil {
		f.Fatal(err)
	}
	f.Fuzz(func(t *testing.T, input string) {
		_, _ = parser.removeCommentsAndTrim(input)
	})
}

func TestFindParams(t *testing.T) {
	tests := map[string]struct {
		input   string
		wantSQL map[databasepb.DatabaseDialect]string
		want    map[databasepb.DatabaseDialect][]string
		wantErr map[databasepb.DatabaseDialect]error
	}{
		"id=@id": {
			input: `SELECT * FROM PersonsTable WHERE id=@id`,
			wantSQL: map[databasepb.DatabaseDialect]string{
				databasepb.DatabaseDialect_DATABASE_DIALECT_UNSPECIFIED: `SELECT * FROM PersonsTable WHERE id=@id`,
			},
			want: map[databasepb.DatabaseDialect][]string{
				databasepb.DatabaseDialect_DATABASE_DIALECT_UNSPECIFIED: {"id"},
			},
		},
		"simple multi-line comment": {
			input: `/* comment */ SELECT * FROM PersonsTable WHERE id=@id`,
			wantSQL: map[databasepb.DatabaseDialect]string{
				databasepb.DatabaseDialect_DATABASE_DIALECT_UNSPECIFIED: `/* comment */ SELECT * FROM PersonsTable WHERE id=@id`,
			},
			want: map[databasepb.DatabaseDialect][]string{
				databasepb.DatabaseDialect_DATABASE_DIALECT_UNSPECIFIED: {"id"},
			},
		},
		"simple single-line comment": {
			input: `-- comment
SELECT * FROM PersonsTable WHERE id=@id`,
			wantSQL: map[databasepb.DatabaseDialect]string{
				databasepb.DatabaseDialect_DATABASE_DIALECT_UNSPECIFIED: `-- comment
SELECT * FROM PersonsTable WHERE id=@id`,
			},
			want: map[databasepb.DatabaseDialect][]string{
				databasepb.DatabaseDialect_DATABASE_DIALECT_UNSPECIFIED: {"id"},
			},
		},
		"single-line hash comment with potential query parameter": {
			input: `# This is not a @param in GoogleSQL, but it is in PostgreSQL
SELECT * FROM PersonsTable WHERE id=@id`,
			wantSQL: map[databasepb.DatabaseDialect]string{
				databasepb.DatabaseDialect_DATABASE_DIALECT_UNSPECIFIED: `# This is not a @param in GoogleSQL, but it is in PostgreSQL
SELECT * FROM PersonsTable WHERE id=@id`,
			},
			want: map[databasepb.DatabaseDialect][]string{
				databasepb.DatabaseDialect_GOOGLE_STANDARD_SQL: {"id"},
				databasepb.DatabaseDialect_POSTGRESQL:          {"param", "id"},
			},
		},
		"commented where clause": {
			input: `SELECT * FROM PersonsTable WHERE id=@id /* and value=@value */`,
			wantSQL: map[databasepb.DatabaseDialect]string{
				databasepb.DatabaseDialect_DATABASE_DIALECT_UNSPECIFIED: `SELECT * FROM PersonsTable WHERE id=@id /* and value=@value */`,
			},
			want: map[databasepb.DatabaseDialect][]string{
				databasepb.DatabaseDialect_DATABASE_DIALECT_UNSPECIFIED: {"id"},
			},
		},
		"id=@id and name=@name": {
			input: `SELECT * FROM PersonsTable WHERE id=@id AND name=@name`,
			wantSQL: map[databasepb.DatabaseDialect]string{
				databasepb.DatabaseDialect_DATABASE_DIALECT_UNSPECIFIED: `SELECT * FROM PersonsTable WHERE id=@id AND name=@name`,
			},
			want: map[databasepb.DatabaseDialect][]string{
				databasepb.DatabaseDialect_DATABASE_DIALECT_UNSPECIFIED: {"id", "name"},
			},
		},
		"id=@id and email literal": {
			input: `SELECT * FROM PersonsTable WHERE Name like @name AND Email='test@test.com'`,
			wantSQL: map[databasepb.DatabaseDialect]string{
				databasepb.DatabaseDialect_DATABASE_DIALECT_UNSPECIFIED: `SELECT * FROM PersonsTable WHERE Name like @name AND Email='test@test.com'`,
			},
			want: map[databasepb.DatabaseDialect][]string{
				databasepb.DatabaseDialect_DATABASE_DIALECT_UNSPECIFIED: {"name"},
			},
		},
		"multibyte character in string literal": {
			//lint:ignore ST1018 allow control characters to verify the correct behavior of multibyte chars.
			input: `SELECT * FROM PersonsTable WHERE Name like @name AND Email='@test.com'`,
			wantSQL: map[databasepb.DatabaseDialect]string{
				//lint:ignore ST1018 allow control characters to verify the correct behavior of multibyte chars.
				databasepb.DatabaseDialect_DATABASE_DIALECT_UNSPECIFIED: `SELECT * FROM PersonsTable WHERE Name like @name AND Email='@test.com'`,
			},
			want: map[databasepb.DatabaseDialect][]string{
				databasepb.DatabaseDialect_DATABASE_DIALECT_UNSPECIFIED: {"name"},
			},
		},
		"multibyte character in comment": {
			//lint:ignore ST1018 allow control characters to verify the correct behavior of multibyte chars.
			input: `/*  */SELECT * FROM PersonsTable WHERE Name like @name AND Email='test@test.com'`,
			wantSQL: map[databasepb.DatabaseDialect]string{
				//lint:ignore ST1018 allow control characters to verify the correct behavior of multibyte chars.
				databasepb.DatabaseDialect_DATABASE_DIALECT_UNSPECIFIED: `/*  */SELECT * FROM PersonsTable WHERE Name like @name AND Email='test@test.com'`,
			},
			want: map[databasepb.DatabaseDialect][]string{
				databasepb.DatabaseDialect_DATABASE_DIALECT_UNSPECIFIED: {"name"},
			},
		},
		"table name with @": {
			input: `SELECT * FROM """strange
		@table
		""" WHERE Name like @name AND Email='test@test.com'`,
			wantSQL: map[databasepb.DatabaseDialect]string{
				databasepb.DatabaseDialect_DATABASE_DIALECT_UNSPECIFIED: `SELECT * FROM """strange
		@table
		""" WHERE Name like @name AND Email='test@test.com'`,
			},
			want: map[databasepb.DatabaseDialect][]string{
				databasepb.DatabaseDialect_DATABASE_DIALECT_UNSPECIFIED: {"name"},
			},
		},
		"statement hint": {
			input: `@{JOIN_METHOD=HASH_JOIN} SELECT * FROM PersonsTable WHERE Name like @name AND Email='test@test.com'`,
			wantSQL: map[databasepb.DatabaseDialect]string{
				databasepb.DatabaseDialect_DATABASE_DIALECT_UNSPECIFIED: `@{JOIN_METHOD=HASH_JOIN} SELECT * FROM PersonsTable WHERE Name like @name AND Email='test@test.com'`,
			},
			want: map[databasepb.DatabaseDialect][]string{
				databasepb.DatabaseDialect_DATABASE_DIALECT_UNSPECIFIED: {"name"},
			},
		},
		"multiple parameters": {
			input: "INSERT INTO Foo (Col1, Col2, Col3) VALUES (@param1, @param2, @param3)",
			wantSQL: map[databasepb.DatabaseDialect]string{
				databasepb.DatabaseDialect_DATABASE_DIALECT_UNSPECIFIED: "INSERT INTO Foo (Col1, Col2, Col3) VALUES (@param1, @param2, @param3)",
			},
			want: map[databasepb.DatabaseDialect][]string{
				databasepb.DatabaseDialect_DATABASE_DIALECT_UNSPECIFIED: {"param1", "param2", "param3"},
			},
		},
		"force index hint with quoted index name": {
			input: "SELECT * FROM PersonsTable@{FORCE_INDEX=`my_index`} WHERE id=@id AND name=@name",
			wantSQL: map[databasepb.DatabaseDialect]string{
				databasepb.DatabaseDialect_DATABASE_DIALECT_UNSPECIFIED: "SELECT * FROM PersonsTable@{FORCE_INDEX=`my_index`} WHERE id=@id AND name=@name",
			},
			want: map[databasepb.DatabaseDialect][]string{
				databasepb.DatabaseDialect_DATABASE_DIALECT_UNSPECIFIED: {"id", "name"},
			},
		},
		"force index hint": {
			input: "SELECT * FROM PersonsTable @{FORCE_INDEX=my_index} WHERE id=@id AND name=@name",
			wantSQL: map[databasepb.DatabaseDialect]string{
				databasepb.DatabaseDialect_DATABASE_DIALECT_UNSPECIFIED: "SELECT * FROM PersonsTable @{FORCE_INDEX=my_index} WHERE id=@id AND name=@name",
			},
			want: map[databasepb.DatabaseDialect][]string{
				databasepb.DatabaseDialect_DATABASE_DIALECT_UNSPECIFIED: {"id", "name"},
			},
		},
		"positional parameter": {
			input: `SELECT * FROM PersonsTable WHERE id=?`,
			wantSQL: map[databasepb.DatabaseDialect]string{
				databasepb.DatabaseDialect_GOOGLE_STANDARD_SQL: `SELECT * FROM PersonsTable WHERE id=@p1`,
				databasepb.DatabaseDialect_POSTGRESQL:          `SELECT * FROM PersonsTable WHERE id=$1`,
			},
			want: map[databasepb.DatabaseDialect][]string{
				databasepb.DatabaseDialect_DATABASE_DIALECT_UNSPECIFIED: {"p1"},
			},
		},
		"two positional parameters and string literal with question marks": {
			input: `?'?test?"?test?"?'?`,
			wantSQL: map[databasepb.DatabaseDialect]string{
				databasepb.DatabaseDialect_GOOGLE_STANDARD_SQL: `@p1'?test?"?test?"?'@p2`,
				databasepb.DatabaseDialect_POSTGRESQL:          `$1'?test?"?test?"?'$2`,
			},
			want: map[databasepb.DatabaseDialect][]string{
				databasepb.DatabaseDialect_DATABASE_DIALECT_UNSPECIFIED: {"p1", "p2"},
			},
		},
		"two positional parameters and string literal with escaped quote": {
			input: `?'?it\'?s'?`,
			wantSQL: map[databasepb.DatabaseDialect]string{
				databasepb.DatabaseDialect_GOOGLE_STANDARD_SQL: `@p1'?it\'?s'@p2`,
			},
			want: map[databasepb.DatabaseDialect][]string{
				databasepb.DatabaseDialect_GOOGLE_STANDARD_SQL: {"p1", "p2"},
			},
			wantErr: map[databasepb.DatabaseDialect]error{
				databasepb.DatabaseDialect_POSTGRESQL: spanner.ToSpannerError(status.Errorf(codes.InvalidArgument, "SQL statement contains an unclosed literal: %s", `?'?it\'?s'?`)),
			},
		},
		"two positional parameters and string literal with escaped double quote": {
			input: `?'?it\"?s'?`,
			wantSQL: map[databasepb.DatabaseDialect]string{
				databasepb.DatabaseDialect_GOOGLE_STANDARD_SQL: `@p1'?it\"?s'@p2`,
				databasepb.DatabaseDialect_POSTGRESQL:          `$1'?it\"?s'$2`,
			},
			want: map[databasepb.DatabaseDialect][]string{
				databasepb.DatabaseDialect_DATABASE_DIALECT_UNSPECIFIED: {"p1", "p2"},
			},
		},
		"two positional parameters and double quoted string literal with escaped quote": {
			input: `?"?it\"?s"?`,
			wantSQL: map[databasepb.DatabaseDialect]string{
				databasepb.DatabaseDialect_GOOGLE_STANDARD_SQL: `@p1"?it\"?s"@p2`,
			},
			want: map[databasepb.DatabaseDialect][]string{
				databasepb.DatabaseDialect_GOOGLE_STANDARD_SQL: {"p1", "p2"},
			},
			wantErr: map[databasepb.DatabaseDialect]error{
				databasepb.DatabaseDialect_POSTGRESQL: spanner.ToSpannerError(status.Errorf(codes.InvalidArgument, "SQL statement contains an unclosed literal: %s", `?"?it\"?s"?`)),
			},
		},
		"triple-quoted string": {
			input: `?'''?it\'?s'''?`,
			wantSQL: map[databasepb.DatabaseDialect]string{
				// In GoogleSQL, the triple-quoted string means that single quotes are allowed inside the string.
				databasepb.DatabaseDialect_GOOGLE_STANDARD_SQL: `@p1'''?it\'?s'''@p2`,
				// In PostgreSQL, triple-quoted strings are not a thing. Instead, the ''' sequence starts a new string
				// literal where the first character is a single quote. This means that the string ?'''?it\'?s'''?
				// in PostgreSQL is invalid, because it consists of these parts:
				// ?   '''?it\'   ?s'''?
				// The first part is a query parameter.
				// The second part is a string where the first character is a single quote. The last character is a
				// backslash (which has no special meaning in a normal PostgreSQL string).
				// The third part is invalid, as it contains an unclosed string literal.
			},
			want: map[databasepb.DatabaseDialect][]string{
				databasepb.DatabaseDialect_GOOGLE_STANDARD_SQL: {"p1", "p2"},
			},
			wantErr: map[databasepb.DatabaseDialect]error{
				databasepb.DatabaseDialect_POSTGRESQL: spanner.ToSpannerError(status.Errorf(codes.InvalidArgument, "SQL statement contains an unclosed literal: %s", `?'''?it\'?s'''?`)),
			},
		},
		"triple-quoted string using double quotes": {
			input: `?"""?it\"?s"""?`,
			wantSQL: map[databasepb.DatabaseDialect]string{
				databasepb.DatabaseDialect_GOOGLE_STANDARD_SQL: `@p1"""?it\"?s"""@p2`,
			},
			want: map[databasepb.DatabaseDialect][]string{
				databasepb.DatabaseDialect_GOOGLE_STANDARD_SQL: {"p1", "p2"},
			},
			wantErr: map[databasepb.DatabaseDialect]error{
				databasepb.DatabaseDialect_POSTGRESQL: spanner.ToSpannerError(status.Errorf(codes.InvalidArgument, "SQL statement contains an unclosed literal: %s", `?"""?it\"?s"""?`)),
			},
		},
		"backtick string with escaped quote": {
			input: `?` + "`?it" + `\` + "`?s`" + `?`,
			wantSQL: map[databasepb.DatabaseDialect]string{
				databasepb.DatabaseDialect_GOOGLE_STANDARD_SQL: `@p1` + "`?it" + `\` + "`?s`" + `@p2`,
				// Backticks are not valid quotes in PostgreSQL, so these are just ignored.
				databasepb.DatabaseDialect_POSTGRESQL: `$1` + "`$2it" + `\` + "`$3s`" + `$4`,
			},
			want: map[databasepb.DatabaseDialect][]string{
				databasepb.DatabaseDialect_GOOGLE_STANDARD_SQL: {"p1", "p2"},
				databasepb.DatabaseDialect_POSTGRESQL:          {"p1", "p2", "p3", "p4"},
			},
		},
		"triple-quoted string with escaped quote and linefeed": {
			input: `?'''?it\'?s
					?it\'?s'''?`,
			wantSQL: map[databasepb.DatabaseDialect]string{
				databasepb.DatabaseDialect_GOOGLE_STANDARD_SQL: `@p1'''?it\'?s
					?it\'?s'''@p2`,
				databasepb.DatabaseDialect_POSTGRESQL: `$1'''?it\'$2s
					$3it\'?s'''$4`,
			},
			want: map[databasepb.DatabaseDialect][]string{
				databasepb.DatabaseDialect_GOOGLE_STANDARD_SQL: {"p1", "p2"},
				databasepb.DatabaseDialect_POSTGRESQL:          {"p1", "p2", "p3", "p4"},
			},
		},
		"triple-quoted string with escaped quote and linefeed (2)": {
			input: `?'''?it\'?s
					?it\'?s'''?`,
			wantSQL: map[databasepb.DatabaseDialect]string{
				databasepb.DatabaseDialect_GOOGLE_STANDARD_SQL: `@p1'''?it\'?s
					?it\'?s'''@p2`,
				databasepb.DatabaseDialect_POSTGRESQL: `$1'''?it\'$2s
					$3it\'?s'''$4`,
			},
			want: map[databasepb.DatabaseDialect][]string{
				databasepb.DatabaseDialect_GOOGLE_STANDARD_SQL: {"p1", "p2"},
				databasepb.DatabaseDialect_POSTGRESQL:          {"p1", "p2", "p3", "p4"},
			},
		},
		"positional parameters in select and where clause": {
			input: `select 1, ?, 'test?test', "test?test", foo.* from` + "`foo`" + `where col1=? and col2='test' and col3=? and col4='?' and col5="?" and col6='?''?''?'`,
			wantSQL: map[databasepb.DatabaseDialect]string{
				databasepb.DatabaseDialect_GOOGLE_STANDARD_SQL: `select 1, @p1, 'test?test', "test?test", foo.* from` + "`foo`" + `where col1=@p2 and col2='test' and col3=@p3 and col4='?' and col5="?" and col6='?''?''?'`,
				databasepb.DatabaseDialect_POSTGRESQL:          `select 1, $1, 'test?test', "test?test", foo.* from` + "`foo`" + `where col1=$2 and col2='test' and col3=$3 and col4='?' and col5="?" and col6='?''?''?'`,
			},
			want: map[databasepb.DatabaseDialect][]string{
				databasepb.DatabaseDialect_DATABASE_DIALECT_UNSPECIFIED: {"p1", "p2", "p3"},
			},
		},
		"three positional parameters": {
			input: `select * from foo where name=? and col2 like ? and col3 > ?`,
			wantSQL: map[databasepb.DatabaseDialect]string{
				databasepb.DatabaseDialect_GOOGLE_STANDARD_SQL: `select * from foo where name=@p1 and col2 like @p2 and col3 > @p3`,
				databasepb.DatabaseDialect_POSTGRESQL:          `select * from foo where name=$1 and col2 like $2 and col3 > $3`,
			},
			want: map[databasepb.DatabaseDialect][]string{
				databasepb.DatabaseDialect_DATABASE_DIALECT_UNSPECIFIED: {"p1", "p2", "p3"},
			},
		},
		"two positional parameters": {
			input: `select * from foo where id between ? and ?`,
			wantSQL: map[databasepb.DatabaseDialect]string{
				databasepb.DatabaseDialect_GOOGLE_STANDARD_SQL: `select * from foo where id between @p1 and @p2`,
				databasepb.DatabaseDialect_POSTGRESQL:          `select * from foo where id between $1 and $2`,
			},
			want: map[databasepb.DatabaseDialect][]string{
				databasepb.DatabaseDialect_DATABASE_DIALECT_UNSPECIFIED: {"p1", "p2"},
			},
		},
		"positional parameters in limit/offset": {
			input: `select * from foo limit ? offset ?`,
			wantSQL: map[databasepb.DatabaseDialect]string{
				databasepb.DatabaseDialect_GOOGLE_STANDARD_SQL: `select * from foo limit @p1 offset @p2`,
				databasepb.DatabaseDialect_POSTGRESQL:          `select * from foo limit $1 offset $2`,
			},
			want: map[databasepb.DatabaseDialect][]string{
				databasepb.DatabaseDialect_DATABASE_DIALECT_UNSPECIFIED: {"p1", "p2"},
			},
		},
		"13 positional parameters": {
			input: `select * from foo where col1=? and col2 like ? and col3 > ? and col4 < ? and col5 != ? and col6 not in (?, ?, ?) and col7 in (?, ?, ?) and col8 between ? and ?`,
			wantSQL: map[databasepb.DatabaseDialect]string{
				databasepb.DatabaseDialect_GOOGLE_STANDARD_SQL: `select * from foo where col1=@p1 and col2 like @p2 and col3 > @p3 and col4 < @p4 and col5 != @p5 and col6 not in (@p6, @p7, @p8) and col7 in (@p9, @p10, @p11) and col8 between @p12 and @p13`,
				databasepb.DatabaseDialect_POSTGRESQL:          `select * from foo where col1=$1 and col2 like $2 and col3 > $3 and col4 < $4 and col5 != $5 and col6 not in ($6, $7, $8) and col7 in ($9, $10, $11) and col8 between $12 and $13`,
			},
			want: map[databasepb.DatabaseDialect][]string{
				databasepb.DatabaseDialect_DATABASE_DIALECT_UNSPECIFIED: {"p1", "p2", "p3", "p4", "p5", "p6", "p7", "p8", "p9", "p10", "p11", "p12", "p13"},
			},
		},
		"positional parameter compared to string literal with potential named parameter": {
			input: `select * from foo where ?='''strange @table'''`,
			wantSQL: map[databasepb.DatabaseDialect]string{
				databasepb.DatabaseDialect_GOOGLE_STANDARD_SQL: `select * from foo where @p1='''strange @table'''`,
				databasepb.DatabaseDialect_POSTGRESQL:          `select * from foo where $1='''strange @table'''`,
			},
			want: map[databasepb.DatabaseDialect][]string{
				databasepb.DatabaseDialect_DATABASE_DIALECT_UNSPECIFIED: {"p1"},
			},
		},
		"incomplete named parameter": {
			input: `select foo from bar where id=@ order by value`,
			wantSQL: map[databasepb.DatabaseDialect]string{
				databasepb.DatabaseDialect_DATABASE_DIALECT_UNSPECIFIED: `select foo from bar where id=@ order by value`,
			},
			want: map[databasepb.DatabaseDialect][]string{
				databasepb.DatabaseDialect_DATABASE_DIALECT_UNSPECIFIED: {},
			},
		},
		"unclosed literal 1": {
			input: `?'?it\'?s
		?it\'?s'?`,
			wantSQL: map[databasepb.DatabaseDialect]string{
				databasepb.DatabaseDialect_POSTGRESQL: `$1'?it\'$2s
		$3it\'?s'$4`,
			},
			want: map[databasepb.DatabaseDialect][]string{
				databasepb.DatabaseDialect_POSTGRESQL: {"p1", "p2", "p3", "p4"},
			},
			wantErr: map[databasepb.DatabaseDialect]error{
				databasepb.DatabaseDialect_GOOGLE_STANDARD_SQL: spanner.ToSpannerError(status.Errorf(codes.InvalidArgument, "SQL statement contains an unclosed literal: %s", `?'?it\'?s
		?it\'?s'?`)),
			},
		},
		"unclosed literal 2": {
			input: `?'?it\'?s
		?it\'?s?`,
			wantErr: map[databasepb.DatabaseDialect]error{
				databasepb.DatabaseDialect_DATABASE_DIALECT_UNSPECIFIED: spanner.ToSpannerError(status.Errorf(codes.InvalidArgument, "SQL statement contains an unclosed literal: %s", `?'?it\'?s
		?it\'?s?`)),
			},
		},
		"unclosed literal 3": {
			input: `?'''?it\'?s
		?it\'?s'?`,
			wantSQL: map[databasepb.DatabaseDialect]string{
				databasepb.DatabaseDialect_POSTGRESQL: `$1'''?it\'$2s
		$3it\'?s'$4`,
			},
			want: map[databasepb.DatabaseDialect][]string{
				databasepb.DatabaseDialect_POSTGRESQL: {"p1", "p2", "p3", "p4"},
			},
			wantErr: map[databasepb.DatabaseDialect]error{
				databasepb.DatabaseDialect_GOOGLE_STANDARD_SQL: spanner.ToSpannerError(status.Errorf(codes.InvalidArgument, "SQL statement contains an unclosed literal: %s", `?'''?it\'?s
		?it\'?s'?`)),
			},
		},
	}
	for _, dialect := range []databasepb.DatabaseDialect{databasepb.DatabaseDialect_GOOGLE_STANDARD_SQL, databasepb.DatabaseDialect_POSTGRESQL} {
		parser, err := newStatementParser(dialect, 1000)
		if err != nil {
			t.Fatal(err)
		}
		for name, tc := range tests {
			t.Run(name, func(t *testing.T) {
				sql := tc.input
				gotSQL, got, err := parser.parseParameters(sql)
				wantErr := expectedTestValue(dialect, tc.wantErr)
				if err != nil && wantErr == nil {
					t.Error(err)
				}
				if wantErr != nil {
					if err == nil {
						t.Errorf("missing expected error for %q", tc.input)
					} else if !cmp.Equal(err.Error(), wantErr.Error()) {
						t.Errorf("parseParameters error mismatch\nGot: %s\nWant: %s", err.Error(), wantErr)
					}
				}
				want := expectedTestValue(dialect, tc.want)
				if !cmp.Equal(got, want) {
					t.Errorf("%v: parseParameters result mismatch\n Got: %s\nWant: %s", dialect, got, want)
				}
				wantSQL := expectedTestValue(dialect, tc.wantSQL)
				if !cmp.Equal(gotSQL, wantSQL) {
					t.Errorf("%v: parseParameters sql mismatch\n Got: %s\nWant: %s", dialect, gotSQL, wantSQL)
				}
			})
		}
	}
}

func TestFindParamsPostgreSQL(t *testing.T) {
	tests := map[string]struct {
		input   string
		wantSQL string
		want    []string
		wantErr error
	}{
		"id=$1": {
			input:   `SELECT * FROM PersonsTable WHERE id=$1`,
			wantSQL: `SELECT * FROM PersonsTable WHERE id=$1`,
			want:    []string{"p1"},
		},
		"simple multi-line comment": {
			input:   `/* comment */ SELECT * FROM PersonsTable WHERE id=$1`,
			wantSQL: `/* comment */ SELECT * FROM PersonsTable WHERE id=$1`,
			want:    []string{"p1"},
		},
		"simple single-line comment": {
			input: `-- comment
SELECT * FROM PersonsTable WHERE id=$1`,
			wantSQL: `-- comment
SELECT * FROM PersonsTable WHERE id=$1`,
			want: []string{"p1"},
		},
		"single-line hash comment with potential query parameter": {
			input: `# This is not a comment, so this is a param $1
		SELECT * FROM PersonsTable WHERE id=$2`,
			wantSQL: `# This is not a comment, so this is a param $1
		SELECT * FROM PersonsTable WHERE id=$2`,
			want: []string{"p1", "p2"},
		},
		"commented where clause": {
			input:   `SELECT * FROM PersonsTable WHERE id=$1 /* and value=$2 */`,
			wantSQL: `SELECT * FROM PersonsTable WHERE id=$1 /* and value=$2 */`,
			want:    []string{"p1"},
		},
		"id=$1 and name=$2": {
			input:   `SELECT * FROM PersonsTable WHERE id=$1 AND name=$2`,
			wantSQL: `SELECT * FROM PersonsTable WHERE id=$1 AND name=$2`,
			want:    []string{"p1", "p2"},
		},
		"id=$1 and email literal": {
			input:   `SELECT * FROM PersonsTable WHERE Name like $1 AND Email='test@test.com'`,
			wantSQL: `SELECT * FROM PersonsTable WHERE Name like $1 AND Email='test@test.com'`,
			want:    []string{"p1"},
		},
		"multibyte character in string literal": {
			//lint:ignore ST1018 allow control characters to verify the correct behavior of multibyte chars.
			input: `SELECT * FROM PersonsTable WHERE Name like $1 AND Email='@test.com'`,
			//lint:ignore ST1018 allow control characters to verify the correct behavior of multibyte chars.
			wantSQL: `SELECT * FROM PersonsTable WHERE Name like $1 AND Email='@test.com'`,
			want:    []string{"p1"},
		},
		"multibyte character in comment": {
			//lint:ignore ST1018 allow control characters to verify the correct behavior of multibyte chars.
			input: `/*  */SELECT * FROM PersonsTable WHERE Name like $1 AND Email='test@test.com'`,
			//lint:ignore ST1018 allow control characters to verify the correct behavior of multibyte chars.
			wantSQL: `/*  */SELECT * FROM PersonsTable WHERE Name like $1 AND Email='test@test.com'`,
			want:    []string{"p1"},
		},
		"table name with @": {
			input: `SELECT * FROM """strange
				@table
				""" WHERE Name like $1 AND Email='test@test.com'`,
			wantSQL: `SELECT * FROM """strange
				@table
				""" WHERE Name like $1 AND Email='test@test.com'`,
			want: []string{"p1"},
		},
		"statement hint": {
			input:   `/*@{JOIN_METHOD=HASH_JOIN}*/ SELECT * FROM PersonsTable WHERE Name like $1 AND Email='test@test.com'`,
			wantSQL: `/*@{JOIN_METHOD=HASH_JOIN}*/ SELECT * FROM PersonsTable WHERE Name like $1 AND Email='test@test.com'`,
			want:    []string{"p1"},
		},
		"multiple parameters": {
			input:   "INSERT INTO Foo (Col1, Col2, Col3) VALUES ($1, $2, $3)",
			wantSQL: "INSERT INTO Foo (Col1, Col2, Col3) VALUES ($1, $2, $3)",
			want:    []string{"p1", "p2", "p3"},
		},
		"force index hint with quoted index name": {
			input:   "SELECT * FROM PersonsTable/*@{FORCE_INDEX=`my_index`}*/ WHERE id=$1 AND name=$2",
			wantSQL: "SELECT * FROM PersonsTable/*@{FORCE_INDEX=`my_index`}*/ WHERE id=$1 AND name=$2",
			want:    []string{"p1", "p2"},
		},
		"force index hint": {
			input:   "SELECT * FROM PersonsTable /*@{FORCE_INDEX=my_index}*/ WHERE id=$1 AND name=$2",
			wantSQL: "SELECT * FROM PersonsTable /*@{FORCE_INDEX=my_index}*/ WHERE id=$1 AND name=$2",
			want:    []string{"p1", "p2"},
		},
		"positional parameter": {
			input:   `SELECT * FROM PersonsTable WHERE id=?`,
			wantSQL: `SELECT * FROM PersonsTable WHERE id=$1`,
			want:    []string{"p1"},
		},
		"two positional parameters and string literal with question marks": {
			input:   `?'?test?"?test?"?'?`,
			wantSQL: `$1'?test?"?test?"?'$2`,
			want:    []string{"p1", "p2"},
		},
		"two positional parameters and string literal with escaped quote": {
			input:   `?'?it\'?s'?`,
			wantErr: spanner.ToSpannerError(status.Errorf(codes.InvalidArgument, "SQL statement contains an unclosed literal: %s", `?'?it\'?s'?`)),
		},
		"two positional parameters and string literal with escaped double quote": {
			input:   `?'?it\"?s'?`,
			wantSQL: `$1'?it\"?s'$2`,
			want:    []string{"p1", "p2"},
		},
		"two positional parameters and double quoted string literal with escaped quote": {
			input:   `?"?it\"?s"?`,
			wantErr: spanner.ToSpannerError(status.Errorf(codes.InvalidArgument, "SQL statement contains an unclosed literal: %s", `?"?it\"?s"?`)),
		},
		"triple-quoted string": {
			input:   `?'''?it\'?s'''?`,
			wantErr: spanner.ToSpannerError(status.Errorf(codes.InvalidArgument, "SQL statement contains an unclosed literal: %s", `?'''?it\'?s'''?`)),
		},
		"triple-quoted string using double quotes": {
			input:   `?"""?it\"?s"""?`,
			wantErr: spanner.ToSpannerError(status.Errorf(codes.InvalidArgument, "SQL statement contains an unclosed literal: %s", `?"""?it\"?s"""?`)),
		},
		"backtick string with escaped quote": {
			input:   `?` + "`?it" + `\` + "`?s`" + `?`,
			wantSQL: `$1` + "`$2it" + `\` + "`$3s`" + `$4`,
			want:    []string{"p1", "p2", "p3", "p4"},
		},
		"triple-quoted string with escaped quote and linefeed": {
			input: `?'''?it\'?s
				?it\'?s'''?`,
			wantSQL: `$1'''?it\'$2s
				$3it\'?s'''$4`,
			want: []string{"p1", "p2", "p3", "p4"},
		},
		"triple-quoted string with escaped quote and linefeed (2)": {
			input: `?'''?it\'?s
				?it\'?s'''?`,
			wantSQL: `$1'''?it\'$2s
				$3it\'?s'''$4`,
			want: []string{"p1", "p2", "p3", "p4"},
		},
		"positional parameters in select and where clause": {
			input:   `select 1, ?, 'test?test', "test?test", foo.* from` + "`foo`" + `where col1=? and col2='test' and col3=? and col4='?' and col5="?" and col6='?''?''?'`,
			wantSQL: `select 1, $1, 'test?test', "test?test", foo.* from` + "`foo`" + `where col1=$2 and col2='test' and col3=$3 and col4='?' and col5="?" and col6='?''?''?'`,
			want:    []string{"p1", "p2", "p3"},
		},
		"three positional parameters": {
			input:   `select * from foo where name=? and col2 like ? and col3 > ?`,
			wantSQL: `select * from foo where name=$1 and col2 like $2 and col3 > $3`,
			want:    []string{"p1", "p2", "p3"},
		},
		"two positional parameters": {
			input:   `select * from foo where id between ? and ?`,
			wantSQL: `select * from foo where id between $1 and $2`,
			want:    []string{"p1", "p2"},
		},
		"positional parameters in limit/offset": {
			input:   `select * from foo limit ? offset ?`,
			wantSQL: `select * from foo limit $1 offset $2`,
			want:    []string{"p1", "p2"},
		},
		"13 positional parameters": {
			input:   `select * from foo where col1=? and col2 like ? and col3 > ? and col4 < ? and col5 != ? and col6 not in (?, ?, ?) and col7 in (?, ?, ?) and col8 between ? and ?`,
			wantSQL: `select * from foo where col1=$1 and col2 like $2 and col3 > $3 and col4 < $4 and col5 != $5 and col6 not in ($6, $7, $8) and col7 in ($9, $10, $11) and col8 between $12 and $13`,
			want:    []string{"p1", "p2", "p3", "p4", "p5", "p6", "p7", "p8", "p9", "p10", "p11", "p12", "p13"},
		},
		"positional parameter compared to string literal with potential named parameter": {
			input:   `select * from foo where ?='''strange $1table'''`,
			wantSQL: `select * from foo where $1='''strange $1table'''`,
			want:    []string{"p1"},
		},
		"incomplete named parameter": {
			input:   `select foo from bar where id=$ order by value`,
			wantSQL: `select foo from bar where id=$ order by value`,
			want:    []string{},
		},
		"unclosed literal 1": {
			input: `?'?it\'?s
				?it\'?s'?`,
			wantSQL: `$1'?it\'$2s
				$3it\'?s'$4`,
			want: []string{"p1", "p2", "p3", "p4"},
		},
		"unclosed literal 2": {
			input: `?'?it\'?s
				?it\'?s?`,
			wantErr: spanner.ToSpannerError(status.Errorf(codes.InvalidArgument, "SQL statement contains an unclosed literal: %s", `?'?it\'?s
				?it\'?s?`)),
		},
		"unclosed literal 3": {
			input: `?'''?it\'?s
				?it\'?s'?`,
			wantSQL: `$1'''?it\'$2s
				$3it\'?s'$4`,
			want: []string{"p1", "p2", "p3", "p4"},
		},
	}
	parser, err := newStatementParser(databasepb.DatabaseDialect_POSTGRESQL, 1000)
	if err != nil {
		t.Fatal(err)
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			sql := tc.input
			gotSQL, got, err := parser.parseParameters(sql)
			if err != nil && tc.wantErr == nil {
				t.Error(err)
			}
			if tc.wantErr != nil {
				if err == nil {
					t.Errorf("missing expected error for %q", tc.input)
				} else if !cmp.Equal(err.Error(), tc.wantErr.Error()) {
					t.Errorf("parseParameters error mismatch\nGot: %s\nWant: %s", err.Error(), tc.wantErr)
				}
			}
			want := tc.want
			if !cmp.Equal(got, want) {
				t.Errorf("parseParameters result mismatch\n Got: %s\nWant: %s", got, want)
			}
			if !cmp.Equal(gotSQL, tc.wantSQL) {
				t.Errorf("parseParameters sql mismatch\n Got: %s\nWant: %s", gotSQL, tc.wantSQL)
			}
		})
	}
}

func FuzzFindParams(f *testing.F) {
	for _, sample := range fuzzQuerySamples {
		f.Add(sample)
	}

	parser, err := newStatementParser(databasepb.DatabaseDialect_GOOGLE_STANDARD_SQL, 1000)
	if err != nil {
		f.Fatal(err)
	}
	f.Fuzz(func(t *testing.T, input string) {
		_, _, _ = parser.parseParameters(input)
	})
}

// Note: The detectStatementType function does not check validity of a statement,
// only whether the statement begins with a DDL instruction.
// Actual validity checks are performed by the database.
func TestStatementIsDdl(t *testing.T) {
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
		{
			name:  "short input",
			input: "",
			want:  false,
		},
		{
			name:  "empty input",
			input: "",
			want:  false,
		},
		{
			name:  "input with only spaces",
			input: "    \t\n  ",
			want:  false,
		},
		{
			name:  "analyze",
			input: `analyze`,
			want:  true,
		},
		{
			name:  "grant",
			input: `GRANT SELECT ON TABLE employees TO ROLE hr_rep;`,
			want:  true,
		},
		{
			name:  "revoke",
			input: `REVOKE SELECT ON TABLE employees TO ROLE hr_rep;`,
			want:  true,
		},
		{
			name:  "rename",
			input: `rename table foo to bar`,
			want:  true,
		},
	}

	parser, err := newStatementParser(databasepb.DatabaseDialect_GOOGLE_STANDARD_SQL, 1000)
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range tests {
		got := parser.detectStatementType(tc.input).statementType == statementTypeDdl
		if got != tc.want {
			t.Errorf("isDDL test failed, %s: wanted %t got %t.", tc.name, tc.want, got)
		}
	}
}

func FuzzIsDdl(f *testing.F) {
	for _, sample := range fuzzQuerySamples {
		f.Add(sample)
	}

	parser, err := newStatementParser(databasepb.DatabaseDialect_GOOGLE_STANDARD_SQL, 1000)
	if err != nil {
		f.Fatal(err)
	}
	f.Fuzz(func(t *testing.T, input string) {
		_ = parser.isDDL(input)
	})
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
		statement, err := parseClientSideStatement(&conn{logger: noopLogger}, tc.input)
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

func FuzzParseClientSideStatement(f *testing.F) {
	for _, sample := range fuzzQuerySamples {
		f.Add(sample)
	}

	f.Fuzz(func(t *testing.T, input string) {
		_, _ = parseClientSideStatement(&conn{logger: noopLogger}, input)
	})
}

func TestRemoveCommentsAndTrim_Errors(t *testing.T) {
	parser, err := newStatementParser(databasepb.DatabaseDialect_GOOGLE_STANDARD_SQL, 1000)
	if err != nil {
		t.Fatal(err)
	}
	_, err = parser.removeCommentsAndTrim("SELECT 'Hello World FROM SomeTable")
	if g, w := spanner.ErrCode(err), codes.InvalidArgument; g != w {
		t.Errorf("error code mismatch\nGot: %v\nWant: %v\n", g, w)
	}
	_, err = parser.removeCommentsAndTrim("SELECT 'Hello World\nFROM SomeTable")
	if g, w := spanner.ErrCode(err), codes.InvalidArgument; g != w {
		t.Errorf("error code mismatch\nGot: %v\nWant: %v\n", g, w)
	}
}

func TestFindParams_Errors(t *testing.T) {
	parser, err := newStatementParser(databasepb.DatabaseDialect_GOOGLE_STANDARD_SQL, 1000)
	if err != nil {
		t.Fatal(err)
	}
	_, _, err = parser.findParams("SELECT 'Hello World FROM SomeTable WHERE id=@id")
	if g, w := spanner.ErrCode(err), codes.InvalidArgument; g != w {
		t.Errorf("error code mismatch\nGot: %v\nWant: %v\n", g, w)
	}
	_, _, err = parser.findParams("SELECT 'Hello World\nFROM SomeTable WHERE id=@id")
	if g, w := spanner.ErrCode(err), codes.InvalidArgument; g != w {
		t.Errorf("error code mismatch\nGot: %v\nWant: %v\n", g, w)
	}
}

func TestSkip(t *testing.T) {
	tests := map[string]struct {
		input   string
		pos     int
		skipped map[databasepb.DatabaseDialect]string
		invalid map[databasepb.DatabaseDialect]bool
	}{
		"empty string": {
			input:   "",
			skipped: map[databasepb.DatabaseDialect]string{databasepb.DatabaseDialect_DATABASE_DIALECT_UNSPECIFIED: ""},
		},
		"single digit": {
			input:   "1 ",
			skipped: map[databasepb.DatabaseDialect]string{databasepb.DatabaseDialect_DATABASE_DIALECT_UNSPECIFIED: "1"},
		},
		"double digit": {
			input:   "12 ",
			skipped: map[databasepb.DatabaseDialect]string{databasepb.DatabaseDialect_DATABASE_DIALECT_UNSPECIFIED: "1"},
		},
		"double digit, pos 1": {
			input:   "12 ",
			pos:     1,
			skipped: map[databasepb.DatabaseDialect]string{databasepb.DatabaseDialect_DATABASE_DIALECT_UNSPECIFIED: "2"},
		},
		"end of statement": {
			input:   "12",
			pos:     2,
			skipped: map[databasepb.DatabaseDialect]string{databasepb.DatabaseDialect_DATABASE_DIALECT_UNSPECIFIED: ""},
		},
		"string": {
			input:   "'foo'  ",
			skipped: map[databasepb.DatabaseDialect]string{databasepb.DatabaseDialect_DATABASE_DIALECT_UNSPECIFIED: "'foo'"},
		},
		"two strings directly after each other": {
			input: "'foo''bar'  ",
			skipped: map[databasepb.DatabaseDialect]string{
				databasepb.DatabaseDialect_GOOGLE_STANDARD_SQL: "'foo'",
				databasepb.DatabaseDialect_POSTGRESQL:          "'foo''bar'",
			},
		},
		"two strings with space between": {
			input:   "'foo'  'bar'  ",
			skipped: map[databasepb.DatabaseDialect]string{databasepb.DatabaseDialect_DATABASE_DIALECT_UNSPECIFIED: "'foo'"},
		},
		"two strings directly after each other, starting at second string": {
			input:   "'foo''bar'  ",
			pos:     5,
			skipped: map[databasepb.DatabaseDialect]string{databasepb.DatabaseDialect_DATABASE_DIALECT_UNSPECIFIED: "'bar'"},
		},
		"string with quoted string inside": {
			input:   `'foo"bar"'  `,
			skipped: map[databasepb.DatabaseDialect]string{databasepb.DatabaseDialect_DATABASE_DIALECT_UNSPECIFIED: `'foo"bar"'`},
		},
		"double-quoted string with string inside": {
			input:   `"foo'bar'"  `,
			skipped: map[databasepb.DatabaseDialect]string{databasepb.DatabaseDialect_DATABASE_DIALECT_UNSPECIFIED: `"foo'bar'"`},
		},
		"backtick string with string inside": {
			input: "`foo'bar'`  ",
			skipped: map[databasepb.DatabaseDialect]string{
				databasepb.DatabaseDialect_GOOGLE_STANDARD_SQL: "`foo'bar'`",
				databasepb.DatabaseDialect_POSTGRESQL:          "`",
			},
		},
		"triple-quoted string with quote inside": {
			input: "'''foo'bar'''  ",
			skipped: map[databasepb.DatabaseDialect]string{
				databasepb.DatabaseDialect_GOOGLE_STANDARD_SQL: "'''foo'bar'''",
				databasepb.DatabaseDialect_POSTGRESQL:          "'''foo'",
			},
		},
		"triple-quoted string with escaped quote inside": {
			input: "'''foo\\'bar'''  ",
			skipped: map[databasepb.DatabaseDialect]string{
				databasepb.DatabaseDialect_GOOGLE_STANDARD_SQL: "'''foo\\'bar'''",
				databasepb.DatabaseDialect_POSTGRESQL:          "'''foo\\'",
			},
		},
		"triple-quoted string with two escaped quotes inside": {
			input: "'''foo\\'\\'bar'''  ",
			skipped: map[databasepb.DatabaseDialect]string{
				databasepb.DatabaseDialect_GOOGLE_STANDARD_SQL: "'''foo\\'\\'bar'''",
				databasepb.DatabaseDialect_POSTGRESQL:          "'''foo\\'",
			},
		},
		"triple-quoted string with three escaped quotes inside": {
			input: "'''foo\\'\\'\\'bar'''  ",
			skipped: map[databasepb.DatabaseDialect]string{
				databasepb.DatabaseDialect_GOOGLE_STANDARD_SQL: "'''foo\\'\\'\\'bar'''",
				databasepb.DatabaseDialect_POSTGRESQL:          "'''foo\\'",
			},
		},
		"triple-quoted backtick string with backtick inside": {
			input: "```foo`bar```  ",
			skipped: map[databasepb.DatabaseDialect]string{
				databasepb.DatabaseDialect_GOOGLE_STANDARD_SQL: "```foo`bar```",
				// Backticks are not valid quote characters in PostgreSQL, so only a single character is skipped.
				databasepb.DatabaseDialect_POSTGRESQL: "`",
			},
		},
		"triple-double quote string with double quote inside": {
			input: `"""foo"bar"""  `,
			skipped: map[databasepb.DatabaseDialect]string{
				databasepb.DatabaseDialect_GOOGLE_STANDARD_SQL: `"""foo"bar"""`,
				databasepb.DatabaseDialect_POSTGRESQL:          `"""foo"`,
			},
		},
		"single line comment": {
			input:   "-- comment",
			skipped: map[databasepb.DatabaseDialect]string{databasepb.DatabaseDialect_DATABASE_DIALECT_UNSPECIFIED: "-- comment"},
		},
		"single line comment followed by select": {
			input:   "-- comment\nselect * from foo",
			skipped: map[databasepb.DatabaseDialect]string{databasepb.DatabaseDialect_DATABASE_DIALECT_UNSPECIFIED: "-- comment\n"},
		},
		"single line comment (#)": {
			input: "# comment",
			skipped: map[databasepb.DatabaseDialect]string{
				databasepb.DatabaseDialect_GOOGLE_STANDARD_SQL: "# comment",
				// PostgreSQL does not consider '#' to be the start of a single line comment.
				databasepb.DatabaseDialect_POSTGRESQL: "#",
			},
		},
		"single line comment (#) followed by select": {
			input: "# comment\nselect * from foo",
			skipped: map[databasepb.DatabaseDialect]string{
				databasepb.DatabaseDialect_GOOGLE_STANDARD_SQL: "# comment\n",
				// PostgreSQL does not consider '#' to be the start of a single line comment.
				databasepb.DatabaseDialect_POSTGRESQL: "#",
			},
		},
		"multi-line comment": {
			input:   "/* comment */",
			skipped: map[databasepb.DatabaseDialect]string{databasepb.DatabaseDialect_DATABASE_DIALECT_UNSPECIFIED: "/* comment */"},
		},
		"multi-line comment followed by select": {
			input:   "/* comment */ select * from foo",
			skipped: map[databasepb.DatabaseDialect]string{databasepb.DatabaseDialect_DATABASE_DIALECT_UNSPECIFIED: "/* comment */"},
		},
		"nested comment": {
			input: "/* comment /* GoogleSQL does not support nested comments */ select * from foo",
			skipped: map[databasepb.DatabaseDialect]string{
				databasepb.DatabaseDialect_GOOGLE_STANDARD_SQL: "/* comment /* GoogleSQL does not support nested comments */",
				databasepb.DatabaseDialect_POSTGRESQL:          "/* comment /* GoogleSQL does not support nested comments */ select * from foo",
			},
		},
		"nested comment 2": {
			input: "/* comment /* GoogleSQL does not support nested comments */ But PostgreSQL does support them */ select * from foo ",
			skipped: map[databasepb.DatabaseDialect]string{
				databasepb.DatabaseDialect_GOOGLE_STANDARD_SQL: "/* comment /* GoogleSQL does not support nested comments */",
				databasepb.DatabaseDialect_POSTGRESQL:          "/* comment /* GoogleSQL does not support nested comments */ But PostgreSQL does support them */",
			},
		},
		"dollar-quoted string": {
			// GoogleSQL does not support dollar-quoted strings.
			input:   "$tag$not a string$tag$ select * from foo",
			skipped: map[databasepb.DatabaseDialect]string{databasepb.DatabaseDialect_DATABASE_DIALECT_UNSPECIFIED: "$"},
		},
		"string in comment": {
			input:   "/* 'test' */ foo",
			skipped: map[databasepb.DatabaseDialect]string{databasepb.DatabaseDialect_DATABASE_DIALECT_UNSPECIFIED: "/* 'test' */"},
		},
		"string in single-line comment": {
			input:   "-- 'test' \n foo",
			skipped: map[databasepb.DatabaseDialect]string{databasepb.DatabaseDialect_DATABASE_DIALECT_UNSPECIFIED: "-- 'test' \n"},
		},
		"string in single-line comment (#)": {
			input: "# 'test' \n foo",
			skipped: map[databasepb.DatabaseDialect]string{
				databasepb.DatabaseDialect_GOOGLE_STANDARD_SQL: "# 'test' \n",
				databasepb.DatabaseDialect_POSTGRESQL:          "#",
			},
		},
		"comment in string": {
			input:   "'/* test */' foo",
			skipped: map[databasepb.DatabaseDialect]string{databasepb.DatabaseDialect_DATABASE_DIALECT_UNSPECIFIED: "'/* test */'"},
		},
		"escaped quote": {
			input: "'foo\\''  ",
			skipped: map[databasepb.DatabaseDialect]string{
				databasepb.DatabaseDialect_GOOGLE_STANDARD_SQL: "'foo\\''",
			},
			invalid: map[databasepb.DatabaseDialect]bool{
				// This is invalid for PostgreSQL, because:
				// 1. The first 'foo\ part of the string is 'just a normal string'. The backslash has no special meaning.
				// 2. The last part is two single quotes; In PostgreSQL, this is an escaped quote inside the string
				// literal. This again means that this string literal is unclosed.
				databasepb.DatabaseDialect_POSTGRESQL: true,
			},
		},
		"escaped quote in raw string": {
			input: "r'foo\\''  ",
			pos:   1,
			skipped: map[databasepb.DatabaseDialect]string{
				databasepb.DatabaseDialect_GOOGLE_STANDARD_SQL: "'foo\\''",
			},
			invalid: map[databasepb.DatabaseDialect]bool{
				// This is an unclosed literal in PostgreSQL
				databasepb.DatabaseDialect_POSTGRESQL: true,
			},
		},
		"escaped quotes in triple-quoted string": {
			input: "'''foo\\'\\'\\'bar'''  ",
			skipped: map[databasepb.DatabaseDialect]string{
				databasepb.DatabaseDialect_GOOGLE_STANDARD_SQL: "'''foo\\'\\'\\'bar'''",
				databasepb.DatabaseDialect_POSTGRESQL:          "'''foo\\'",
			},
		},
		"string with linefeed": {
			input: "'foo\n'  ",
			skipped: map[databasepb.DatabaseDialect]string{
				databasepb.DatabaseDialect_POSTGRESQL: "'foo\n'",
			},
			invalid: map[databasepb.DatabaseDialect]bool{
				databasepb.DatabaseDialect_GOOGLE_STANDARD_SQL: true,
			},
		},
		"triple-quoted string with linefeed": {
			input:   "'''foo\n'''  ",
			skipped: map[databasepb.DatabaseDialect]string{databasepb.DatabaseDialect_DATABASE_DIALECT_UNSPECIFIED: "'''foo\n'''"},
		},
		"multibyte character in string": {
			input:   "'⌘'",
			skipped: map[databasepb.DatabaseDialect]string{databasepb.DatabaseDialect_DATABASE_DIALECT_UNSPECIFIED: "'⌘'"},
		},
		"multibyte character in string (2)": {
			input:   "'€'",
			skipped: map[databasepb.DatabaseDialect]string{databasepb.DatabaseDialect_DATABASE_DIALECT_UNSPECIFIED: "'€'"},
		},
		"multibyte character in string (3)": {
			input:   "'€ 100,-'  ",
			skipped: map[databasepb.DatabaseDialect]string{databasepb.DatabaseDialect_DATABASE_DIALECT_UNSPECIFIED: "'€ 100,-'"},
		},
		"multibyte character in string (4)": {
			input:   "'𒀀'",
			skipped: map[databasepb.DatabaseDialect]string{databasepb.DatabaseDialect_DATABASE_DIALECT_UNSPECIFIED: "'𒀀'"},
		},
	}
	for _, dialect := range []databasepb.DatabaseDialect{databasepb.DatabaseDialect_GOOGLE_STANDARD_SQL, databasepb.DatabaseDialect_POSTGRESQL} {
		parser, err := newStatementParser(dialect, 1000)
		if err != nil {
			t.Fatal(err)
		}
		for name, test := range tests {
			t.Run(fmt.Sprintf("Dialect: %v, test: %v", databasepb.DatabaseDialect_name[int32(dialect)], name), func(t *testing.T) {
				pos, err := parser.skip([]byte(test.input), test.pos)
				wantInvalid := expectedTestValue(dialect, test.invalid)
				if wantInvalid && err == nil {
					t.Errorf("missing expected error for %s", test.input)
				} else if !wantInvalid && err != nil {
					t.Errorf("got unexpected error for %s: %v", test.input, err)
				} else if !wantInvalid {
					skipped := test.input[test.pos:pos]
					wantSkipped := expectedTestValue(dialect, test.skipped)
					if skipped != wantSkipped {
						t.Errorf("skipped mismatch\nGot:  %v\nWant: %v", skipped, wantSkipped)
					}
				}
			})
		}
	}
}

func expectedTestValue[V any](dialect databasepb.DatabaseDialect, values map[databasepb.DatabaseDialect]V) V {
	if values == nil {
		var zero V
		return zero
	}
	if v, ok := values[dialect]; ok {
		return v
	} else if v, ok := values[databasepb.DatabaseDialect_DATABASE_DIALECT_UNSPECIFIED]; ok {
		return v
	} else {
		var zero V
		return zero
	}
}

func TestSkipStatementHint(t *testing.T) {
	tests := []struct {
		input string
		want  string
	}{
		{
			input: "select * from foo",
			want:  "select * from foo",
		},
		{
			input: "@{key=value}select * from foo",
			want:  "select * from foo",
		},
		{
			input: "  @   {  key=value  }select * from foo",
			want:  "select * from foo",
		},
		{
			input: " \t @  \n {  key=value  }\nselect * from foo",
			want:  "\nselect * from foo",
		},
	}
	statementParser, err := newStatementParser(databasepb.DatabaseDialect_GOOGLE_STANDARD_SQL, 1000)
	if err != nil {
		t.Fatal(err)
	}
	for _, test := range tests {
		p := &simpleParser{sql: []byte(test.input), statementParser: statementParser}
		p.skipStatementHint()
		if g, w := test.input[p.pos:], test.want; g != w {
			t.Errorf("unexpected query string after statement hint %q\n Got: %v\nWant: %v", test.input, g, w)
		}

	}
}

func TestReadKeyword(t *testing.T) {
	tests := []struct {
		input string
		want  string
	}{
		{
			input: "select * from my_table",
			want:  "select",
		},
		{
			input: "select\n* from my_table",
			want:  "select",
		},
		{
			input: "  \t select\n* from my_table",
			want:  "select",
		},
		{
			input: "/*comment*/select/*comment*/* from my_table",
			want:  "select",
		},
		{
			input: "-- comment\nselect--comment\n* from my_table",
			want:  "select",
		},
		{
			input: "insert into my_table (id, value) values (1, 'one')",
			want:  "insert",
		},
		{
			input: "update my_table set value='two' where id = 1",
			want:  "update",
		},
		{
			input: "update",
			want:  "update",
		},
		{
			input: "select* from my_table",
			want:  "select",
		},
		{
			input: "select(1) from my_table",
			want:  "select",
		},
		{
			input: "SELECT * from my_table",
			want:  "SELECT",
		},
		{
			input: "Select from my_table",
			want:  "Select",
		},
	}
	statementParser, err := newStatementParser(databasepb.DatabaseDialect_GOOGLE_STANDARD_SQL, 1000)
	if err != nil {
		t.Fatal(err)
	}
	for _, test := range tests {
		p := simpleParser{sql: []byte(test.input), statementParser: statementParser}
		if g, w := p.readKeyword(), test.want; g != w {
			t.Errorf("keyword mismatch for %q\n Got: %v\nWant: %v", test.input, g, w)
		}
	}
}

type detectStatementTypeTest struct {
	input string
	want  statementType
}

func generateDetectStatementTypeTests() []detectStatementTypeTest {
	return []detectStatementTypeTest{
		{
			input: "select 1",
			want:  statementTypeQuery,
		},
		{
			input: "from test",
			want:  statementTypeQuery,
		},
		{
			input: "with t as (select 1) select * from t",
			want:  statementTypeQuery,
		},
		{
			input: "GRAPH FinGraph\nMATCH (n)\nRETURN LABELS(n) AS label, n.id",
			want:  statementTypeQuery,
		},
		{
			input: "/* this is a comment */ -- this is also a comment\n @  { statement_hint_key=value } select 1",
			want:  statementTypeQuery,
		},
		{
			input: "update foo set bar=1 where true",
			want:  statementTypeDml,
		},
		{
			input: "insert into foo (id, value) select 1, 'test'",
			want:  statementTypeDml,
		},
		{
			input: "delete from foo where true",
			want:  statementTypeDml,
		},
		{
			input: "delete from foo where true then return *",
			want:  statementTypeDml,
		},
		{
			input: "create table foo (id int64) primary key (id)",
			want:  statementTypeDdl,
		},
		{
			input: "drop table if exists foo",
			want:  statementTypeDdl,
		},
		{
			input: "input from borkisland",
			want:  statementTypeUnknown,
		},
	}
}

func TestDetectStatementType(t *testing.T) {
	parser, err := newStatementParser(databasepb.DatabaseDialect_GOOGLE_STANDARD_SQL, 1000)
	if err != nil {
		t.Fatal(err)
	}
	tests := generateDetectStatementTypeTests()
	for _, test := range tests {
		if g, w := parser.detectStatementType(test.input).statementType, test.want; g != w {
			t.Errorf("statement type mismatch for %q\n Got: %v\nWant: %v", test.input, g, w)
		}
	}
}

func BenchmarkDetectStatementType(b *testing.B) {
	parser, err := newStatementParser(databasepb.DatabaseDialect_GOOGLE_STANDARD_SQL, 1000)
	if err != nil {
		b.Fatal(err)
	}
	tests := generateDetectStatementTypeTests()
	for b.Loop() {
		for _, test := range tests {
			if g, w := parser.detectStatementType(test.input).statementType, test.want; g != w {
				b.Errorf("statement type mismatch for %q\n Got: %v\nWant: %v", test.input, g, w)
			}
		}
	}
}

var fuzzQuerySamples = []string{"", "SELECT 1;", "RUN BATCH", "ABORT BATCH", "Show variable Retry_Aborts_Internally", "@{JOIN_METHOD=HASH_JOIN SELECT * FROM PersonsTable"}

func init() {
	for ddl := range ddlStatements {
		fuzzQuerySamples = append(fuzzQuerySamples, ddl)
	}
	for ddl := range selectStatements {
		fuzzQuerySamples = append(fuzzQuerySamples, ddl)
	}
	for ddl := range dmlStatements {
		fuzzQuerySamples = append(fuzzQuerySamples, ddl)
	}
}
