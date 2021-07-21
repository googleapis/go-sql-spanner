// Copyright 2021 Google Inc. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package internal

import (
	"github.com/google/go-cmp/cmp"
	"testing"
)

func TestRemoveCommentsAndTrim(t *testing.T) {
	tests := []struct {
		input   string
		want    string
		wantErr bool
	}{
		{
			input: ``,
			want: ``,
		},
		{
			input: `SELECT 1;`,
			want: `SELECT 1`,
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
			want: `SELECT "TEST -- This is not a comment"`,
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
			want: `SELECT "TEST # This is not a comment"`,
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
			want: `SELECT "TEST /* This is not a comment */"`,
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
			want: `SELECT 'TEST -- This is not a comment'`,
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
			want: `SELECT 'TEST # This is not a comment'`,
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
			want: `SELECT 'TEST /* This is not a comment */'`,
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
			want:  []string{"name"},
		},
		{
			input: `@{JOIN_METHOD=HASH_JOIN} SELECT * FROM PersonsTable WHERE Name like @name AND Email='test@test.com'`,
			want:  []string{"name"},
		},
	}
	for _, tc := range tests {
		sql, err := removeCommentsAndTrim(tc.input)
		if err != nil {
			t.Fatal(err)
		}
		got, err := FindParams(removeStatementHint(sql))
		if err != nil && !tc.wantErr {
			t.Error(err)
			continue
		}
		if tc.wantErr {
			t.Errorf("missing expected error for %q", tc.input)
			continue
		}
		if !cmp.Equal(got, tc.want) {
			t.Errorf("FindParams result mismatch\nGot: %s\nWant: %s", got, tc.want)
		}
	}
}
