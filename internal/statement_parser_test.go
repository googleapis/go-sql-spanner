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

import "testing"

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
		got, err := RemoveCommentsAndTrim(tc.input)
		if err != nil && !tc.wantErr {
			t.Error(err)
			continue
		}
		if tc.wantErr {
			t.Errorf("missing expected error for %q", tc.input)
			continue
		}
		if got != tc.want {
			t.Errorf("RemoveCommentsAndTrim result mismatch\nGot: %q\nWant: %q", got, tc.want)
		}
	}
}
