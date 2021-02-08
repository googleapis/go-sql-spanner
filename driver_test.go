// Copyright 2020 Google Inc. All Rights Reserved.
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

package spannerdriver

import (
	"context"
	"database/sql"
	"os"
	"reflect"
	"testing"
)

var (
	dsn string
)

func init() {

	var projectId, instanceId, databaseId string
	var ok bool

	// Get environment variables or set to default.
	if instanceId, ok = os.LookupEnv("SPANNER_TEST_INSTANCE"); !ok {
		instanceId = "test-instance"
	}
	if projectId, ok = os.LookupEnv("SPANNER_TEST_PROJECT"); !ok {
		projectId = "test-project"
	}
	if databaseId, ok = os.LookupEnv("SPANNER_TEST_DBID"); !ok {
		databaseId = "gotest"
	}

	// Derive data source name.
	dsn = "projects/" + projectId + "/instances/" + instanceId + "/databases/" + databaseId
}

func TestQueryContext(t *testing.T) {

	// Open db.
	ctx := context.Background()
	db, err := sql.Open("spanner", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// Set up test table.
	_, err = db.ExecContext(ctx,
		`CREATE TABLE TestQueryContext (
			A   STRING(1024),
			B  STRING(1024),
			C   STRING(1024)
		)	 PRIMARY KEY (A)`)
	if err != nil {
		t.Fatal(err)
	}

	_, err = db.ExecContext(ctx, `INSERT INTO TestQueryContext (A, B, C) 
	VALUES ("a1", "b1", "c1"), ("a2", "b2", "c2") , ("a3", "b3", "c3") `)
	if err != nil {
		t.Fatal(err)
	}

	type testQueryContextRow struct {
		A, B, C string
	}

	tests := []struct {
		name           string
		input          string
		want           []testQueryContextRow
		wantErrorQuery bool
		wantErrorScan  bool
		wantErrorClose bool
	}{
		{
			name:           "empty query",
			wantErrorClose: true,
			input:          "",
			want:           []testQueryContextRow{},
		},
		{
			name:           "syntax error",
			wantErrorClose: true,
			input:          "SELECT SELECT * FROM TestQueryContext",
			want:           []testQueryContextRow{},
		},
		{
			name:  "return nothing",
			input: "SELECT * FROM TestQueryContext WHERE A = \"hihihi\"",
			want:  []testQueryContextRow{},
		},
		{
			name:  "select one tuple",
			input: "SELECT * FROM TestQueryContext WHERE A = \"a1\"",
			want: []testQueryContextRow{
				{A: "a1", B: "b1", C: "c1"},
			},
		},
		{
			name:  "select subset of tuples",
			input: "SELECT * FROM TestQueryContext WHERE A = \"a1\" OR A = \"a2\"",
			want: []testQueryContextRow{
				{A: "a1", B: "b1", C: "c1"},
				{A: "a2", B: "b2", C: "c2"},
			},
		},
		{
			name:  "select subset of tuples with !=",
			input: "SELECT * FROM TestQueryContext WHERE A != \"a3\"",
			want: []testQueryContextRow{
				{A: "a1", B: "b1", C: "c1"},
				{A: "a2", B: "b2", C: "c2"},
			},
		},
		{
			name:  "select entire table",
			input: "SELECT * FROM TestQueryContext ORDER BY A",
			want: []testQueryContextRow{
				{A: "a1", B: "b1", C: "c1"},
				{A: "a2", B: "b2", C: "c2"},
				{A: "a3", B: "b3", C: "c3"},
			},
		},
		{
			name:           "query non existent table",
			wantErrorClose: true,
			input:          "SELECT * FROM NonExistent",
			want:           []testQueryContextRow{},
		},
	}

	// Run tests
	for _, tc := range tests {

		rows, err := db.QueryContext(ctx, tc.input)
		if (err != nil) && (!tc.wantErrorQuery) {
			t.Errorf("%s: unexpected query error: %v", tc.name, err)
		}
		if (err == nil) && (tc.wantErrorQuery) {
			t.Errorf("%s: expected query error but error was %v", tc.name, err)
		}

		got := []testQueryContextRow{}
		for rows.Next() {
			var curr testQueryContextRow
			err := rows.Scan(&curr.A, &curr.B, &curr.C)
			if (err != nil) && (!tc.wantErrorScan) {
				t.Errorf("%s: unexpected query error: %v", tc.name, err)
			}
			if (err == nil) && (tc.wantErrorScan) {
				t.Errorf("%s: expected query error but error was %v", tc.name, err)
			}

			got = append(got, curr)
		}

		rows.Close()
		err = rows.Err()
		if (err != nil) && (!tc.wantErrorClose) {
			t.Errorf("%s: unexpected query error: %v", tc.name, err)
		}
		if (err == nil) && (tc.wantErrorClose) {
			t.Errorf("%s: expected query error but error was %v", tc.name, err)
		}
		if !reflect.DeepEqual(tc.want, got) {
			t.Errorf("Test failed: %s. want: %v, got: %v", tc.name, tc.want, got)
		}

	}

	// Drop table.
	if _, err = db.ExecContext(ctx, `DROP TABLE TestQueryContext`); err != nil {
		t.Error(err)
	}
}

// note: isDdl function does not check validity of statement
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
	}

	for _, tc := range tests {
		got, err := isDdl(tc.input)
		if err != nil {
			t.Error(err)
		}
		if got != tc.want {
			t.Errorf("isDdl test failed, %s: wanted %t got %t.", tc.name, tc.want, got)
		}
	}
}

func TestExecContextDml(t *testing.T) {

	// Open db.
	ctx := context.Background()
	db, err := sql.Open("spanner", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// Set up test table.
	_, err = db.ExecContext(ctx,
		`CREATE TABLE TestExecContextDml (
			key	INT64,
			testString	STRING(1024),
			testBytes	BYTES(1024),
			testInt	INT64,
			testFloat	FLOAT64,
			testBool	BOOL
		) PRIMARY KEY (key)`)
	if err != nil {
		t.Fatal(err)
	}

	type testExecContextDmlRow struct {
		key        int
		testString string
		testBytes  []byte
		testInt    int
		testFloat  float64
		testBool   bool
	}

	tests := []struct {
		name        string
		givenInput  string
		whenQueried string
		setUp       string
		wantRows    []testExecContextDmlRow
		wantError   bool
	}{
		{
			name: "insert single tuple",
			givenInput: `
				INSERT INTO TestExecContextDml 
				(key, testString, testBytes, testInt, testFloat, testBool)
				VALUES (1, "one", CAST("one" as bytes), 42, 42, true ) `,
			whenQueried: `SELECT * FROM TestExecContextDml WHERE key = 1`,
			wantRows: []testExecContextDmlRow{
				{key: 1, testString: "one", testBytes: []byte("one"), testInt: 42, testFloat: 42, testBool: true},
			},
		},
		{
			name: "insert multiple tuples",
			givenInput: `
				INSERT INTO TestExecContextDml 
				(key, testString, testBytes, testInt, testFloat, testBool)
				VALUES (2, "two", CAST("two" as bytes), 42, 42, true ), (3, "three", CAST("three" as bytes), 42, 42, true )`,
			whenQueried: `SELECT * FROM TestExecContextDml ORDER BY key`,
			wantRows: []testExecContextDmlRow{
				{key: 2, testString: "two", testBytes: []byte("two"), testInt: 42, testFloat: 42, testBool: true},
				{key: 3, testString: "three", testBytes: []byte("three"), testInt: 42, testFloat: 42, testBool: true},
			},
		},
		{
			name: "insert syntax error",
			givenInput: `
				INSERT INSERT INTO TestExecContextDml 
				(key, testString, testBytes, testInt, testFloat, testBool)
				VALUES (101, "one", CAST("one" as bytes), 42, 42, true ) `,
			wantError: true,
		},
		{
			name: "insert lower case dml",
			givenInput: `
				insert into TestExecContextDml 
				(key, testString, testBytes, testInt, testFloat, testBool)
				VALUES (4, "four", CAST("four" as bytes), 42, 42, true ) `,
			whenQueried: `SELECT * FROM TestExecContextDml ORDER BY key`,
			wantRows: []testExecContextDmlRow{
				{key: 4, testString: "four", testBytes: []byte("four"), testInt: 42, testFloat: 42, testBool: true},
			},
		},
		{
			name: "insert lower case table name",
			givenInput: `
				INSERT INTO testexeccontextdml (key, testString, testBytes, testInt, testFloat, testBool)
				VALUES (5, "five", CAST("five" as bytes), 42, 42, true ) `,
			whenQueried: `SELECT * FROM TestExecContextDml ORDER BY key`,
			wantRows: []testExecContextDmlRow{
				{key: 5, testString: "five", testBytes: []byte("five"), testInt: 42, testFloat: 42, testBool: true},
			},
		},
		{
			name: "wrong types",
			givenInput: `
				INSERT INSERT INTO TestExecContextDml (key, testString, testBytes, testInt, testFloat, testBool)
				VALUES (102, 56, 56, 42, hello, "i should be a bool" ) `,
			wantError: true,
		},
		{
			name: "insert primary key duplicate",
			givenInput: `
				INSERT INSERT INTO TestExecContextDml (key, testString, testBytes, testInt, testFloat, testBool)
				VALUES (1, "one", CAST("one" as bytes), 42, 42, true ), (1, "one", CAST("one" as bytes), 42, 42, true ) `,
			wantError: true,
		},
		{
			name: "insert null into primary key",
			givenInput: `
				INSERT INSERT INTO TestExecContextDml 
				(key, testString, testBytes, testInt, testFloat, testBool)
				VALUES (null, "one", CAST("one" as bytes), 42, 42, true ) `,
			wantError: true,
		},
		{
			name: "insert too many values",
			givenInput: `
				INSERT INSERT INTO TestExecContextDml 
				(key, testString, testBytes, testInt, testFloat, testBool)
				VALUES (null, "one", CAST("one" as bytes), 42, 42, true, 42 ) `,
			wantError: true,
		},
		{
			name: "insert too few values",
			givenInput: `
				INSERT INSERT INTO TestExecContextDml 
				(key, testString, testBytes, testInt, testFloat, testBool)
				VALUES (null, "one", CAST("one" as bytes), 42 ) `,
			wantError: true,
		},
		{
			name: "delete single tuple",
			setUp: `
				INSERT INTO TestExecContextDml (key, testString, testBytes, testInt, testFloat, testBool)
				VALUES (1, "one", CAST("one" as bytes), 42, 42, true ), (2, "two", CAST("two" as bytes), 42, 42, true )`,
			givenInput:  `DELETE FROM TestExecContextDml WHERE key = 1`,
			whenQueried: `SELECT * FROM TestExecContextDml`,
			wantRows: []testExecContextDmlRow{
				{key: 2, testString: "two", testBytes: []byte("two"), testInt: 42, testFloat: 42, testBool: true},
			},
		},
		{
			name: "delete multiple tuples with subwhenQueried",
			setUp: `
				INSERT INTO TestExecContextDml (key, testString, testBytes, testInt, testFloat, testBool)
				VALUES (1, "one", CAST("one" as bytes), 42, 42, true ), (2, "two", CAST("two" as bytes), 42, 42, true ),
				(3, "three", CAST("three" as bytes), 42, 42, true ), (4, "four", CAST("four" as bytes), 42, 42, true )`,
			givenInput: `
				DELETE FROM TestExecContextDml WHERE key
				IN (SELECT key FROM TestExecContextDml WHERE key != 4)`,
			whenQueried: `SELECT * FROM TestExecContextDml ORDER BY key`,
			wantRows: []testExecContextDmlRow{
				{key: 4, testString: "four", testBytes: []byte("four"), testInt: 42, testFloat: 42, testBool: true},
			},
		},
		{
			name: "delete all tuples",
			setUp: `
				INSERT INTO TestExecContextDml (key, testString, testBytes, testInt, testFloat, testBool)
				VALUES (1, "one", CAST("one" as bytes), 42, 42, true ), (2, "two", CAST("two" as bytes), 42, 42, true ),
				(3, "three", CAST("three" as bytes), 42, 42, true ), (4, "four", CAST("four" as bytes), 42, 42, true )`,
			givenInput:  `DELETE FROM TestExecContextDml WHERE true`,
			whenQueried: `SELECT * FROM TestExecContextDml`,
			wantRows:    []testExecContextDmlRow{},
		},
		{
			name: "update one tuple",
			setUp: `
				INSERT INTO TestExecContextDml (key, testString, testBytes, testInt, testFloat, testBool)
				VALUES (1, "one", CAST("one" as bytes), 42, 42, true ), (2, "two", CAST("two" as bytes), 42, 42, true ),
				(3, "three", CAST("three" as bytes), 42, 42, true )`,
			givenInput:  `UPDATE TestExecContextDml SET testSTring = "updated" WHERE key = 1`,
			whenQueried: `SELECT * FROM TestExecContextDml ORDER BY key`,
			wantRows: []testExecContextDmlRow{
				{key: 1, testString: "updated", testBytes: []byte("one"), testInt: 42, testFloat: 42, testBool: true},
				{key: 2, testString: "two", testBytes: []byte("two"), testInt: 42, testFloat: 42, testBool: true},
				{key: 3, testString: "three", testBytes: []byte("three"), testInt: 42, testFloat: 42, testBool: true},
			},
		},
		{
			name: "update multiple tuples",
			setUp: `
				INSERT INTO TestExecContextDml (key, testString, testBytes, testInt, testFloat, testBool)
				VALUES (1, "one", CAST("one" as bytes), 42, 42, true ), (2, "two", CAST("two" as bytes), 42, 42, true ),
				(3, "three", CAST("three" as bytes), 42, 42, true )`,
			givenInput:  `UPDATE TestExecContextDml SET testSTring = "updated" WHERE key = 2 OR key = 3`,
			whenQueried: `SELECT * FROM TestExecContextDml ORDER BY key`,
			wantRows: []testExecContextDmlRow{
				{key: 1, testString: "one", testBytes: []byte("one"), testInt: 42, testFloat: 42, testBool: true},
				{key: 2, testString: "updated", testBytes: []byte("two"), testInt: 42, testFloat: 42, testBool: true},
				{key: 3, testString: "updated", testBytes: []byte("three"), testInt: 42, testFloat: 42, testBool: true},
			},
		},
		{
			name: "update primary key",
			setUp: `
				INSERT INTO TestExecContextDml (key, testString, testBytes, testInt, testFloat, testBool)
				VALUES (1, "one", CAST("one" as bytes), 42, 42, true )`,
			givenInput: `UPDATE TestExecContextDml SET key = 100 WHERE key = 1`,
			wantError:  true,
		},
		{
			name: "update nothing",
			setUp: `
				INSERT INTO TestExecContextDml (key, testString, testBytes, testInt, testFloat, testBool)
				VALUES (1, "one", CAST("one" as bytes), 42, 42, true ), (2, "two", CAST("two" as bytes), 42, 42, true ),
				(3, "three", CAST("three" as bytes), 42, 42, true )`,
			givenInput:  `UPDATE TestExecContextDml SET testSTring = "nothing" WHERE false`,
			whenQueried: `SELECT * FROM TestExecContextDml ORDER BY key`,
			wantRows: []testExecContextDmlRow{
				{key: 1, testString: "one", testBytes: []byte("one"), testInt: 42, testFloat: 42, testBool: true},
				{key: 2, testString: "two", testBytes: []byte("two"), testInt: 42, testFloat: 42, testBool: true},
				{key: 3, testString: "three", testBytes: []byte("three"), testInt: 42, testFloat: 42, testBool: true},
			},
		},
		{
			name: "update everything",
			setUp: `
				INSERT INTO TestExecContextDml (key, testString, testBytes, testInt, testFloat, testBool)
				VALUES (1, "one", CAST("one" as bytes), 42, 42, true ), (2, "two", CAST("two" as bytes), 42, 42, true ),
				(3, "three", CAST("three" as bytes), 42, 42, true )`,
			givenInput:  `UPDATE TestExecContextDml SET testSTring = "everything" WHERE true `,
			whenQueried: `SELECT * FROM TestExecContextDml WHERE testString = "everything"`,
			wantRows: []testExecContextDmlRow{
				{key: 1, testString: "everything", testBytes: []byte("one"), testInt: 42, testFloat: 42, testBool: true},
				{key: 2, testString: "everything", testBytes: []byte("two"), testInt: 42, testFloat: 42, testBool: true},
				{key: 3, testString: "everything", testBytes: []byte("three"), testInt: 42, testFloat: 42, testBool: true},
			},
		},
		{
			name:       "update to wrong type",
			givenInput: `UPDATE TestExecContextDml SET testBool = 100 WHERE key = 1`,
			wantError:  true,
		},
	}

	// Run tests.
	for _, tc := range tests {

		// Set up test table.
		if tc.setUp != "" {
			if _, err = db.ExecContext(ctx, tc.setUp); err != nil {
				t.Error(err)
			}
		}

		// Run DML.
		_, err = db.ExecContext(ctx, tc.givenInput)
		if (err != nil) && (!tc.wantError) {
			t.Errorf("%s: unexpected query error: %v", tc.name, err)
		}
		if (err == nil) && (tc.wantError) {
			t.Errorf("%s: expected query error but error was %v", tc.name, err)
		}

		// Check rows returned.
		if tc.whenQueried != "" {
			rows, err := db.QueryContext(ctx, tc.whenQueried)
			if err != nil {
				t.Errorf("%s: error from QueryContext : %v", tc.name, err)
			}

			got := []testExecContextDmlRow{}
			for rows.Next() {
				var curr testExecContextDmlRow
				err := rows.Scan(
					&curr.key, &curr.testString, &curr.testBytes, &curr.testInt, &curr.testFloat, &curr.testBool)
				if err != nil {
					t.Errorf("%s: error while scanning rows: %v", tc.name, err)
				}
				got = append(got, curr)
			}

			rows.Close()
			if err = rows.Err(); err != nil {
				t.Errorf("%s: error on rows.close: %v", tc.name, err)
			}
			if !reflect.DeepEqual(tc.wantRows, got) {
				t.Errorf("Unexpecred rows: %s. want: %v, got: %v", tc.name, tc.wantRows, got)
			}
		}

		// Clear table for next test.
		_, err = db.ExecContext(ctx, `DELETE FROM TestExecContextDml WHERE true`)
		if (err != nil) && (!tc.wantError) {
			t.Errorf("%s: unexpected query error: %v", tc.name, err)
		}

	}

	// Drop table.
	if _, err = db.ExecContext(ctx, `DROP TABLE TestExecContextDml`); err != nil {
		t.Error(err)
	}

}
