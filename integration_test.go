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

package spannerdriver

import (
	"context"
	"database/sql"
	"flag"
	"log"
	"math"
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

	// Derive data source dsn.
	dsn = "projects/" + projectId + "/instances/" + instanceId + "/databases/" + databaseId
}

func initIntegrationTests() (cleanup func()) {
	flag.Parse() // Needed for testing.Short().
	noop := func() {}

	if testing.Short() {
		log.Println("Integration tests skipped in -short mode.")
		return noop
	}
	return noop
}

func TestMain(m *testing.M) {
	cleanup := initIntegrationTests()
	res := m.Run()
	cleanup()
	os.Exit(res)
}

func skipIfShort(t *testing.T) {
	if testing.Short() {
		t.Skip("Integration tests skipped in -short mode.")
	}
}

func TestQueryContext(t *testing.T) {
	skipIfShort(t)

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

func TestExecContextDml(t *testing.T) {
	skipIfShort(t)

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

	type then struct {
		query     string
		wantRows  []testExecContextDmlRow
		wantError bool
	}

	tests := []struct {
		name  string
		given string
		when  string
		then  then
	}{
		{
			name: "insert single tuple",
			when: `
				INSERT INTO TestExecContextDml 
				(key, testString, testBytes, testInt, testFloat, testBool)
				VALUES (1, "one", CAST("one" as bytes), 42, 42, true ) `,
			then: then{
				query: `SELECT * FROM TestExecContextDml WHERE key = 1`,
				wantRows: []testExecContextDmlRow{
					{key: 1, testString: "one", testBytes: []byte("one"), testInt: 42, testFloat: 42, testBool: true},
				},
			},
		},
		{
			name: "insert multiple tuples",
			when: `
				INSERT INTO TestExecContextDml 
				(key, testString, testBytes, testInt, testFloat, testBool)
				VALUES (2, "two", CAST("two" as bytes), 42, 42, true ), (3, "three", CAST("three" as bytes), 42, 42, true )`,
			then: then{
				query: `SELECT * FROM TestExecContextDml ORDER BY key`,
				wantRows: []testExecContextDmlRow{
					{key: 2, testString: "two", testBytes: []byte("two"), testInt: 42, testFloat: 42, testBool: true},
					{key: 3, testString: "three", testBytes: []byte("three"), testInt: 42, testFloat: 42, testBool: true},
				},
			},
		},
		{
			name: "insert syntax error",
			when: `
				INSERT INSERT INTO TestExecContextDml 
				(key, testString, testBytes, testInt, testFloat, testBool)
				VALUES (101, "one", CAST("one" as bytes), 42, 42, true ) `,
			then: then{
				wantError: true,
			},
		},
		{
			name: "insert lower case dml",
			when: `
					insert into TestExecContextDml
					(key, testString, testBytes, testInt, testFloat, testBool)
					VALUES (4, "four", CAST("four" as bytes), 42, 42, true ) `,
			then: then{
				query: `SELECT * FROM TestExecContextDml ORDER BY key`,
				wantRows: []testExecContextDmlRow{
					{key: 4, testString: "four", testBytes: []byte("four"), testInt: 42, testFloat: 42, testBool: true},
				},
			},
		},

		{
			name: "insert lower case table name",
			when: `
					INSERT INTO testexeccontextdml (key, testString, testBytes, testInt, testFloat, testBool)
					VALUES (5, "five", CAST("five" as bytes), 42, 42, true ) `,
			then: then{
				query: `SELECT * FROM TestExecContextDml ORDER BY key`,
				wantRows: []testExecContextDmlRow{
					{key: 5, testString: "five", testBytes: []byte("five"), testInt: 42, testFloat: 42, testBool: true},
				},
			},
		},

		{
			name: "wrong types",
			when: `
					INSERT INSERT INTO TestExecContextDml (key, testString, testBytes, testInt, testFloat, testBool)
					VALUES (102, 56, 56, 42, hello, "i should be a bool" ) `,
			then: then{
				wantError: true,
			},
		},

		{
			name: "insert primary key duplicate",
			when: `
					INSERT INSERT INTO TestExecContextDml (key, testString, testBytes, testInt, testFloat, testBool)
					VALUES (1, "one", CAST("one" as bytes), 42, 42, true ), (1, "one", CAST("one" as bytes), 42, 42, true ) `,
			then: then{
				wantError: true,
			},
		},

		{
			name: "insert null into primary key",
			when: `
					INSERT INSERT INTO TestExecContextDml
					(key, testString, testBytes, testInt, testFloat, testBool)
					VALUES (null, "one", CAST("one" as bytes), 42, 42, true ) `,
			then: then{
				wantError: true,
			},
		},

		{
			name: "insert too many values",
			when: `
					INSERT INSERT INTO TestExecContextDml
					(key, testString, testBytes, testInt, testFloat, testBool)
					VALUES (null, "one", CAST("one" as bytes), 42, 42, true, 42 ) `,
			then: then{
				wantError: true,
			},
		},

		{
			name: "insert too few values",
			when: `
					INSERT INSERT INTO TestExecContextDml
					(key, testString, testBytes, testInt, testFloat, testBool)
					VALUES (null, "one", CAST("one" as bytes), 42 ) `,
			then: then{
				wantError: true,
			},
		},

		{
			name: "delete single tuple",
			given: `
					INSERT INTO TestExecContextDml (key, testString, testBytes, testInt, testFloat, testBool)
					VALUES (1, "one", CAST("one" as bytes), 42, 42, true ), (2, "two", CAST("two" as bytes), 42, 42, true )`,
			when: `DELETE FROM TestExecContextDml WHERE key = 1`,
			then: then{
				query: `SELECT * FROM TestExecContextDml`,
				wantRows: []testExecContextDmlRow{
					{key: 2, testString: "two", testBytes: []byte("two"), testInt: 42, testFloat: 42, testBool: true},
				},
			},
		},

		{
			name: "delete multiple tuples with subthen",
			given: `
					INSERT INTO TestExecContextDml (key, testString, testBytes, testInt, testFloat, testBool)
					VALUES (1, "one", CAST("one" as bytes), 42, 42, true ), (2, "two", CAST("two" as bytes), 42, 42, true ),
					(3, "three", CAST("three" as bytes), 42, 42, true ), (4, "four", CAST("four" as bytes), 42, 42, true )`,
			when: `
					DELETE FROM TestExecContextDml WHERE key
					IN (SELECT key FROM TestExecContextDml WHERE key != 4)`,
			then: then{
				query: `SELECT * FROM TestExecContextDml ORDER BY key`,
				wantRows: []testExecContextDmlRow{
					{key: 4, testString: "four", testBytes: []byte("four"), testInt: 42, testFloat: 42, testBool: true},
				},
			},
		},

		{
			name: "delete all tuples",
			given: `
					INSERT INTO TestExecContextDml (key, testString, testBytes, testInt, testFloat, testBool)
					VALUES (1, "one", CAST("one" as bytes), 42, 42, true ), (2, "two", CAST("two" as bytes), 42, 42, true ),
					(3, "three", CAST("three" as bytes), 42, 42, true ), (4, "four", CAST("four" as bytes), 42, 42, true )`,
			when: `DELETE FROM TestExecContextDml WHERE true`,
			then: then{
				query:    `SELECT * FROM TestExecContextDml`,
				wantRows: []testExecContextDmlRow{},
			},
		},
		{
			name: "update one tuple",
			given: `
					INSERT INTO TestExecContextDml (key, testString, testBytes, testInt, testFloat, testBool)
					VALUES (1, "one", CAST("one" as bytes), 42, 42, true ), (2, "two", CAST("two" as bytes), 42, 42, true ),
					(3, "three", CAST("three" as bytes), 42, 42, true )`,
			when: `UPDATE TestExecContextDml SET testSTring = "updated" WHERE key = 1`,
			then: then{
				query: `SELECT * FROM TestExecContextDml ORDER BY key`,
				wantRows: []testExecContextDmlRow{
					{key: 1, testString: "updated", testBytes: []byte("one"), testInt: 42, testFloat: 42, testBool: true},
					{key: 2, testString: "two", testBytes: []byte("two"), testInt: 42, testFloat: 42, testBool: true},
					{key: 3, testString: "three", testBytes: []byte("three"), testInt: 42, testFloat: 42, testBool: true},
				},
			},
		},
		{
			name: "update multiple tuples",
			given: `
					INSERT INTO TestExecContextDml (key, testString, testBytes, testInt, testFloat, testBool)
					VALUES (1, "one", CAST("one" as bytes), 42, 42, true ), (2, "two", CAST("two" as bytes), 42, 42, true ),
					(3, "three", CAST("three" as bytes), 42, 42, true )`,
			when: `UPDATE TestExecContextDml SET testSTring = "updated" WHERE key = 2 OR key = 3`,
			then: then{
				query: `SELECT * FROM TestExecContextDml ORDER BY key`,
				wantRows: []testExecContextDmlRow{
					{key: 1, testString: "one", testBytes: []byte("one"), testInt: 42, testFloat: 42, testBool: true},
					{key: 2, testString: "updated", testBytes: []byte("two"), testInt: 42, testFloat: 42, testBool: true},
					{key: 3, testString: "updated", testBytes: []byte("three"), testInt: 42, testFloat: 42, testBool: true},
				},
			},
		},

		{
			name: "update primary key",
			given: `
					INSERT INTO TestExecContextDml (key, testString, testBytes, testInt, testFloat, testBool)
					VALUES (1, "one", CAST("one" as bytes), 42, 42, true )`,
			when: `UPDATE TestExecContextDml SET key = 100 WHERE key = 1`,
			then: then{
				wantError: true,
			},
		},

		{
			name: "update nothing",
			given: `
					INSERT INTO TestExecContextDml (key, testString, testBytes, testInt, testFloat, testBool)
					VALUES (1, "one", CAST("one" as bytes), 42, 42, true ), (2, "two", CAST("two" as bytes), 42, 42, true ),
					(3, "three", CAST("three" as bytes), 42, 42, true )`,
			when: `UPDATE TestExecContextDml SET testSTring = "nothing" WHERE false`,
			then: then{
				query: `SELECT * FROM TestExecContextDml ORDER BY key`,
				wantRows: []testExecContextDmlRow{
					{key: 1, testString: "one", testBytes: []byte("one"), testInt: 42, testFloat: 42, testBool: true},
					{key: 2, testString: "two", testBytes: []byte("two"), testInt: 42, testFloat: 42, testBool: true},
					{key: 3, testString: "three", testBytes: []byte("three"), testInt: 42, testFloat: 42, testBool: true},
				},
			},
		},

		{
			name: "update everything",
			given: `
					INSERT INTO TestExecContextDml (key, testString, testBytes, testInt, testFloat, testBool)
					VALUES (1, "one", CAST("one" as bytes), 42, 42, true ), (2, "two", CAST("two" as bytes), 42, 42, true ),
					(3, "three", CAST("three" as bytes), 42, 42, true )`,
			when: `UPDATE TestExecContextDml SET testSTring = "everything" WHERE true `,
			then: then{
				query: `SELECT * FROM TestExecContextDml WHERE testString = "everything"`,
				wantRows: []testExecContextDmlRow{
					{key: 1, testString: "everything", testBytes: []byte("one"), testInt: 42, testFloat: 42, testBool: true},
					{key: 2, testString: "everything", testBytes: []byte("two"), testInt: 42, testFloat: 42, testBool: true},
					{key: 3, testString: "everything", testBytes: []byte("three"), testInt: 42, testFloat: 42, testBool: true},
				},
			},
		},
		{
			name: "update to wrong type",
			when: `UPDATE TestExecContextDml SET testBool = 100 WHERE key = 1`,
			then: then{
				wantError: true,
			},
		},
	}

	// Run tests.
	for _, tc := range tests {

		// Set up test table.
		if tc.given != "" {
			if _, err = db.ExecContext(ctx, tc.given); err != nil {
				t.Errorf("%s: error setting up table: %v", tc.name, err)
			}
		}

		// Run DML.
		_, err = db.ExecContext(ctx, tc.when)
		if (err != nil) && (!tc.then.wantError) {
			t.Errorf("%s: unexpected query error: %v", tc.name, err)
		}
		if (err == nil) && (tc.then.wantError) {
			t.Errorf("%s: expected query error but error was %v", tc.name, err)
		}

		// Check rows returned.
		if tc.then.query != "" {
			rows, err := db.QueryContext(ctx, tc.then.query)
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
			if !reflect.DeepEqual(tc.then.wantRows, got) {
				t.Errorf("Unexpecred rows: %s. want: %v, got: %v", tc.name, tc.then.wantRows, got)
			}
		}

		// Clear table for next test.
		_, err = db.ExecContext(ctx, `DELETE FROM TestExecContextDml WHERE true`)
		if (err != nil) && (!tc.then.wantError) {
			t.Errorf("%s: unexpected query error: %v", tc.name, err)
		}

	}

	// Drop table.
	if _, err = db.ExecContext(ctx, `DROP TABLE TestExecContextDml`); err != nil {
		t.Error(err)
	}

}
func TestRowsAtomicTypes(t *testing.T) {
	skipIfShort(t)

	// Open db.
	ctx := context.Background()
	db, err := sql.Open("spanner", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// Set up test table.
	_, err = db.ExecContext(ctx,
		`CREATE TABLE TestAtomicTypes (
			key	STRING(1024),
			testString	STRING(1024),
			testBytes	BYTES(1024),
			testInt	INT64,
			testFloat	FLOAT64,
			testBool	BOOL
		) PRIMARY KEY (key)`)
	if err != nil {
		t.Fatal(err)
	}

	_, err = db.ExecContext(ctx,
		`INSERT INTO TestAtomicTypes (key, testString, testBytes, testInt, testFloat, testBool) VALUES
		("general", "hello", CAST ("hello" as bytes), 42, 42, TRUE), 
		("negative", "hello", CAST ("hello" as bytes), -42, -42, TRUE),
		("maxint", "hello", CAST ("hello" as bytes), 9223372036854775807 , 42, TRUE),
		("minint", "hello", CAST ("hello" as bytes), -9223372036854775808 , 42, TRUE),
		("nan", "hello" ,CAST("hello" as bytes), 42, CAST("nan" AS FLOAT64), TRUE), 
		("pinf", "hello" ,CAST("hello" as bytes), 42, CAST("inf" AS FLOAT64), TRUE),
		("ninf", "hello" ,CAST("hello" as bytes), 42, CAST("-inf" AS FLOAT64), TRUE), 
		("nullstring", null, CAST ("hello" as bytes), 42, 42, TRUE), 
		("nullbytes", "hello", null, 42, 42, TRUE),
		("nullint", "hello", CAST ("hello" as bytes), null, 42, TRUE), 
		("nullfloat", "hello", CAST ("hello" as bytes), 42, null, TRUE), 
		("nullbool", "hello", CAST ("hello" as bytes), 42, 42, null)
		`)
	if err != nil {
		t.Fatal(err)
	}

	type testAtomicTypesRow struct {
		key        string
		testString string
		testBytes  []byte
		testInt    int
		testFloat  float64
		testBool   bool
	}

	type wantError struct {
		scan  bool
		close bool
		query bool
	}

	tests := []struct {
		name      string
		input     string
		want      []testAtomicTypesRow
		wantError wantError
	}{

		{
			name:  "general read",
			input: `SELECT * FROM TestAtomicTypes WHERE key = "general"`,
			want: []testAtomicTypesRow{
				{key: "general", testString: "hello", testBytes: []byte("hello"), testInt: 42, testFloat: 42, testBool: true},
			},
		},
		{
			name:  "negative int and float",
			input: `SELECT * FROM TestAtomicTypes WHERE key = "negative"`,
			want: []testAtomicTypesRow{
				{key: "negative", testString: "hello", testBytes: []byte("hello"), testInt: -42, testFloat: -42, testBool: true},
			},
		},
		{
			name:  "max int",
			input: `SELECT * FROM TestAtomicTypes WHERE key = "maxint"`,
			want: []testAtomicTypesRow{
				{key: "maxint", testString: "hello", testBytes: []byte("hello"), testInt: 9223372036854775807, testFloat: 42, testBool: true},
			},
		},
		{
			name:  "min int",
			input: `SELECT * FROM TestAtomicTypes WHERE key = "minint"`,
			want: []testAtomicTypesRow{
				{key: "minint", testString: "hello", testBytes: []byte("hello"), testInt: -9223372036854775808, testFloat: 42, testBool: true},
			},
		},
		{
			name:  "special float positive infinity",
			input: `SELECT * FROM TestAtomicTypes WHERE key = "pinf"`,
			want: []testAtomicTypesRow{
				{key: "pinf", testString: "hello", testBytes: []byte("hello"), testInt: 42, testFloat: math.Inf(1), testBool: true},
			},
		},
		{
			name:  "special float negative infinity",
			input: `SELECT * FROM TestAtomicTypes WHERE key = "ninf"`,
			want: []testAtomicTypesRow{
				{key: "ninf", testString: "hello", testBytes: []byte("hello"), testInt: 42, testFloat: math.Inf(-1), testBool: true},
			},
		},
	}

	// Run tests.
	for _, tc := range tests {

		rows, err := db.QueryContext(ctx, tc.input)
		if (err != nil) && (!tc.wantError.query) {
			t.Errorf("%s: unexpected query error: %v", tc.name, err)
		}
		if (err == nil) && (tc.wantError.query) {
			t.Errorf("%s: expected query error but error was %v", tc.name, err)
		}

		got := []testAtomicTypesRow{}
		for rows.Next() {
			var curr testAtomicTypesRow
			err := rows.Scan(
				&curr.key, &curr.testString, &curr.testBytes, &curr.testInt, &curr.testFloat, &curr.testBool)
			if (err != nil) && (!tc.wantError.scan) {
				t.Errorf("%s: unexpected scan error: %v", tc.name, err)
			}
			if (err == nil) && (tc.wantError.scan) {
				t.Errorf("%s: expected scan error but error was %v", tc.name, err)
			}

			got = append(got, curr)
		}

		rows.Close()
		err = rows.Err()
		if (err != nil) && (!tc.wantError.close) {
			t.Errorf("%s: unexpected rows.Err error: %v", tc.name, err)
		}
		if (err == nil) && (tc.wantError.close) {
			t.Errorf("%s: expected rows.Err error but error was %v", tc.name, err)
		}

		if !reflect.DeepEqual(tc.want, got) {
			t.Errorf("Unexpected rows: %s. want: %v, got: %v", tc.name, tc.want, got)
		}

	}

	// Drop table.
	_, err = db.ExecContext(ctx, `DROP TABLE TestAtomicTypes`)
	if err != nil {
		t.Fatal(err)
	}

}

func TestRowsOverflowRead(t *testing.T) {
	skipIfShort(t)

	// Open db.
	ctx := context.Background()
	db, err := sql.Open("spanner", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// Set up test table.
	_, err = db.ExecContext(ctx,
		`CREATE TABLE TestOverflowRead (
			key	STRING(1024),
			testString	STRING(1024),
			testBytes	BYTES(1024),
			testInt	INT64,
			testFloat	FLOAT64,
			testBool	BOOL
		) PRIMARY KEY (key)`)
	if err != nil {
		t.Fatal(err)
	}

	_, err = db.ExecContext(ctx,
		`INSERT INTO TestOverflowRead  (key, testString, testBytes, testInt, testFloat, testBool) VALUES
		("general", "hello", CAST ("hello" as bytes), 42, 42, TRUE), 
		("maxint", "hello", CAST ("hello" as bytes), 9223372036854775807 , 42, TRUE),
		("minint", "hello", CAST ("hello" as bytes), -9223372036854775808 , 42, TRUE),
		("max int8", "hello", CAST ("hello" as bytes), 127 , 42, TRUE),
		("max uint8", "hello", CAST ("hello" as bytes), 255 , 42, TRUE)
		`)
	if err != nil {
		t.Fatal(err)
	}

	type testAtomicTypesRowSmallInt struct {
		key        string
		testString string
		testBytes  []byte
		testInt    int8
		testFloat  float64
		testBool   bool
	}

	type wantError struct {
		scan  bool
		close bool
		query bool
	}

	tests := []struct {
		name      string
		input     string
		want      []testAtomicTypesRowSmallInt
		wantError wantError
	}{
		{
			name:  "read int64 into int8 ok",
			input: `SELECT * FROM TestOverflowRead WHERE key = "general"`,
			want: []testAtomicTypesRowSmallInt{
				{key: "general", testString: "hello", testBytes: []byte("hello"), testInt: 42, testFloat: 42, testBool: true},
			},
		},
		{
			name:      "read int64 into int8 overflow",
			input:     `SELECT * FROM TestOverflowRead WHERE key = "maxint"`,
			wantError: wantError{scan: true},
			want: []testAtomicTypesRowSmallInt{
				{key: "maxint", testString: "hello", testBytes: []byte("hello"), testInt: 0, testFloat: 0, testBool: false},
			},
		},
		{
			name:  "read max int8 ",
			input: `SELECT * FROM TestOverflowRead WHERE key = "max int8"`,
			want: []testAtomicTypesRowSmallInt{
				{key: "max int8", testString: "hello", testBytes: []byte("hello"), testInt: 127, testFloat: 42, testBool: true},
			},
		},
		{
			name:      "read max uint8 into int8",
			input:     `SELECT * FROM TestOverflowRead WHERE key = "max uint8"`,
			wantError: wantError{scan: true},
			want: []testAtomicTypesRowSmallInt{
				{key: "max uint8", testString: "hello", testBytes: []byte("hello"), testInt: 0, testFloat: 0, testBool: false},
			},
		},
	}

	// Run tests.
	for _, tc := range tests {

		rows, err := db.QueryContext(ctx, tc.input)
		if (err != nil) && (!tc.wantError.query) {
			t.Errorf("%s: unexpected query error: %v", tc.name, err)
		}
		if (err == nil) && (tc.wantError.query) {
			t.Errorf("%s: expected query error but error was %v", tc.name, err)
		}

		got := []testAtomicTypesRowSmallInt{}
		for rows.Next() {
			var curr testAtomicTypesRowSmallInt
			err := rows.Scan(
				&curr.key, &curr.testString, &curr.testBytes, &curr.testInt, &curr.testFloat, &curr.testBool)
			if (err != nil) && (!tc.wantError.scan) {
				t.Errorf("%s: unexpected scan error: %v", tc.name, err)
			}
			if (err == nil) && (tc.wantError.scan) {
				t.Errorf("%s: expected scan error but error was %v", tc.name, err)
			}

			got = append(got, curr)
		}

		rows.Close()
		err = rows.Err()
		if (err != nil) && (!tc.wantError.close) {
			t.Errorf("%s: unexpected rows.Err error: %v", tc.name, err)
		}
		if (err == nil) && (tc.wantError.close) {
			t.Errorf("%s: expected rows.Err error but error was %v", tc.name, err)
		}

		if !reflect.DeepEqual(tc.want, got) {
			t.Errorf("%s: unexpected rows. want: %v, got: %v", tc.name, tc.want, got)
		}

	}

	// Drop table.
	_, err = db.ExecContext(ctx, `DROP TABLE TestOverflowRead`)
	if err != nil {
		t.Fatal(err)
	}

}
