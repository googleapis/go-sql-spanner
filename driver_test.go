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

func TestExecContextDdl(t *testing.T) {

	// Open db.
	ctx := context.Background()
	db, err := sql.Open("spanner", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// Run tests.
	tests := []struct {
		name          string
		given         string
		withInsert    string
		when          string
		thenWantError bool
		drop          string
	}{
		{
			name: "create table ok",
			when: `CREATE TABLE TestTable (
				A   STRING(1024),
				B  STRING(1024),
			)	 PRIMARY KEY (A)`,
			drop: `DROP TABLE TestTable`,
		},
		{
			name: "create table name duplicate",
			given: `CREATE TABLE TestTable (
				A   STRING(1024),
				B  STRING(1024),
			)	 PRIMARY KEY (A)`,
			when: `CREATE TABLE TestTable (
				A   STRING(1024),
				B  STRING(1024),
			)	 PRIMARY KEY (A)`,
			thenWantError: true,
			drop:          `DROP TABLE TestTable`,
		},
		{
			name: "create table syntax error",
			when: `CREATE CREATE TABLE SyntaxError (
				A   STRING(1024),
				B  STRING(1024),
				C   STRING(1024)
			)	 PRIMARY KEY (A)`,
			thenWantError: true,
		},
		{
			name: "create table no primary key",
			when: `CREATE TABLE NoPrimaryKey (
				A   STRING(1024),
				B  STRING(1024),
			)`,
			thenWantError: true,
		},
		{
			name: "create table float primary key",
			when: `CREATE TABLE FloatPrimaryKey (
				A   FLOAT64,
				B  STRING(1024),
			)	PRIMARY KEY (A)`,
			drop: "DROP TABLE FloatPrimaryKey",
		},
		{
			name: "create table bool primary key",
			when: `CREATE TABLE BoolPrimaryKey (
				A   BOOL,
				B  STRING(1024),
			)	PRIMARY KEY (A)`,
			drop: "DROP TABLE BoolPrimaryKey",
		},
		{
			name: "create table lowercase ddl",
			when: `create table LowerDdl (
				A   INT64,
				B  STRING(1024),
			)	PRIMARY KEY (A)`,
			drop: "DROP TABLE LowerDdl",
		},
		{
			name: "create table integer name",
			when: `CREATE TABLE 42 (
				A   INT64,
				B  STRING(1024),
			)	PRIMARY KEY (A)`,
			thenWantError: true,
		},
		{
			name: "drop table ok",
			given: `CREATE TABLE TestTable (
				A   STRING(1024),
				B  STRING(1024),
			)	 PRIMARY KEY (A)`,
			when: "DROP TABLE TestTable",
		},
		{
			name:          "drop non existent table",
			when:          "DROP TABLE NonExistent",
			thenWantError: true,
		},
		{
			name: "alter table, add column ok",
			given: `CREATE TABLE TestTable (
				A   STRING(1024),
				B  STRING(1024),
			)	 PRIMARY KEY (A)`,
			when: "ALTER TABLE TestTable ADD COLUMN C STRING(1024)",
			drop: "DROP TABLE TestTable ",
		},
		{
			name: "alter table, add column name duplicate",
			given: `CREATE TABLE TestTable (
				A   STRING(1024),
				B  STRING(1024),
			)	 PRIMARY KEY (A)`,
			when:          "ALTER TABLE TestTable ADD COLUMN B STRING(1024)",
			thenWantError: true,
			drop:          "DROP TABLE TestTable ",
		},
		{
			name: "alter table, drop column ok",
			given: `CREATE TABLE TestTable (
				A   STRING(1024),
				B  STRING(1024),
			)	 PRIMARY KEY (A)`,
			when: "ALTER TABLE TestTable DROP COLUMN B",
			drop: "DROP TABLE TestTable ",
		},
		{
			name: "alter table, drop primary key",
			given: `CREATE TABLE TestTable (
				A   STRING(1024),
				B  STRING(1024),
			)	 PRIMARY KEY (A)`,
			when:          "ALTER TABLE TestTable DROP COLUMN A",
			thenWantError: true,
			drop:          "DROP TABLE TestTable ",
		},
		{
			name: "alter table, reduce string length safe",
			given: `CREATE TABLE TestTable (
				A   STRING(1024),
				B  STRING(1024),
			)	 PRIMARY KEY (A)`,
			when: "ALTER TABLE TestTable ALTER COLUMN A STRING(10) ",
			drop: "DROP TABLE TestTable ",
		},
		{
			name: "alter table, reduce string length not safe",
			given: `CREATE TABLE TestTable (
				A   STRING(1024),
				B  STRING(1024),
			)	 PRIMARY KEY (A)`,
			withInsert:    `INSERT INTO TestTable (A,B) VALUES ("aaaaaaaaaaaaaaa", "hi")`,
			when:          "ALTER TABLE TestTable ALTER COLUMN A STRING(3) ",
			thenWantError: true,
			drop:          "DROP TABLE TestTable ",
		},
		{
			name: "alter table, change string to bytes",
			given: `CREATE TABLE TestTable (
				A   STRING(1024),
				B  STRING(1024),
			)	 PRIMARY KEY (A)`,
			withInsert: `INSERT INTO TestTable (A,B) VALUES ("aaaaaaaaaaaaaaa", "hi")`,
			when:       "ALTER TABLE TestTable ALTER COLUMN A BYTES(1024) ",
			drop:       "DROP TABLE TestTable ",
		},
	}

	// Run tests.
	for _, tc := range tests {

		// Set up, if required
		if tc.given != "" {
			_, err = db.ExecContext(ctx, tc.given)
			if err != nil {
				t.Errorf("%s: error setting up: %v", tc.name, err)
			}
		}
		if tc.withInsert != "" {
			_, err = db.ExecContext(ctx, tc.withInsert)
			if err != nil {
				t.Errorf("%s: error setting up (insert): %v", tc.name, err)
			}
		}

		// Run test.
		_, err = db.ExecContext(ctx, tc.when)
		if (err != nil) && (!tc.thenWantError) {
			t.Errorf("%s: unexpected query error: %v", tc.name, err)
		}
		if (err == nil) && (tc.thenWantError) {
			t.Errorf("%s: expected query error but error was %v", tc.name, err)
		}

		// Drop table, if created.
		if tc.drop != "" {
			_, err = db.ExecContext(ctx, tc.drop)
			if err != nil {
				t.Errorf("%s: error dropping table: %v", tc.name, err)
			}
		}

	}
}

func TestExecContextReferentialIntegrity(t *testing.T) {

	// Open db.
	ctx := context.Background()
	db, err := sql.Open("spanner", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	tests := []struct {
		name             string
		givenParent      string
		givenChild       string
		withInsertParent string
		withInsertChild  string
		when             string
		thenWantError    bool
		dropParent       string
		dropChild        string
	}{
		// DDL.
		{
			name: "drop table referential integrity violation no cascade & foreign key",
			givenParent: `
			CREATE TABLE ParentNoCascade (
				id   INT64,
			)	PRIMARY KEY (id)`,
			givenChild: `CREATE TABLE ChildNoCascade (
				id   INT64,
				parent_id	INT64,
				CONSTRAINT fk_nc FOREIGN KEY (parent_id) REFERENCES ParentNoCascade (id)
			)	PRIMARY KEY (id)`,
			withInsertParent: `INSERT INTO  ParentNoCascade (id) VALUES (1), (2)`,
			withInsertChild:  `INSERT INTO  ChildNoCascade (id, parent_id) VALUES (2, 1), (4, 2)`,
			when:             `DROP TABLE ParentNoCascade`,
			thenWantError:    true,
			dropParent:       `DROP TABLE ParentNoCascade`,
			dropChild:        `DROP TABLE ChildNoCascade`,
		},
		{
			name: "drop table referential integrity violation cascade & interleave",
			givenParent: `
			CREATE TABLE ParentCascade (
				parent_id   INT64,
			)	PRIMARY KEY (parent_id)`,
			givenChild: `CREATE TABLE ChildCascade (
				parent_id	INT64,
				id   INT64,
			)	PRIMARY KEY (parent_id, id),
			INTERLEAVE IN PARENT ParentCascade ON DELETE CASCADE`,
			withInsertParent: `INSERT INTO  ParentCascade (parent_id) VALUES (1), (2)`,
			withInsertChild:  `INSERT INTO  ChildCascade (id, parent_id) VALUES (2, 1), (4, 2)`,
			when:             `DROP TABLE ParentCascade`,
			thenWantError:    true,
			dropParent:       `DROP TABLE ParentCascade`,
			dropChild:        `DROP TABLE ChildCascade`,
		},
		// DML.
		{
			name: "delete row integrity violation no cascade & foreign key",
			givenParent: `
			CREATE TABLE ParentNoCascade (
				id   INT64,
			)	PRIMARY KEY (id)`,
			givenChild: `CREATE TABLE ChildNoCascade (
				id   INT64,
				parent_id	INT64,
				CONSTRAINT fk_nc FOREIGN KEY (parent_id) REFERENCES ParentNoCascade (id)
			)	PRIMARY KEY (id)`,
			withInsertParent: `INSERT INTO  ParentNoCascade (id) VALUES (1), (2)`,
			withInsertChild:  `INSERT INTO  ChildNoCascade (id, parent_id) VALUES (2, 1), (4, 2)`,
			when:             `DELETE FROM ParentNoCascade WHERE id = 1`,
			thenWantError:    true,
			dropParent:       `DROP TABLE ParentNoCascade`,
			dropChild:        `DROP TABLE ChildNoCascade`,
		},
		{
			name: "delete row integrity violation cascade & interleave",
			givenParent: `
			CREATE TABLE ParentCascade (
				parent_id   INT64,
			)	PRIMARY KEY (parent_id)`,
			givenChild: `CREATE TABLE ChildCascade (
				parent_id	INT64,
				id   INT64,
			)	PRIMARY KEY (parent_id, id),
			INTERLEAVE IN PARENT ParentCascade ON DELETE CASCADE`,
			withInsertParent: `INSERT INTO  ParentCascade (parent_id) VALUES (1), (2)`,
			withInsertChild:  `INSERT INTO  ChildCascade (id, parent_id) VALUES (2, 1), (4, 2)`,
			when:             `DELETE FROM ParentCascade WHERE parent_id = 1`,
			dropParent:       `DROP TABLE ParentCascade`,
			dropChild:        `DROP TABLE ChildCascade`,
		},
	}

	for _, tc := range tests {

		// Set up tables.
		if tc.givenParent != "" {
			_, err = db.ExecContext(ctx, tc.givenParent)
			if err != nil {
				t.Errorf("%s: error setting up child: %v", tc.name, err)
			}
		}
		if tc.givenChild != "" {
			_, err = db.ExecContext(ctx, tc.givenChild)
			if err != nil {
				t.Errorf("%s: error setting up child: %v", tc.name, err)
			}
		}
		if tc.withInsertParent != "" {
			_, err = db.ExecContext(ctx, tc.withInsertParent)
			if err != nil {
				t.Errorf("%s: error setting up parent: %v", tc.name, err)
			}
		}
		if tc.withInsertChild != "" {
			_, err = db.ExecContext(ctx, tc.withInsertChild)
			if err != nil {
				t.Errorf("%s: error setting up child: %v", tc.name, err)
			}
		}

		// Run test.
		_, err = db.ExecContext(ctx, tc.when)
		if (err != nil) && (!tc.thenWantError) {
			t.Errorf("%s: unexpected query error: %v", tc.name, err)
		}
		if (err == nil) && (tc.thenWantError) {
			t.Errorf("%s: expected query error but error was %v", tc.name, err)
		}

		// Clean up.
		if tc.dropChild != "" {
			_, err = db.ExecContext(ctx, tc.dropChild)
			if err != nil {
				t.Errorf("%s: error dropping up child: %v", tc.name, err)
			}
		}
		if tc.dropParent != "" {
			_, err = db.ExecContext(ctx, tc.dropParent)
			if err != nil {
				t.Errorf("%s: error droppinh up parent: %v", tc.name, err)
			}
		}

	}

}
