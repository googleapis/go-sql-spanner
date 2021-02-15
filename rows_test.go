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
	"math"
	"reflect"
	"testing"
)

func TestRowsAtomicTypes(t *testing.T) {

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

func TestRowsAtomicTypePermute(t *testing.T) {

	// Open db.
	ctx := context.Background()
	db, err := sql.Open("spanner", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	type then struct {
		wantStr      string
		wantErrorStr bool
		wantByt      []byte
		wantErrorByt bool
		wantInt      int
		wantErrorInt bool
		wantFlo      float64
		wantErrorFlo bool
		wantBoo      bool
		wantErrorBoo bool
	}

	tests := []struct {
		name     string
		typeName string
		given    []string
		when     string
		then     then
		tearDown string
	}{
		{
			name:     "read spanner bytes (\"hello\") into go types",
			typeName: "bytes",
			given: []string{
				`CREATE TABLE TestBadTypeInt (val BYTES(1024)) PRIMARY KEY (val)`,
				`INSERT INTO TestBadTypeInt (val) VALUES (CAST("hello" as bytes)) `,
			},
			when: `SELECT * FROM TestBadTypeInt`,
			then: then{
				wantStr:      "hello",
				wantByt:      []byte("hello"),
				wantErrorInt: true,
				wantErrorFlo: true,
				wantErrorBoo: true,
			},
			tearDown: `DROP TABLE TestBadTypeInt`,
		},

		{
			name:     "read spanner bytes (\"1\") into go types",
			typeName: "bytes",
			given: []string{
				`CREATE TABLE TestBadTypeInt (val BYTES(1024)) PRIMARY KEY (val)`,
				`INSERT INTO TestBadTypeInt (val) VALUES (CAST("1" as bytes)) `,
			},
			when: `SELECT * FROM TestBadTypeInt`,
			then: then{
				wantStr: "1",
				wantByt: []byte("1"),
				wantInt: 1,
				wantFlo: 1,
				wantBoo: true,
			},
			tearDown: `DROP TABLE TestBadTypeInt`,
		},
		{
			name:     "read spanner string (\"hello\") into go types",
			typeName: "string",
			given: []string{
				`CREATE TABLE TestBadTypeInt (val STRING(1024)) PRIMARY KEY (val)`,
				`INSERT INTO TestBadTypeInt (val) VALUES ("hello")`,
			},
			when: `SELECT * FROM TestBadTypeInt`,
			then: then{
				wantStr:      "hello",
				wantByt:      []byte("hello"),
				wantErrorInt: true,
				wantErrorFlo: true,
				wantErrorBoo: true,
			},
			tearDown: `DROP TABLE TestBadTypeInt`,
		},
		{
			name:     "read spanner int (1) into go types",
			typeName: "int",
			given: []string{
				`CREATE TABLE TestBadTypeInt (val INT64) PRIMARY KEY (val)`,
				`INSERT INTO TestBadTypeInt (val) VALUES (1)`,
			},
			when: `SELECT * FROM TestBadTypeInt`,
			then: then{
				wantStr: "1",
				wantByt: []byte("1"),
				wantInt: 1,
				wantFlo: 1,
				wantBoo: true,
			},
			tearDown: `DROP TABLE TestBadTypeInt`,
		},

		{
			name:     "read spanner int (42) into go types",
			typeName: "int",
			given: []string{
				`CREATE TABLE TestBadTypeInt (val INT64) PRIMARY KEY (val)`,
				`INSERT INTO TestBadTypeInt (val) VALUES (42)`,
			},
			when: `SELECT * FROM TestBadTypeInt`,
			then: then{
				wantStr:      "42",
				wantByt:      []byte("42"),
				wantInt:      42,
				wantFlo:      42,
				wantErrorBoo: true,
			},
			tearDown: `DROP TABLE TestBadTypeInt`,
		},
		{
			name:     "read spanner float (42) into go types",
			typeName: "float",
			given: []string{
				`CREATE TABLE TestBadTypeInt (val FLOAT64) PRIMARY KEY (val)`,
				`INSERT INTO TestBadTypeInt (val) VALUES (42)`,
			},
			when: `SELECT * FROM TestBadTypeInt`,
			then: then{
				wantStr:      "42",
				wantByt:      []byte("42"),
				wantInt:      42,
				wantFlo:      42,
				wantErrorBoo: true,
			},
			tearDown: `DROP TABLE TestBadTypeInt`,
		},
		{
			name:     "read spanner float (42.5) into go types",
			typeName: "float",
			given: []string{
				`CREATE TABLE TestBadTypeInt (val FLOAT64) PRIMARY KEY (val)`,
				`INSERT INTO TestBadTypeInt (val) VALUES (42.5)`,
			},
			when: `SELECT * FROM TestBadTypeInt`,
			then: then{
				wantStr:      "42.5",
				wantByt:      []byte("42.5"),
				wantErrorInt: true,
				wantFlo:      42.5,
				wantErrorBoo: true,
			},
			tearDown: `DROP TABLE TestBadTypeInt`,
		},
		{
			name:     "read spanner bool into go types",
			typeName: "bool",
			given: []string{
				`CREATE TABLE TestBadTypeInt (val BOOL) PRIMARY KEY (val)`,
				`INSERT INTO TestBadTypeInt (val) VALUES (TRUE)`,
			},
			when: `SELECT * FROM TestBadTypeInt`,
			then: then{
				wantStr:      "true",
				wantByt:      []byte("true"),
				wantErrorInt: true,
				wantErrorFlo: true,
				wantBoo:      true,
			},
			tearDown: `DROP TABLE TestBadTypeInt`,
		},
	}

	// Run tests.
	for _, tc := range tests {

		// Set up table.
		for _, statement := range tc.given {
			_, err = db.ExecContext(ctx, statement)
			if err != nil {
				t.Fatalf("%s: error in setting up table: %v", tc.name, err)
			}
		}

		// Attempt read.
		rows, err := db.QueryContext(ctx, tc.when)
		if err != nil {
			t.Errorf("%s: unexpected query error: %v", tc.name, err)
		}
		for rows.Next() {

			// String.
			var strScan string
			err := rows.Scan(&strScan)
			if (err != nil) && (!tc.then.wantErrorStr) {
				t.Errorf("%s: unexpected string scan error: %v", tc.name, err)
			}
			if (err == nil) && (tc.then.wantErrorStr) {
				t.Errorf("%s: expected string scan error, but error was %v", tc.name, err)
			}
			if strScan != tc.then.wantStr {
				t.Errorf("Unexpected %s to string conversion, want %s got %s\n", tc.typeName, tc.then.wantStr, strScan)
			}

			// Bytes.
			var bytScan []byte
			err = rows.Scan(&bytScan)
			if (err != nil) && !tc.then.wantErrorByt {
				t.Errorf("%s: unexpected bytes scan error: %v", tc.name, err)
			}
			if (err == nil) && (tc.then.wantErrorByt) {
				t.Errorf("%s: expected bytes scan error, but error was %v", tc.name, err)
			}
			if !reflect.DeepEqual(bytScan, tc.then.wantByt) {
				t.Errorf("Unexpected %s to bytes conversion, want %s got %s\n", tc.typeName, tc.then.wantByt, bytScan)
			}

			// Int
			var intScan int
			err = rows.Scan(&intScan)
			if (err != nil) && !tc.then.wantErrorInt {
				t.Errorf("%s: unexpected int scan error: %v", tc.name, err)
			}
			if (err == nil) && (tc.then.wantErrorInt) {
				t.Errorf("%s: expected int scan error, but error was %v", tc.name, err)
			}
			if intScan != tc.then.wantInt {
				t.Errorf("Unexpected %s to int conversion, want %d got %d\n", tc.typeName, tc.then.wantInt, intScan)
			}

			// Float
			var floScan float64
			err = rows.Scan(&floScan)
			if (err != nil) && !tc.then.wantErrorFlo {
				t.Errorf("%s: unexpected float scan error: %v", tc.name, err)
			}
			if (err == nil) && (tc.then.wantErrorFlo) {
				t.Errorf("%s: expected float scan error, but error was %v", tc.name, err)
			}
			if floScan != tc.then.wantFlo {
				t.Errorf("Unexpected %s to float conversion, want %f got %f\n", tc.typeName, tc.then.wantFlo, floScan)
			}

			// Bool
			var booScan bool
			err = rows.Scan(&booScan)
			if (err != nil) && !tc.then.wantErrorBoo {
				t.Errorf("%s: unexpected bool scan error: %v", tc.name, err)
			}
			if (err == nil) && (tc.then.wantErrorBoo) {
				t.Errorf("%s: expected bool scan error, but error was %v", tc.name, err)
			}
			if booScan != tc.then.wantBoo {
				t.Errorf("Unexpected int to bool conversion, want %t got %t\n", tc.then.wantBoo, booScan)
			}
		}

		// tear down.
		if tc.tearDown != "" {
			_, err = db.ExecContext(ctx, tc.tearDown)
			if err != nil {
				t.Fatalf("%s: error in tearing down table: %v", tc.name, err)
			}
		}

	}

}
