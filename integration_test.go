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
	"fmt"
	"log"
	"math"
	"math/big"
	"math/rand"
	"os"
	"reflect"
	"sync"
	"testing"
	"time"

	"cloud.google.com/go/civil"
	"cloud.google.com/go/spanner"
	database "cloud.google.com/go/spanner/admin/database/apiv1"
	instance "cloud.google.com/go/spanner/admin/instance/apiv1"
	"github.com/google/go-cmp/cmp"
	databasepb "google.golang.org/genproto/googleapis/spanner/admin/database/v1"
	instancepb "google.golang.org/genproto/googleapis/spanner/admin/instance/v1"
	"google.golang.org/grpc/codes"
)

var projectId, instanceId string

func init() {
	var ok bool

	// Get environment variables or set to default.
	if instanceId, ok = os.LookupEnv("SPANNER_TEST_INSTANCE"); !ok {
		instanceId = "test-instance"
	}
	if projectId, ok = os.LookupEnv("SPANNER_TEST_PROJECT"); !ok {
		projectId = "test-project"
	}
}

func runsOnEmulator() bool {
	if _, ok := os.LookupEnv("SPANNER_EMULATOR_HOST"); ok {
		return true
	}
	return false
}

func initTestInstance(config string) (cleanup func(), err error) {
	ctx := context.Background()
	instanceAdmin, err := instance.NewInstanceAdminClient(ctx)
	if err != nil {
		return nil, err
	}
	defer instanceAdmin.Close()
	// Check if the instance exists or not.
	_, err = instanceAdmin.GetInstance(ctx, &instancepb.GetInstanceRequest{
		Name: fmt.Sprintf("projects/%s/instances/%s", projectId, instanceId),
	})
	if err == nil {
		return func() {}, nil
	}
	if spanner.ErrCode(err) != codes.NotFound {
		return nil, err
	}

	// Instance does not exist. Create a temporary instance for this test run.
	// The instance will be deleted after the test run.
	op, err := instanceAdmin.CreateInstance(ctx, &instancepb.CreateInstanceRequest{
		Parent:     fmt.Sprintf("projects/%s", projectId),
		InstanceId: instanceId,
		Instance: &instancepb.Instance{
			Config:      fmt.Sprintf("projects/%s/instanceConfigs/%s", projectId, config),
			DisplayName: instanceId,
			NodeCount:   1,
		},
	})
	if err != nil {
		return nil, fmt.Errorf("could not create instance %s: %v", fmt.Sprintf("projects/%s/instances/%s", projectId, instanceId), err)
	} else {
		// Wait for the instance creation to finish.
		_, err := op.Wait(ctx)
		if err != nil {
			return nil, fmt.Errorf("waiting for instance creation to finish failed: %v", err)
		}
	}
	// Delete the instance after all tests have finished.
	return func() {
		instanceAdmin, err := instance.NewInstanceAdminClient(ctx)
		if err != nil {
			return
		}
		instanceAdmin.DeleteInstance(ctx, &instancepb.DeleteInstanceRequest{
			Name: fmt.Sprintf("projects/%s/instances/%s", projectId, instanceId),
		})
	}, nil
}

func createTestDb(ctx context.Context, statements ...string) (dsn string, cleanup func(), err error) {
	databaseAdminClient, err := database.NewDatabaseAdminClient(ctx)
	if err != nil {
		return "", nil, err
	}
	defer databaseAdminClient.Close()
	prefix, ok := os.LookupEnv("SPANNER_TEST_DBID")
	if !ok {
		prefix = "gotest"
	}
	currentTime := time.Now().UnixNano()
	databaseId := fmt.Sprintf("%s-%d", prefix, currentTime)
	opDb, err := databaseAdminClient.CreateDatabase(ctx, &databasepb.CreateDatabaseRequest{
		Parent:          fmt.Sprintf("projects/%s/instances/%s", projectId, instanceId),
		CreateStatement: fmt.Sprintf("CREATE DATABASE `%s`", databaseId),
		ExtraStatements: statements,
	})
	if err != nil {
		return "", nil, err
	} else {
		// Wait for the database creation to finish.
		_, err := opDb.Wait(ctx)
		if err != nil {
			return "", nil, fmt.Errorf("waiting for database creation to finish failed: %v", err)
		}
	}
	dsn = "projects/" + projectId + "/instances/" + instanceId + "/databases/" + databaseId
	cleanup = func() {
		databaseAdminClient, err := database.NewDatabaseAdminClient(ctx)
		if err != nil {
			return
		}
		defer databaseAdminClient.Close()
		databaseAdminClient.DropDatabase(ctx, &databasepb.DropDatabaseRequest{
			Database: fmt.Sprintf("projects/%s/instances/%s/databases/%s", projectId, instanceId, databaseId),
		})
	}
	return
}

func initIntegrationTests() (cleanup func(), err error) {
	flag.Parse() // Needed for testing.Short().
	noop := func() {}

	if testing.Short() {
		log.Println("Integration tests skipped in -short mode.")
		return noop, nil
	}

	// Automatically create test instance if necessary.
	config := "regional-us-east1"
	if _, ok := os.LookupEnv("SPANNER_EMULATOR_HOST"); ok {
		config = "emulator-config"
	}
	cleanup, err = initTestInstance(config)
	if err != nil {
		return nil, err
	}

	return cleanup, nil
}

func TestMain(m *testing.M) {
	cleanup, err := initIntegrationTests()
	if err != nil {
		log.Fatalf("could not init integration tests: %v", err)
		os.Exit(1)
	}
	res := m.Run()
	cleanup()
	os.Exit(res)
}

func skipIfShort(t *testing.T) {
	if testing.Short() {
		t.Skip("Integration tests skipped in -short mode.")
	}
}

func skipIfEmulator(t *testing.T, msg string) {
	if runsOnEmulator() {
		t.Skip(msg)
	}
}

func TestQueryContext(t *testing.T) {
	skipIfShort(t)
	t.Parallel()

	// Create test database.
	ctx := context.Background()
	dsn, cleanup, err := createTestDb(ctx,
		`CREATE TABLE TestQueryContext (
			A   STRING(1024),
			B  STRING(1024),
			C   STRING(1024)
		)	 PRIMARY KEY (A)`)
	if err != nil {
		t.Fatalf("failed to create test db: %v", err)
	}
	defer cleanup()
	// Open db.
	db, err := sql.Open("spanner", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

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
}

func TestExecContextDml(t *testing.T) {
	skipIfShort(t)
	t.Parallel()

	ctx := context.Background()
	dsn, cleanup, err := createTestDb(ctx,
		`CREATE TABLE TestExecContextDml (
			key	INT64,
			testString	STRING(1024),
			testBytes	BYTES(1024),
			testInt	INT64,
			testFloat	FLOAT64,
			testBool	BOOL
		) PRIMARY KEY (key)`)
	if err != nil {
		t.Fatalf("failed to create test db: %v", err)
	}
	defer cleanup()

	// Open db.
	db, err := sql.Open("spanner", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

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
}
func TestRowsAtomicTypes(t *testing.T) {
	skipIfShort(t)
	t.Parallel()

	ctx := context.Background()
	dsn, cleanup, err := createTestDb(ctx,
		`CREATE TABLE TestAtomicTypes (
			key	STRING(1024),
			testString	STRING(1024),
			testBytes	BYTES(1024),
			testInt	INT64,
			testFloat	FLOAT64,
			testBool	BOOL
		) PRIMARY KEY (key)`)
	if err != nil {
		t.Fatalf("failed to create test db: %v", err)
	}
	defer cleanup()

	// Open db.
	db, err := sql.Open("spanner", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

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
}

func TestRowsOverflowRead(t *testing.T) {
	skipIfShort(t)
	t.Parallel()

	ctx := context.Background()
	dsn, cleanup, err := createTestDb(ctx,
		`CREATE TABLE TestOverflowRead (
			key	STRING(1024),
			testString	STRING(1024),
			testBytes	BYTES(1024),
			testInt	INT64,
			testFloat	FLOAT64,
			testBool	BOOL
		) PRIMARY KEY (key)`)
	if err != nil {
		t.Fatalf("failed to create test db: %v", err)
	}
	defer cleanup()

	// Open db.
	db, err := sql.Open("spanner", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

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
}

func TestReadOnlyTransaction(t *testing.T) {
	skipIfShort(t)
	t.Parallel()

	ctx := context.Background()
	dsn, cleanup, err := createTestDb(ctx,
		`CREATE TABLE Singers (
          SingerId INT64,
          FirstName STRING(MAX),
          LastName STRING(MAX),
        ) PRIMARY KEY (SingerId)`)
	if err != nil {
		t.Fatalf("failed to create test db: %v", err)
	}
	defer cleanup()

	db, err := sql.Open("spanner", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	tx, err := db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		t.Fatalf("failed to begin read-only transaction: %v", err)
	}
	stmt, err := tx.PrepareContext(ctx, "SELECT * FROM Singers")
	if err != nil {
		t.Fatalf("failed to prepare query: %v", err)
	}
	// The result should be empty.
	if err := verifyResult(ctx, stmt, true); err != nil {
		t.Fatal(err)
	}
	// Insert a row in the table using a different transaction.
	res, err := db.ExecContext(ctx, "INSERT INTO Singers (SingerId, FirstName, LastName) VALUES (1, 'First', 'Last')")
	if err != nil {
		t.Fatalf("failed to insert a row: %v", err)
	}
	affected, err := res.RowsAffected()
	if err != nil {
		t.Fatalf("failed to get affected row count: %v", err)
	}
	if affected != 1 {
		t.Fatalf("affected rows mismatch\nGot: %v\nWant: %v", affected, 1)
	}
	// Executing the same statement on the read-only transaction again should
	// still return an empty result, as it is reading using a timestamp that is
	// before the new row was inserted.
	if err := verifyResult(ctx, stmt, true); err != nil {
		t.Fatal(err)
	}

	// Closing the current read-only transaction and starting a new one should
	// return the new row.
	if err := tx.Commit(); err != nil {
		t.Fatalf("failed to commit read-only transaction: %v", err)
	}
	tx, err = db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		t.Fatalf("failed to begin a new read-only transaction: %v", err)
	}
	stmt, err = tx.PrepareContext(ctx, "SELECT * FROM Singers")
	if err != nil {
		t.Fatalf("failed to prepare query: %v", err)
	}
	// The result should no longer be empty.
	if err := verifyResult(ctx, stmt, false); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("failed to commit read-only transaction: %v", err)
	}
}

func verifyResult(ctx context.Context, stmt *sql.Stmt, empty bool) error {
	rows, err := stmt.QueryContext(ctx)
	if err != nil {
		return fmt.Errorf("failed to query empty table: %v", err)
	}
	if rows.Next() {
		if empty {
			return fmt.Errorf("received unexpected row from empty table")
		}
	} else {
		if !empty {
			return fmt.Errorf("received unexpected empty result")
		}
	}
	if rows.Err() != nil {
		return fmt.Errorf("received unexpected error from query: %v", rows.Err())
	}
	if err := rows.Close(); err != nil {
		return fmt.Errorf("failed to close rows: %v", err)
	}
	return nil
}

func TestAllTypes(t *testing.T) {
	skipIfShort(t)
	t.Parallel()

	ctx := context.Background()
	dsn, cleanup, err := createTestDb(ctx, getTableWithAllTypesDdl("TestAllTypes"))
	if err != nil {
		t.Fatalf("failed to create test db: %v", err)
	}
	defer cleanup()

	// Open db.
	db, err := sql.Open("spanner", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	type AllTypesRow struct {
		key               int64
		boolCol           sql.NullBool
		stringCol         sql.NullString
		bytesCol          []byte
		int64Col          sql.NullInt64
		float64Col        sql.NullFloat64
		numericCol        spanner.NullNumeric
		dateCol           spanner.NullDate
		timestampCol      sql.NullTime
		boolArrayCol      []spanner.NullBool
		stringArrayCol    []spanner.NullString
		bytesArrayCol     [][]byte
		int64ArrayCol     []spanner.NullInt64
		float64ArrayCol   []spanner.NullFloat64
		numericArrayCol   []spanner.NullNumeric
		dateArrayCol      []spanner.NullDate
		timestampArrayCol []spanner.NullTime
	}

	tests := []struct {
		name           string
		key            int64
		input          []interface{}
		want           AllTypesRow
		skipOnEmulator bool
	}{
		{
			name: "Non-null values",
			key:  1,
			input: []interface{}{
				1, true, "test", []byte("testbytes"), int64(1), 3.14, numeric("6.626"), date("2021-07-28"), time.Date(2021, 7, 28, 15, 8, 30, 30294, time.UTC),
				[]spanner.NullBool{{Valid: true, Bool: true}, {}, {Valid: true, Bool: false}},
				[]spanner.NullString{{Valid: true, StringVal: "test1"}, {}, {Valid: true, StringVal: "test2"}},
				[][]byte{[]byte("testbytes1"), nil, []byte("testbytes2")},
				[]spanner.NullInt64{{Valid: true, Int64: 1}, {}, {Valid: true, Int64: 2}},
				[]spanner.NullFloat64{{Valid: true, Float64: 3.14}, {}, {Valid: true, Float64: 6.626}},
				[]spanner.NullNumeric{{Valid: true, Numeric: numeric("3.14")}, {}, {Valid: true, Numeric: numeric("6.626")}},
				[]spanner.NullDate{{Valid: true, Date: date("2021-07-28")}, {}, {Valid: true, Date: date("2000-02-29")}},
				[]spanner.NullTime{{Valid: true, Time: time.Date(2021, 7, 28, 15, 16, 1, 999999999, time.UTC)}},
			},
			want: AllTypesRow{1,
				sql.NullBool{Valid: true, Bool: true}, sql.NullString{Valid: true, String: "test"}, []byte("testbytes"),
				sql.NullInt64{Valid: true, Int64: 1}, sql.NullFloat64{Valid: true, Float64: 3.14}, spanner.NullNumeric{Valid: true, Numeric: numeric("6.626")},
				spanner.NullDate{Valid: true, Date: date("2021-07-28")}, sql.NullTime{Valid: true, Time: time.Date(2021, 7, 28, 15, 8, 30, 30294, time.UTC)},
				[]spanner.NullBool{{Valid: true, Bool: true}, {}, {Valid: true, Bool: false}},
				[]spanner.NullString{{Valid: true, StringVal: "test1"}, {}, {Valid: true, StringVal: "test2"}},
				[][]byte{[]byte("testbytes1"), nil, []byte("testbytes2")},
				[]spanner.NullInt64{{Valid: true, Int64: 1}, {}, {Valid: true, Int64: 2}},
				[]spanner.NullFloat64{{Valid: true, Float64: 3.14}, {}, {Valid: true, Float64: 6.626}},
				[]spanner.NullNumeric{{Valid: true, Numeric: numeric("3.14")}, {}, {Valid: true, Numeric: numeric("6.626")}},
				[]spanner.NullDate{{Valid: true, Date: date("2021-07-28")}, {}, {Valid: true, Date: date("2000-02-29")}},
				[]spanner.NullTime{{Valid: true, Time: time.Date(2021, 7, 28, 15, 16, 1, 999999999, time.UTC)}},
			},
		},
		{
			name: "Untyped null values",
			key:  2,
			input: []interface{}{
				2, nil, nil, nil, nil, nil, nil, nil, nil,
				nil, nil, nil, nil, nil, nil, nil, nil,
			},
			want: AllTypesRow{2,
				sql.NullBool{}, sql.NullString{}, []byte(nil),
				sql.NullInt64{}, sql.NullFloat64{}, spanner.NullNumeric{},
				spanner.NullDate{}, sql.NullTime{},
				[]spanner.NullBool(nil),
				[]spanner.NullString(nil),
				[][]byte(nil),
				[]spanner.NullInt64(nil),
				[]spanner.NullFloat64(nil),
				[]spanner.NullNumeric(nil),
				[]spanner.NullDate(nil),
				[]spanner.NullTime(nil),
			},
			// The emulator does not support untyped null values.
			skipOnEmulator: true,
		},
		{
			name: "Typed null values",
			key:  3,
			input: []interface{}{
				3, nilBool(), nilString(), []byte(nil), nilInt64(), nilFloat64(), nilRat(), nilDate(), nilTime(),
				[]spanner.NullBool(nil),
				[]spanner.NullString(nil),
				[][]byte(nil),
				[]spanner.NullInt64(nil),
				[]spanner.NullFloat64(nil),
				[]spanner.NullNumeric(nil),
				[]spanner.NullDate(nil),
				[]spanner.NullTime(nil),
			},
			want: AllTypesRow{3,
				sql.NullBool{}, sql.NullString{}, []byte(nil),
				sql.NullInt64{}, sql.NullFloat64{}, spanner.NullNumeric{},
				spanner.NullDate{}, sql.NullTime{},
				[]spanner.NullBool(nil),
				[]spanner.NullString(nil),
				[][]byte(nil),
				[]spanner.NullInt64(nil),
				[]spanner.NullFloat64(nil),
				[]spanner.NullNumeric(nil),
				[]spanner.NullDate(nil),
				[]spanner.NullTime(nil),
			},
		},
		{
			name: "Null* struct values",
			key:  4,
			input: []interface{}{
				// TODO: Fix the requirement to use spanner.NullString here.
				4, sql.NullBool{}, spanner.NullString{}, []byte(nil), sql.NullInt64{}, sql.NullFloat64{},
				spanner.NullNumeric{}, spanner.NullDate{}, sql.NullTime{},
				[]spanner.NullBool(nil),
				[]spanner.NullString(nil),
				[][]byte(nil),
				[]spanner.NullInt64(nil),
				[]spanner.NullFloat64(nil),
				[]spanner.NullNumeric(nil),
				[]spanner.NullDate(nil),
				[]spanner.NullTime(nil),
			},
			want: AllTypesRow{4,
				sql.NullBool{}, sql.NullString{}, []byte(nil),
				sql.NullInt64{}, sql.NullFloat64{}, spanner.NullNumeric{},
				spanner.NullDate{}, sql.NullTime{},
				[]spanner.NullBool(nil),
				[]spanner.NullString(nil),
				[][]byte(nil),
				[]spanner.NullInt64(nil),
				[]spanner.NullFloat64(nil),
				[]spanner.NullNumeric(nil),
				[]spanner.NullDate(nil),
				[]spanner.NullTime(nil),
			},
		},
	}
	stmt, err := db.PrepareContext(ctx, `INSERT INTO TestAllTypes (key, boolCol, stringCol, bytesCol, int64Col, 
                                               float64Col, numericCol, dateCol, timestampCol, boolArrayCol,
                                               stringArrayCol, bytesArrayCol, int64ArrayCol, float64ArrayCol,
                                               numericArrayCol, dateArrayCol, timestampArrayCol) VALUES (@key, @bool,
                                               @string, @bytes, @int64, @float64, @numeric, @date, @timestamp,
                                               @boolArray, @stringArray, @bytesArray, @int64Array, @float64Array,
                                               @numericArray, @dateArray, @timestampArray)`)
	for _, test := range tests {
		if runsOnEmulator() && test.skipOnEmulator {
			t.Logf("skipping test %q on emulator", test.name)
			continue
		}
		res, err := stmt.ExecContext(ctx, test.input...)
		if err != nil {
			t.Fatalf("insert failed: %v", err)
		}
		affected, err := res.RowsAffected()
		if err != nil {
			t.Fatalf("could not get rows affected: %v", err)
		}
		if g, w := affected, int64(1); g != w {
			t.Fatalf("affected rows mismatch\nGot: %v\nWant: %v", g, w)
		}
		// Read the row back.
		row := db.QueryRowContext(ctx, "SELECT * FROM TestAllTypes WHERE key=@key", test.key)
		var allTypesRow AllTypesRow
		err = row.Scan(
			&allTypesRow.key, &allTypesRow.boolCol, &allTypesRow.stringCol, &allTypesRow.bytesCol, &allTypesRow.int64Col,
			&allTypesRow.float64Col, &allTypesRow.numericCol, &allTypesRow.dateCol, &allTypesRow.timestampCol,
			&allTypesRow.boolArrayCol, &allTypesRow.stringArrayCol, &allTypesRow.bytesArrayCol, &allTypesRow.int64ArrayCol,
			&allTypesRow.float64ArrayCol, &allTypesRow.numericArrayCol, &allTypesRow.dateArrayCol, &allTypesRow.timestampArrayCol,
		)
		if err != nil {
			t.Fatalf("could not query row: %v", err)
		}
		if !cmp.Equal(allTypesRow, test.want, cmp.AllowUnexported(AllTypesRow{}, big.Rat{}, big.Int{})) {
			t.Fatalf("row mismatch\nGot:  %v\nWant: %v", allTypesRow, test.want)
		}
	}
}

func TestQueryInReadWriteTransaction(t *testing.T) {
	skipIfShort(t)
	t.Parallel()

	ctx := context.Background()
	dsn, cleanup, err := createTestDb(ctx, getTableWithAllTypesDdl("QueryReadWrite"))
	if err != nil {
		t.Fatalf("failed to create test db: %v", err)
	}
	defer cleanup()

	wantRowCount := int64(100)
	db, err := sql.Open("spanner", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	tx, err := db.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		t.Fatalf("begin transaction failed: %v", err)
	}
	stmt, err := tx.PrepareContext(ctx, `INSERT INTO QueryReadWrite (key, boolCol, stringCol, bytesCol, int64Col, 
                                               float64Col, numericCol, dateCol, timestampCol, boolArrayCol,
                                               stringArrayCol, bytesArrayCol, int64ArrayCol, float64ArrayCol,
                                               numericArrayCol, dateArrayCol, timestampArrayCol) VALUES (@key, @bool,
                                               @string, @bytes, @int64, @float64, @numeric, @date, @timestamp,
                                               @boolArray, @stringArray, @bytesArray, @int64Array, @float64Array,
                                               @numericArray, @dateArray, @timestampArray)`)
	for row := int64(0); row < wantRowCount; row++ {
		res, err := stmt.ExecContext(ctx, row, row%2 == 0, fmt.Sprintf("%v", row), []byte(fmt.Sprintf("%v", row)),
			row, float64(row)/float64(3), numeric(fmt.Sprintf("%v.%v", row, row)),
			civil.DateOf(time.Unix(row, row)), time.Unix(row*1000, row),
			[]bool{row%2 == 0, row%2 != 0}, []string{fmt.Sprintf("%v", row), fmt.Sprintf("%v", row*2)},
			[][]byte{[]byte(fmt.Sprintf("%v", row)), []byte(fmt.Sprintf("%v", row*2))},
			[]int64{row, row * 2}, []float64{float64(row) / float64(3), float64(row*2) / float64(3)},
			[]big.Rat{numeric(fmt.Sprintf("%v.%v", row, row)), numeric(fmt.Sprintf("%v.%v", row*2, row*2))},
			[]civil.Date{civil.DateOf(time.Unix(row, row)), civil.DateOf(time.Unix(row*2, row*2))},
			[]time.Time{time.Unix(row*1000, row), time.Unix(row*2000, row)},
		)
		if err != nil {
			t.Fatalf("insert failed: %v", err)
		}
		if c, _ := res.RowsAffected(); c != 1 {
			t.Fatalf("update count mismatch\nGot: %v\nWant: %v", c, 1)
		}
	}
	if err = tx.Commit(); err != nil {
		t.Fatalf("commit failed: %v", err)
	}

	// Select and iterate over all rows in a read/write transaction.
	tx, err = db.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		t.Fatalf("begin failed: %v", err)
	}
	rows, err := tx.QueryContext(ctx, "SELECT * FROM QueryReadWrite")
	if err != nil {
		t.Fatalf("query failed: %v", err)
	}
	rc := int64(0)
	for rows.Next() {
		// We don't care about the values here, as that is tested by other tests.
		// This test just verifies that we can iterate over a large result set in
		// a read/write transaction without any problems, and that the transaction
		// will never fail with an aborted error.
		rc++
	}
	if rows.Err() != nil {
		t.Fatalf("iterating over all rows failed: %v", err)
	}
	if err = rows.Close(); err != nil {
		t.Fatalf("closing rows failed: %v", err)
	}
	if rc != wantRowCount {
		t.Fatalf("row count mismatch\nGot: %v\nWant: %v", rc, wantRowCount)
	}
	if err = tx.Commit(); err != nil {
		t.Fatalf("commit failed: %v", err)
	}
}

// TestCanRetryTransaction shows that:
// 1. If the internal retry of aborted transactions is enabled, the transactions will be retried
//    successfully when that is possible, without any action needed from the caller.
// 2. If the internal retry of aborted transactions is disabled, the transactions in this test
//    will be aborted by Cloud Spanner, and these Aborted errors will be propagated to the caller.
func TestCanRetryTransaction(t *testing.T) {
	skipIfShort(t)
	t.Parallel()

	ctx := context.Background()
	dsn, cleanup, err := createTestDb(ctx, "CREATE TABLE RndSingers (SingerId INT64, FirstName STRING(MAX), LastName STRING(MAX)) PRIMARY KEY (SingerId)")
	if err != nil {
		t.Fatalf("failed to create test db: %v", err)
	}
	defer cleanup()

	db, err := sql.Open("spanner", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	for _, withInternalRetries := range []bool{true, false} {
		dsnTx := dsn
		if !withInternalRetries {
			dsnTx = fmt.Sprintf("%s;retryAbortsInternally=false", dsnTx)
		}
		dbTx, err := sql.Open("spanner", dsnTx)
		if err != nil {
			t.Fatalf("failed to open db: %v", err)
		}
		defer func() {
			dbTx.Close()
		}()

		numTransactions := 8
		results := make([]error, numTransactions)
		wg := &sync.WaitGroup{}
		wg.Add(numTransactions)
		for i := 0; i < numTransactions; i++ {
			<-time.After(time.Millisecond)
			go func(i int) {
				defer wg.Done()
				results[i] = insertRandomSingers(ctx, dbTx)
			}(i)
		}
		wg.Wait()
		aborted := false
		for i, err := range results {
			if err != nil {
				if withInternalRetries {
					t.Fatalf("insert of random singers failed: %d %v", i, err)
				} else {
					code := spanner.ErrCode(err)
					if code == codes.Aborted {
						aborted = true
					} else {
						t.Fatalf("insert of random singers failed: %d %v", i, err)
					}
				}
			}
		}
		// If the internal retry feature has been disabled, the aborted errors should
		// be propagated to the caller. The way that the insertRandomSinger function is
		// defined ensures that transactions will be aborted when multiple of these are
		// executed in parallel.
		if !withInternalRetries && !aborted {
			t.Fatalf("missing aborted error with internal retries disabled")
		}
	}
}

func insertRandomSingers(ctx context.Context, db *sql.DB) (err error) {
	rnd := rand.New(rand.NewSource(time.Now().UnixNano()))

	tx, err := db.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		return err
	}
	cleanup := func() {
		if err != nil {
			_ = tx.Rollback()
		}
	}
	defer cleanup()
	type Singer struct {
		SingerId  int64
		FirstName string
		LastName  string
	}
	// Insert at random between 1 and 10 rows.
	rows := rnd.Intn(10) + 1
	for row := 0; row < rows; row++ {
		id := rnd.Int63()
		prefix := fmt.Sprintf("%020d", id)
		firstName := fmt.Sprintf("FirstName-%04d", rnd.Intn(10000))
		// Last name contains the same value as the primary key with a random suffix.
		// This makes it possible to search for a singer using the last name and knowing
		// that the search will at most deliver one row (and it will be the same row each time).
		lastName := fmt.Sprintf("%s-%04d", prefix, rnd.Intn(10000))

		// Yes, this is highly inefficient, but that is intentional. This
		// will cause a large number of the transactions to be aborted.
		search := fmt.Sprintf("%s%%", prefix)
		r := tx.QueryRowContext(ctx, "SELECT * FROM RndSingers WHERE LastName LIKE @lastName ORDER BY LastName LIMIT 1", search)
		var singer Singer
		if err = r.Scan(&singer.SingerId, &singer.FirstName, &singer.LastName); err == sql.ErrNoRows {
			// Singer does not yet exist, so add it.
			res, err := tx.ExecContext(ctx, "INSERT INTO RndSingers (SingerId, FirstName, LastName) VALUES (@id, @firstName, @lastName)",
				id, firstName, lastName)
			if err != nil {
				return err
			}
			c, err := res.RowsAffected()
			if err != nil {
				return err
			}
			if c != 1 {
				err = fmt.Errorf("unexpected insert count: %v", c)
				return err
			}
		} else if err != nil {
			return err
		} else {
			// Singer already exists, update the first name.
			res, err := tx.ExecContext(ctx, "UPDATE RndSingers SET FirstName=@firstName WHERE SingerId=@id", firstName, id)
			if err != nil {
				return err
			}
			c, err := res.RowsAffected()
			if err != nil {
				return err
			}
			if c != 1 {
				err = fmt.Errorf("unexpected update count: %v", c)
				return err
			}
		}
	}
	err = tx.Commit()
	return err
}

func getTableWithAllTypesDdl(name string) string {
	return fmt.Sprintf(`CREATE TABLE %s (
			key          INT64,
			boolCol      BOOL,
			stringCol    STRING(MAX),
			bytesCol     BYTES(MAX),
			int64Col     INT64,
			float64Col   FLOAT64,
			numericCol   NUMERIC,
			dateCol      DATE,
			timestampCol TIMESTAMP,
			boolArrayCol      ARRAY<BOOL>,
			stringArrayCol    ARRAY<STRING(MAX)>,
			bytesArrayCol     ARRAY<BYTES(MAX)>,
			int64ArrayCol     ARRAY<INT64>,
			float64ArrayCol   ARRAY<FLOAT64>,
			numericArrayCol   ARRAY<NUMERIC>,
			dateArrayCol      ARRAY<DATE>,
			timestampArrayCol ARRAY<TIMESTAMP>,
		) PRIMARY KEY (key)`, fmt.Sprintf("`%s`", name))
}

func nilBool() *bool {
	var b *bool
	return b
}

func nilString() *string {
	var s *string
	return s
}

func nilInt64() *int64 {
	var i *int64
	return i
}

func nilFloat64() *float64 {
	var f *float64
	return f
}

func nilRat() *big.Rat {
	var r *big.Rat
	return r
}

func nilDate() *civil.Date {
	var d *civil.Date
	return d
}

func nilTime() *time.Time {
	var t *time.Time
	return t
}
