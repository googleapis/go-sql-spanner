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
	"context"
	cryptorand "crypto/rand"
	"database/sql"
	"flag"
	"fmt"
	"log"
	"math"
	"math/big"
	"math/rand"
	"net"
	"os"
	"reflect"
	"strconv"
	"sync"
	"testing"
	"time"

	"cloud.google.com/go/civil"
	"cloud.google.com/go/spanner"
	database "cloud.google.com/go/spanner/admin/database/apiv1"
	"cloud.google.com/go/spanner/admin/database/apiv1/databasepb"
	instance "cloud.google.com/go/spanner/admin/instance/apiv1"
	"cloud.google.com/go/spanner/admin/instance/apiv1/instancepb"
	"github.com/google/go-cmp/cmp"
	"google.golang.org/api/iterator"
	"google.golang.org/grpc/codes"
)

var projectId, instanceId string
var skipped bool

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
			Labels: map[string]string{
				"gosqltestinstance": "true",
				"createdat":         fmt.Sprintf("t%d", time.Now().Unix()),
			},
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
	// Also delete any stale test instances that might still be around on the project.
	return func() {
		instanceAdmin, err := instance.NewInstanceAdminClient(ctx)
		if err != nil {
			return
		}
		// Delete this test instance.
		instanceAdmin.DeleteInstance(ctx, &instancepb.DeleteInstanceRequest{
			Name: fmt.Sprintf("projects/%s/instances/%s", projectId, instanceId),
		})
		// Also delete any other stale test instance.
		instances := instanceAdmin.ListInstances(ctx, &instancepb.ListInstancesRequest{
			Parent: fmt.Sprintf("projects/%s", projectId),
			Filter: "label.gosqltestinstance:*",
		})
		for {
			instance, err := instances.Next()
			if err == iterator.Done {
				break
			}
			if err != nil {
				log.Printf("failed to fetch instances during cleanup: %v", err)
				break
			}
			if createdAtString, ok := instance.Labels["createdat"]; ok {
				// Strip the leading 't' from the value.
				seconds, err := strconv.ParseInt(createdAtString[1:], 10, 64)
				if err != nil {
					log.Printf("failed to parse created time from string %q of instance %s: %v", createdAtString, instance.Name, err)
				} else {
					diff := time.Duration(time.Now().Unix()-seconds) * time.Second
					if diff > time.Hour*2 {
						log.Printf("deleting stale test instance %s", instance.Name)
						instanceAdmin.DeleteInstance(ctx, &instancepb.DeleteInstanceRequest{
							Name: instance.Name,
						})
					}
				}
			}
		}
	}, nil
}

func createTestDB(ctx context.Context, statements ...string) (dsn string, cleanup func(), err error) {
	databaseAdminClient, err := database.NewDatabaseAdminClient(ctx)
	if err != nil {
		return "", nil, err
	}
	defer databaseAdminClient.Close()
	prefix, ok := os.LookupEnv("SPANNER_TEST_DBID")
	if !ok {
		prefix = "gotest"
	}

	var uniqueid [4]byte
	if _, err = cryptorand.Read(uniqueid[:]); err != nil {
		return "", nil, err
	}

	databaseId := fmt.Sprintf("%s-%x", prefix, uniqueid)
	opDB, err := databaseAdminClient.CreateDatabase(ctx, &databasepb.CreateDatabaseRequest{
		Parent:          fmt.Sprintf("projects/%s/instances/%s", projectId, instanceId),
		CreateStatement: fmt.Sprintf("CREATE DATABASE `%s`", databaseId),
		ExtraStatements: statements,
	})
	if err != nil {
		return "", nil, err
	} else {
		// Wait for the database creation to finish.
		_, err := opDB.Wait(ctx)
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
	_, hasCredentials := os.LookupEnv("GOOGLE_APPLICATION_CREDENTIALS")
	_, hasEmulator := os.LookupEnv("SPANNER_EMULATOR_HOST")
	if !(hasCredentials || hasEmulator) {
		log.Println("Skipping integration tests as no credentials and no emulator host has been set")
		skipped = true
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
	if skipped {
		t.Skip("Integration tests skipped")
	}
}

func TestQueryContext(t *testing.T) {
	skipIfShort(t)
	t.Parallel()

	// Create test database.
	ctx := context.Background()
	dsn, cleanup, err := createTestDB(ctx,
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

func TestTypeRoundtrip(t *testing.T) {
	skipIfShort(t)
	t.Parallel()

	// This test checks that all types in checkIsValidType can be used as a query argument and scanned as a result.
	//
	// The commented out types are currently not working correctly.
	//
	// The tests that use "scan" could be improved to allow scanning into the same type,
	// that it was converted to.

	ctx := context.Background()
	dsn, cleanup, err := createTestDB(ctx)
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

	now := time.Now().UTC() // spanner doesn't preserve timezone

	tests := []struct {
		in     any
		scan   any // can be nil, if `in` matches output type.
		skipeq bool
	}{
		{in: sql.NullInt64{Valid: true, Int64: 197}},
		{in: sql.NullTime{Valid: true, Time: now}},
		{in: sql.NullString{Valid: true, String: "hello"}},
		{in: sql.NullFloat64{Valid: true, Float64: 3.14}},
		{in: sql.NullBool{Valid: true, Bool: true}},
		// {in: sql.NullInt32{Valid: true, Int32: 197}},
		// string variants
		{in: "hello"},
		{in: spanner.NullString{Valid: true, StringVal: "hello"}},
		{in: []string{"hello"}, scan: pointerTo([]spanner.NullString{})},
		{in: []spanner.NullString{{Valid: true, StringVal: "hello"}}},
		// *string variants
		{in: pointerTo("hello")},
		{in: []*string{pointerTo("hello")}, scan: pointerTo([]spanner.NullString{})},
		// []byte variants
		{in: []byte{1, 2, 3}},
		{in: [][]byte{{1, 2, 3}}},
		// uint, *uint variants
		{in: uint(197)},
		{in: []uint{197}, scan: pointerTo([]spanner.NullInt64{})},
		{in: pointerTo(uint(197))},
		{in: []*uint{pointerTo(uint(197))}, scan: pointerTo([]spanner.NullInt64{})},
		{in: pointerTo([]uint{197}), scan: pointerTo([]spanner.NullInt64{})},
		// int, *int variants
		{in: int(197)},
		{in: []int{197}, scan: pointerTo([]spanner.NullInt64{})},
		{in: pointerTo(int(197))},
		{in: []*int{pointerTo(197)}, scan: pointerTo([]spanner.NullInt64{})},
		{in: pointerTo([]int{197}), scan: pointerTo([]spanner.NullInt64{})},
		// int64, *int64 variants
		{in: int64(197)},
		{in: []int64{}, scan: pointerTo([]spanner.NullInt64{})},
		{in: spanner.NullInt64{Valid: true, Int64: 197}},
		{in: []spanner.NullInt64{{Valid: true, Int64: 197}}},
		{in: pointerTo(int64(197))},
		{in: []*int64{pointerTo(int64(197))}, scan: pointerTo([]spanner.NullInt64{})},
		// uint64, *uint64 variants
		{in: uint64(197)},
		{in: []uint64{}, scan: pointerTo([]spanner.NullInt64{})},
		{in: pointerTo(uint64(197))},
		{in: []*uint64{pointerTo(uint64(197))}, scan: pointerTo([]spanner.NullInt64{})},
		// bool variants
		{in: true},
		{in: []bool{true}, scan: pointerTo([]spanner.NullBool{})},
		{in: spanner.NullBool{Valid: true, Bool: true}},
		{in: []spanner.NullBool{{Valid: true, Bool: true}}, scan: pointerTo([]spanner.NullBool{})},
		{in: pointerTo(true)},
		{in: []*bool{pointerTo(true)}, scan: pointerTo([]spanner.NullBool{})},
		// float32 variants
		{in: float32(3.14)},
		{in: []float32{3.14}, scan: pointerTo([]spanner.NullFloat32{})},
		{in: spanner.NullFloat32{Valid: true, Float32: 3.14}},
		{in: []spanner.NullFloat32{{Valid: true, Float32: 3.14}}},
		{in: pointerTo(float32(3.14))},
		{in: []*float32{pointerTo(float32(3.14))}, scan: pointerTo([]spanner.NullFloat32{})},
		// float64 variants
		{in: float64(3.14)},
		{in: []float64{3.14}, scan: pointerTo([]spanner.NullFloat64{})},
		{in: spanner.NullFloat64{Valid: true, Float64: 3.14}},
		{in: []spanner.NullFloat64{{Valid: true, Float64: 3.14}}},
		{in: pointerTo(float64(3.14))},
		{in: []*float64{pointerTo(float64(3.14))}, scan: pointerTo([]spanner.NullFloat64{})},
		// Numeric, big.Rat variants
		{in: *big.NewRat(19, 17), skipeq: true},
		{in: []big.Rat{*big.NewRat(19, 17)}, scan: pointerTo([]spanner.NullNumeric{})},
		{in: big.NewRat(19, 17), skipeq: true},
		{in: []*big.Rat{big.NewRat(19, 17)}, scan: pointerTo([]spanner.NullNumeric{})},
		{in: spanner.NullNumeric{Valid: true, Numeric: *big.NewRat(19, 100)}},
		{in: []spanner.NullNumeric{{Valid: true, Numeric: *big.NewRat(19, 100)}}},
		// time.Time variants
		{in: now},
		{in: []time.Time{now}, scan: pointerTo([]spanner.NullTime{})},
		{in: spanner.NullTime{Valid: true, Time: now}},
		{in: []spanner.NullTime{{Valid: true, Time: now}}},
		{in: pointerTo(now)},
		{in: []*time.Time{pointerTo(now)}, scan: pointerTo([]spanner.NullTime{})},
		// civil.Date variants
		{in: civil.DateOf(now)},
		{in: []civil.Date{civil.DateOf(now)}, scan: pointerTo([]spanner.NullDate{})},
		{in: spanner.NullDate{Valid: true, Date: civil.DateOf(now)}},
		{in: []spanner.NullDate{{Valid: true, Date: civil.DateOf(now)}}},
		{in: pointerTo(civil.DateOf(now))},
		{in: []*civil.Date{pointerTo(civil.DateOf(now))}, scan: pointerTo([]spanner.NullDate{})},
		// JSON variants
		{in: spanner.NullJSON{Valid: true, Value: map[string]any{"a": 13}}, skipeq: true},
		{in: []spanner.NullJSON{{Valid: true, Value: map[string]any{"a": 13}}}, skipeq: true},
		// Standard library type alias examples
		{in: net.IPv6loopback},
		{in: time.Duration(1)},
	}

	for _, test := range tests {
		t.Run(fmt.Sprintf("%T", test.in), func(t *testing.T) {
			out := test.scan
			if out == nil {
				inType := reflect.TypeOf(test.in)
				out = reflect.New(inType).Interface()
			}

			err := db.QueryRow("SELECT @in", test.in).Scan(out)
			if err != nil {
				t.Fatalf("failed to query and scan: %v", err)
			}

			if test.scan == nil && !test.skipeq {
				// if the scanned type and input type are the same
				// then we can easily check the result.
				outelem := reflect.ValueOf(out).Elem().Interface()
				if !reflect.DeepEqual(test.in, outelem) {
					t.Fatalf("wrong result:\ngot  %#v\nwant %#v", outelem, test.in)
				}
			}
		})
	}
}

func TestExecContextDml(t *testing.T) {
	skipIfShort(t)
	t.Parallel()

	ctx := context.Background()
	dsn, cleanup, err := createTestDB(ctx,
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
	dsn, cleanup, err := createTestDB(ctx,
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

func TestRowsAtomicTypePermute(t *testing.T) {
	skipIfShort(t)
	t.Parallel()

	ctx := context.Background()
	dsn, cleanup, err := createTestDB(ctx,
		`CREATE TABLE TestDiffTypeBytes (val BYTES(1024)) PRIMARY KEY (val)`,
		`CREATE TABLE TestDiffTypeString (val STRING(1024)) PRIMARY KEY (val)`,
		`CREATE TABLE TestDiffTypeInt (val INT64) PRIMARY KEY (val)`,
		`CREATE TABLE  TestDiffTypeFloat (val FLOAT64) PRIMARY KEY (val)`,
		`CREATE TABLE TestDiffTypeBool (val BOOL) PRIMARY KEY (val)`,
	)
	if err != nil {
		t.Fatalf("failed to create test db: %v", err)
	}
	defer cleanup()

	db, err := sql.Open("spanner", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	type then struct {
		wantString      string
		wantErrorString bool
		wantBytes       []byte
		wantErrorBytes  bool
		wantInt         int
		wantErrorInt    bool
		wantFloat       float64
		wantErrorFloat  bool
		wantBool        bool
		wantErrorBool   bool
		wantInt8        int8
		wantErrorInt8   bool
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
				`INSERT INTO TestDiffTypeBytes (val) VALUES (CAST("hello" as bytes)) `,
			},
			when: `SELECT * FROM TestDiffTypeBytes`,
			then: then{
				wantString:     "hello",
				wantBytes:      []byte("hello"),
				wantErrorInt:   true,
				wantErrorFloat: true,
				wantErrorBool:  true,
				wantErrorInt8:  true,
			},
			tearDown: `DELETE FROM TestDiffTypeBytes WHERE TRUE`,
		},

		{
			name:     "read spanner bytes (\"1\") into go types",
			typeName: "bytes",
			given: []string{
				`INSERT INTO TestDiffTypeBytes  (val) VALUES (CAST("1" as bytes)) `,
			},
			when: `SELECT * FROM TestDiffTypeBytes `,
			then: then{
				wantString: "1",
				wantBytes:  []byte("1"),
				wantInt:    1,
				wantFloat:  1,
				wantBool:   true,
				wantInt8:   1,
			},
			tearDown: `DELETE FROM TestDiffTypeBytes WHERE TRUE`,
		},
		{
			name:     "read spanner string (\"hello\") into go types",
			typeName: "string",
			given: []string{
				`INSERT INTO  TestDiffTypeString (val) VALUES ("hello")`,
			},
			when: `SELECT * FROM TestDiffTypeString `,
			then: then{
				wantString:     "hello",
				wantBytes:      []byte("hello"),
				wantErrorInt:   true,
				wantErrorFloat: true,
				wantErrorBool:  true,
				wantErrorInt8:  true,
			},
			tearDown: `DELETE FROM TestDiffTypeString WHERE TRUE`,
		},
		{
			name:     "read spanner int (1) into go types",
			typeName: "int",
			given: []string{
				`INSERT INTO TestDiffTypeInt (val) VALUES (1)`,
			},
			when: `SELECT * FROM TestDiffTypeInt`,
			then: then{
				wantString: "1",
				wantBytes:  []byte("1"),
				wantInt:    1,
				wantFloat:  1,
				wantBool:   true,
				wantInt8:   1,
			},
			tearDown: `DELETE FROM TestDiffTypeInt WHERE TRUE`,
		},

		{
			name:     "read spanner int (42) into go types",
			typeName: "int",
			given: []string{
				`INSERT INTO TestDiffTypeInt (val) VALUES (42)`,
			},
			when: `SELECT * FROM TestDiffTypeInt`,
			then: then{
				wantString:    "42",
				wantBytes:     []byte("42"),
				wantInt:       42,
				wantFloat:     42,
				wantErrorBool: true,
				wantInt8:      42,
			},
			tearDown: `DELETE FROM TestDiffTypeInt WHERE TRUE`,
		},
		{
			name:     "read spanner float (42) into go types",
			typeName: "float",
			given: []string{
				`INSERT INTO TestDiffTypeFloat (val) VALUES (42)`,
			},
			when: `SELECT * FROM TestDiffTypeFloat`,
			then: then{
				wantString:    "42",
				wantBytes:     []byte("42"),
				wantInt:       42,
				wantFloat:     42,
				wantErrorBool: true,
				wantInt8:      42,
			},
			tearDown: `DELETE FROM TestDiffTypeFloat WHERE TRUE`,
		},
		{
			name:     "read spanner float (42.5) into go types",
			typeName: "float",
			given: []string{
				`INSERT INTO TestDiffTypeFloat (val) VALUES (42.5)`,
			},
			when: `SELECT * FROM TestDiffTypeFloat`,
			then: then{
				wantString:    "42.5",
				wantBytes:     []byte("42.5"),
				wantErrorInt:  true,
				wantFloat:     42.5,
				wantErrorBool: true,
				wantErrorInt8: true,
			},
			tearDown: `DELETE FROM TestDiffTypeFloat WHERE TRUE`,
		},
		{
			name:     "read spanner bool into go types",
			typeName: "bool",
			given: []string{
				`INSERT INTO  TestDiffTypeBool (val) VALUES (TRUE)`,
			},
			when: `SELECT * FROM TestDiffTypeBool `,
			then: then{
				wantString:     "true",
				wantBytes:      []byte("true"),
				wantErrorInt:   true,
				wantErrorFloat: true,
				wantBool:       true,
				wantErrorInt8:  true,
			},
			tearDown: `DELETE FROM TestDiffTypeBool WHERE TRUE`,
		},
		{
			name:     "read spanner max int64 into go types",
			typeName: "int",
			given: []string{
				`INSERT INTO TestDiffTypeInt (val) VALUES (9223372036854775807)`,
			},
			when: `SELECT * FROM TestDiffTypeInt`,
			then: then{
				wantString:    "9223372036854775807",
				wantBytes:     []byte("9223372036854775807"),
				wantInt:       9223372036854775807,
				wantFloat:     9223372036854775807,
				wantErrorBool: true,
				wantErrorInt8: true,
			},
			tearDown: `DELETE FROM TestDiffTypeInt WHERE TRUE`,
		},
		{
			name:     "read spanner min int64 into go types",
			typeName: "int",
			given: []string{
				`INSERT INTO TestDiffTypeInt (val) VALUES (-9223372036854775808)`,
			},
			when: `SELECT * FROM TestDiffTypeInt`,
			then: then{
				wantString:    "-9223372036854775808",
				wantBytes:     []byte("-9223372036854775808"),
				wantInt:       -9223372036854775808,
				wantFloat:     -9223372036854775808,
				wantErrorBool: true,
				wantErrorInt8: true,
			},
			tearDown: `DELETE FROM TestDiffTypeInt WHERE TRUE`,
		},
		{
			name:     "read spanner max int8 into go types",
			typeName: "int",
			given: []string{
				`INSERT INTO TestDiffTypeInt (val) VALUES (127)`,
			},
			when: `SELECT * FROM TestDiffTypeInt`,
			then: then{
				wantString:    "127",
				wantBytes:     []byte("127"),
				wantInt:       127,
				wantFloat:     127,
				wantErrorBool: true,
				wantInt8:      127,
			},
			tearDown: `DELETE FROM TestDiffTypeInt WHERE TRUE`,
		},
		{
			name:     "read spanner max uint8 into go types",
			typeName: "int",
			given: []string{
				`INSERT INTO TestDiffTypeInt (val) VALUES (255)`,
			},
			when: `SELECT * FROM TestDiffTypeInt`,
			then: then{
				wantString:    "255",
				wantBytes:     []byte("255"),
				wantInt:       255,
				wantFloat:     255,
				wantErrorBool: true,
				wantErrorInt8: true,
			},
			tearDown: `DELETE FROM TestDiffTypeInt WHERE TRUE`,
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
			if (err != nil) && (!tc.then.wantErrorString) {
				t.Errorf("%s: unexpected string scan error: %v", tc.name, err)
			}
			if (err == nil) && (tc.then.wantErrorString) {
				t.Errorf("%s: expected string scan error, but error was %v", tc.name, err)
			}
			if strScan != tc.then.wantString {
				t.Errorf("Unexpected %s to string conversion, want %s got %s\n", tc.typeName, tc.then.wantString, strScan)
			}

			// Bytes.
			var bytScan []byte
			err = rows.Scan(&bytScan)
			if (err != nil) && !tc.then.wantErrorBytes {
				t.Errorf("%s: unexpected bytes scan error: %v", tc.name, err)
			}
			if (err == nil) && (tc.then.wantErrorBytes) {
				t.Errorf("%s: expected bytes scan error, but error was %v", tc.name, err)
			}
			if !reflect.DeepEqual(bytScan, tc.then.wantBytes) {
				t.Errorf("Unexpected %s to bytes conversion, want %s got %s\n", tc.typeName, tc.then.wantBytes, bytScan)
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
			if (err != nil) && !tc.then.wantErrorFloat {
				t.Errorf("%s: unexpected float scan error: %v", tc.name, err)
			}
			if (err == nil) && (tc.then.wantErrorFloat) {
				t.Errorf("%s: expected float scan error, but error was %v", tc.name, err)
			}
			if floScan != tc.then.wantFloat {
				t.Errorf("Unexpected %s to float conversion, want %f got %f\n", tc.typeName, tc.then.wantFloat, floScan)
			}

			// Bool.
			var booScan bool
			err = rows.Scan(&booScan)
			if (err != nil) && !tc.then.wantErrorBool {
				t.Errorf("%s: unexpected bool scan error: %v", tc.name, err)
			}
			if (err == nil) && (tc.then.wantErrorBool) {
				t.Errorf("%s: expected bool scan error, but error was %v", tc.name, err)
			}
			if booScan != tc.then.wantBool {
				t.Errorf("Unexpected int to bool conversion, want %t got %t\n", tc.then.wantBool, booScan)
			}

			// Int8.
			var int8Scan int8
			err = rows.Scan(&int8Scan)
			if (err != nil) && !tc.then.wantErrorInt8 {
				t.Errorf("%s: unexpected int8 scan error: %v", tc.name, err)
			}
			if (err == nil) && (tc.then.wantErrorInt8) {
				t.Errorf("%s: expected int8 scan error, but error was %v", tc.name, err)
			}
			if int8Scan != tc.then.wantInt8 {
				t.Errorf("Unexpected int to int8 conversion, want %d got %d\n", tc.then.wantInt8, int8Scan)
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

func TestRowsOverflowRead(t *testing.T) {
	skipIfShort(t)
	t.Parallel()

	ctx := context.Background()
	dsn, cleanup, err := createTestDB(ctx,
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
	dsn, cleanup, err := createTestDB(ctx,
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

func TestStaleRead(t *testing.T) {
	skipIfShort(t)
	t.Parallel()

	ctx := context.Background()
	dsn, cleanup, err := createTestDB(ctx,
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

	conn, err := db.Conn(ctx)
	if err != nil {
		t.Fatalf("failed to get connection: %v", err)
	}
	var ts time.Time
	if err := conn.QueryRowContext(ctx, "SELECT CURRENT_TIMESTAMP").Scan(&ts); err != nil {
		t.Fatalf("failed to get current timestamp: %v", err)
	}
	_, err = db.ExecContext(ctx, "INSERT INTO Singers (SingerId, FirstName, LastName) VALUES (1, 'First', 'Last')")
	if err != nil {
		t.Fatalf("failed to insert a row: %v", err)
	}
	stmt, err := conn.PrepareContext(ctx, "SELECT * FROM Singers")
	if err != nil {
		t.Fatalf("failed to prepare query: %v", err)
	}

	// Set a timestamp that is before the row was inserted as the read timestamp.
	if _, err := conn.ExecContext(ctx, fmt.Sprintf("SET READ_ONLY_STALENESS='READ_TIMESTAMP %s'", ts.Format(time.RFC3339Nano))); err != nil {
		t.Fatalf("failed to set read-only staleness: %v", err)
	}
	// The result should now be empty as we are reading at a timestamp before the row was inserted.
	if err := verifyResult(ctx, stmt, true); err != nil {
		t.Fatal(err)
	}

	// Now use a timestamp that is after the row was inserted.
	if err := conn.QueryRowContext(ctx, "SELECT CURRENT_TIMESTAMP").Scan(&ts); err != nil {
		t.Fatalf("failed to get current timestamp: %v", err)
	}
	if _, err := conn.ExecContext(ctx, fmt.Sprintf("SET READ_ONLY_STALENESS='MIN_READ_TIMESTAMP %s'", ts.Format(time.RFC3339Nano))); err != nil {
		t.Fatalf("failed to set read-only staleness: %v", err)
	}

	// Executing the same statement again should now return a value, as the min_read_timestamp has
	// been set to a value that is after the moment the row was inserted.
	if err := verifyResult(ctx, stmt, false); err != nil {
		t.Fatal(err)
	}
}

func verifyResult(ctx context.Context, stmt *sql.Stmt, empty bool) error {
	rows, err := stmt.QueryContext(ctx)
	if err != nil {
		return fmt.Errorf("failed to execute query: %v", err)
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
	dsn, cleanup, err := createTestDB(ctx, getTableWithAllTypesDdl("TestAllTypes"))
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
		jsonCol           interface{}
		boolArrayCol      []spanner.NullBool
		stringArrayCol    []spanner.NullString
		bytesArrayCol     [][]byte
		int64ArrayCol     []spanner.NullInt64
		float64ArrayCol   []spanner.NullFloat64
		numericArrayCol   []spanner.NullNumeric
		dateArrayCol      []spanner.NullDate
		timestampArrayCol []spanner.NullTime
		jsonArrayCol      interface{}
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
				1, true, "test", []byte("testbytes"), int64(1), 3.14, numeric("6.626"), date("2021-07-28"),
				time.Date(2021, 7, 28, 15, 8, 30, 30294, time.UTC),
				nullJsonOrString(true, `{"key": "value", "other-key": ["value1", "value2"]}`),
				[]spanner.NullBool{{Valid: true, Bool: true}, {}, {Valid: true, Bool: false}},
				[]spanner.NullString{{Valid: true, StringVal: "test1"}, {}, {Valid: true, StringVal: "test2"}},
				[][]byte{[]byte("testbytes1"), nil, []byte("testbytes2")},
				[]spanner.NullInt64{{Valid: true, Int64: 1}, {}, {Valid: true, Int64: 2}},
				[]spanner.NullFloat64{{Valid: true, Float64: 3.14}, {}, {Valid: true, Float64: 6.626}},
				[]spanner.NullNumeric{{Valid: true, Numeric: numeric("3.14")}, {}, {Valid: true, Numeric: numeric("6.626")}},
				[]spanner.NullDate{{Valid: true, Date: date("2021-07-28")}, {}, {Valid: true, Date: date("2000-02-29")}},
				[]spanner.NullTime{{Valid: true, Time: time.Date(2021, 7, 28, 15, 16, 1, 999999999, time.UTC)}},
				nullJsonOrStringArray([]spanner.NullJSON{nullJson(true, `{"key1": "value1", "other-key1": ["value1", "value2"]}`)}),
			},
			want: AllTypesRow{1,
				sql.NullBool{Valid: true, Bool: true}, sql.NullString{Valid: true, String: "test"}, []byte("testbytes"),
				sql.NullInt64{Valid: true, Int64: 1}, sql.NullFloat64{Valid: true, Float64: 3.14}, spanner.NullNumeric{Valid: true, Numeric: numeric("6.626")},
				spanner.NullDate{Valid: true, Date: date("2021-07-28")},
				sql.NullTime{Valid: true, Time: time.Date(2021, 7, 28, 15, 8, 30, 30294, time.UTC)},
				nullJsonOrString(true, `{"key": "value", "other-key": ["value1", "value2"]}`),
				[]spanner.NullBool{{Valid: true, Bool: true}, {}, {Valid: true, Bool: false}},
				[]spanner.NullString{{Valid: true, StringVal: "test1"}, {}, {Valid: true, StringVal: "test2"}},
				[][]byte{[]byte("testbytes1"), nil, []byte("testbytes2")},
				[]spanner.NullInt64{{Valid: true, Int64: 1}, {}, {Valid: true, Int64: 2}},
				[]spanner.NullFloat64{{Valid: true, Float64: 3.14}, {}, {Valid: true, Float64: 6.626}},
				[]spanner.NullNumeric{{Valid: true, Numeric: numeric("3.14")}, {}, {Valid: true, Numeric: numeric("6.626")}},
				[]spanner.NullDate{{Valid: true, Date: date("2021-07-28")}, {}, {Valid: true, Date: date("2000-02-29")}},
				[]spanner.NullTime{{Valid: true, Time: time.Date(2021, 7, 28, 15, 16, 1, 999999999, time.UTC)}},
				nullJsonOrStringArray([]spanner.NullJSON{nullJson(true, `{"key1": "value1", "other-key1": ["value1", "value2"]}`)}),
			},
		},
		{
			name: "Untyped null values",
			key:  2,
			input: []interface{}{
				2, nil, nil, nil, nil, nil, nil, nil, nil, nil,
				nil, nil, nil, nil, nil, nil, nil, nil, nil,
			},
			want: AllTypesRow{2,
				sql.NullBool{}, sql.NullString{}, []byte(nil),
				sql.NullInt64{}, sql.NullFloat64{}, spanner.NullNumeric{},
				spanner.NullDate{}, sql.NullTime{}, nullJsonOrString(false, ""),
				[]spanner.NullBool(nil),
				[]spanner.NullString(nil),
				[][]byte(nil),
				[]spanner.NullInt64(nil),
				[]spanner.NullFloat64(nil),
				[]spanner.NullNumeric(nil),
				[]spanner.NullDate(nil),
				[]spanner.NullTime(nil),
				nullJsonOrStringArray([]spanner.NullJSON(nil)),
			},
			// The emulator does not support untyped null values.
			skipOnEmulator: true,
		},
		{
			name: "Typed null values",
			key:  3,
			input: []interface{}{
				3, nilBool(), nilString(), []byte(nil), nilInt64(), nilFloat64(), nilRat(), nilDate(), nilTime(), nullJsonOrString(false, ""),
				[]spanner.NullBool(nil),
				[]spanner.NullString(nil),
				[][]byte(nil),
				[]spanner.NullInt64(nil),
				[]spanner.NullFloat64(nil),
				[]spanner.NullNumeric(nil),
				[]spanner.NullDate(nil),
				[]spanner.NullTime(nil),
				nullJsonOrStringArray([]spanner.NullJSON(nil)),
			},
			want: AllTypesRow{3,
				sql.NullBool{}, sql.NullString{}, []byte(nil),
				sql.NullInt64{}, sql.NullFloat64{}, spanner.NullNumeric{},
				spanner.NullDate{}, sql.NullTime{}, nullJsonOrString(false, ""),
				[]spanner.NullBool(nil),
				[]spanner.NullString(nil),
				[][]byte(nil),
				[]spanner.NullInt64(nil),
				[]spanner.NullFloat64(nil),
				[]spanner.NullNumeric(nil),
				[]spanner.NullDate(nil),
				[]spanner.NullTime(nil),
				nullJsonOrStringArray([]spanner.NullJSON(nil)),
			},
		},
		{
			name: "Null* struct values",
			key:  4,
			input: []interface{}{
				4, sql.NullBool{}, sql.NullString{}, []byte(nil), sql.NullInt64{}, sql.NullFloat64{},
				spanner.NullNumeric{}, spanner.NullDate{}, sql.NullTime{}, nullJsonOrString(false, ""),
				[]spanner.NullBool(nil),
				[]spanner.NullString(nil),
				[][]byte(nil),
				[]spanner.NullInt64(nil),
				[]spanner.NullFloat64(nil),
				[]spanner.NullNumeric(nil),
				[]spanner.NullDate(nil),
				[]spanner.NullTime(nil),
				nullJsonOrStringArray([]spanner.NullJSON(nil)),
			},
			want: AllTypesRow{4,
				sql.NullBool{}, sql.NullString{}, []byte(nil),
				sql.NullInt64{}, sql.NullFloat64{}, spanner.NullNumeric{},
				spanner.NullDate{}, sql.NullTime{}, nullJsonOrString(false, ""),
				[]spanner.NullBool(nil),
				[]spanner.NullString(nil),
				[][]byte(nil),
				[]spanner.NullInt64(nil),
				[]spanner.NullFloat64(nil),
				[]spanner.NullNumeric(nil),
				[]spanner.NullDate(nil),
				[]spanner.NullTime(nil),
				nullJsonOrStringArray([]spanner.NullJSON(nil)),
			},
		},
	}
	stmt, err := db.PrepareContext(ctx, `INSERT INTO TestAllTypes (key, boolCol, stringCol, bytesCol, int64Col, 
                                               float64Col, numericCol, dateCol, timestampCol, jsonCol, boolArrayCol,
                                               stringArrayCol, bytesArrayCol, int64ArrayCol, float64ArrayCol,
                                               numericArrayCol, dateArrayCol, timestampArrayCol, jsonArrayCol) VALUES (
                                               @key, @bool, @string, @bytes, @int64, @float64, @numeric, @date,
                                               @timestamp, @json, @boolArray, @stringArray, @bytesArray, @int64Array,
                                               @float64Array, @numericArray, @dateArray, @timestampArray, @jsonArray)`)
	if err != nil {
		t.Fatal(err)
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if test.skipOnEmulator && runsOnEmulator() {
				t.Skip("skipping on emulator")
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
				&allTypesRow.float64Col, &allTypesRow.numericCol, &allTypesRow.dateCol, &allTypesRow.timestampCol, &allTypesRow.jsonCol,
				&allTypesRow.boolArrayCol, &allTypesRow.stringArrayCol, &allTypesRow.bytesArrayCol, &allTypesRow.int64ArrayCol,
				&allTypesRow.float64ArrayCol, &allTypesRow.numericArrayCol, &allTypesRow.dateArrayCol, &allTypesRow.timestampArrayCol,
				&allTypesRow.jsonArrayCol,
			)
			if err != nil {
				t.Fatalf("could not query row: %v", err)
			}
			if !cmp.Equal(allTypesRow, test.want, cmp.AllowUnexported(AllTypesRow{}, big.Rat{}, big.Int{}), cmp.FilterValues(func(v1, v2 interface{}) bool {
				if !runsOnEmulator() {
					return false
				}
				// TODO: Remove the following exceptions once the emulator supports JSON.
				if reflect.TypeOf(v1) == reflect.TypeOf("") && reflect.TypeOf(v2) == reflect.TypeOf(spanner.NullString{}) {
					return true
				}
				if reflect.TypeOf(v2) == reflect.TypeOf("") && reflect.TypeOf(v1) == reflect.TypeOf(spanner.NullString{}) {
					return true
				}
				if reflect.TypeOf(v1) == reflect.TypeOf(nil) && reflect.TypeOf(v2) == reflect.TypeOf(spanner.NullString{}) {
					return true
				}
				if reflect.TypeOf(v2) == reflect.TypeOf(nil) && reflect.TypeOf(v1) == reflect.TypeOf(spanner.NullString{}) {
					return true
				}
				return false
			}, cmp.Ignore())) {
				t.Fatalf("row mismatch\nGot:  %v\nWant: %v", allTypesRow, test.want)
			}
		})
	}
}

func TestQueryInReadWriteTransaction(t *testing.T) {
	skipIfShort(t)
	t.Parallel()

	ctx := context.Background()
	dsn, cleanup, err := createTestDB(ctx, getTableWithAllTypesDdl("QueryReadWrite"))
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
                                               float64Col, numericCol, dateCol, timestampCol, jsonCol, boolArrayCol,
                                               stringArrayCol, bytesArrayCol, int64ArrayCol, float64ArrayCol,
                                               numericArrayCol, dateArrayCol, timestampArrayCol, jsonArrayCol) VALUES (
                                               @key, @bool, @string, @bytes, @int64, @float64, @numeric, @date,
                                               @timestamp, @json, @boolArray, @stringArray, @bytesArray, @int64Array,
                                               @float64Array, @numericArray, @dateArray, @timestampArray, @jsonArray)`)
	if err != nil {
		t.Fatal(err)
	}
	for row := int64(0); row < wantRowCount; row++ {
		res, err := stmt.ExecContext(ctx, row, row%2 == 0, fmt.Sprintf("%v", row), []byte(fmt.Sprintf("%v", row)),
			row, float64(row)/float64(3), numeric(fmt.Sprintf("%v.%v", row, row)),
			civil.DateOf(time.Unix(row, row)), time.Unix(row*1000, row),
			nullJsonOrString(true, fmt.Sprintf(`"key": "value%d"`, row)),
			[]bool{row%2 == 0, row%2 != 0}, []string{fmt.Sprintf("%v", row), fmt.Sprintf("%v", row*2)},
			[][]byte{[]byte(fmt.Sprintf("%v", row)), []byte(fmt.Sprintf("%v", row*2))},
			[]int64{row, row * 2}, []float64{float64(row) / float64(3), float64(row*2) / float64(3)},
			[]big.Rat{numeric(fmt.Sprintf("%v.%v", row, row)), numeric(fmt.Sprintf("%v.%v", row*2, row*2))},
			[]civil.Date{civil.DateOf(time.Unix(row, row)), civil.DateOf(time.Unix(row*2, row*2))},
			[]time.Time{time.Unix(row*1000, row), time.Unix(row*2000, row)},
			nullJsonOrStringArray([]spanner.NullJSON{
				nullJson(true, fmt.Sprintf(`"key1": "value%d"`, row)),
				nullJson(true, fmt.Sprintf(`"key2": "value%d"`, row*1000)),
			}),
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

func TestPDML(t *testing.T) {
	skipIfShort(t)
	t.Parallel()

	ctx := context.Background()
	dsn, cleanup, err := createTestDB(ctx,
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
	// Insert a couple of test rows.
	_, err = db.ExecContext(
		ctx, `INSERT INTO Singers (SingerId, FirstName, LastName)
					VALUES (1, 'First1', 'Last1'),
					       (2, 'First2', 'Last2'),
					       (3, 'First3', 'Last3'),
					       (4, 'First4', 'Last4'),
					       (5, 'First5', 'Last5')`)
	if err != nil {
		t.Fatalf("failed to insert test rows: %v", err)
	}

	// Delete all rows using PDML.
	conn, err := db.Conn(ctx)
	if err != nil {
		t.Fatalf("failed to get a connection: %v", err)
	}
	defer conn.Close()
	_, err = conn.ExecContext(ctx, "SET AUTOCOMMIT_DML_MODE='PARTITIONED_NON_ATOMIC'")
	if err != nil {
		t.Fatalf("failed to switch to PDML mode: %v", err)
	}
	res, err := conn.ExecContext(ctx, "DELETE FROM Singers WHERE TRUE")
	if err != nil {
		t.Fatalf("delete statement failed: %v", err)
	}
	affected, _ := res.RowsAffected()
	if g, w := affected, int64(5); g != w {
		t.Fatalf("rows affected mismatch\nGot: %v\nWant: %v", g, w)
	}
}

func TestBatchDml(t *testing.T) {
	skipIfShort(t)
	t.Parallel()

	ctx := context.Background()
	dsn, cleanup, err := createTestDB(ctx,
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

	conn, err := db.Conn(ctx)
	if err != nil {
		t.Fatalf("failed to get a connection: %v", err)
	}
	defer conn.Close()
	_, err = conn.ExecContext(ctx, "START BATCH DML")
	if err != nil {
		t.Fatalf("failed to start dml batch: %v", err)
	}

	rowCount := 10
	stmt, err := conn.PrepareContext(ctx, "INSERT INTO Singers (SingerId, FirstName, LastName) VALUES (@id, @first, @last)")
	if err != nil {
		t.Fatalf("failed to prepare statement: %v", err)
	}
	for i := 0; i < rowCount; i++ {
		_, err := stmt.ExecContext(ctx, i, fmt.Sprintf("First%d", i), fmt.Sprintf("Last%d", i))
		if err != nil {
			t.Fatalf("failed to insert test row: %v", err)
		}
	}
	res, err := conn.ExecContext(ctx, "RUN BATCH")
	if err != nil {
		t.Fatalf("failed to run dml batch: %v", err)
	}
	affected, _ := res.RowsAffected()
	if g, w := affected, int64(rowCount); g != w {
		t.Fatalf("rows affected mismatch\nGot: %v\nWant: %v", g, w)
	}
}

func TestBatchDdl(t *testing.T) {
	skipIfShort(t)
	t.Parallel()

	ctx := context.Background()
	dsn, cleanup, err := createTestDB(ctx)
	if err != nil {
		t.Fatalf("failed to create test db: %v", err)
	}
	defer cleanup()

	db, err := sql.Open("spanner", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	conn, err := db.Conn(ctx)
	if err != nil {
		t.Fatalf("failed to get a connection: %v", err)
	}
	defer conn.Close()
	// Start a DDL batch and create two test tables.
	_, err = conn.ExecContext(ctx, "START BATCH DDL")
	if err != nil {
		t.Fatalf("failed to start ddl batch: %v", err)
	}
	_, _ = conn.ExecContext(ctx, "CREATE TABLE Test1 (K INT64, V STRING(MAX)) PRIMARY KEY (K)")
	_, _ = conn.ExecContext(ctx, "CREATE TABLE Test2 (K INT64, V STRING(MAX)) PRIMARY KEY (K)")

	// Verify that the tables have not yet been created.
	row := conn.QueryRowContext(ctx, "SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA=''")
	var count int64
	_ = row.Scan(&count)
	if g, w := count, int64(0); g != w {
		t.Fatalf("table count mismatch\nGot: %v\nWant: %v", g, w)
	}
	// Run the batch and verify that the tables are created.
	if _, err := conn.ExecContext(ctx, "RUN BATCH"); err != nil {
		t.Fatalf("run batch failed: %v", err)
	}
	row = conn.QueryRowContext(ctx, "SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA=''")
	_ = row.Scan(&count)
	if g, w := count, int64(2); g != w {
		t.Fatalf("table count mismatch\nGot: %v\nWant: %v", g, w)
	}
}

// TestCanRetryTransaction shows that:
//  1. If the internal retry of aborted transactions is enabled, the transactions will be retried
//     successfully when that is possible, without any action needed from the caller.
//  2. If the internal retry of aborted transactions is disabled, the transactions in this test
//     will be aborted by Cloud Spanner, and these Aborted errors will be propagated to the caller.
func TestCanRetryTransaction(t *testing.T) {
	skipIfShort(t)
	t.Parallel()

	ctx := context.Background()
	dsn, cleanup, err := createTestDB(ctx, "CREATE TABLE RndSingers (SingerId INT64, FirstName STRING(MAX), LastName STRING(MAX)) PRIMARY KEY (SingerId)")
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
	jsonType := "JSON"
	if runsOnEmulator() {
		jsonType = "STRING(MAX)"
	}
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
            jsonCol      %s,
			boolArrayCol      ARRAY<BOOL>,
			stringArrayCol    ARRAY<STRING(MAX)>,
			bytesArrayCol     ARRAY<BYTES(MAX)>,
			int64ArrayCol     ARRAY<INT64>,
			float64ArrayCol   ARRAY<FLOAT64>,
			numericArrayCol   ARRAY<NUMERIC>,
			dateArrayCol      ARRAY<DATE>,
			timestampArrayCol ARRAY<TIMESTAMP>,
            jsonArrayCol      ARRAY<%s>,
		) PRIMARY KEY (key)`, fmt.Sprintf("`%s`", name), jsonType, jsonType)
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

func nullJsonOrString(valid bool, v string) interface{} {
	if runsOnEmulator() {
		return spanner.NullString{Valid: valid, StringVal: v}
	}
	return nullJson(valid, v)
}

func nullJsonOrStringArray(v []spanner.NullJSON) interface{} {
	if !runsOnEmulator() {
		return v
	}
	if reflect.ValueOf(v).IsNil() {
		return []spanner.NullString(nil)
	}
	res := make([]spanner.NullString, len(v))
	for i, j := range v {
		res[i] = spanner.NullString{Valid: j.Valid, StringVal: j.String()}
	}
	return res
}
