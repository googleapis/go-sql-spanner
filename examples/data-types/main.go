// Copyright 2021 Google LLC All Rights Reserved.
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

package main

import (
	"context"
	"database/sql"
	"fmt"
	"math/big"
	"os"
	"time"

	"cloud.google.com/go/civil"
	"cloud.google.com/go/spanner"
	_ "github.com/googleapis/go-sql-spanner"
	"github.com/googleapis/go-sql-spanner/examples"
)

var createTableStatement = `CREATE TABLE AllTypes (
			key            INT64,
			bool           BOOL,
			string         STRING(MAX),
			bytes          BYTES(MAX),
			int64          INT64,
			float32        FLOAT32,
			float64        FLOAT64,
			numeric        NUMERIC,
			date           DATE,
			timestamp      TIMESTAMP,
			boolArray      ARRAY<BOOL>,
			stringArray    ARRAY<STRING(MAX)>,
			bytesArray     ARRAY<BYTES(MAX)>,
			int64Array     ARRAY<INT64>,
			float32Array   ARRAY<FLOAT32>,
			float64Array   ARRAY<FLOAT64>,
			numericArray   ARRAY<NUMERIC>,
			dateArray      ARRAY<DATE>,
			timestampArray ARRAY<TIMESTAMP>,
		) PRIMARY KEY (key)`

// Sample showing how to work with the different data types that are supported by Cloud Spanner:
// 1. How to get data from columns of each type.
// 2. How to set data of each type as a statement parameter.
//
// Execute the sample with the command `go run main.go` from this directory.
func dataTypes(projectId, instanceId, databaseId string) error {
	ctx := context.Background()
	db, err := sql.Open("spanner", fmt.Sprintf("projects/%s/instances/%s/databases/%s", projectId, instanceId, databaseId))
	if err != nil {
		return fmt.Errorf("failed to open database connection: %v\n", err)
	}
	defer db.Close()

	// Statement parameters can be given as either native types (string, bool, int64, ...) or as nullable struct types
	// defined in the Spanner client library (spanner.NullString, spanner.NullBool, spanner.NullInt64, ...).

	// Insert a test row with all non-null values using DML and native types.
	if _, err := db.ExecContext(ctx, `INSERT INTO AllTypes (
                      key, bool, string, bytes, int64, float32, float64, numeric, date, timestamp,
                      boolArray, stringArray, bytesArray, int64Array, float32Array, float64Array, numericArray, dateArray, timestampArray)
                      VALUES (@key, @bool, @string, @bytes, @int64, @float32, @float64, @numeric, @date, @timestamp,
                              @boolArray, @stringArray, @bytesArray, @int64Array, @float32Array, @float64Array, @numericArray, @dateArray, @timestampArray)`,
		1, true, "string", []byte("bytes"), 100, float32(3.14), 3.14, *big.NewRat(1, 1), civil.DateOf(time.Now()), time.Now(),
		[]bool{true, false}, []string{"s1", "s2"}, [][]byte{[]byte("b1"), []byte("b2")}, []int64{1, 2},
		[]float32{1.1, 2.2}, []float64{1.1, 2.2}, []big.Rat{*big.NewRat(1, 2), *big.NewRat(1, 3)},
		[]civil.Date{{2021, 10, 12}, {2021, 10, 13}},
		[]time.Time{time.Now(), time.Now().Add(24 * time.Hour)}); err != nil {
		return fmt.Errorf("failed to insert a record with all non-null values using DML: %v", err)
	}
	fmt.Print("Inserted a test record with all non-null values\n")

	// Insert a test row with all null values using DML and Spanner Null* types.
	if _, err := db.ExecContext(ctx, `INSERT INTO AllTypes (
                      key, bool, string, bytes, int64, float32, float64, numeric, date, timestamp,
                      boolArray, stringArray, bytesArray, int64Array, float32Array, float64Array, numericArray, dateArray, timestampArray)
                      VALUES (@key, @bool, @string, @bytes, @int64, @float32, @float64, @numeric, @date, @timestamp,
                              @boolArray, @stringArray, @bytesArray, @int64Array, @float32Array, @float64Array, @numericArray, @dateArray, @timestampArray)`,
		2, spanner.NullBool{}, spanner.NullString{}, []byte(nil), // There is no NullBytes type
		spanner.NullInt64{}, spanner.NullFloat32{}, spanner.NullFloat64{}, spanner.NullNumeric{}, spanner.NullDate{}, spanner.NullTime{},
		// These array values all contain two NULL values in the (non-null) array.
		[]spanner.NullBool{{}, {}}, []spanner.NullString{{}, {}}, [][]byte{[]byte(nil), []byte(nil)},
		[]spanner.NullInt64{{}, {}}, []spanner.NullFloat32{{}, {}}, []spanner.NullFloat64{{}, {}}, []spanner.NullNumeric{{}, {}},
		[]spanner.NullDate{{}, {}}, []spanner.NullTime{{}, {}}); err != nil {
		return fmt.Errorf("failed to insert a record with all null values using DML: %v", err)
	}
	fmt.Print("Inserted a test record with all typed null values\n")

	// The Go sql driver supports inserting untyped nil values for NULL values. Cloud Spanner also supports untyped NULL
	// values. The Spanner emulator however does not (yet) support this, which is why this part of the code sample is
	// currently disabled on the emulator. Running it against a real Cloud Spanner database works.
	if os.Getenv("SPANNER_EMULATOR_HOST") == "" {
		if _, err := db.ExecContext(ctx, `INSERT INTO AllTypes (
                      key, bool, string, bytes, int64, float32, float64, numeric, date, timestamp,
                      boolArray, stringArray, bytesArray, int64Array, float32Array, float64Array, numericArray, dateArray, timestampArray)
                      VALUES (@key, @bool, @string, @bytes, @int64, @float32, @float64, @numeric, @date, @timestamp,
                              @boolArray, @stringArray, @bytesArray, @int64Array, @float32Array, @float64Array, @numericArray, @dateArray, @timestampArray)`,
			3, nil, nil, nil, nil, nil, nil, nil, nil, nil,
			nil, nil, nil, nil, nil, nil, nil, nil, nil); err != nil {
			return fmt.Errorf("failed to insert a record with all untyped null values using DML: %v", err)
		}
		fmt.Print("Inserted a test record with all untyped null values\n")
	}

	// You can use the same types for getting data from a column as for setting the data in a statement parameter,
	// except for ARRAY columns. Arrays must be stored in a []spanner.Null* variable, as all arrays may always
	// contain NULL elements in the array.
	var r1 nativeTypes
	if err := db.QueryRowContext(ctx, "SELECT * FROM AllTypes WHERE key=@key", 1).Scan(
		&r1.key, &r1.bool, &r1.string, &r1.bytes, &r1.int64, &r1.float32, &r1.float64, &r1.numeric, &r1.date, &r1.timestamp,
		&r1.boolArray, &r1.stringArray, &r1.bytesArray, &r1.int64Array, &r1.float32Array, &r1.float64Array, &r1.numericArray, &r1.dateArray, &r1.timestampArray,
	); err != nil {
		return fmt.Errorf("failed to get row with non-null values: %v", err)
	}
	fmt.Print("Queried a test record with all non-null values\n")

	// You can also use the spanner.Null* types to get data. These types can store both non-null and null values.
	var r2 nullTypes
	if err := db.QueryRowContext(ctx, "SELECT * FROM AllTypes WHERE key=@key", 1).Scan(
		&r2.key, &r2.bool, &r2.string, &r2.bytes, &r2.int64, &r2.float32, &r2.float64, &r2.numeric, &r2.date, &r2.timestamp,
		&r2.boolArray, &r2.stringArray, &r2.bytesArray, &r2.int64Array, &r2.float32Array, &r2.float64Array, &r2.numericArray, &r2.dateArray, &r2.timestampArray,
	); err != nil {
		return fmt.Errorf("failed to get row with null values: %v", err)
	}
	fmt.Print("Queried a test record with all null values and stored these in spanner.Null* variables\n")

	// You can also use the sql.Null* types where these are available. Note that the Go sql package
	// does not contain any types for nullable numeric and date values, so for these you must always
	// use spanner.NullNumeric and spanner.NullDate.
	var r3 sqlNullTypes
	if err := db.QueryRowContext(ctx, "SELECT * FROM AllTypes WHERE key=@key", 1).Scan(
		&r3.key, &r3.bool, &r3.string, &r3.bytes, &r3.int64, &r3.float32, &r3.float64, &r3.numeric, &r3.date, &r3.timestamp,
		&r3.boolArray, &r3.stringArray, &r3.bytesArray, &r3.int64Array, &r3.float32Array, &r3.float64Array, &r3.numericArray, &r3.dateArray, &r3.timestampArray,
	); err != nil {
		return fmt.Errorf("failed to get row with null values using Go sql null types: %v", err)
	}
	fmt.Print("Queried a test record with all null values and stored these in sql.Null* variables\n")

	return nil
}

type nativeTypes struct {
	key       int64
	bool      bool
	string    string
	bytes     []byte
	int64     int64
	float32   float32
	float64   float64
	numeric   big.Rat
	date      civil.Date
	timestamp time.Time
	// Array types must always use the Null* types, because an array may always
	// contain NULL elements in the array itself (even if the ARRAY column is
	// defined as NOT NULL).
	boolArray      []spanner.NullBool
	stringArray    []spanner.NullString
	bytesArray     [][]byte
	int64Array     []spanner.NullInt64
	float32Array   []spanner.NullFloat32
	float64Array   []spanner.NullFloat64
	numericArray   []spanner.NullNumeric
	dateArray      []spanner.NullDate
	timestampArray []spanner.NullTime
}

type nullTypes struct {
	key            int64
	bool           spanner.NullBool
	string         spanner.NullString
	bytes          []byte // There is no spanner.NullBytes type
	int64          spanner.NullInt64
	float32        spanner.NullFloat32
	float64        spanner.NullFloat64
	numeric        spanner.NullNumeric
	date           spanner.NullDate
	timestamp      spanner.NullTime
	boolArray      []spanner.NullBool
	stringArray    []spanner.NullString
	bytesArray     [][]byte
	int64Array     []spanner.NullInt64
	float32Array   []spanner.NullFloat32
	float64Array   []spanner.NullFloat64
	numericArray   []spanner.NullNumeric
	dateArray      []spanner.NullDate
	timestampArray []spanner.NullTime
}

type sqlNullTypes struct {
	key       int64
	bool      sql.NullBool
	string    sql.NullString
	bytes     []byte // There is no sql.NullBytes type
	int64     sql.NullInt64
	float32   sql.Null[float32]
	float64   sql.NullFloat64
	numeric   spanner.NullNumeric // There is no sql.NullNumeric type
	date      spanner.NullDate    // There is no sql.NullDate type
	timestamp sql.NullTime
	// Array types must always use the spanner.Null* structs.
	boolArray      []spanner.NullBool
	stringArray    []spanner.NullString
	bytesArray     [][]byte
	int64Array     []spanner.NullInt64
	float32Array   []spanner.NullFloat32
	float64Array   []spanner.NullFloat64
	numericArray   []spanner.NullNumeric
	dateArray      []spanner.NullDate
	timestampArray []spanner.NullTime
}

func main() {
	examples.RunSampleOnEmulator(dataTypes, createTableStatement)
}
