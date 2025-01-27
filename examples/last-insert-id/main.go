// Copyright 2025 Google LLC
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

package main

import (
	"context"
	"database/sql"
	"fmt"

	_ "github.com/googleapis/go-sql-spanner"
	"github.com/googleapis/go-sql-spanner/examples"
)

var createSequenceStatement = `CREATE SEQUENCE SingerIdSequence OPTIONS (
  sequence_kind="bit_reversed_positive"
)`
var createTableStatement = `CREATE TABLE Singers (
    SingerId INT64 DEFAULT (GET_NEXT_SEQUENCE_VALUE(SEQUENCE SingerIdSequence)),
    Name STRING(MAX)
) PRIMARY KEY (SingerId)`

// Example using LastInsertId with Spanner.
//
// Execute the sample with the command `go run main.go` from this directory.
func lastInsertId(projectId, instanceId, databaseId string) error {
	ctx := context.Background()
	db, err := sql.Open("spanner", fmt.Sprintf("projects/%s/instances/%s/databases/%s", projectId, instanceId, databaseId))
	if err != nil {
		return err
	}
	defer db.Close()

	// Insert a new Singer row. Spanner will generate a new primary key value from
	// the bit-reversed sequence and return this value to the client.
	//
	// NOTE: ExecContext can only be used for INSERT statements with a THEN RETURN clause
	// that return exactly ONE row and ONE column of type INT64. The Spanner database/sql
	// driver disallows execution of any other type of DML statement with a THEN RETURN
	// clause using ExecContext. Use QueryContext for these statements instead.
	res, err := db.ExecContext(ctx, "INSERT INTO Singers (Name) VALUES (@name) THEN RETURN SingerId", "Bruce Allison")
	if err != nil {
		return fmt.Errorf("failed to insert test record: %v", err)
	}
	rowsInserted, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("failed to get rows affected: %v", err)
	}
	// We can get the generated ID by calling LastInsertId().
	singerId, err := res.LastInsertId()
	if err != nil {
		return fmt.Errorf("failed to get LastInsertId: %v", err)
	}

	fmt.Printf("Inserted %v singer with auto-generated id %v\n", rowsInserted, singerId)

	return nil
}

func main() {
	examples.RunSampleOnEmulator(lastInsertId, createSequenceStatement, createTableStatement)
}
