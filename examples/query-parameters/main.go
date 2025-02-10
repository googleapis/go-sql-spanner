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

var createTableStatement = "CREATE TABLE Singers (SingerId INT64, Name STRING(MAX)) PRIMARY KEY (SingerId)"

// Sample showing how to use both positional and named query parameters.
// Using query parameters instead of literal values in SQL statements
// improves the execution time of those statements, as Spanner can cache
// and re-use the execution plan for those statements.
//
// Execute the sample with the command `go run main.go` from this directory.
func queryParameters(projectId, instanceId, databaseId string) error {
	ctx := context.Background()
	db, err := sql.Open("spanner", fmt.Sprintf("projects/%s/instances/%s/databases/%s", projectId, instanceId, databaseId))
	if err != nil {
		return err
	}
	defer db.Close()

	// The Spanner database/sql driver supports both named parameters and positional
	// parameters. This DML statement uses named parameters.
	_, err = db.ExecContext(ctx,
		"INSERT INTO Singers (SingerId, Name) VALUES (@id, @name)",
		sql.Named("id", int64(1)),
		sql.Named("name", "Bruce Allison"))
	if err != nil {
		return err
	}

	// You can also use '@name' style parameters in the SQL statement
	// in combination with positional arguments in Go.
	_, err = db.ExecContext(ctx,
		"INSERT INTO Singers (SingerId, Name) VALUES (@id, @name)",
		int64(2),
		"Alice Henderson")
	if err != nil {
		return err
	}

	// SQL statements can also contain positional parameters using '?'.
	row := db.QueryRowContext(ctx,
		"SELECT Name FROM Singers WHERE SingerId = ?",
		int64(1))
	var name string
	if err := row.Scan(&name); err != nil {
		return err
	}
	fmt.Printf("Found singer %s\n", name)

	return nil
}

func main() {
	examples.RunSampleOnEmulator(queryParameters, createTableStatement)
}
