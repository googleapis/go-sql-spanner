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

// Sample showing how to customize the size of the internal statement cache
// of the Spanner database/sql driver. The driver needs to determine the
// number of query parameters in each statement that is executed. This involves
// partially parsing the SQL statement. The driver uses an internal cache for
// this, so it does not need to reparse SQL statements that are executed
// multiple times.
//
// Execute the sample with the command `go run main.go` from this directory.
func statementCache(projectId, instanceId, databaseId string) error {
	ctx := context.Background()

	// The default statement cache size is 1,000 statements.
	// The cache size can be changed by setting the StatementCacheSize=N
	// parameter in the connection string.

	// Add DisableStatementCache=true to the connection string if you want to
	// disable the use of the statement cache.

	// Create a database connection with statement cache size 50.
	dsn := fmt.Sprintf("projects/%s/instances/%s/databases/%s?StatementCacheSize=50", projectId, instanceId, databaseId)
	db, err := sql.Open("spanner", dsn)
	if err != nil {
		return err
	}
	defer db.Close()
	fmt.Println("Created a database connection with a statement cache size of 50")

	// The driver needs to determine the number and names of any query parameters
	// in a SQL string that is executed in order to assign the query parameters correctly.
	// To do this, the driver inspects and partially parses the SQL string. The result
	// of this parsing is stored in the internal statement cache and is reused for
	// subsequent executions of the same SQL string.
	_, err = db.ExecContext(ctx,
		"INSERT INTO Singers (SingerId, Name) VALUES (@id, @name)",
		sql.Named("id", int64(1)),
		sql.Named("name", "Bruce Allison"))
	if err != nil {
		return err
	}

	// This execution uses the exact same SQL string as the previous statement,
	// and it will not be parsed again by the driver. Instead, the parse result
	// of the previous execution will be used.
	_, err = db.ExecContext(ctx,
		"INSERT INTO Singers (SingerId, Name) VALUES (@id, @name)",
		sql.Named("id", int64(2)),
		sql.Named("name", "Alice Marquez"))
	if err != nil {
		return err
	}

	fmt.Println("Executed two SQL statements")

	return nil
}

func main() {
	examples.RunSampleOnEmulator(statementCache, createTableStatement)
}
