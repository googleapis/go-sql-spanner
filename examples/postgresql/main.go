// Copyright 2025 Google LLC All Rights Reserved.
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

	"cloud.google.com/go/spanner/admin/database/apiv1/databasepb"
	_ "github.com/googleapis/go-sql-spanner"
	"github.com/googleapis/go-sql-spanner/examples"
)

// Sample application that uses a PostgreSQL-dialect Spanner database.
//
// Execute the sample with the command `go run main.go` from this directory.
func helloWorldFromPostgreSQL(projectId, instanceId, databaseId string) error {
	ctx := context.Background()
	db, err := sql.Open("spanner", fmt.Sprintf("projects/%s/instances/%s/databases/%s", projectId, instanceId, databaseId))
	if err != nil {
		return fmt.Errorf("failed to open database connection: %v", err)
	}
	defer db.Close()

	rows, err := db.QueryContext(ctx, "SELECT $1::varchar as message", "Hello World from Spanner PostgreSQL")
	if err != nil {
		return fmt.Errorf("failed to execute query: %v", err)
	}
	defer rows.Close()

	var msg string
	for rows.Next() {
		if err := rows.Scan(&msg); err != nil {
			return fmt.Errorf("failed to scan row values: %v", err)
		}
		fmt.Printf("%s\n", msg)
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("failed to execute query: %v", err)
	}
	return nil
}

func main() {
	examples.RunSampleOnEmulatorWithDialect(helloWorldFromPostgreSQL, databasepb.DatabaseDialect_POSTGRESQL)
}
