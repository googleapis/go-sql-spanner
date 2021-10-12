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

	_ "github.com/cloudspannerecosystem/go-sql-spanner"
	"github.com/cloudspannerecosystem/go-sql-spanner/examples"
)

// Simple sample application that shows how to use the Spanner Go sql driver.
//
// Execute the sample with the command `go run main` from this directory.
func helloWorld(projectId, instanceId, databaseId string) error {
	ctx := context.Background()
	db, err := sql.Open("spanner", fmt.Sprintf("projects/%s/instances/%s/databases/%s", projectId, instanceId, databaseId))
	if err != nil {
		return fmt.Errorf("failed to open database connection: %v\n", err)
	}
	defer db.Close()

	rows, err := db.QueryContext(ctx, "SELECT 'Hello World!'")
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
	examples.RunSampleOnEmulator(helloWorld)
}
