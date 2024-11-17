// Copyright 2024 Google LLC
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

// Example for executing a query with struct arguments described in:
//
// * https://cloud.google.com/spanner/docs/structs
// * https://pkg.go.dev/cloud.google.com/go/spanner#hdr-Structs
func structTypes(projectId, instanceId, databaseId string) error {
	ctx := context.Background()
	db, err := sql.Open("spanner", fmt.Sprintf("projects/%s/instances/%s/databases/%s", projectId, instanceId, databaseId))
	if err != nil {
		return fmt.Errorf("failed to open database connection: %v", err)
	}
	defer db.Close()

	type Entry struct {
		ID   int64
		Name string
	}

	entries := []Entry{
		{ID: 0, Name: "Hello"},
		{ID: 1, Name: "World"},
	}

	rows, err := db.QueryContext(ctx, "SELECT id, name FROM UNNEST(@entries)", entries)
	if err != nil {
		return fmt.Errorf("failed to execute query: %v", err)
	}
	defer rows.Close()

	for rows.Next() {
		var id int64
		var name string

		if err := rows.Scan(&id, &name); err != nil {
			return fmt.Errorf("failed to scan row values: %v", err)
		}
		fmt.Printf("%v %v\n", id, name)
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("failed to execute query: %v", err)
	}
	return nil
}

func main() {
	examples.RunSampleOnEmulator(structTypes)
}
