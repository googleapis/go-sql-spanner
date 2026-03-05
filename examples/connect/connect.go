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

// [START spanner_database_sql_connect]
import (
	"context"
	"database/sql"
	"fmt"

	_ "github.com/googleapis/go-sql-spanner"
)

func connect(projectId, instanceId, databaseId string) error {
	ctx := context.Background()
	dsn := fmt.Sprintf("projects/%s/instances/%s/databases/%s",
		projectId, instanceId, databaseId)
	db, err := sql.Open("spanner", dsn)
	if err != nil {
		return fmt.Errorf("failed to open database connection: %v", err)
	}
	defer func() { _ = db.Close() }()

	fmt.Printf("Connected to %s\n", dsn)
	row := db.QueryRowContext(ctx, "select cast(@greeting as string)", "Hello from Spanner")
	var greeting string
	if err := row.Scan(&greeting); err != nil {
		return fmt.Errorf("failed to get greeting: %v", err)
	}
	fmt.Printf("Greeting: %s\n", greeting)

	return nil
}

// [END spanner_database_sql_connect]
