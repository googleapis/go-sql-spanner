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

	"cloud.google.com/go/spanner"
	sppb "cloud.google.com/go/spanner/apiv1/spannerpb"
	spannerdriver "github.com/googleapis/go-sql-spanner"
	"github.com/googleapis/go-sql-spanner/examples"
)

var createTableStatement = "CREATE TABLE Singers (SingerId INT64, Name STRING(MAX)) PRIMARY KEY (SingerId)"

// Example for using directed reads with the Spanner database/sql driver.
func directedRead(projectId, instanceId, databaseId string) error {
	ctx := context.Background()
	db, err := sql.Open("spanner", fmt.Sprintf("projects/%s/instances/%s/databases/%s", projectId, instanceId, databaseId))
	if err != nil {
		return fmt.Errorf("failed to open database connection: %v", err)
	}
	defer db.Close()

	// Pass an ExecOptions value as an argument to QueryContext to specify
	// specific query options for a query.
	directedReadOptions := &sppb.DirectedReadOptions{
		Replicas: &sppb.DirectedReadOptions_IncludeReplicas_{
			IncludeReplicas: &sppb.DirectedReadOptions_IncludeReplicas{
				ReplicaSelections: []*sppb.DirectedReadOptions_ReplicaSelection{
					{
						Type: sppb.DirectedReadOptions_ReplicaSelection_READ_ONLY,
					},
				},
				AutoFailoverDisabled: true,
			},
		},
	}
	fmt.Println("Executing a query with a DirectedRead option")
	rows, err := db.QueryContext(ctx,
		`SELECT SingerId, Name FROM Singers`,
		spannerdriver.ExecOptions{QueryOptions: spanner.QueryOptions{DirectedReadOptions: directedReadOptions}})
	if err != nil {
		return fmt.Errorf("failed to execute query: %v", err)
	}
	defer rows.Close()

	for rows.Next() {
		var singerId int64
		var name string

		if err := rows.Scan(&singerId, &name); err != nil {
			return fmt.Errorf("failed to scan row values: %v", err)
		}
		fmt.Printf("Singer: %v: %v\n", singerId, name)
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("failed to execute query: %v", err)
	}
	return nil
}

func main() {
	examples.RunSampleOnEmulator(directedRead, createTableStatement)
}
