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

	"cloud.google.com/go/spanner"
	_ "github.com/googleapis/go-sql-spanner"
	spannerdriver "github.com/googleapis/go-sql-spanner"
	"github.com/googleapis/go-sql-spanner/examples"
)

// Example of using the underlying *spanner.Client.
func underlyingClient(projectId, instanceId, databaseId string) error {
	ctx := context.Background()
	db, err := sql.Open("spanner", fmt.Sprintf("projects/%s/instances/%s/databases/%s", projectId, instanceId, databaseId))
	if err != nil {
		return fmt.Errorf("failed to open database connection: %v\n", err)
	}
	defer db.Close()

	conn, err := db.Conn(ctx)
	if err != nil {
		return err
	}
	defer conn.Close()

	if err := conn.Raw(func(driverConn any) error {
		spannerConn, ok := driverConn.(spannerdriver.SpannerConn)
		if !ok {
			return fmt.Errorf("unexpected driver connection %v, expected SpannerConn", driverConn)
		}
		client, err := spannerConn.UnderlyingClient()
		if err != nil {
			return fmt.Errorf("unable to access underlying client: %w", err)
		}

		row := client.Single().Query(ctx, spanner.Statement{SQL: "SELECT 1"})
		return row.Do(func(r *spanner.Row) error {
			var value int64
			err := r.Columns(&value)
			if err != nil {
				return fmt.Errorf("failed to read column: %w", err)
			}
			fmt.Println(value)
			return nil
		})
	}); err != nil {
		return err
	}
	return nil
}

func main() {
	examples.RunSampleOnEmulator(underlyingClient)
}
