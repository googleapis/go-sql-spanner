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

	"cloud.google.com/go/spanner"
	"cloud.google.com/go/spanner/apiv1/spannerpb"
	spannerdriver "github.com/googleapis/go-sql-spanner"
	"github.com/googleapis/go-sql-spanner/examples"
	"google.golang.org/api/option"
)

// Sample that shows how to use supply a custom configuration for the Spanner client
// that is used by the Go sql driver.
//
// Execute the sample with the command `go run main.go` from this directory.
func customClientConfiguration(projectId, instanceId, databaseId string) error {
	ctx := context.Background()

	connectorConfig := spannerdriver.ConnectorConfig{
		Project:  projectId,
		Instance: instanceId,
		Database: databaseId,

		// Create a function that sets the Spanner client configuration for the database connection.
		Configurator: func(config *spanner.ClientConfig, opts *[]option.ClientOption) {
			// Set a default query optimizer version that the client should use.
			config.QueryOptions = spanner.QueryOptions{Options: &spannerpb.ExecuteSqlRequest_QueryOptions{OptimizerVersion: "1"}}
		},
	}

	// Create a Connector for Spanner to create a DB with a custom configuration.
	c, err := spannerdriver.CreateConnector(connectorConfig)
	if err != nil {
		return fmt.Errorf("failed to create connector: %v", err)
	}

	// Create a DB using the Connector.
	db := sql.OpenDB(c)
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
	examples.RunSampleOnEmulator(customClientConfiguration)
}
