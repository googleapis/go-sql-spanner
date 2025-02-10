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
	"time"

	"cloud.google.com/go/spanner"
	spannerdriver "github.com/googleapis/go-sql-spanner"
	"github.com/googleapis/go-sql-spanner/examples"
)

// Sample showing how to execute a read-only transaction with specific options on a Spanner database.
//
// Execute the sample with the command `go run main.go` from this directory.
func readOnlyTransactionWithOptions(projectId, instanceId, databaseId string) error {
	ctx := context.Background()
	db, err := sql.Open("spanner", fmt.Sprintf("projects/%s/instances/%s/databases/%s", projectId, instanceId, databaseId))
	if err != nil {
		return err
	}
	defer db.Close()

	// Start a read-only transaction on the Spanner database using a staleness option.
	tx, err := spannerdriver.BeginReadOnlyTransaction(
		ctx, db, spannerdriver.ReadOnlyTransactionOptions{
			TimestampBound: spanner.ExactStaleness(time.Second * 10),
		})
	if err != nil {
		return err
	}
	fmt.Println("Started a read-only transaction with a staleness option")

	// Use the read-only transaction...

	// Committing or rolling back a read-only transaction will not execute an actual Commit or Rollback
	// on the database, but it is needed in order to release the resources that are held by the read-only
	// transaction. Failing to do so will cause the connection that is used for the transaction to be leaked.
	if err := tx.Commit(); err != nil {
		return err
	}

	return nil
}

func main() {
	examples.RunSampleOnEmulator(readOnlyTransactionWithOptions)
}
