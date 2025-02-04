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
	spannerdriver "github.com/googleapis/go-sql-spanner"
	"github.com/googleapis/go-sql-spanner/examples"
)

var createTableStatement = "CREATE TABLE Singers (SingerId INT64, Name STRING(MAX)) PRIMARY KEY (SingerId)"

// Sample showing how to use a batch read-only transaction to auto-partition a query
// and execute all partitions as if it was one query. This sample also shows how to
// use DataBoost.
//
// See https://cloud.google.com/spanner/docs/databoost/databoost-overview for more
// information.
//
// Execute the sample with the command `go run main.go` from this directory.
func autoPartitionQuery(projectId, instanceId, databaseId string) error {
	ctx := context.Background()
	db, err := sql.Open("spanner", fmt.Sprintf("projects/%s/instances/%s/databases/%s", projectId, instanceId, databaseId))
	if err != nil {
		return err
	}
	defer db.Close()

	// Insert 10 test rows.
	if err := insertTestRows(db, 10); err != nil {
		return err
	}

	// Start a batch read-only transaction.
	tx, err := spannerdriver.BeginBatchReadOnlyTransaction(ctx, db, spannerdriver.BatchReadOnlyTransactionOptions{})
	if err != nil {
		return err
	}
	// Committing or rolling back a read-only transaction will not execute an actual Commit or Rollback
	// on the database, but it is needed in order to release the resources that are held by the read-only
	// transaction.
	defer func() { _ = tx.Commit() }()

	// Auto-partition and execute a query. The driver internal calls PartitionQuery
	// and then executes each partition in a goroutine. The results of the various
	// partitions are streamed into the row iterator that is returned for the query.
	// The iterator will return the rows in arbitrary order.
	rows, err := tx.QueryContext(ctx, "select * from singers", spannerdriver.ExecOptions{
		PartitionedQueryOptions: spannerdriver.PartitionedQueryOptions{
			// Set this option to true to automatically partition the query
			// and execute each partition. The results are returned as one
			// row iterator.
			AutoPartitionQuery: true,
			// MaxParallelism sets the maximum number of goroutines that
			// will be used by the driver to execute the returned partitions.
			// If not set, it will default to the number of available CPUs.
			// The number of actual goroutines will never exceed the number of
			// partitions that is returned by Spanner.
			MaxParallelism: 8,
		},
		QueryOptions: spanner.QueryOptions{
			// Set DataBoostEnabled to true to enable DataBoost.
			// See https://cloud.google.com/spanner/docs/databoost/databoost-overview
			// for more information.
			DataBoostEnabled: true,
		},
	})
	if err != nil {
		return err
	}
	defer rows.Close()

	count := 0
	var id int64
	var name string
	for rows.Next() {
		if err := rows.Scan(&id, &name); err != nil {
			return err
		}
		fmt.Printf("Id: %v, Name: %v\n", id, name)
		count++
	}
	if rows.Err() != nil {
		return rows.Err()
	}
	fmt.Printf("Executing all partitions returned a total of %v rows\n", count)

	return nil
}

func main() {
	examples.RunSampleOnEmulator(autoPartitionQuery, createTableStatement)
}

func insertTestRows(db *sql.DB, numRows int) error {
	ctx := context.Background()

	// Insert a few test rows.
	tx, err := db.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		return err
	}
	// Insert the test rows in one batch.
	if _, err := tx.ExecContext(ctx, "start batch dml"); err != nil {
		tx.Rollback()
		return err
	}
	stmt, err := tx.PrepareContext(ctx, "INSERT INTO Singers (SingerId, Name) VALUES (?, ?)")
	if err != nil {
		tx.Rollback()
		return err
	}
	for i := 0; i < numRows; i++ {
		if _, err := stmt.ExecContext(ctx, int64(i), fmt.Sprintf("Test %v", i)); err != nil {
			_, _ = tx.ExecContext(ctx, "abort batch")
			tx.Rollback()
			return err
		}
	}
	if _, err := tx.ExecContext(ctx, "run batch"); err != nil {
		tx.Rollback()
		return err
	}
	return tx.Commit()
}
