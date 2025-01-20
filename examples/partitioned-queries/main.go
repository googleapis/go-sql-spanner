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
	"sync"
	"sync/atomic"

	spannerdriver "github.com/googleapis/go-sql-spanner"
	"github.com/googleapis/go-sql-spanner/examples"
)

var createTableStatement = "CREATE TABLE Singers (SingerId INT64, Name STRING(MAX)) PRIMARY KEY (SingerId)"

// Sample showing how to use a batch read-only transaction to partition a query and
// execute each of the partitions.
//
// Execute the sample with the command `go run main.go` from this directory.
func readOnlyTransaction(projectId, instanceId, databaseId string) error {
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

	// Partition a query and get the partitions.
	row := tx.QueryRowContext(ctx, "select * from singers", spannerdriver.ExecOptions{
		PartitionedQueryOptions: spannerdriver.PartitionedQueryOptions{
			// Set this option to true to only partition the query.
			// The query will return one row containing a spannerdriver.PartitionedQuery
			PartitionQuery: true,
		},
	})
	var pq spannerdriver.PartitionedQuery
	if err := row.Scan(&pq); err != nil {
		return err
	}

	fmt.Printf("PartitionQuery returned %v partitions\n", len(pq.Partitions))

	// Execute all partitions in parallel.
	// Each partition will be executed on a separate connection.
	// The BatchReadOnlyTransaction must be kept open while the
	// partitions are being executed.
	count := atomic.Int64{}
	var wg sync.WaitGroup
	for index := range pq.Partitions {
		partitionIndex := index
		wg.Add(1)
		go func() {
			defer wg.Done()
			rows, err := pq.Execute(ctx, partitionIndex, db)
			if err != nil {
				fmt.Printf("executing partition failed: %v", err)
				return
			}
			partitionCount := 0
			for rows.Next() {
				partitionCount++
				count.Add(1)
			}
			_ = rows.Close()
			fmt.Printf("Partition %v found %v rows\n", partitionIndex, partitionCount)
		}()
	}
	wg.Wait()
	fmt.Printf("Executing all partitions returned a total of %v rows\n", count.Load())

	return nil
}

func main() {
	examples.RunSampleOnEmulator(readOnlyTransaction, createTableStatement)
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
