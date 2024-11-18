// Copyright 2021 Google LLC
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

	spannerdriver "github.com/googleapis/go-sql-spanner"
	"github.com/googleapis/go-sql-spanner/examples"
)

var createTableStatement = "CREATE TABLE Singers (SingerId INT64, Name STRING(MAX)) PRIMARY KEY (SingerId)"

// Sample showing how to execute a batch of DML statements.
// Batching DML statements together instead of executing them one by one reduces the number of round trips to Spanner
// that are needed.
//
// DML batches can be executed in two ways using the Spanner go sql driver:
//  1. By executing the SQL statements `START BATCH DML` and `RUN BATCH`.
//  2. By unwrapping the Spanner specific driver interface spannerdriver.Driver and calling the
//     spannerdriver.Driver#StartBatchDML and spannerdriver.Driver#RunBatch methods.
//
// This sample shows how to use both possibilities.
//
// Execute the sample with the command `go run main.go` from this directory.
func dmlBatch(projectId, instanceId, databaseId string) error {
	ctx := context.Background()
	db, err := sql.Open("spanner", fmt.Sprintf("projects/%s/instances/%s/databases/%s", projectId, instanceId, databaseId))
	if err != nil {
		return err
	}
	defer db.Close()

	// Start a read/write transaction.
	tx, err := db.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %v", err)
	}

	// A DML batch can be executed using custom SQL statements.
	// Start a DML batch on the transaction.
	if _, err := tx.ExecContext(ctx, "START BATCH DML"); err != nil {
		return fmt.Errorf("failed to execute START BATCH DML: %v", err)
	}
	// Insert a number of DML statements on the transaction. These statements will be buffered locally in the
	// transaction and will only be sent to Spanner once RUN BATCH is executed.
	if _, err := tx.ExecContext(ctx, "INSERT INTO Singers (SingerId, Name) VALUES (@id, @name)", 1, "Singer 1"); err != nil {
		return fmt.Errorf("failed to insert: %v", err)
	}
	if _, err := tx.ExecContext(ctx, "INSERT INTO Singers (SingerId, Name) VALUES (@id, @name)", 2, "Singer 2"); err != nil {
		return fmt.Errorf("failed to insert: %v", err)
	}
	if _, err := tx.ExecContext(ctx, "INSERT INTO Singers (SingerId, Name) VALUES (@id, @name)", 3, "Singer 3"); err != nil {
		return fmt.Errorf("failed to insert: %v", err)
	}
	// Run the active DML batch.
	if _, err := tx.ExecContext(ctx, "RUN BATCH"); err != nil {
		return fmt.Errorf("failed to execute RUN BATCH: %v", err)
	}
	// Commit the transaction.
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit transaction: %v", err)
	}

	// Verify that all three records were inserted.
	var c int64
	if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM Singers").Scan(&c); err != nil {
		return fmt.Errorf("failed to get singers count: %v", err)
	}
	fmt.Printf("# of Singer records after first batch: %v\n", c)

	// A DML batch can also be executed using the StartBatchDML and RunBatch methods on the SpannerConn interface.
	// A DML batch also does not need to be executed on a transaction.
	conn, err := db.Conn(ctx)
	if err != nil {
		return fmt.Errorf("failed to get connection: %v", err)
	}
	defer conn.Close()
	if err := conn.Raw(func(driverConn interface{}) error {
		return driverConn.(spannerdriver.SpannerConn).StartBatchDML()
	}); err != nil {
		return fmt.Errorf("failed to start DML batch: %v", err)
	}
	// Note that we execute the DML statements on the connection that started the DML batch.
	if _, err := conn.ExecContext(ctx, "INSERT INTO Singers (SingerId, Name) VALUES (@id, @name)", 4, "Singer 4"); err != nil {
		return fmt.Errorf("failed to insert: %v", err)
	}
	if _, err := conn.ExecContext(ctx, "INSERT INTO Singers (SingerId, Name) VALUES (@id, @name)", 5, "Singer 5"); err != nil {
		return fmt.Errorf("failed to insert: %v", err)
	}
	if _, err := conn.ExecContext(ctx, "INSERT INTO Singers (SingerId, Name) VALUES (@id, @name)", 6, "Singer 6"); err != nil {
		return fmt.Errorf("failed to insert: %v", err)
	}
	// Run the batch. This will apply all the batched DML statements to the database in one atomic operation.
	if err := conn.Raw(func(driverConn interface{}) error {
		return driverConn.(spannerdriver.SpannerConn).RunBatch(ctx)
	}); err != nil {
		return fmt.Errorf("failed to run DML batch: %v", err)
	}
	if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM Singers").Scan(&c); err != nil {
		return fmt.Errorf("failed to get singers count: %v", err)
	}
	fmt.Printf("# of Singer records after second batch: %v\n", c)

	return nil
}

func main() {
	examples.RunSampleOnEmulator(dmlBatch, createTableStatement)
}
