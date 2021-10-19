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

	_ "github.com/cloudspannerecosystem/go-sql-spanner"
	"github.com/cloudspannerecosystem/go-sql-spanner/examples"
)

var createTableStatement = "CREATE TABLE Singers (SingerId INT64, Name STRING(MAX)) PRIMARY KEY (SingerId)"

// Sample showing how to execute a read-only transaction on a Spanner database.
//
// Execute the sample with the command `go run main.go` from this directory.
func readOnlyTransaction(projectId, instanceId, databaseId string) error {
	ctx := context.Background()
	db, err := sql.Open("spanner", fmt.Sprintf("projects/%s/instances/%s/databases/%s", projectId, instanceId, databaseId))
	if err != nil {
		return err
	}
	defer db.Close()

	// Start a read-only transaction on the Spanner database.
	tx, err := db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return err
	}
	// Committing or rolling back a read-only transaction will not execute an actual Commit or Rollback
	// on the database, but it is needed in order to release the resources that are held by the read-only
	// transaction.
	defer tx.Commit()

	// Verify that the Singers table is empty.
	var c int64
	if err := tx.QueryRowContext(ctx, "SELECT COUNT(*) FROM Singers").Scan(&c); err != nil {
		return err
	}
	fmt.Printf("Singers count is initially %v\n", c)

	// Now insert a new record in the Singers table using an implicit transaction.
	// This change will be applied immediately to the database.
	if _, err := db.ExecContext(ctx, "INSERT INTO Singers (SingerId, Name) VALUES (@id, @name)", int64(1), "Bruce Allison"); err != nil {
		return err
	}

	// The read-only transaction was started before the row was inserted and will continue to read data at a timestamp
	// that was before the row was inserted. It will therefore not see the new record.
	if err := tx.QueryRowContext(ctx, "SELECT COUNT(*) FROM Singers").Scan(&c); err != nil {
		return err
	}
	fmt.Printf("Singers count as seen in the read-only transaction is %v\n", c)

	// Start a new read-only transaction on the Spanner database. This transaction will be started after the new test
	// row was inserted, and the test row should now be visible to the read-only transaction.
	tx, err = db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return err
	}
	defer tx.Commit()
	if err := tx.QueryRowContext(ctx, "SELECT COUNT(*) FROM Singers").Scan(&c); err != nil {
		return err
	}
	fmt.Printf("Singers count in a new read-only transaction is %v\n", c)

	return nil
}

func main() {
	examples.RunSampleOnEmulator(readOnlyTransaction, createTableStatement)
}
