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

// Example for executing a read/write transaction on a Google Cloud Spanner database.
//
// Execute the sample with the command `go run main.go` from this directory.
func transaction(projectId, instanceId, databaseId string) error {
	ctx := context.Background()
	db, err := sql.Open("spanner", fmt.Sprintf("projects/%s/instances/%s/databases/%s", projectId, instanceId, databaseId))
	if err != nil {
		return err
	}
	defer db.Close()

	tx, err := db.BeginTx(ctx, &sql.TxOptions{}) // The default options will start a read/write transaction.
	if err != nil {
		return err
	}

	// Insert a new record using the transaction that we just started.
	_, err = tx.ExecContext(ctx, "INSERT INTO Singers (SingerId, Name) VALUES (@id, @name)", 123, "Bruce Allison")
	if err != nil {
		_ = tx.Rollback()
		return err
	}

	// The row that we inserted will be readable for the same transaction that started it.
	rows, err := tx.QueryContext(ctx, "SELECT SingerId, Name FROM Singers WHERE SingerId = @id", 123)
	if err != nil {
		_ = tx.Rollback()
		return err
	}
	var (
		id   int64
		name string
	)
	for rows.Next() {
		if err := rows.Scan(&id, &name); err != nil {
			_ = tx.Rollback()
			return err
		}
		fmt.Printf("Found singer: %v %v\n", id, name)
	}
	if err := rows.Err(); err != nil {
		_ = tx.Rollback()
		return err
	}
	_ = rows.Close()

	// The row that has been inserted using the transaction is not readable for other transactions before the
	// transaction has been committed. Note that the following statement does not use `tx` to try to read the
	// row, but executes a read directly on `db`. This operation will use a separate single use read-only transaction.
	row := db.QueryRowContext(ctx, "SELECT SingerId, Name FROM Singers WHERE SingerId = @id", 123)
	if err := row.Err(); err != nil {
		_ = tx.Rollback()
		return err
	}
	// This should return sql.ErrNoRows as the row should not be visible to other transactions.
	if err := row.Scan(&id, &name); err != sql.ErrNoRows {
		_ = tx.Rollback()
		return fmt.Errorf("expected sql.ErrNoRows, but got %v", err)
	}
	fmt.Printf("Could not read singer outside of transaction before commit\n")

	// Commit the transaction to make the changes permanent and readable for other transactions.
	if err := tx.Commit(); err != nil {
		return err
	}

	// This should now find the row.
	row = db.QueryRowContext(ctx, "SELECT SingerId, Name FROM Singers WHERE SingerId = @id", 123)
	if err := row.Err(); err != nil {
		return err
	}
	if err := row.Scan(&id, &name); err != nil {
		return err
	}
	fmt.Printf("Found singer after commit: %v %v\n", id, name)

	return nil
}

func main() {
	examples.RunSampleOnEmulator(transaction, createTableStatement)
}
