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
	"sync"

	_ "github.com/googleapis/go-sql-spanner"
	spannerdriver "github.com/googleapis/go-sql-spanner"
	"github.com/googleapis/go-sql-spanner/examples"
)

var createTableStatement = "CREATE TABLE Singers (SingerId INT64, Name STRING(MAX)) PRIMARY KEY (SingerId)"

// Example for running a read/write transaction in a retry loop on a Spanner database.
// The RunTransaction function automatically retries Aborted transactions using a
// retry loop. This guarantees that the transaction will not fail with an
// ErrAbortedDueToConcurrentModification.
//
// Execute the sample with the command `go run main.go` from this directory.
func runTransaction(projectId, instanceId, databaseId string) error {
	ctx := context.Background()
	db, err := sql.Open("spanner", fmt.Sprintf("projects/%s/instances/%s/databases/%s", projectId, instanceId, databaseId))
	if err != nil {
		return err
	}
	defer db.Close()

	// Insert a new record that will be updated by multiple different transactions at the same time.
	_, err = db.ExecContext(ctx, "INSERT INTO Singers (SingerId, Name) VALUES (@id, @name)", 123, "Bruce Allison")
	if err != nil {
		return err
	}

	numTransactions := 10
	errors := make([]error, numTransactions)
	wg := sync.WaitGroup{}
	for i := 0; i < numTransactions; i++ {
		index := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			// Run a transaction that adds an index to the name of the singer.
			// As we are doing this multiple times in parallel, these transactions
			// will be aborted and retried by Spanner multiple times. The end result
			// will still be that all transactions succeed and the name contains all
			// indexes in an undefined order.
			errors[index] = spannerdriver.RunTransaction(ctx, db, &sql.TxOptions{}, func(ctx context.Context, tx *sql.Tx) error {
				// Query the singer in the transaction. This will take a lock on the row and guarantee that
				// the value that we read is still the same when the transaction is committed. If not, Spanner
				// will abort the transaction, and the transaction will be retried.
				row := tx.QueryRowContext(ctx, "select Name from Singers where SingerId=@id", 123)
				var name string
				if err := row.Scan(&name); err != nil {
					return err
				}
				// Update the name with the transaction index.
				name = fmt.Sprintf("%s %d", name, index)
				res, err := tx.ExecContext(ctx, "update Singers set Name=@name where SingerId=@id", name, 123)
				if err != nil {
					return err
				}
				affected, err := res.RowsAffected()
				if err != nil {
					return err
				}
				if affected != 1 {
					return fmt.Errorf("unexpected affected row count: %d", affected)
				}
				return nil
			})
		}()
	}
	wg.Wait()

	// The name of the singer should now contain all the indexes that were added in the
	// transactions above in arbitrary order.
	row := db.QueryRowContext(ctx, "SELECT SingerId, Name FROM Singers WHERE SingerId = ?", 123)
	if err := row.Err(); err != nil {
		return err
	}
	var id int64
	var name string
	if err := row.Scan(&id, &name); err != nil {
		return err
	}
	fmt.Printf("Singer after %d transactions: %v %v\n", numTransactions, id, name)

	return nil
}

func main() {
	examples.RunSampleOnEmulator(runTransaction, createTableStatement)
}
