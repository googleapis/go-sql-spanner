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
	"time"

	_ "github.com/googleapis/go-sql-spanner"
	"github.com/googleapis/go-sql-spanner/examples"
)

var createTableStatement = "CREATE TABLE Singers (SingerId INT64, Name STRING(MAX)) PRIMARY KEY (SingerId)"

// Example for executing read-only transactions with a different timestamp bound than Strong. Both single-use and
// multi-use read-only transactions can be set up to use a different timestamp bound by executing a
// `SET READ_ONLY_STALENESS=<staleness>` statement before executing the read-only transaction. `<staleness>` must
// be one of the following:
//
// 'READ_TIMESTAMP yyyy-mm-ddTHH:mi:ssZ'
// 'MIN_READ_TIMESTAMP yyyy-mm-ddTHH:mi:ssZ' (only for single queries outside a multi-use read-only transaction)
// 'EXACT_STALENESS <int64>s|ms|us|ns' (example: 'EXACT_STALENESS 10s' for 10 seconds)
// 'MAX_STALENESS <int64>s|ms|us|ns' (only for single queries outside a multi-use read-only transaction)
//
// Execute the sample with the command `go run main.go` from this directory.
func staleReads(projectId, instanceId, databaseId string) error {
	ctx := context.Background()
	db, err := sql.Open("spanner", fmt.Sprintf("projects/%s/instances/%s/databases/%s", projectId, instanceId, databaseId))
	if err != nil {
		return err
	}
	defer db.Close()

	// First get the current time on the server. This timestamp can be used to execute
	// a stale read at an exact timestamp that lays before any data was inserted to the
	// test database.
	var t time.Time
	if err := db.QueryRowContext(ctx, "SELECT CURRENT_TIMESTAMP").Scan(&t); err != nil {
		return err
	}
	// Insert a sample row.
	if _, err := db.ExecContext(ctx, "INSERT INTO Singers (SingerId, Name) VALUES (@id, @name)", int64(1), "Bruce Allison"); err != nil {
		return err
	}

	// Execute a single read using the timestamp that was gotten before we inserted any data.
	// We need to get a connection from the pool in order to be able to set the read-only
	// staleness on that connection. The connection will use the specified read-only staleness
	// until it is set to a different value, or until it is returned to the pool.
	conn, err := db.Conn(ctx)
	if err != nil {
		return err
	}
	if _, err := conn.ExecContext(ctx, fmt.Sprintf("SET READ_ONLY_STALENESS='READ_TIMESTAMP %s'", t.Format(time.RFC3339Nano))); err != nil {
		return err
	}
	var c int64
	if err := conn.QueryRowContext(ctx, "SELECT COUNT(*) FROM Singers").Scan(&c); err != nil {
		return err
	}
	// The count at the specified timestamp should be zero, as it was before the test data was inserted.
	fmt.Printf("Singers count at timestamp %s was: %v\n", t.Format(time.RFC3339Nano), c)

	// Get a new timestamp that is after the data was inserted and use that as the read-timestamp.
	if err := db.QueryRowContext(ctx, "SELECT CURRENT_TIMESTAMP").Scan(&t); err != nil {
		return err
	}
	if _, err := conn.ExecContext(ctx, fmt.Sprintf("SET READ_ONLY_STALENESS='READ_TIMESTAMP %s'", t.Format(time.RFC3339Nano))); err != nil {
		return err
	}
	// Now start a read-only transaction that will read at the given timestamp.
	tx, err := db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return err
	}
	// This should now find the singer we inserted, as the read timestamp is after the moment we
	// inserted the test data.
	if err := tx.QueryRowContext(ctx, "SELECT COUNT(*) FROM Singers").Scan(&c); err != nil {
		return err
	}
	fmt.Printf("Singers count at timestamp %s was: %v\n", t.Format(time.RFC3339Nano), c)

	// Commit the read-only transaction and close the connection to release the resources they are using.
	// Committing or rolling back a read-only transaction will execute an actual Commit or Rollback on the database,
	// but it is needed in order to release the resources that are held by the read-only transaction.
	if err := tx.Commit(); err != nil {
		return err
	}
	_ = conn.Close()

	return nil
}

func main() {
	examples.RunSampleOnEmulator(staleReads, createTableStatement)
}
