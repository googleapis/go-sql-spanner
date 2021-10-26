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

	_ "github.com/cloudspannerecosystem/go-sql-spanner"
	spannerdriver "github.com/cloudspannerecosystem/go-sql-spanner"
	"github.com/cloudspannerecosystem/go-sql-spanner/examples"
)

var createTableStatement = "CREATE TABLE Singers (SingerId INT64, Name STRING(MAX)) PRIMARY KEY (SingerId)"

// Example for getting the commit timestamp of a read/write transaction.
//
// Execute the sample with the command `go run main.go` from this directory.
func commitTimestamp(projectId, instanceId, databaseId string) error {
	ctx := context.Background()
	db, err := sql.Open("spanner", fmt.Sprintf("projects/%s/instances/%s/databases/%s", projectId, instanceId, databaseId))
	if err != nil {
		return err
	}
	defer db.Close()

	// Get a connection from the pool and start the transaction on that connection.
	conn, err := db.Conn(ctx)
	if err != nil {
		return fmt.Errorf("failed to get a connection from the pool: %v", err)
	}
	defer conn.Close()

	tx, err := conn.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, "INSERT INTO Singers (SingerId, Name) VALUES (@id, @name)", 123, "Bruce Allison"); err != nil {
		_ = tx.Rollback()
		return fmt.Errorf("failed to insert test record: %v", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit transaction: %v", err)
	}

	// Get the commit timestamp of the last transaction that was executed on the connection.
	var ct time.Time
	if err := conn.Raw(func(driverConn interface{}) (err error) {
		ct, err = driverConn.(spannerdriver.SpannerConn).CommitTimestamp()
		return err
	}); err != nil {
		return fmt.Errorf("failed to get commit timestamp: %v", err)
	}

	// The commit timestamp can also be obtained by executing the custom SQL statement `SHOW VARIABLE COMMIT_TIMESTAMP`
	var ct2 time.Time
	if err := conn.QueryRowContext(ctx, "SHOW VARIABLE COMMIT_TIMESTAMP").Scan(&ct2); err != nil {
		return fmt.Errorf("failed to execute SHOW VARIABLE COMMIT_TIMESTAMP")
	}

	// The commit timestamp that is obtained directly from the connection is in the local timezone.
	fmt.Printf("Transaction committed at                %v\n", ct)
	// The commit timestamp that is obtained through SHOW VARIABLE COMMIT_TIMESTAMP is in UTC.
	fmt.Printf("SHOW VARIABLE COMMIT_TIMESTAMP returned %v\n", ct2)

	return nil
}

func main() {
	examples.RunSampleOnEmulator(commitTimestamp, createTableStatement)
}
