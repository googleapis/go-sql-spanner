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

	"cloud.google.com/go/spanner"
	"github.com/cloudspannerecosystem/go-sql-spanner"
	"github.com/cloudspannerecosystem/go-sql-spanner/examples"
)

var createTableStatement = "CREATE TABLE Singers (SingerId INT64, Name STRING(MAX)) PRIMARY KEY (SingerId)"

// Example that shows how to use Mutations to insert data in a Cloud Spanner database using the Go sql driver.
// Mutations can be more efficient than DML statements for bulk insert/update operations.
//
// Execute the sample with the command `go run main` from this directory.
func mutations(projectId, instanceId, databaseId string) error {
	ctx := context.Background()
	db, err := sql.Open("spanner", fmt.Sprintf("projects/%s/instances/%s/databases/%s", projectId, instanceId, databaseId))
	if err != nil {
		return err
	}
	defer db.Close()

	// Get a connection so that we can get access to the Spanner specific connection interface SpannerConn.
	conn, err := db.Conn(ctx)
	if err != nil {
		return err
	}

	// Mutations can be written outside an explicit transaction using SpannerConn#Apply.
	var commitTimestamp time.Time
	if err := conn.Raw(func(driverConn interface{}) error {
		spannerConn, ok := driverConn.(spannerdriver.SpannerConn)
		if !ok {
			return fmt.Errorf("unexpected driver connection %v, expected SpannerConn", driverConn)
		}
		commitTimestamp, err = spannerConn.Apply(ctx, []*spanner.Mutation{
			spanner.Insert("Singers", []string{"SingerId", "Name"}, []interface{}{int64(1), "Richard Moore"}),
			spanner.Insert("Singers", []string{"SingerId", "Name"}, []interface{}{int64(2), "Alice Henderson"}),
		})
		return err
	}); err != nil {
		return err
	}
	fmt.Printf("The transaction with two singer mutations was committed at %v\n", commitTimestamp)

	// Mutations can also be executed as part of a read/write transaction.
	// Note: The transaction is started using the connection that we had obtained. This is necessary in order to
	// ensure that the conn.Raw call below will use the same connection as the one that just started the transaction.
	tx, err := conn.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		return err
	}

	// Get the name of a singer and update it using a mutation.
	id := int64(1)
	row := tx.QueryRowContext(ctx, "SELECT Name FROM Singers WHERE SingerId=@id", id)
	var name string
	if err := row.Scan(&name); err != nil {
		return err
	}
	if err := conn.Raw(func(driverConn interface{}) error {
		spannerConn, ok := driverConn.(spannerdriver.SpannerConn)
		if !ok {
			return fmt.Errorf("unexpected driver connection %v, expected SpannerConn", driverConn)
		}
		return spannerConn.BufferWrite([]*spanner.Mutation{
			spanner.Update("Singers", []string{"SingerId", "Name"}, []interface{}{id, name + "-Henderson"}),
		})
	}); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	fmt.Print("Updated the name of the first singer\n")

	// Read back the updated row.
	row = db.QueryRowContext(ctx, "SELECT SingerId, Name FROM Singers WHERE SingerId = @id", id)
	if err := row.Err(); err != nil {
		return err
	}
	if err := row.Scan(&id, &name); err != nil {
		return err
	}
	fmt.Printf("Updated singer: %v %v\n", id, name)

	return nil
}

func main() {
	examples.RunSampleOnEmulator(mutations, createTableStatement)
}
