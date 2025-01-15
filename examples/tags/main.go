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

// Example for using transaction tags and statement tags through SQL statements.
//
// Tags can also be set programmatically using spannerdriver.RunTransactionWithOptions
// and the spannerdriver.ExecOptions.
//
// Execute the sample with the command `go run main.go` from this directory.
func tagsWithSqlStatements(projectId, instanceId, databaseId string) error {
	fmt.Println("Running sample for setting tags with SQL statements")

	ctx := context.Background()
	db, err := sql.Open("spanner", fmt.Sprintf("projects/%s/instances/%s/databases/%s", projectId, instanceId, databaseId))
	if err != nil {
		return err
	}
	defer db.Close()

	// Obtain a connection for the database in order to ensure that we
	// set the transaction tag on the same connection as the connection
	// that will execute the transaction.
	conn, err := db.Conn(ctx)
	if err != nil {
		return err
	}

	// Set a transaction tag on the connection and start a transaction.
	// This transaction tag will be applied to the next transaction that
	// is executed by this connection. The transaction tag is automatically
	// included with all statements of that transaction.
	if _, err := conn.ExecContext(ctx, "set transaction_tag = 'my_transaction_tag'"); err != nil {
		return err
	}
	fmt.Println("Executing transaction with transaction tag 'my_transaction_tag'")
	tx, err := conn.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		return err
	}

	// Set a statement tag and insert a new record using the transaction that we just started.
	if _, err := tx.ExecContext(ctx, "set statement_tag = 'insert_singer'"); err != nil {
		_ = tx.Rollback()
		return err
	}
	fmt.Println("Executing statement with tag 'insert_singer'")
	_, err = tx.ExecContext(ctx, "INSERT INTO Singers (SingerId, Name) VALUES (@id, @name)", 123, "Bruce Allison")
	if err != nil {
		_ = tx.Rollback()
		return err
	}

	// Set another statement tag and execute a query.
	if _, err := tx.ExecContext(ctx, "set statement_tag = 'select_singer'"); err != nil {
		_ = tx.Rollback()
		return err
	}
	fmt.Println("Executing statement with tag 'select_singer'")
	rows, err := tx.QueryContext(ctx, "SELECT SingerId, Name FROM Singers WHERE SingerId = ?", 123)
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
	if err := tx.Commit(); err != nil {
		return err
	}

	fmt.Println("Finished transaction with tag 'my_transaction_tag'")

	return nil
}

// tagsProgrammatically shows how to set transaction tags and statement tags
// programmatically.
//
// Note: It is not recommended to mix using SQL statements and passing in
// tags or other QueryOptions programmatically.
func tagsProgrammatically(projectId, instanceId, databaseId string) error {
	fmt.Println("Running sample for setting tags programmatically")

	ctx := context.Background()
	db, err := sql.Open("spanner", fmt.Sprintf("projects/%s/instances/%s/databases/%s", projectId, instanceId, databaseId))
	if err != nil {
		return err
	}
	defer db.Close()

	// Use RunTransactionWithOptions to set a transaction tag programmatically.
	fmt.Println("Executing transaction with transaction tag 'my_transaction_tag'")
	if err := spannerdriver.RunTransactionWithOptions(ctx, db, &sql.TxOptions{}, func(ctx context.Context, tx *sql.Tx) error {
		fmt.Println("Executing statement with tag 'insert_singer'")
		// Pass in a value of spanner.QueryOptions to specify the options that should be used for a DML statement.
		_, err = tx.ExecContext(ctx, "INSERT INTO Singers (SingerId, Name) VALUES (@id, @name)", spannerdriver.ExecOptions{QueryOptions: spanner.QueryOptions{RequestTag: "insert_singer"}}, 123, "Bruce Allison")
		if err != nil {
			return err
		}

		fmt.Println("Executing statement with tag 'select_singer'")
		// Pass in a value of spanner.QueryOptions to specify the options that should be used for a query.
		rows, err := tx.QueryContext(ctx, "SELECT SingerId, Name FROM Singers WHERE SingerId = ?", spannerdriver.ExecOptions{QueryOptions: spanner.QueryOptions{RequestTag: "select_singer"}}, 123)
		if err != nil {
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
			return err
		}
		_ = rows.Close()

		return nil
	}, spanner.TransactionOptions{TransactionTag: "my_transaction_tag"}); err != nil {
		return err
	}

	fmt.Println("Finished transaction with tag 'my_transaction_tag'")

	return nil
}

func main() {
	examples.RunSampleOnEmulator(tagsWithSqlStatements, createTableStatement)
	fmt.Println()
	examples.RunSampleOnEmulator(tagsProgrammatically, createTableStatement)
}
