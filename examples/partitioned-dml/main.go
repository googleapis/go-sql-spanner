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

var createTableStatement = "CREATE TABLE Singers (SingerId INT64, Name STRING(MAX), Active BOOL NOT NULL DEFAULT (TRUE)) PRIMARY KEY (SingerId)"

// Example for executing a Partitioned DML transaction on a Google Cloud Spanner database.
// See https://cloud.google.com/spanner/docs/dml-partitioned for more information on Partitioned DML.
//
// Execute the sample with the command `go run main.go` from this directory.
func partitionedDml(projectId, instanceId, databaseId string) error {
	ctx := context.Background()
	db, err := sql.Open("spanner", fmt.Sprintf("projects/%s/instances/%s/databases/%s", projectId, instanceId, databaseId))
	if err != nil {
		return err
	}
	defer db.Close()

	// First insert a couple of test records that we will update and delete using Partitioned DML.
	conn, err := db.Conn(ctx)
	if err != nil {
		return fmt.Errorf("failed to get a connection: %v", err)
	}
	if err := conn.Raw(func(driverConn interface{}) error {
		_, err := driverConn.(spannerdriver.SpannerConn).Apply(ctx, []*spanner.Mutation{
			spanner.InsertOrUpdateMap("Singers", map[string]interface{}{"SingerId": 1, "Name": "Singer 1"}),
			spanner.InsertOrUpdateMap("Singers", map[string]interface{}{"SingerId": 2, "Name": "Singer 2"}),
			spanner.InsertOrUpdateMap("Singers", map[string]interface{}{"SingerId": 3, "Name": "Singer 3"}),
			spanner.InsertOrUpdateMap("Singers", map[string]interface{}{"SingerId": 4, "Name": "Singer 4"}),
		})
		return err
	}); err != nil {
		return fmt.Errorf("failed to insert test records: %v", err)
	}

	// Now update all records in the Singers table using Partitioned DML.
	if _, err := conn.ExecContext(ctx, "SET AUTOCOMMIT_DML_MODE='PARTITIONED_NON_ATOMIC'"); err != nil {
		return fmt.Errorf("failed to change DML mode to Partitioned_Non_Atomic: %v", err)
	}
	res, err := conn.ExecContext(ctx, "UPDATE Singers SET Active = FALSE WHERE TRUE")
	if err != nil {
		return fmt.Errorf("failed to execute UPDATE statement: %v", err)
	}
	affected, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("failed to get affected rows: %v", err)
	}

	// Partitioned DML returns the minimum number of records that were affected.
	fmt.Printf("Updated %v records using Partitioned DML\n", affected)

	// Closing the connection will return it to the connection pool. The DML mode will automatically be reset to the
	// default TRANSACTIONAL mode when the connection is returned to the pool, so we do not need to change it back
	// manually.
	_ = conn.Close()

	// The AutoCommitDMLMode can also be specified as an ExecOption for a single statement.
	conn, err = db.Conn(ctx)
	if err != nil {
		return fmt.Errorf("failed to get a connection: %v", err)
	}
	res, err = conn.ExecContext(ctx, "DELETE FROM Singers WHERE NOT Active",
		spannerdriver.ExecOptions{AutocommitDMLMode: spannerdriver.PartitionedNonAtomic})
	if err != nil {
		return fmt.Errorf("failed to execute DELETE statement: %v", err)
	}
	affected, err = res.RowsAffected()
	if err != nil {
		return fmt.Errorf("failed to get affected rows: %v", err)
	}

	// Partitioned DML returns the minimum number of records that were affected.
	fmt.Printf("Deleted %v records using Partitioned DML\n", affected)

	return nil
}

func main() {
	examples.RunSampleOnEmulator(partitionedDml, createTableStatement)
}
