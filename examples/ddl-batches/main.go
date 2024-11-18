// Copyright 2021 Google LLC All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
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

// Sample showing how to execute a batch of DDL statements.
// Batching DDL statements together instead of executing them one by one leads to a much lower total execution time.
// It is therefore recommended that DDL statements are always executed in batches whenever possible.
//
// DDL batches can be executed in two ways using the Spanner go sql driver:
//  1. By executing the SQL statements `START BATCH DDL` and `RUN BATCH`.
//  2. By unwrapping the Spanner specific driver interface spannerdriver.Driver and calling the
//     spannerdriver.Driver#StartBatchDDL and spannerdriver.Driver#RunBatch methods.
//
// This sample shows how to use both possibilities.
//
// Execute the sample with the command `go run main.go` from this directory.
func ddlBatches(projectId, instanceId, databaseId string) error {
	ctx := context.Background()
	db, err := sql.Open("spanner", fmt.Sprintf("projects/%s/instances/%s/databases/%s", projectId, instanceId, databaseId))
	if err != nil {
		return fmt.Errorf("failed to open database connection: %v", err)
	}
	defer db.Close()

	// Execute a DDL batch by executing the `START BATCH DDL` and `RUN BATCH` statements.
	// First we need to get a connection from the pool to ensure that all statements are executed on the same
	// connection.
	conn, err := db.Conn(ctx)
	if err != nil {
		return fmt.Errorf("failed to get connection: %v", err)
	}
	defer conn.Close()
	// Start a DDL batch on the connection.
	if _, err := conn.ExecContext(ctx, "START BATCH DDL"); err != nil {
		return fmt.Errorf("START BATCH DDL failed: %v", err)
	}
	// Execute the DDL statements on the same connection as where we started the batch.
	// These statements will be buffered in the connection and executed on Spanner when we execute `RUN BATCH`.
	_, _ = conn.ExecContext(ctx, "CREATE TABLE Singers (SingerId INT64, Name STRING(MAX)) PRIMARY KEY (SingerId)")
	_, _ = conn.ExecContext(ctx, "CREATE INDEX Idx_Singers_Name ON Singers (Name)")
	// Executing `RUN BATCH` will run the previous DDL statements as one batch.
	if _, err := conn.ExecContext(ctx, "RUN BATCH"); err != nil {
		return fmt.Errorf("RUN BATCH failed: %v", err)
	}
	fmt.Printf("Executed DDL batch using SQL statements\n")

	// A DDL batch can also be executed programmatically by unwrapping the spannerdriver.Driver interface.
	if err := conn.Raw(func(driverConn interface{}) error {
		// Get the Spanner connection interface and start a DDL batch on the connection.
		return driverConn.(spannerdriver.SpannerConn).StartBatchDDL()
	}); err != nil {
		return fmt.Errorf("conn.Raw failed: %v", err)
	}
	_, _ = conn.ExecContext(ctx, "CREATE TABLE Albums (SingerId INT64, AlbumId INT64, Title STRING(MAX)) PRIMARY KEY (SingerId, AlbumId), INTERLEAVE IN PARENT Singers")
	_, _ = conn.ExecContext(ctx, "CREATE TABLE Tracks (SingerId INT64, AlbumId INT64, TrackId INT64, Title STRING(MAX)) PRIMARY KEY (SingerId, AlbumId, TrackId), INTERLEAVE IN PARENT Albums")
	if err := conn.Raw(func(driverConn interface{}) error {
		return driverConn.(spannerdriver.SpannerConn).RunBatch(ctx)
	}); err != nil {
		return fmt.Errorf("conn.Raw failed: %v", err)
	}
	fmt.Printf("Executed DDL batch using Spanner connection methods\n")

	// Get the number of tables and indexes.
	var tc int64
	if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_CATALOG='' AND TABLE_SCHEMA=''").Scan(&tc); err != nil {
		return fmt.Errorf("failed to execute count tables query: %v", err)
	}
	var ic int64
	if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM INFORMATION_SCHEMA.INDEXES WHERE TABLE_CATALOG='' AND TABLE_SCHEMA='' AND INDEX_TYPE != 'PRIMARY_KEY'").Scan(&ic); err != nil {
		return fmt.Errorf("failed to execute count indexes query: %v", err)
	}
	fmt.Println()
	fmt.Printf("The database now contains %v tables and %v indexes\n", tc, ic)

	return nil
}

func main() {
	examples.RunSampleOnEmulator(ddlBatches)
}
