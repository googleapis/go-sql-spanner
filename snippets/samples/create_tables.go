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

package samples

// [START spanner_create_database]
import (
	"context"
	"database/sql"
	"fmt"
	"io"

	_ "github.com/googleapis/go-sql-spanner"
)

func CreateTables(ctx context.Context, w io.Writer, databaseName string) error {
	db, err := sql.Open("spanner", databaseName)
	if err != nil {
		return err
	}
	defer db.Close()

	// Create two tables in one batch on Spanner.
	conn, err := db.Conn(ctx)
	defer conn.Close()

	// Start a DDL batch on the connection.
	// This instructs the connection to buffer all DDL statements until the
	// command `run batch` is executed.
	if _, err := conn.ExecContext(ctx, "start batch ddl"); err != nil {
		return err
	}
	if _, err := conn.ExecContext(ctx,
		`CREATE TABLE Singers (
				SingerId   INT64 NOT NULL,
				FirstName  STRING(1024),
				LastName   STRING(1024),
				SingerInfo BYTES(MAX)
			) PRIMARY KEY (SingerId)`); err != nil {
		return err
	}
	if _, err := conn.ExecContext(ctx,
		`CREATE TABLE Albums (
				SingerId     INT64 NOT NULL,
				AlbumId      INT64 NOT NULL,
				AlbumTitle   STRING(MAX)
			) PRIMARY KEY (SingerId, AlbumId),
			INTERLEAVE IN PARENT Singers ON DELETE CASCADE`); err != nil {
		return err
	}
	// `run batch` sends the DDL statements to Spanner and blocks until
	// all statements have finished executing.
	if _, err := conn.ExecContext(ctx, "run batch"); err != nil {
		return err
	}

	fmt.Fprintf(w, "Created Singers & Albums tables in database: [%s]\n", databaseName)
	return nil
}

// [END spanner_create_database]
