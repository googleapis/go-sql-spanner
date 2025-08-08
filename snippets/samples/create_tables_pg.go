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

func CreateTablesPostgreSQL(ctx context.Context, w io.Writer, databaseName string) error {
	db, err := sql.Open("spanner", databaseName)
	if err != nil {
		return err
	}
	defer func() { _ = db.Close() }()

	// Create two tables in one batch on Spanner PostgreSQL.
	conn, err := db.Conn(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close() }()

	// Start a DDL batch on the connection.
	// This instructs the connection to buffer all DDL statements until the
	// command `run batch` is executed.
	if _, err := conn.ExecContext(ctx, "start batch ddl"); err != nil {
		return err
	}
	if _, err := conn.ExecContext(ctx,
		`create table singers (
				singer_id   bigint not null primary key,
				first_name  varchar(1024),
				last_name   varchar(1024),
				singer_info bytea
			)`); err != nil {
		return err
	}
	if _, err := conn.ExecContext(ctx,
		`create table albums (
				singer_id     bigint not null,
				album_id      bigint not null,
				album_title   varchar,
				primary key (singer_id, album_id)
			)
			interleave in parent singers on delete cascade`); err != nil {
		return err
	}
	// `run batch` sends the DDL statements to Spanner and blocks until
	// all statements have finished executing.
	if _, err := conn.ExecContext(ctx, "run batch"); err != nil {
		return err
	}

	_, _ = fmt.Fprintf(w, "Created singers & albums tables in database: [%s]\n", databaseName)
	return nil
}

// [END spanner_create_database]
