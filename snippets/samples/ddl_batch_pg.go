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

// [START spanner_ddl_batch]
import (
	"context"
	"database/sql"
	"fmt"
	"io"

	_ "github.com/googleapis/go-sql-spanner"
)

func DdlBatchPostgreSQL(ctx context.Context, w io.Writer, databaseName string) error {
	db, err := sql.Open("spanner", databaseName)
	if err != nil {
		return err
	}
	defer func() { _ = db.Close() }()

	// Executing multiple DDL statements as one batch is
	// more efficient than executing each statement
	// individually.
	conn, err := db.Conn(ctx)
	defer func() { _ = conn.Close() }()

	if _, err := conn.ExecContext(ctx, "start batch ddl"); err != nil {
		return err
	}
	if _, err := conn.ExecContext(ctx,
		`create table venues (
			venue_id    bigint not null primary key,
			name        varchar(1024),
			description jsonb
		)`); err != nil {
		return err
	}
	if _, err := conn.ExecContext(ctx,
		`create table concerts (
			concert_id bigint not null primary key,
			venue_id   bigint not null,
			singer_id  bigint not null,
			start_time timestamptz,
			end_time   timestamptz,
			constraint fk_concerts_venues foreign key
				(venue_id) references venues (venue_id),
			constraint fk_concerts_singers foreign key
				(singer_id) references singers (singer_id)
		)`); err != nil {
		return err
	}
	// `run batch` sends the DDL statements to Spanner and blocks until
	// all statements have finished executing.
	if _, err := conn.ExecContext(ctx, "run batch"); err != nil {
		return err
	}

	_, _ = fmt.Fprint(w, "Added venues and concerts tables\n")
	return nil
}

// [END spanner_ddl_batch]
