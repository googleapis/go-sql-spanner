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

func DdlBatch(ctx context.Context, w io.Writer, databaseName string) error {
	db, err := sql.Open("spanner", databaseName)
	if err != nil {
		return err
	}
	defer db.Close()

	// Executing multiple DDL statements as one batch is
	// more efficient than executing each statement
	// individually.
	conn, err := db.Conn(ctx)
	defer conn.Close()

	if _, err := conn.ExecContext(ctx, "start batch ddl"); err != nil {
		return err
	}
	if _, err := conn.ExecContext(ctx,
		`CREATE TABLE Venues (
			VenueId     INT64 NOT NULL,
			Name        STRING(1024),
			Description JSON,
		) PRIMARY KEY (VenueId)`); err != nil {
		return err
	}
	if _, err := conn.ExecContext(ctx,
		`CREATE TABLE Concerts (
			ConcertId INT64 NOT NULL,
			VenueId   INT64 NOT NULL,
			SingerId  INT64 NOT NULL,
			StartTime TIMESTAMP,
			EndTime   TIMESTAMP,
			CONSTRAINT Fk_Concerts_Venues FOREIGN KEY
				(VenueId) REFERENCES Venues (VenueId),
			CONSTRAINT Fk_Concerts_Singers FOREIGN KEY
				(SingerId) REFERENCES Singers (SingerId),
		) PRIMARY KEY (ConcertId)`); err != nil {
		return err
	}
	// `run batch` sends the DDL statements to Spanner and blocks until
	// all statements have finished executing.
	if _, err := conn.ExecContext(ctx, "run batch"); err != nil {
		return err
	}

	fmt.Fprint(w, "Added Venues and Concerts tables\n")
	return nil
}

// [END spanner_ddl_batch]
