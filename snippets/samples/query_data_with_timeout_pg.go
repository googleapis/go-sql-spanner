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

// [START spanner_statement_timeout]
import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"time"

	_ "github.com/googleapis/go-sql-spanner"
)

func QueryDataWithTimeoutPostgreSQL(ctx context.Context, w io.Writer, databaseName string) error {
	db, err := sql.Open("spanner", databaseName)
	if err != nil {
		return err
	}
	defer func() { _ = db.Close() }()

	// Use QueryContext and pass in a context with a timeout to execute
	// a query with a timeout.
	ctx, cancel := context.WithTimeout(ctx, time.Second*5)
	defer cancel()
	rows, err := db.QueryContext(ctx,
		`select singer_id, album_id, album_title
			from albums
			where album_title in (
			  select first_name
			  from singers
			  where last_name LIKE '%a%'
			     or last_name LIKE '%m%'
			)`)
	defer func() { _ = rows.Close() }()
	if err != nil {
		return err
	}
	for rows.Next() {
		var singerId, albumId int64
		var title string
		err = rows.Scan(&singerId, &albumId, &title)
		if err != nil {
			return err
		}
		_, _ = fmt.Fprintf(w, "%v %v %v\n", singerId, albumId, title)
	}
	if rows.Err() != nil {
		return rows.Err()
	}
	return rows.Close()
}

// [END spanner_statement_timeout]
