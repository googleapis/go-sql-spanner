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

// [START spanner_dml_getting_started_insert]
import (
	"context"
	"database/sql"
	"fmt"
	"io"

	_ "github.com/googleapis/go-sql-spanner"
)

func WriteDataWithDmlPostgreSQL(ctx context.Context, w io.Writer, databaseName string) error {
	db, err := sql.Open("spanner", databaseName)
	if err != nil {
		return err
	}
	defer func() { _ = db.Close() }()

	// Add 4 rows in one statement.
	// The database/sql driver supports positional query parameters.
	res, err := db.ExecContext(ctx,
		"insert into singers (singer_id, first_name, last_name) "+
			"values (?, ?, ?), (?, ?, ?), "+
			"       (?, ?, ?), (?, ?, ?)",
		12, "Melissa", "Garcia",
		13, "Russel", "Morales",
		14, "Jacqueline", "Long",
		15, "Dylan", "Shaw")
	if err != nil {
		return err
	}
	c, err := res.RowsAffected()
	if err != nil {
		return err
	}
	_, _ = fmt.Fprintf(w, "%v records inserted\n", c)

	return nil
}

// [END spanner_dml_getting_started_insert]
