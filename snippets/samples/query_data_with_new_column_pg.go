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

// [START spanner_query_data_with_new_column]
import (
	"context"
	"database/sql"
	"fmt"
	"io"

	_ "github.com/googleapis/go-sql-spanner"
)

func QueryNewColumnPostgreSQL(ctx context.Context, w io.Writer, databaseName string) error {
	db, err := sql.Open("spanner", databaseName)
	if err != nil {
		return err
	}
	defer func() { _ = db.Close() }()

	rows, err := db.QueryContext(ctx,
		`select singer_id, album_id, marketing_budget
		from albums
		order by singer_id, album_id`)
	defer func() { _ = rows.Close() }()
	if err != nil {
		return err
	}
	for rows.Next() {
		var singerId, albumId int64
		var marketingBudget sql.NullInt64
		err = rows.Scan(&singerId, &albumId, &marketingBudget)
		if err != nil {
			return err
		}
		budget := "null"
		if marketingBudget.Valid {
			budget = fmt.Sprintf("%v", marketingBudget.Int64)
		}
		_, _ = fmt.Fprintf(w, "%v %v %v\n", singerId, albumId, budget)
	}
	if rows.Err() != nil {
		return rows.Err()
	}
	return rows.Close()
}

// [END spanner_query_data_with_new_column]
