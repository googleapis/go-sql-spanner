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

// [START spanner_query_with_parameter]
import (
	"context"
	"database/sql"
	"fmt"
	"io"

	_ "github.com/googleapis/go-sql-spanner"
)

func QueryDataWithParameter(ctx context.Context, w io.Writer, databaseName string) error {
	db, err := sql.Open("spanner", databaseName)
	if err != nil {
		return err
	}
	defer db.Close()

	rows, err := db.QueryContext(ctx,
		`SELECT SingerId, FirstName, LastName
         FROM Singers
         WHERE LastName = ?`, "Garcia")
	defer rows.Close()
	if err != nil {
		return err
	}
	for rows.Next() {
		var singerId int64
		var firstName, lastName string
		err = rows.Scan(&singerId, &firstName, &lastName)
		if err != nil {
			return err
		}
		fmt.Fprintf(w, "%v %v %v\n", singerId, firstName, lastName)
	}
	if rows.Err() != nil {
		return rows.Err()
	}
	return rows.Close()
}

// [END spanner_query_with_parameter]
