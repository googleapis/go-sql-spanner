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

// [START spanner_create_connection]
import (
	"context"
	"database/sql"
	"fmt"
	"io"

	_ "github.com/googleapis/go-sql-spanner"
)

func CreateConnection(ctx context.Context, w io.Writer, databaseName string) error {
	// The dataSourceName should start with a fully qualified Spanner database name.
	// Additional properties can be added after the database name by
	// adding `;name=value`.

	dsn := fmt.Sprintf("%s;numChannels=8", databaseName)
	db, err := sql.Open("spanner", dsn)
	if err != nil {
		return err
	}
	defer db.Close()

	row := db.QueryRowContext(ctx, "select 'Hello world!' as hello")
	var msg string
	if err := row.Scan(&msg); err != nil {
		return err
	}
	fmt.Fprintf(w, "Greeting from Spanner: %s\n", msg)
	return nil
}

// [END spanner_create_connection]
