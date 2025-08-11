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

// [START spanner_update_data]
import (
	"context"
	"database/sql"
	"fmt"
	"io"

	"cloud.google.com/go/spanner"
	spannerdriver "github.com/googleapis/go-sql-spanner"
)

func UpdateDataWithMutationsPostgreSQL(ctx context.Context, w io.Writer, databaseName string) error {
	db, err := sql.Open("spanner", databaseName)
	if err != nil {
		return err
	}
	defer func() { _ = db.Close() }()

	// Get a connection so that we can get access to the Spanner specific
	// connection interface SpannerConn.
	conn, err := db.Conn(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close() }()

	cols := []string{"singer_id", "album_id", "marketing_budget"}
	mutations := []*spanner.Mutation{
		spanner.Update("albums", cols, []interface{}{1, 1, 100000}),
		spanner.Update("albums", cols, []interface{}{2, 2, 500000}),
	}
	if err := conn.Raw(func(driverConn interface{}) error {
		spannerConn, ok := driverConn.(spannerdriver.SpannerConn)
		if !ok {
			return fmt.Errorf("unexpected driver connection %v, "+
				"expected SpannerConn", driverConn)
		}
		_, err = spannerConn.Apply(ctx, mutations)
		return err
	}); err != nil {
		return err
	}
	_, _ = fmt.Fprintf(w, "Updated %v albums\n", len(mutations))

	return nil
}

// [END spanner_update_data]
