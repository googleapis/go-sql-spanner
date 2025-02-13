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

// [START spanner_insert_data]
import (
	"context"
	"database/sql"
	"fmt"
	"io"

	"cloud.google.com/go/spanner"
	spannerdriver "github.com/googleapis/go-sql-spanner"
)

func WriteDataWithMutations(ctx context.Context, w io.Writer, databaseName string) error {
	db, err := sql.Open("spanner", databaseName)
	if err != nil {
		return err
	}
	defer db.Close()

	// Get a connection so that we can get access to the Spanner specific
	// connection interface SpannerConn.
	conn, err := db.Conn(ctx)
	if err != nil {
		return err
	}
	defer conn.Close()

	singerColumns := []string{"SingerId", "FirstName", "LastName"}
	albumColumns := []string{"SingerId", "AlbumId", "AlbumTitle"}
	mutations := []*spanner.Mutation{
		spanner.Insert("Singers", singerColumns, []interface{}{int64(1), "Marc", "Richards"}),
		spanner.Insert("Singers", singerColumns, []interface{}{int64(2), "Catalina", "Smith"}),
		spanner.Insert("Singers", singerColumns, []interface{}{int64(3), "Alice", "Trentor"}),
		spanner.Insert("Singers", singerColumns, []interface{}{int64(4), "Lea", "Martin"}),
		spanner.Insert("Singers", singerColumns, []interface{}{int64(5), "David", "Lomond"}),
		spanner.Insert("Albums", albumColumns, []interface{}{int64(1), int64(1), "Total Junk"}),
		spanner.Insert("Albums", albumColumns, []interface{}{int64(1), int64(2), "Go, Go, Go"}),
		spanner.Insert("Albums", albumColumns, []interface{}{int64(2), int64(1), "Green"}),
		spanner.Insert("Albums", albumColumns, []interface{}{int64(2), int64(2), "Forever Hold Your Peace"}),
		spanner.Insert("Albums", albumColumns, []interface{}{int64(2), int64(3), "Terrified"}),
	}
	// Mutations can be written outside an explicit transaction using SpannerConn#Apply.
	if err := conn.Raw(func(driverConn interface{}) error {
		spannerConn, ok := driverConn.(spannerdriver.SpannerConn)
		if !ok {
			return fmt.Errorf("unexpected driver connection %v, expected SpannerConn", driverConn)
		}
		_, err = spannerConn.Apply(ctx, mutations)
		return err
	}); err != nil {
		return err
	}
	fmt.Fprintf(w, "Inserted %v rows\n", len(mutations))

	return nil
}

// [END spanner_insert_data]
