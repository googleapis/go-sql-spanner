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

// [START spanner_data_boost]
import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"slices"

	"cloud.google.com/go/spanner"
	spannerdriver "github.com/googleapis/go-sql-spanner"
)

func DataBoost(ctx context.Context, w io.Writer, databaseName string) error {
	db, err := sql.Open("spanner", databaseName)
	if err != nil {
		return err
	}
	defer db.Close()

	// Run a partitioned query that uses Data Boost.
	rows, err := db.QueryContext(ctx,
		"SELECT SingerId, FirstName, LastName from Singers",
		spannerdriver.ExecOptions{
			PartitionedQueryOptions: spannerdriver.PartitionedQueryOptions{
				// AutoPartitionQuery instructs the Spanner database/sql driver to
				// automatically partition the query and execute each partition in parallel.
				// The rows are returned as one result set in undefined order.
				AutoPartitionQuery: true,
			},
			QueryOptions: spanner.QueryOptions{
				// Set DataBoostEnabled to true to enable DataBoost.
				// See https://cloud.google.com/spanner/docs/databoost/databoost-overview
				// for more information.
				DataBoostEnabled: true,
			},
		})
	defer rows.Close()
	if err != nil {
		return err
	}
	type Singer struct {
		SingerId  int64
		FirstName string
		LastName  string
	}
	var singers []Singer
	for rows.Next() {
		var singer Singer
		err = rows.Scan(&singer.SingerId, &singer.FirstName, &singer.LastName)
		if err != nil {
			return err
		}
		singers = append(singers, singer)
	}
	// Queries that use the AutoPartition option return rows in undefined order,
	// so we need to sort them in memory to guarantee the output order.
	slices.SortFunc(singers, func(a, b Singer) int {
		return int(a.SingerId - b.SingerId)
	})
	for _, s := range singers {
		fmt.Fprintf(w, "%v %v %v\n", s.SingerId, s.FirstName, s.LastName)
	}

	return nil
}

// [END spanner_data_boost]
