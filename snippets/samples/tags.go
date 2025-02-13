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

// [START spanner_transaction_and_statement_tag]
import (
	"context"
	"database/sql"
	"fmt"
	"io"

	"cloud.google.com/go/spanner"
	spannerdriver "github.com/googleapis/go-sql-spanner"
)

func Tags(ctx context.Context, w io.Writer, databaseName string) error {
	db, err := sql.Open("spanner", databaseName)
	if err != nil {
		return err
	}
	defer db.Close()

	// Use the spannerdriver.BeginReadWriteTransaction function
	// to specify specific Spanner options, such as transaction tags.
	tx, err := spannerdriver.BeginReadWriteTransaction(ctx, db,
		spannerdriver.ReadWriteTransactionOptions{
			TransactionOptions: spanner.TransactionOptions{
				TransactionTag: "example-tx-tag",
			},
		})
	if err != nil {
		return err
	}

	// Pass in an argument of type spannerdriver.ExecOptions to supply
	// additional options for a statement.
	row := tx.QueryRowContext(ctx, "SELECT MarketingBudget "+
		"FROM Albums "+
		"WHERE SingerId=? and AlbumId=?",
		spannerdriver.ExecOptions{
			QueryOptions: spanner.QueryOptions{RequestTag: "query-marketing-budget"},
		}, 1, 1)
	var budget int64
	if err := row.Scan(&budget); err != nil {
		tx.Rollback()
		return err
	}

	// Reduce the marketing budget by 10% if it is more than 1,000.
	if budget > 1000 {
		budget = int64(float64(budget) - float64(budget)*0.1)
		if _, err := tx.ExecContext(ctx,
			`UPDATE Albums SET MarketingBudget=@budget 
               WHERE SingerId=@singerId AND AlbumId=@albumId`,
			spannerdriver.ExecOptions{
				QueryOptions: spanner.QueryOptions{RequestTag: "reduce-marketing-budget"},
			},
			sql.Named("budget", budget),
			sql.Named("singerId", 1),
			sql.Named("albumId", 1)); err != nil {
			tx.Rollback()
			return err
		}
	}
	// Commit the current transaction.
	if err := tx.Commit(); err != nil {
		return err
	}
	fmt.Fprintln(w, "Reduced marketing budget")

	return nil
}

// [END spanner_transaction_and_statement_tag]
