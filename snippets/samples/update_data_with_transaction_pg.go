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

// [START spanner_dml_getting_started_update]
import (
	"context"
	"database/sql"
	"fmt"
	"io"

	_ "github.com/googleapis/go-sql-spanner"
)

func WriteWithTransactionUsingDmlPostgreSQL(ctx context.Context, w io.Writer, databaseName string) error {
	db, err := sql.Open("spanner", databaseName)
	if err != nil {
		return err
	}
	defer func() { _ = db.Close() }()

	// Transfer marketing budget from one album to another. We do it in a
	// transaction to ensure that the transfer is atomic.
	tx, err := db.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		return err
	}
	const selectSql = "select marketing_budget " +
		"from albums " +
		"where singer_id = $1 and album_id = $2"
	// Get the marketing_budget of singer 2 / album 2.
	row := tx.QueryRowContext(ctx, selectSql, 2, 2)
	var budget2 int64
	if err := row.Scan(&budget2); err != nil {
		_ = tx.Rollback()
		return err
	}
	const transfer = 20000
	// The transaction will only be committed if this condition still holds
	// at the time of commit. Otherwise, the transaction will be aborted.
	if budget2 >= transfer {
		// Get the marketing_budget of singer 1 / album 1.
		row := tx.QueryRowContext(ctx, selectSql, 1, 1)
		var budget1 int64
		if err := row.Scan(&budget1); err != nil {
			_ = tx.Rollback()
			return err
		}
		// Transfer part of the marketing budget of Album 2 to Album 1.
		budget1 += transfer
		budget2 -= transfer
		const updateSql = "update albums " +
			"set marketing_budget = $1 " +
			"where singer_id = $2 and album_id = $3"
		// Start a DML batch and execute it as part of the current transaction.
		if _, err := tx.ExecContext(ctx, "start batch dml"); err != nil {
			_ = tx.Rollback()
			return err
		}
		if _, err := tx.ExecContext(ctx, updateSql, budget1, 1, 1); err != nil {
			_, _ = tx.ExecContext(ctx, "abort batch")
			_ = tx.Rollback()
			return err
		}
		if _, err := tx.ExecContext(ctx, updateSql, budget2, 2, 2); err != nil {
			_, _ = tx.ExecContext(ctx, "abort batch")
			_ = tx.Rollback()
			return err
		}
		// `run batch` sends the DML statements to Spanner.
		// The result contains the total affected rows across the entire batch.
		result, err := tx.ExecContext(ctx, "run batch")
		if err != nil {
			_ = tx.Rollback()
			return err
		}
		if affected, err := result.RowsAffected(); err != nil {
			_ = tx.Rollback()
			return err
		} else if affected != 2 {
			// The batch should update 2 rows.
			_ = tx.Rollback()
			return fmt.Errorf("unexpected number of rows affected: %v", affected)
		}
	}
	// Commit the current transaction.
	if err := tx.Commit(); err != nil {
		return err
	}

	_, _ = fmt.Fprintln(w, "Transferred marketing budget from Album 2 to Album 1")

	return nil
}

// [END spanner_dml_getting_started_update]
