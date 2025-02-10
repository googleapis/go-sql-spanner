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

// [START spanner_partitioned_dml]
import (
	"context"
	"database/sql"
	"fmt"
	"io"

	_ "github.com/googleapis/go-sql-spanner"
)

func PartitionedDml(ctx context.Context, w io.Writer, databaseName string) error {
	db, err := sql.Open("spanner", databaseName)
	if err != nil {
		return err
	}
	defer db.Close()

	conn, err := db.Conn(ctx)
	if err != nil {
		return err
	}
	// Enable Partitioned DML on this connection.
	if _, err := conn.ExecContext(ctx, "SET AUTOCOMMIT_DML_MODE='PARTITIONED_NON_ATOMIC'"); err != nil {
		return fmt.Errorf("failed to change DML mode to Partitioned_Non_Atomic: %v", err)
	}
	// Back-fill a default value for the MarketingBudget column.
	res, err := conn.ExecContext(ctx, "UPDATE Albums SET MarketingBudget=0 WHERE MarketingBudget IS NULL")
	if err != nil {
		return err
	}
	affected, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("failed to get affected rows: %v", err)
	}

	// Partitioned DML returns the minimum number of records that were affected.
	fmt.Fprintf(w, "Updated at least %v albums\n", affected)

	// Closing the connection will return it to the connection pool. The DML mode will automatically be reset to the
	// default TRANSACTIONAL mode when the connection is returned to the pool, so we do not need to change it back
	// manually.
	_ = conn.Close()

	return nil
}

// [END spanner_partitioned_dml]
