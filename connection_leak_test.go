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

package spannerdriver

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"sync"
	"testing"
	"time"

	"cloud.google.com/go/spanner"
	"github.com/googleapis/go-sql-spanner/testutil"
	"google.golang.org/grpc/codes"
	gstatus "google.golang.org/grpc/status"
)

func TestNoLeak(t *testing.T) {
	t.Parallel()

	db, server, teardown := setupTestDBConnection(t)
	defer teardown()
	// Set MaxOpenConns to 1 to force an error if anything leaks a connection.
	db.SetMaxOpenConns(1)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*30)
	defer cancel()

	for i := 0; i < 2; i++ {
		pingContext(ctx, t, db)
		pingFailed(ctx, t, server, db)
		simpleQuery(ctx, t, db)
		concurrentScanAndClose(ctx, t, db)
		queryWithTimestampBound(ctx, t, db)
		simpleReadOnlyTx(ctx, t, db)
		readOnlyTxWithStaleness(ctx, t, db)
		readOnlyTxWithStaleness(ctx, t, db)
		simpleReadWriteTx(ctx, t, db)
		runTransactionRetry(ctx, t, server, db)
		readOnlyTxWithOptions(ctx, t, db)
	}
}

func pingContext(ctx context.Context, t *testing.T, db *sql.DB) {
	if err := db.PingContext(ctx); err != nil {
		t.Fatalf("unexpected error for ping: %v", err)
	}
}

func pingFailed(ctx context.Context, t *testing.T, server *testutil.MockedSpannerInMemTestServer, db *sql.DB) {
	s := gstatus.Newf(codes.PermissionDenied, "Permission denied for database")
	server.TestSpanner.PutExecutionTime(testutil.MethodExecuteStreamingSql, testutil.SimulatedExecutionTime{
		Errors: []error{s.Err()},
	})
	if g, w := db.PingContext(ctx), driver.ErrBadConn; g != w {
		t.Fatalf("ping error mismatch\nGot: %v\nWant: %v", g, w)
	}
}

func simpleQuery(ctx context.Context, t *testing.T, db *sql.DB) {
	rows, err := db.QueryContext(ctx, testutil.SelectFooFromBar)
	if err != nil {
		t.Fatal(err)
	}
	defer silentClose(rows)

	for want := int64(1); rows.Next(); want++ {
		_, err := rows.Columns()
		if err != nil {
			t.Fatal(err)
		}
		var val int64
		err = rows.Scan(&val)
		if err != nil {
			t.Fatal(err)
		}
	}
	if rows.Err() != nil {
		t.Fatal(rows.Err())
	}
}

func concurrentScanAndClose(ctx context.Context, t *testing.T, db *sql.DB) {
	conn, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	rows, err := conn.QueryContext(ctx, testutil.SelectFooFromBar)
	if err != nil {
		t.Fatal(err)
	}

	// Only fetch the first row of the query to make sure that the rows are not auto-closed
	// when the end of the stream is reached.
	rows.Next()
	var got int64
	err = rows.Scan(&got)
	if err != nil {
		t.Fatal(err)
	}

	// Close both the connection and the rows in parallel.
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		_ = conn.Close()
	}()
	go func() {
		defer wg.Done()
		_ = rows.Close()
	}()
	wg.Wait()
}

func queryWithTimestampBound(ctx context.Context, t *testing.T, db *sql.DB) {
	conn, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := conn.ExecContext(ctx, "SET READ_ONLY_STALENESS = 'MAX_STALENESS 10s'"); err != nil {
		t.Fatalf("Set read-only staleness: %v", err)
	}
	rows, err := conn.QueryContext(ctx, testutil.SelectFooFromBar)
	if err != nil {
		t.Fatal(err)
	}

	for rows.Next() {
	}
	if rows.Err() != nil {
		t.Fatal(rows.Err())
	}
	if err := rows.Close(); err != nil {
		t.Fatal(err)
	}

	// Close the connection and execute a new query. This should use a strong read.
	_ = conn.Close()
	rows, err = db.QueryContext(ctx, testutil.SelectFooFromBar)
	if err != nil {
		t.Fatal(err)
	}

	for rows.Next() {
	}
	if rows.Err() != nil {
		t.Fatal(rows.Err())
	}
	if err := rows.Close(); err != nil {
		t.Fatal(err)
	}
}

func simpleReadOnlyTx(ctx context.Context, t *testing.T, db *sql.DB) {
	tx, err := db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		t.Fatal(err)
	}
	rows, err := tx.Query(testutil.SelectFooFromBar)
	if err != nil {
		t.Fatal(err)
	}

	for want := int64(1); rows.Next(); want++ {
		_, err := rows.Columns()
		if err != nil {
			t.Fatal(err)
		}
		var val int64
		err = rows.Scan(&val)
		if err != nil {
			t.Fatal(err)
		}
	}
	if rows.Err() != nil {
		t.Fatal(rows.Err())
	}
	if err := rows.Close(); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
}

func readOnlyTxWithStaleness(ctx context.Context, t *testing.T, db *sql.DB) {
	conn, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer silentClose(conn)
	if _, err := conn.ExecContext(ctx, "SET READ_ONLY_STALENESS = 'EXACT_STALENESS 10s'"); err != nil {
		t.Fatalf("Set read-only staleness: %v", err)
	}
	tx, err := conn.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		t.Fatal(err)
	}
	rows, err := tx.Query(testutil.SelectFooFromBar)
	if err != nil {
		t.Fatal(err)
	}

	for rows.Next() {
	}
	if rows.Err() != nil {
		t.Fatal(rows.Err())
	}
	if err := rows.Close(); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
}

func simpleReadWriteTx(ctx context.Context, t *testing.T, db *sql.DB) {
	conn, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer silentClose(conn)
	if _, err := conn.ExecContext(ctx, "set max_commit_delay='10ms'"); err != nil {
		t.Fatal(err)
	}

	tx, err := conn.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		t.Fatal(err)
	}
	rows, err := tx.Query(testutil.SelectFooFromBar)
	if err != nil {
		t.Fatal(err)
	}

	for want := int64(1); rows.Next(); want++ {
		_, err := rows.Columns()
		if err != nil {
			t.Fatal(err)
		}
		var val int64
		err = rows.Scan(&val)
		if err != nil {
			t.Fatal(err)
		}
	}
	if rows.Err() != nil {
		t.Fatal(rows.Err())
	}
	if err := rows.Close(); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
}

func runTransactionRetry(ctx context.Context, t *testing.T, server *testutil.MockedSpannerInMemTestServer, db *sql.DB) {
	var attempts int
	err := RunTransactionWithOptions(ctx, db, &sql.TxOptions{}, func(ctx context.Context, tx *sql.Tx) error {
		attempts++
		rows, err := tx.QueryContext(ctx, testutil.SelectFooFromBar, ExecOptions{QueryOptions: spanner.QueryOptions{RequestTag: "tag_1"}})
		if err != nil {
			t.Fatal(err)
		}
		for rows.Next() {
		}
		if err := rows.Close(); err != nil {
			t.Fatal(err)
		}

		if _, err := tx.ExecContext(ctx, testutil.UpdateBarSetFoo, ExecOptions{QueryOptions: spanner.QueryOptions{RequestTag: "tag_2"}}); err != nil {
			t.Fatal(err)
		}

		if _, err := tx.ExecContext(ctx, "start batch dml", ExecOptions{QueryOptions: spanner.QueryOptions{RequestTag: "tag_3"}}); err != nil {
			t.Fatal(err)
		}
		if _, err := tx.ExecContext(ctx, testutil.UpdateBarSetFoo); err != nil {
			t.Fatal(err)
		}
		if _, err := tx.ExecContext(ctx, "run batch"); err != nil {
			t.Fatal(err)
		}
		if attempts == 1 {
			server.TestSpanner.PutExecutionTime(testutil.MethodCommitTransaction, testutil.SimulatedExecutionTime{
				Errors: []error{gstatus.Error(codes.Aborted, "Aborted")},
			})
		}
		return nil
	}, spanner.TransactionOptions{TransactionTag: "my_transaction_tag"})
	if err != nil {
		t.Fatalf("failed to run transaction: %v", err)
	}
}

func readOnlyTxWithOptions(ctx context.Context, t *testing.T, db *sql.DB) {
	tx, err := BeginReadOnlyTransaction(ctx, db,
		ReadOnlyTransactionOptions{TimestampBound: spanner.ExactStaleness(10 * time.Second)})
	if err != nil {
		t.Fatal(err)
	}
	useTx := func(tx *sql.Tx) {
		rows, err := tx.Query(testutil.SelectFooFromBar)
		if err != nil {
			t.Fatal(err)
		}

		for rows.Next() {
		}
		if rows.Err() != nil {
			t.Fatal(rows.Err())
		}
		if err := rows.Close(); err != nil {
			t.Fatal(err)
		}
		if err := tx.Commit(); err != nil {
			t.Fatal(err)
		}
	}
	useTx(tx)
}
