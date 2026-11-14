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
	"fmt"
	"sync"
	"testing"
	"time"
)

// TestConcurrentDML tests that concurrent DML operations work correctly.
func TestConcurrentDML(t *testing.T) {
	skipIfShort(t)
	t.Parallel()

	ctx := context.Background()
	dsn, cleanup, err := createTestDB(ctx,
		`CREATE TABLE TestConcurrentDML (
			id INT64 NOT NULL,
			value STRING(1024),
			updated_at TIMESTAMP,
		) PRIMARY KEY (id)`)
	if err != nil {
		t.Fatalf("failed to create test db: %v", err)
	}
	defer cleanup()

	dsnWithMaxSessions := dsn + ";max_sessions=400"

	db, err := sql.Open("spanner", dsnWithMaxSessions)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()

	// Insert initial rows in a batch
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.ExecContext(ctx, "START BATCH DML"); err != nil {
		_ = tx.Rollback()
		t.Fatal(err)
	}
	for i := 0; i < 100; i++ {
		_, err := tx.ExecContext(ctx, "INSERT INTO TestConcurrentDML (id, value, updated_at) VALUES (?, ?, ?)",
			i, fmt.Sprintf("value-%d", i), time.Now())
		if err != nil {
			_ = tx.Rollback()
			t.Fatalf("failed to queue insert row %d: %v", i, err)
		}
	}
	if _, err := tx.ExecContext(ctx, "RUN BATCH"); err != nil {
		_ = tx.Rollback()
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}

	// Run concurrent UPDATE statements
	const concurrency = 10
	const iterations = 50

	var wg sync.WaitGroup
	errors := make(chan error, concurrency*iterations)

	for g := 0; g < concurrency; g++ {
		wg.Add(1)
		go func(goroutineID int) {
			defer wg.Done()

			for i := 0; i < iterations; i++ {
				rowID := (goroutineID*iterations + i) % 100
				timestamp := time.Now()

				// This UPDATE pattern uses conditional logic with query parameters.
				res, err := db.ExecContext(ctx, `
					UPDATE TestConcurrentDML
					SET
						value = ?,
						updated_at = CASE WHEN CAST(? AS bool) IS TRUE
							THEN CAST(? AS TIMESTAMP)
							ELSE TestConcurrentDML.updated_at
						END
					WHERE id = ?
				`,
					fmt.Sprintf("updated-%d-%d", goroutineID, i),
					true, timestamp,
					rowID,
				)
				if err != nil {
					errors <- fmt.Errorf("goroutine %d iteration %d: ExecContext failed: %w", goroutineID, i, err)
					continue
				}

				// Check RowsAffected
				affected, err := res.RowsAffected()
				if err != nil {
					errors <- fmt.Errorf("goroutine %d iteration %d: RowsAffected failed: %w", goroutineID, i, err)
					continue
				}
				if affected != 1 {
					errors <- fmt.Errorf("goroutine %d iteration %d: affected rows mismatch. Got: %d, Want: %d", goroutineID, i, affected, 1)
				}
			}
		}(g)
	}

	wg.Wait()
	close(errors)

	var allErrors []error
	for err := range errors {
		allErrors = append(allErrors, err)
	}

	if len(allErrors) > 0 {
		t.Errorf("Got %d errors during concurrent DML operations:", len(allErrors))
		// Print first 10 errors
		for i, err := range allErrors {
			if i >= 10 {
				t.Errorf("  ... and %d more errors", len(allErrors)-10)
				break
			}
			t.Errorf("  %v", err)
		}
	}
}

// TestSequentialDML tests that sequential DML operations work correctly.
// This is a simpler version of the concurrent test.
func TestSequentialDML(t *testing.T) {
	skipIfShort(t)
	t.Parallel()

	ctx := context.Background()
	dsn, cleanup, err := createTestDB(ctx,
		`CREATE TABLE TestSequentialDML (
			id INT64 NOT NULL,
			value STRING(1024),
			counter INT64,
		) PRIMARY KEY (id)`)
	if err != nil {
		t.Fatalf("failed to create test db: %v", err)
	}
	defer cleanup()

	db, err := sql.Open("spanner", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()

	// Insert a single row
	_, err = db.ExecContext(ctx, "INSERT INTO TestSequentialDML (id, value, counter) VALUES (1, 'initial', 0)")
	if err != nil {
		t.Fatalf("failed to insert row: %v", err)
	}

	// Perform many sequential updates
	for i := 0; i < 100; i++ {
		res, err := db.ExecContext(ctx, "UPDATE TestSequentialDML SET value = ?, counter = ? WHERE id = 1",
			fmt.Sprintf("value-%d", i), i)
		if err != nil {
			t.Fatalf("iteration %d: ExecContext failed: %v", i, err)
		}

		affected, err := res.RowsAffected()
		if err != nil {
			t.Fatalf("iteration %d: RowsAffected failed: %v", i, err)
		}
		if affected != 1 {
			t.Errorf("iteration %d: affected rows mismatch. Got: %d, Want: %d", i, affected, 1)
		}
	}

	// Verify final state
	var value string
	var counter int
	err = db.QueryRowContext(ctx, "SELECT value, counter FROM TestSequentialDML WHERE id = 1").Scan(&value, &counter)
	if err != nil {
		t.Fatalf("failed to query final state: %v", err)
	}
	if value != "value-99" || counter != 99 {
		t.Errorf("unexpected final state: value=%q, counter=%d", value, counter)
	}
}

// TestExplicitTransaction tests that explicit transactions work correctly.
func TestExplicitTransaction(t *testing.T) {
	skipIfShort(t)
	t.Parallel()

	ctx := context.Background()
	dsn, cleanup, err := createTestDB(ctx,
		`CREATE TABLE TestExplicitTransaction (
			id INT64 NOT NULL,
			value STRING(1024),
		) PRIMARY KEY (id)`)
	if err != nil {
		t.Fatalf("failed to create test db: %v", err)
	}
	defer cleanup()

	db, err := sql.Open("spanner", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()

	// Run multiple transactions sequentially
	for i := 0; i < 50; i++ {
		tx, err := db.BeginTx(ctx, nil)
		if err != nil {
			t.Fatalf("iteration %d: BeginTx failed: %v", i, err)
		}

		_, err = tx.ExecContext(ctx, "INSERT INTO TestExplicitTransaction (id, value) VALUES (?, ?)", i, fmt.Sprintf("value-%d", i))
		if err != nil {
			_ = tx.Rollback()
			t.Fatalf("iteration %d: INSERT failed: %v", i, err)
		}

		res, err := tx.ExecContext(ctx, "UPDATE TestExplicitTransaction SET value = ? WHERE id = ?", fmt.Sprintf("updated-%d", i), i)
		if err != nil {
			_ = tx.Rollback()
			t.Fatalf("iteration %d: UPDATE failed: %v", i, err)
		}

		affected, err := res.RowsAffected()
		if err != nil {
			_ = tx.Rollback()
			t.Fatalf("iteration %d: RowsAffected failed: %v", i, err)
		}
		if affected != 1 {
			_ = tx.Rollback()
			t.Errorf("iteration %d: affected rows mismatch. Got: %d, Want: %d", i, affected, 1)
			continue
		}

		if err := tx.Commit(); err != nil {
			t.Fatalf("iteration %d: Commit failed: %v", i, err)
		}
	}
}

// TestRapidOpenClose tests rapid connection open/close cycles to stress the connection pool.
func TestRapidOpenClose(t *testing.T) {
	skipIfShort(t)
	t.Parallel()

	ctx := context.Background()
	dsn, cleanup, err := createTestDB(ctx,
		`CREATE TABLE TestRapidOpenClose (
			id INT64 NOT NULL,
			value STRING(1024),
		) PRIMARY KEY (id)`)
	if err != nil {
		t.Fatalf("failed to create test db: %v", err)
	}
	defer cleanup()

	dsnWithMaxSessions := dsn + ";max_sessions=5"

	// Keep a single db connection pool open for the duration of the test
	db, err := sql.Open("spanner", dsnWithMaxSessions)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()

	// Seed with initial data using an explicit DML batch
	conn, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := conn.Raw(func(driverConn interface{}) error {
		return driverConn.(SpannerConn).StartBatchDML()
	}); err != nil {
		_ = conn.Close()
		t.Fatal(err)
	}
	for i := 0; i < 10; i++ {
		_, err := conn.ExecContext(ctx, "INSERT INTO TestRapidOpenClose (id, value) VALUES (?, ?)", i, fmt.Sprintf("value-%d", i))
		if err != nil {
			_ = conn.Close()
			t.Fatalf("failed to queue insert row %d: %v", i, err)
		}
	}
	if err := conn.Raw(func(driverConn interface{}) error {
		_, err := driverConn.(SpannerConn).RunDmlBatch(ctx)
		return err
	}); err != nil {
		_ = conn.Close()
		t.Fatal(err)
	}
	_ = conn.Close()

	// Now rapidly open connections from the pool, do work, and close them
	for cycle := 0; cycle < 20; cycle++ {
		func() {
			conn, err := db.Conn(ctx)
			if err != nil {
				t.Fatalf("cycle %d: failed to get connection: %v", cycle, err)
			}
			defer func() { _ = conn.Close() }()

			// Do some DML work
			for i := 0; i < 5; i++ {
				rowID := (cycle*5 + i) % 10
				res, err := conn.ExecContext(ctx, "UPDATE TestRapidOpenClose SET value = ? WHERE id = ?",
					fmt.Sprintf("cycle-%d-iter-%d", cycle, i), rowID)
				if err != nil {
					t.Fatalf("cycle %d iteration %d: ExecContext failed: %v", cycle, i, err)
				}

				res, err = conn.ExecContext(ctx, "UPDATE TestRapidOpenClose SET value = ? WHERE id = ?",
					fmt.Sprintf("cycle-%d-iter-%d", cycle, i), rowID)
				if err != nil {
					t.Fatalf("cycle %d iteration %d: ExecContext failed: %v", cycle, i, err)
				}

				affected, err := res.RowsAffected()
				if err != nil {
					t.Fatalf("cycle %d iteration %d: RowsAffected failed: %v", cycle, i, err)
				}
				if affected != 1 {
					t.Errorf("cycle %d iteration %d: affected rows mismatch. Got: %d, Want: %d", cycle, i, affected, 1)
				}
			}
		}()
	}
}
