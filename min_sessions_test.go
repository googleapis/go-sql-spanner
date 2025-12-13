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

// TestMinSessionsZeroConcurrentDML tests that concurrent DML operations work correctly
// when min_sessions=0 is set.
func TestMinSessionsZeroConcurrentDML(t *testing.T) {
	skipIfShort(t)
	t.Parallel()

	ctx := context.Background()
	dsn, cleanup, err := createTestDB(ctx,
		`CREATE TABLE TestMinSessions (
			id INT64 NOT NULL,
			value STRING(1024),
			updated_at TIMESTAMP,
		) PRIMARY KEY (id)`)
	if err != nil {
		t.Fatalf("failed to create test db: %v", err)
	}
	defer cleanup()

	// Add min_sessions=0 to the DSN to reproduce the issue
	dsnWithMinSessions := dsn + ";min_sessions=0;max_sessions=400"

	db, err := sql.Open("spanner", dsnWithMinSessions)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()

	// Insert initial rows
	for i := range 100 {
		_, err := db.ExecContext(ctx, "INSERT INTO TestMinSessions (id, value, updated_at) VALUES (?, ?, ?)",
			i, fmt.Sprintf("value-%d", i), time.Now())
		if err != nil {
			t.Fatalf("failed to insert row %d: %v", i, err)
		}
	}

	// Run concurrent UPDATE statements - this is where the issue manifests
	const concurrency = 10
	const iterations = 50

	var wg sync.WaitGroup
	errors := make(chan error, concurrency*iterations)

	for g := range concurrency {
		wg.Add(1)
		go func(goroutineID int) {
			defer wg.Done()

			for i := range iterations {
				rowID := (goroutineID*iterations + i) % 100
				timestamp := time.Now()

				// This UPDATE pattern is similar to what overlaycache.updateCheckInDirectUpdate does
				res, err := db.ExecContext(ctx, `
					UPDATE TestMinSessions
					SET
						value = ?,
						updated_at = CASE WHEN CAST(? AS bool) IS TRUE
							THEN CAST(? AS TIMESTAMP)
							ELSE TestMinSessions.updated_at
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

				// Check RowsAffected - this should work without error
				affected, err := res.RowsAffected()
				if err != nil {
					errors <- fmt.Errorf("goroutine %d iteration %d: RowsAffected failed: %w", goroutineID, i, err)
					continue
				}
				if affected != 1 {
					errors <- fmt.Errorf("goroutine %d iteration %d: expected 1 row affected, got %d", goroutineID, i, affected)
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

// TestMinSessionsZeroSequentialDML tests that sequential DML operations work correctly
// when min_sessions=0 is set. This is a simpler version of the concurrent test.
func TestMinSessionsZeroSequentialDML(t *testing.T) {
	skipIfShort(t)
	t.Parallel()

	ctx := context.Background()
	dsn, cleanup, err := createTestDB(ctx,
		`CREATE TABLE TestMinSessionsSeq (
			id INT64 NOT NULL,
			value STRING(1024),
			counter INT64,
		) PRIMARY KEY (id)`)
	if err != nil {
		t.Fatalf("failed to create test db: %v", err)
	}
	defer cleanup()

	// Add min_sessions=0 to the DSN
	dsnWithMinSessions := dsn + ";min_sessions=0"

	db, err := sql.Open("spanner", dsnWithMinSessions)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()

	// Insert a single row
	_, err = db.ExecContext(ctx, "INSERT INTO TestMinSessionsSeq (id, value, counter) VALUES (1, 'initial', 0)")
	if err != nil {
		t.Fatalf("failed to insert row: %v", err)
	}

	// Perform many sequential updates
	for i := range 100 {
		res, err := db.ExecContext(ctx, "UPDATE TestMinSessionsSeq SET value = ?, counter = ? WHERE id = 1",
			fmt.Sprintf("value-%d", i), i)
		if err != nil {
			t.Fatalf("iteration %d: ExecContext failed: %v", i, err)
		}

		affected, err := res.RowsAffected()
		if err != nil {
			t.Fatalf("iteration %d: RowsAffected failed: %v", i, err)
		}
		if affected != 1 {
			t.Fatalf("iteration %d: expected 1 row affected, got %d", i, affected)
		}
	}

	// Verify final state
	var value string
	var counter int
	err = db.QueryRowContext(ctx, "SELECT value, counter FROM TestMinSessionsSeq WHERE id = 1").Scan(&value, &counter)
	if err != nil {
		t.Fatalf("failed to query final state: %v", err)
	}
	if value != "value-99" || counter != 99 {
		t.Errorf("unexpected final state: value=%q, counter=%d", value, counter)
	}
}

// TestMinSessionsZeroWithExplicitTransaction tests that explicit transactions
// work correctly with min_sessions=0.
func TestMinSessionsZeroWithExplicitTransaction(t *testing.T) {
	skipIfShort(t)
	t.Parallel()

	ctx := context.Background()
	dsn, cleanup, err := createTestDB(ctx,
		`CREATE TABLE TestMinSessionsTx (
			id INT64 NOT NULL,
			value STRING(1024),
		) PRIMARY KEY (id)`)
	if err != nil {
		t.Fatalf("failed to create test db: %v", err)
	}
	defer cleanup()

	// Add min_sessions=0 to the DSN
	dsnWithMinSessions := dsn + ";min_sessions=0"

	db, err := sql.Open("spanner", dsnWithMinSessions)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()

	// Run multiple transactions sequentially
	for i := range 50 {
		tx, err := db.BeginTx(ctx, nil)
		if err != nil {
			t.Fatalf("iteration %d: BeginTx failed: %v", i, err)
		}

		_, err = tx.ExecContext(ctx, "INSERT INTO TestMinSessionsTx (id, value) VALUES (?, ?)", i, fmt.Sprintf("value-%d", i))
		if err != nil {
			_ = tx.Rollback()
			t.Fatalf("iteration %d: INSERT failed: %v", i, err)
		}

		res, err := tx.ExecContext(ctx, "UPDATE TestMinSessionsTx SET value = ? WHERE id = ?", fmt.Sprintf("updated-%d", i), i)
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
			t.Fatalf("iteration %d: expected 1 row affected, got %d", i, affected)
		}

		if err := tx.Commit(); err != nil {
			t.Fatalf("iteration %d: Commit failed: %v", i, err)
		}
	}
}

// TestMinSessionsZeroRapidOpenClose tests rapid connection open/close cycles
// with min_sessions=0 to stress the session pool.
func TestMinSessionsZeroRapidOpenClose(t *testing.T) {
	skipIfShort(t)
	t.Parallel()

	ctx := context.Background()
	dsn, cleanup, err := createTestDB(ctx,
		`CREATE TABLE TestMinSessionsRapid (
			id INT64 NOT NULL,
			value STRING(1024),
		) PRIMARY KEY (id)`)
	if err != nil {
		t.Fatalf("failed to create test db: %v", err)
	}
	defer cleanup()

	// Add min_sessions=0 to the DSN
	dsnWithMinSessions := dsn + ";min_sessions=0;max_sessions=5"

	// Seed with initial data using a separate connection
	db, err := sql.Open("spanner", dsnWithMinSessions)
	if err != nil {
		t.Fatal(err)
	}
	for i := range 10 {
		_, err := db.ExecContext(ctx, "INSERT INTO TestMinSessionsRapid (id, value) VALUES (?, ?)", i, fmt.Sprintf("value-%d", i))
		if err != nil {
			_ = db.Close()
			t.Fatalf("failed to insert row %d: %v", i, err)
		}
	}
	_ = db.Close()

	// Now rapidly open connections, do work, and close them
	for cycle := range 20 {
		func() {
			db, err := sql.Open("spanner", dsnWithMinSessions)
			if err != nil {
				t.Fatalf("cycle %d: failed to open db: %v", cycle, err)
			}
			defer func() { _ = db.Close() }()

			// Do some DML work
			for i := range 5 {
				rowID := (cycle*5 + i) % 10
				res, err := db.ExecContext(ctx, "UPDATE TestMinSessionsRapid SET value = ? WHERE id = ?",
					fmt.Sprintf("cycle-%d-iter-%d", cycle, i), rowID)
				if err != nil {
					t.Fatalf("cycle %d iteration %d: ExecContext failed: %v", cycle, i, err)
				}

				affected, err := res.RowsAffected()
				if err != nil {
					t.Fatalf("cycle %d iteration %d: RowsAffected failed: %v", cycle, i, err)
				}
				if affected != 1 {
					t.Fatalf("cycle %d iteration %d: expected 1 row affected, got %d", cycle, i, affected)
				}
			}
		}()
	}
}
