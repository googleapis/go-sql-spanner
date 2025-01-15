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
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/googleapis/go-sql-spanner/testutil"
)

func TestPrepareQuery(t *testing.T) {
	t.Parallel()

	db, server, teardown := setupTestDBConnection(t)
	defer teardown()
	ctx := context.Background()

	query := "select value from test where id=?"
	if err := server.TestSpanner.PutStatementResult(
		strings.Replace(query, "?", "@p1", 1),
		&testutil.StatementResult{
			Type:      testutil.StatementResultResultSet,
			ResultSet: testutil.CreateSelect1ResultSet(),
		},
	); err != nil {
		t.Fatal(err)
	}

	stmt, err := db.PrepareContext(ctx, query)
	if err != nil {
		t.Fatalf("failed to prepare query: %v", err)
	}

	rows, err := stmt.QueryContext(ctx, 1)
	if err != nil {
		t.Fatalf("failed to execute query: %v", err)
	}
	defer rows.Close()

	var values []int64
	var value int64
	for rows.Next() {
		if err := rows.Scan(&value); err != nil {
			t.Fatalf("failed to scan row value: %v", err)
		}
		values = append(values, value)
	}
	if g, w := values, []int64{1}; !cmp.Equal(g, w) {
		t.Fatalf("returned values mismatch\n Got: %v\nWant: %v", g, w)
	}
}

func TestPrepareDml(t *testing.T) {
	t.Parallel()

	db, server, teardown := setupTestDBConnection(t)
	defer teardown()
	ctx := context.Background()

	query := "insert into test (id, value) values (?, ?)"
	if err := server.TestSpanner.PutStatementResult(
		"insert into test (id, value) values (@p1, @p2)",
		&testutil.StatementResult{
			Type:        testutil.StatementResultUpdateCount,
			UpdateCount: 1,
		},
	); err != nil {
		t.Fatal(err)
	}

	stmt, err := db.PrepareContext(ctx, query)
	if err != nil {
		t.Fatalf("failed to prepare dml: %v", err)
	}

	res, err := stmt.ExecContext(ctx, 1, "One")
	if err != nil {
		t.Fatalf("failed to execute dml: %v", err)
	}
	if rowsAffected, err := res.RowsAffected(); err != nil {
		t.Fatalf("failed to get rows affected: %v", err)
	} else if g, w := rowsAffected, int64(1); g != w {
		t.Fatalf("rows affected mismatch\n Got: %v\nWant: %v", g, w)
	}
}
