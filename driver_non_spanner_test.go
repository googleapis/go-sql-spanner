// Copyright 2026 Google LLC
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
	"testing"

	"cloud.google.com/go/spanner"
	"google.golang.org/grpc/codes"
)

func init() {
	sql.Register("fake", &fakeDriver{})
}

type fakeDriver struct{}

func (d *fakeDriver) Open(name string) (driver.Conn, error) { return &fakeDriverConn{}, nil }

type fakeDriverConn struct{}

func (c *fakeDriverConn) Prepare(query string) (driver.Stmt, error) { return &fakeStmt{}, nil }
func (c *fakeDriverConn) Close() error                              { return nil }
func (c *fakeDriverConn) Begin() (driver.Tx, error)                 { return &fakeTx{}, nil }
func (c *fakeDriverConn) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	return &fakeTx{}, nil
}

type fakeStmt struct{}

func (s *fakeStmt) Close() error                                    { return nil }
func (s *fakeStmt) NumInput() int                                   { return 0 }
func (s *fakeStmt) Exec(args []driver.Value) (driver.Result, error) { return nil, nil }
func (s *fakeStmt) Query(args []driver.Value) (driver.Rows, error)  { return nil, nil }

type fakeTx struct{}

func (t *fakeTx) Commit() error   { return nil }
func (t *fakeTx) Rollback() error { return nil }

func TestRunTransaction_NonSpannerConnection(t *testing.T) {
	db, err := sql.Open("fake", "any-dsn")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	ctx := context.Background()
	// RunTransaction should succeed and return nil, nil commit response for non-Spanner connections
	resp, err := RunTransactionWithCommitResponse(ctx, db, nil, func(ctx context.Context, tx *sql.Tx) error {
		return nil
	}, spanner.TransactionOptions{})
	if err != nil {
		t.Fatalf("RunTransactionWithCommitResponse failed: %v", err)
	}
	if resp != nil {
		t.Errorf("expected nil commit response, got: %v", resp)
	}
}

func TestBeginTransactions_NonSpannerConnection(t *testing.T) {
	t.Parallel()

	database, err := sql.Open("fake", "any-dsn")
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()

	ctx := context.Background()
	testCases := []struct {
		name      string
		beginFunc func(ctx context.Context, db *sql.DB) (*sql.Tx, error)
	}{
		{
			name: "ReadWriteTransaction",
			beginFunc: func(ctx context.Context, db *sql.DB) (*sql.Tx, error) {
				return BeginReadWriteTransaction(ctx, db, ReadWriteTransactionOptions{})
			},
		},
		{
			name: "ReadOnlyTransaction",
			beginFunc: func(ctx context.Context, db *sql.DB) (*sql.Tx, error) {
				return BeginReadOnlyTransaction(ctx, db, ReadOnlyTransactionOptions{})
			},
		},
		{
			name: "BatchReadOnlyTransaction",
			beginFunc: func(ctx context.Context, db *sql.DB) (*sql.Tx, error) {
				return BeginBatchReadOnlyTransaction(ctx, db, BatchReadOnlyTransactionOptions{})
			},
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			transaction, err := testCase.beginFunc(ctx, database)
			if transaction != nil {
				_ = transaction.Rollback()
				t.Fatalf("expected nil transaction for non-Spanner connection, got: %v", transaction)
			}
			if err == nil {
				t.Fatal("expected error for non-Spanner connection, got nil")
			}
			if g, w := spanner.ErrCode(err), codes.FailedPrecondition; g != w {
				t.Fatalf("error code mismatch\nGot:  %v\nWant: %v", g, w)
			}
		})
	}
}
