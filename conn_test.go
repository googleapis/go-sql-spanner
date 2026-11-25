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
	"errors"
	"testing"

	"cloud.google.com/go/spanner"
	"cloud.google.com/go/spanner/admin/database/apiv1/databasepb"
	"github.com/googleapis/go-sql-spanner/connectionstate"
	"github.com/googleapis/go-sql-spanner/parser"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestApplyStatementScopedValues(t *testing.T) {
	t.Parallel()

	c := &conn{
		logger: noopLogger,
		state:  createInitialConnectionState(connectionstate.TypeNonTransactional, map[string]connectionstate.ConnectionPropertyValue{}),
	}
	if g, w := propertyIsolationLevel.GetValueOrDefault(c.state), sql.LevelDefault; g != w {
		t.Fatalf("default isolation level mismatch\n Got: %v\nWant: %v", g, w)
	}

	// Add a statement-scoped connection property value for isolation_level.
	cleanup, err := c.applyStatementScopedValues(&ExecOptions{
		PropertyValues: []PropertyValue{
			CreatePropertyValue("isolation_level", "repeatable read"),
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if g, w := propertyIsolationLevel.GetValueOrDefault(c.state), sql.LevelRepeatableRead; g != w {
		t.Fatalf("statement isolation level mismatch\n Got: %v\nWant: %v", g, w)
	}
	// Clean up the statement-scoped connection properties.
	cleanup()

	// The isolation level should now be back to default.
	if g, w := propertyIsolationLevel.GetValueOrDefault(c.state), sql.LevelDefault; g != w {
		t.Fatalf("default isolation level mismatch\n Got: %v\nWant: %v", g, w)
	}
}

func TestApplyStatementScopedValuesWithInvalidValue(t *testing.T) {
	t.Parallel()

	c := &conn{
		logger: noopLogger,
		state:  createInitialConnectionState(connectionstate.TypeNonTransactional, map[string]connectionstate.ConnectionPropertyValue{}),
	}

	// Add a statement-scoped connection property value for isolation_level with an invalid value.
	_, err := c.applyStatementScopedValues(&ExecOptions{
		PropertyValues: []PropertyValue{
			CreatePropertyValue("isolation_level", "not an isolation level"),
		},
	})
	if g, w := status.Code(err), codes.InvalidArgument; g != w {
		t.Fatalf("error mismatch\n Got: %v\nWant: %v", g, w)
	}
	if g, w := propertyIsolationLevel.GetValueOrDefault(c.state), sql.LevelDefault; g != w {
		t.Fatalf("statement isolation level mismatch\n Got: %v\nWant: %v", g, w)
	}
}

func TestApplyStatementScopedValuesWithUnknownProperty(t *testing.T) {
	t.Parallel()

	c := &conn{
		logger: noopLogger,
		state:  createInitialConnectionState(connectionstate.TypeNonTransactional, map[string]connectionstate.ConnectionPropertyValue{}),
	}

	// Add a statement-scoped connection property value for an unknown property without an extension.
	_, err := c.applyStatementScopedValues(&ExecOptions{
		PropertyValues: []PropertyValue{
			CreatePropertyValue("non_existing_property", "some-value"),
		},
	})
	if g, w := status.Code(err), codes.InvalidArgument; g != w {
		t.Fatalf("error mismatch\n Got: %v\nWant: %v", g, w)
	}
	if g, w := propertyIsolationLevel.GetValueOrDefault(c.state), sql.LevelDefault; g != w {
		t.Fatalf("statement isolation level mismatch\n Got: %v\nWant: %v", g, w)
	}
}

func TestApplyStatementScopedValuesWithExtension(t *testing.T) {
	t.Parallel()

	c := &conn{
		logger: noopLogger,
		state:  createInitialConnectionState(connectionstate.TypeNonTransactional, map[string]connectionstate.ConnectionPropertyValue{}),
	}

	// Add a statement-scoped connection property value for an unknown property with an extension.
	// Property values for unknown properties with an extension are allowed.
	propValue := PropertyValue{
		Identifier: parser.Identifier{Parts: []string{"my_extension", "my_property"}},
		Value:      "some-value",
	}
	cleanup, err := c.applyStatementScopedValues(&ExecOptions{
		PropertyValues: []PropertyValue{propValue},
	})
	if g, w := status.Code(err), codes.OK; g != w {
		t.Fatalf("error mismatch\n Got: %v\nWant: %v", g, w)
	}
	val, ok, err := c.state.GetValue("my_extension", "my_property")
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("missing my_extension.my_property")
	}
	if g, w := val, "some-value"; g != w {
		t.Fatalf("value mismatch\n Got: %v\nWant: %v", g, w)
	}
	cleanup()

	// The value should now be gone.
	_, ok, err = c.state.GetValue("my_extension", "my_property")
	if g, w := status.Code(err), codes.InvalidArgument; g != w {
		t.Fatalf("error mismatch\n Got: %v\nWant: %v", g, w)
	}
	if ok {
		t.Fatal("got unexpected value for my_extension.my_property")
	}
}

func TestInErrorTxBehavior_Dialects(t *testing.T) {
	t.Parallel()

	t.Run("GoogleSQL_Default", func(t *testing.T) {
		c := &conn{
			logger: noopLogger,
			state:  createInitialConnectionStateWithDialect(databasepb.DatabaseDialect_GOOGLE_STANDARD_SQL, connectionstate.TypeNonTransactional, nil),
		}
		if g, w := c.InErrorTxBehavior(), connectionstate.InErrorTxBehaviorAllowCommands; g != w {
			t.Errorf("GoogleSQL default in_error_tx_behavior mismatch\nGot:  %v\nWant: %v", g, w)
		}
	})

	t.Run("PostgreSQL_Default", func(t *testing.T) {
		c := &conn{
			logger: noopLogger,
			state:  createInitialConnectionStateWithDialect(databasepb.DatabaseDialect_POSTGRESQL, connectionstate.TypeTransactional, nil),
		}
		if g, w := c.InErrorTxBehavior(), connectionstate.InErrorTxBehaviorAllowCommands; g != w {
			t.Errorf("PostgreSQL default in_error_tx_behavior mismatch\nGot:  %v\nWant: %v", g, w)
		}
	})

	t.Run("SetInErrorTxBehavior", func(t *testing.T) {
		c := &conn{
			logger: noopLogger,
			state:  createInitialConnectionStateWithDialect(databasepb.DatabaseDialect_POSTGRESQL, connectionstate.TypeTransactional, nil),
		}
		if err := c.SetInErrorTxBehavior(connectionstate.InErrorTxBehaviorAllowCommands); err != nil {
			t.Fatalf("SetInErrorTxBehavior failed: %v", err)
		}
		if g, w := c.InErrorTxBehavior(), connectionstate.InErrorTxBehaviorAllowCommands; g != w {
			t.Errorf("updated in_error_tx_behavior mismatch\nGot:  %v\nWant: %v", g, w)
		}
	})
}

func TestDatabaseDialect(t *testing.T) {
	t.Parallel()

	pGoogleSQL, err := parser.NewStatementParser(databasepb.DatabaseDialect_GOOGLE_STANDARD_SQL, 0)
	if err != nil {
		t.Fatalf("failed to create GoogleSQL parser: %v", err)
	}
	pPostgreSQL, err := parser.NewStatementParser(databasepb.DatabaseDialect_POSTGRESQL, 0)
	if err != nil {
		t.Fatalf("failed to create PostgreSQL parser: %v", err)
	}

	stateGoogleSQL := createInitialConnectionStateWithDialect(databasepb.DatabaseDialect_GOOGLE_STANDARD_SQL, connectionstate.TypeNonTransactional, nil)
	statePostgreSQL := createInitialConnectionStateWithDialect(databasepb.DatabaseDialect_POSTGRESQL, connectionstate.TypeTransactional, nil)
	stateUnspecified := createConfiguredConnectionState(nil)

	tests := []struct {
		name        string
		conn        *conn
		wantDialect databasepb.DatabaseDialect
		wantIsPG    bool
	}{
		{
			name:        "NilConn",
			conn:        nil,
			wantDialect: databasepb.DatabaseDialect_DATABASE_DIALECT_UNSPECIFIED,
			wantIsPG:    false,
		},
		{
			name:        "EmptyConn",
			conn:        &conn{},
			wantDialect: databasepb.DatabaseDialect_DATABASE_DIALECT_UNSPECIFIED,
			wantIsPG:    false,
		},
		{
			name: "State_GoogleSQL_NoParser",
			conn: &conn{
				state: stateGoogleSQL,
			},
			wantDialect: databasepb.DatabaseDialect_GOOGLE_STANDARD_SQL,
			wantIsPG:    false,
		},
		{
			name: "State_PostgreSQL_NoParser",
			conn: &conn{
				state: statePostgreSQL,
			},
			wantDialect: databasepb.DatabaseDialect_POSTGRESQL,
			wantIsPG:    true,
		},
		{
			name: "Parser_GoogleSQL_NoState",
			conn: &conn{
				parser: pGoogleSQL,
			},
			wantDialect: databasepb.DatabaseDialect_GOOGLE_STANDARD_SQL,
			wantIsPG:    false,
		},
		{
			name: "Parser_PostgreSQL_NoState",
			conn: &conn{
				parser: pPostgreSQL,
			},
			wantDialect: databasepb.DatabaseDialect_POSTGRESQL,
			wantIsPG:    true,
		},
		{
			name: "Precedence_StatePostgreSQL_Overrides_ParserGoogleSQL",
			conn: &conn{
				state:  statePostgreSQL,
				parser: pGoogleSQL,
			},
			wantDialect: databasepb.DatabaseDialect_POSTGRESQL,
			wantIsPG:    true,
		},
		{
			name: "Precedence_StateGoogleSQL_Overrides_ParserPostgreSQL",
			conn: &conn{
				state:  stateGoogleSQL,
				parser: pPostgreSQL,
			},
			wantDialect: databasepb.DatabaseDialect_GOOGLE_STANDARD_SQL,
			wantIsPG:    false,
		},
		{
			name: "Fallback_StateUnspecified_FallsBackToParserPostgreSQL",
			conn: &conn{
				state:  stateUnspecified,
				parser: pPostgreSQL,
			},
			wantDialect: databasepb.DatabaseDialect_POSTGRESQL,
			wantIsPG:    true,
		},
		{
			name: "Fallback_StateUnspecified_FallsBackToParserGoogleSQL",
			conn: &conn{
				state:  stateUnspecified,
				parser: pGoogleSQL,
			},
			wantDialect: databasepb.DatabaseDialect_GOOGLE_STANDARD_SQL,
			wantIsPG:    false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if g, w := tc.conn.DatabaseDialect(), tc.wantDialect; g != w {
				t.Errorf("DatabaseDialect mismatch\nGot:  %v\nWant: %v", g, w)
			}
			if g, w := tc.conn.isPostgreSQL(), tc.wantIsPG; g != w {
				t.Errorf("isPostgreSQL mismatch\nGot:  %v\nWant: %v", g, w)
			}
		})
	}
}

func TestConn_BeginTransactions_Error(t *testing.T) {
	t.Parallel()

	canceledContext, cancel := context.WithCancel(context.Background())
	cancel()

	testCases := []struct {
		name         string
		dialect      databasepb.DatabaseDialect
		activeTx     bool
		activeBatch  bool
		resetRetry   bool
		useCanceled  bool
		beginFunc    func(connection *conn, ctx context.Context) (driver.Tx, error)
		wantErr      error
		wantErrCode  codes.Code
		wantSQLState string
	}{
		{
			name:        "ReadOnlyTransaction_GoogleSQL_Canceled",
			dialect:     databasepb.DatabaseDialect_GOOGLE_STANDARD_SQL,
			useCanceled: true,
			beginFunc: func(connection *conn, ctx context.Context) (driver.Tx, error) {
				return connection.BeginReadOnlyTransaction(ctx, &ReadOnlyTransactionOptions{}, nil)
			},
			wantErr:     context.Canceled,
			wantErrCode: codes.Unknown,
		},
		{
			name:        "ReadOnlyTransaction_PostgreSQL_Canceled",
			dialect:     databasepb.DatabaseDialect_POSTGRESQL,
			useCanceled: true,
			beginFunc: func(connection *conn, ctx context.Context) (driver.Tx, error) {
				return connection.BeginReadOnlyTransaction(ctx, &ReadOnlyTransactionOptions{}, nil)
			},
			wantErr:      context.Canceled,
			wantErrCode:  codes.Unknown,
			wantSQLState: "XX000",
		},
		{
			name:        "ReadWriteTransaction_GoogleSQL_Canceled",
			dialect:     databasepb.DatabaseDialect_GOOGLE_STANDARD_SQL,
			useCanceled: true,
			beginFunc: func(connection *conn, ctx context.Context) (driver.Tx, error) {
				return connection.BeginReadWriteTransaction(ctx, &ReadWriteTransactionOptions{}, nil)
			},
			wantErr:     context.Canceled,
			wantErrCode: codes.Unknown,
		},
		{
			name:        "ReadWriteTransaction_PostgreSQL_Canceled",
			dialect:     databasepb.DatabaseDialect_POSTGRESQL,
			useCanceled: true,
			beginFunc: func(connection *conn, ctx context.Context) (driver.Tx, error) {
				return connection.BeginReadWriteTransaction(ctx, &ReadWriteTransactionOptions{}, nil)
			},
			wantErr:      context.Canceled,
			wantErrCode:  codes.Unknown,
			wantSQLState: "XX000",
		},
		{
			name:     "ReadOnlyTransaction_GoogleSQL_AlreadyInTx",
			dialect:  databasepb.DatabaseDialect_GOOGLE_STANDARD_SQL,
			activeTx: true,
			beginFunc: func(connection *conn, ctx context.Context) (driver.Tx, error) {
				return connection.BeginReadOnlyTransaction(ctx, &ReadOnlyTransactionOptions{}, nil)
			},
			wantErrCode: codes.FailedPrecondition,
		},
		{
			name:     "ReadOnlyTransaction_PostgreSQL_AlreadyInTx",
			dialect:  databasepb.DatabaseDialect_POSTGRESQL,
			activeTx: true,
			beginFunc: func(connection *conn, ctx context.Context) (driver.Tx, error) {
				return connection.BeginReadOnlyTransaction(ctx, &ReadOnlyTransactionOptions{}, nil)
			},
			wantErrCode:  codes.FailedPrecondition,
			wantSQLState: "XX000",
		},
		{
			name:     "ReadWriteTransaction_GoogleSQL_AlreadyInTx",
			dialect:  databasepb.DatabaseDialect_GOOGLE_STANDARD_SQL,
			activeTx: true,
			beginFunc: func(connection *conn, ctx context.Context) (driver.Tx, error) {
				return connection.BeginReadWriteTransaction(ctx, &ReadWriteTransactionOptions{}, nil)
			},
			wantErrCode: codes.FailedPrecondition,
		},
		{
			name:     "ReadWriteTransaction_PostgreSQL_AlreadyInTx",
			dialect:  databasepb.DatabaseDialect_POSTGRESQL,
			activeTx: true,
			beginFunc: func(connection *conn, ctx context.Context) (driver.Tx, error) {
				return connection.BeginReadWriteTransaction(ctx, &ReadWriteTransactionOptions{}, nil)
			},
			wantErrCode:  codes.FailedPrecondition,
			wantSQLState: "XX000",
		},
		{
			name:        "ReadOnlyTransaction_GoogleSQL_InBatch",
			dialect:     databasepb.DatabaseDialect_GOOGLE_STANDARD_SQL,
			activeBatch: true,
			beginFunc: func(connection *conn, ctx context.Context) (driver.Tx, error) {
				return connection.BeginReadOnlyTransaction(ctx, &ReadOnlyTransactionOptions{}, nil)
			},
			wantErrCode: codes.FailedPrecondition,
		},
		{
			name:        "ReadWriteTransaction_PostgreSQL_InBatch",
			dialect:     databasepb.DatabaseDialect_POSTGRESQL,
			activeBatch: true,
			beginFunc: func(connection *conn, ctx context.Context) (driver.Tx, error) {
				return connection.BeginReadWriteTransaction(ctx, &ReadWriteTransactionOptions{}, nil)
			},
			wantErrCode:  codes.FailedPrecondition,
			wantSQLState: "XX000",
		},
		{
			name:       "ReadOnlyTransaction_GoogleSQL_ResetRetryWithoutTx",
			dialect:    databasepb.DatabaseDialect_GOOGLE_STANDARD_SQL,
			resetRetry: true,
			beginFunc: func(connection *conn, ctx context.Context) (driver.Tx, error) {
				return connection.BeginReadOnlyTransaction(ctx, &ReadOnlyTransactionOptions{}, nil)
			},
			wantErrCode: codes.FailedPrecondition,
		},
		{
			name:       "ReadOnlyTransaction_PostgreSQL_ResetRetryWithoutTx",
			dialect:    databasepb.DatabaseDialect_POSTGRESQL,
			resetRetry: true,
			beginFunc: func(connection *conn, ctx context.Context) (driver.Tx, error) {
				return connection.BeginReadOnlyTransaction(ctx, &ReadOnlyTransactionOptions{}, nil)
			},
			wantErrCode:  codes.FailedPrecondition,
			wantSQLState: "XX000",
		},
		{
			name:       "ReadWriteTransaction_GoogleSQL_ResetRetryWithoutTx",
			dialect:    databasepb.DatabaseDialect_GOOGLE_STANDARD_SQL,
			resetRetry: true,
			beginFunc: func(connection *conn, ctx context.Context) (driver.Tx, error) {
				return connection.BeginReadWriteTransaction(ctx, &ReadWriteTransactionOptions{}, nil)
			},
			wantErrCode: codes.FailedPrecondition,
		},
		{
			name:       "ReadWriteTransaction_PostgreSQL_ResetRetryWithoutTx",
			dialect:    databasepb.DatabaseDialect_POSTGRESQL,
			resetRetry: true,
			beginFunc: func(connection *conn, ctx context.Context) (driver.Tx, error) {
				return connection.BeginReadWriteTransaction(ctx, &ReadWriteTransactionOptions{}, nil)
			},
			wantErrCode:  codes.FailedPrecondition,
			wantSQLState: "XX000",
		},
		{
			name:    "ReadOnlyTransaction_GoogleSQL_NilOptions",
			dialect: databasepb.DatabaseDialect_GOOGLE_STANDARD_SQL,
			beginFunc: func(connection *conn, ctx context.Context) (driver.Tx, error) {
				return connection.BeginReadOnlyTransaction(ctx, nil, nil)
			},
		},
		{
			name:    "ReadOnlyTransaction_PostgreSQL_NilOptions",
			dialect: databasepb.DatabaseDialect_POSTGRESQL,
			beginFunc: func(connection *conn, ctx context.Context) (driver.Tx, error) {
				return connection.BeginReadOnlyTransaction(ctx, nil, nil)
			},
		},
		{
			name:    "ReadWriteTransaction_GoogleSQL_NilOptions",
			dialect: databasepb.DatabaseDialect_GOOGLE_STANDARD_SQL,
			beginFunc: func(connection *conn, ctx context.Context) (driver.Tx, error) {
				return connection.BeginReadWriteTransaction(ctx, nil, nil)
			},
		},
		{
			name:    "ReadWriteTransaction_PostgreSQL_NilOptions",
			dialect: databasepb.DatabaseDialect_POSTGRESQL,
			beginFunc: func(connection *conn, ctx context.Context) (driver.Tx, error) {
				return connection.BeginReadWriteTransaction(ctx, nil, nil)
			},
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			statementParser, err := parser.NewStatementParser(testCase.dialect, 0)
			if err != nil {
				t.Fatalf("failed to create parser: %v", err)
			}
			connection := &conn{
				parser:        statementParser,
				state:         createInitialConnectionState(connectionstate.TypeTransactional, nil),
				resetForRetry: testCase.resetRetry,
			}
			if testCase.activeTx {
				connection.tx = &delegatingTransaction{}
			}
			if testCase.activeBatch {
				connection.batch = &batch{}
			}
			ctx := context.Background()
			if testCase.useCanceled {
				ctx = canceledContext
			}
			transaction, err := testCase.beginFunc(connection, ctx)
			if testCase.wantErr == nil && testCase.wantErrCode == codes.OK {
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				if transaction == nil {
					t.Fatal("expected non-nil transaction")
				}
				if err := transaction.Rollback(); err != nil {
					t.Fatalf("unexpected rollback error: %v", err)
				}
				return
			}
			if transaction != nil {
				_ = transaction.Rollback()
				t.Fatalf("expected nil transaction, got: %v", transaction)
			}
			if err == nil {
				t.Fatal("expected error, got nil")
			}
			if testCase.wantErr != nil {
				if g, w := errors.Is(err, testCase.wantErr), true; g != w {
					t.Fatalf("errors.Is(err, %v) mismatch\nGot:  %v\nWant: %v", testCase.wantErr, g, w)
				}
			}
			if g, w := spanner.ErrCode(err), testCase.wantErrCode; g != w {
				t.Fatalf("error code mismatch\nGot:  %v\nWant: %v", g, w)
			}
			var pgErr *pgError
			if testCase.wantSQLState != "" {
				if !errors.As(err, &pgErr) {
					t.Fatalf("expected *pgError, got %T (%v)", err, err)
				}
				if g, w := pgErr.code, testCase.wantSQLState; g != w {
					t.Errorf("SQLSTATE mismatch\nGot:  %v\nWant: %v", g, w)
				}
			} else if errors.As(err, &pgErr) {
				t.Errorf("expected non-pgError for GoogleSQL, got %T (%v)", err, err)
			}
		})
	}
}

func TestConn_ResetSession_ClearsResetForRetry(t *testing.T) {
	t.Parallel()

	connection := &conn{
		state:         createInitialConnectionState(connectionstate.TypeTransactional, nil),
		resetForRetry: true,
	}
	if err := connection.ResetSession(context.Background()); err != nil {
		t.Fatalf("ResetSession failed: %v", err)
	}
	if g, w := connection.resetForRetry, false; g != w {
		t.Errorf("resetForRetry mismatch after ResetSession\nGot:  %v\nWant: %v", g, w)
	}
	// Ensure nil options helpers remain safe to invoke.
	connection.setReadWriteTransactionOptions(nil)
	connection.setReadOnlyTransactionOptions(nil)
	connection.setBatchReadOnlyTransactionOptions(nil)
}

func TestConn_BeginTransactions_CloseCallback(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name       string
		readOnly   bool
		commitTx   bool
		nilClose   bool
		wantCalled int
	}{
		{
			name:       "ReadOnly_Commit",
			readOnly:   true,
			commitTx:   true,
			wantCalled: 1,
		},
		{
			name:       "ReadOnly_Rollback",
			readOnly:   true,
			commitTx:   false,
			wantCalled: 1,
		},
		{
			name:       "ReadWrite_Commit",
			readOnly:   false,
			commitTx:   true,
			wantCalled: 1,
		},
		{
			name:       "ReadWrite_Rollback",
			readOnly:   false,
			commitTx:   false,
			wantCalled: 1,
		},
		{
			name:       "ReadOnly_NilClose",
			readOnly:   true,
			commitTx:   true,
			nilClose:   true,
			wantCalled: 0,
		},
		{
			name:       "ReadWrite_NilClose",
			readOnly:   false,
			commitTx:   false,
			nilClose:   true,
			wantCalled: 0,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			connection := &conn{
				state: createInitialConnectionState(connectionstate.TypeTransactional, nil),
			}
			closeCalls := 0
			var closeFn func()
			if !testCase.nilClose {
				closeFn = func() {
					closeCalls++
				}
			}

			var tx driver.Tx
			var err error
			if testCase.readOnly {
				tx, err = connection.BeginReadOnlyTransaction(context.Background(), &ReadOnlyTransactionOptions{}, closeFn)
			} else {
				tx, err = connection.BeginReadWriteTransaction(context.Background(), &ReadWriteTransactionOptions{}, closeFn)
			}
			if err != nil {
				t.Fatalf("failed to begin transaction: %v", err)
			}
			if testCase.commitTx {
				if err := tx.Commit(); err != nil {
					t.Fatalf("commit failed: %v", err)
				}
			} else {
				if err := tx.Rollback(); err != nil {
					t.Fatalf("rollback failed: %v", err)
				}
			}
			if g, w := closeCalls, testCase.wantCalled; g != w {
				t.Fatalf("close callback count mismatch\nGot:  %v\nWant: %v", g, w)
			}

			// A subsequent BeginTx on the same connection must not reuse the previous close callback.
			nextTx, err := connection.BeginTx(context.Background(), driver.TxOptions{ReadOnly: testCase.readOnly})
			if err != nil {
				t.Fatalf("subsequent BeginTx failed: %v", err)
			}
			if err := nextTx.Commit(); err != nil {
				t.Fatalf("subsequent Commit failed: %v", err)
			}
			if g, w := closeCalls, testCase.wantCalled; g != w {
				t.Fatalf("close callback count changed on subsequent BeginTx\nGot:  %v\nWant: %v", g, w)
			}
		})
	}
}
