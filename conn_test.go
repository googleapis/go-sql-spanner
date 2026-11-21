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

	"cloud.google.com/go/spanner/admin/database/apiv1/databasepb"
	"github.com/google/go-cmp/cmp"
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

func TestDatabaseDialectMethod(t *testing.T) {
	t.Parallel()

	pGoogleSQL, _ := parser.NewStatementParser(databasepb.DatabaseDialect_GOOGLE_STANDARD_SQL, 0)
	cGoogleSQL := &conn{parser: pGoogleSQL}
	if g, w := cGoogleSQL.DatabaseDialect(), databasepb.DatabaseDialect_GOOGLE_STANDARD_SQL; g != w {
		t.Errorf("GoogleSQL DatabaseDialect mismatch\nGot:  %v\nWant: %v", g, w)
	}

	pPostgreSQL, _ := parser.NewStatementParser(databasepb.DatabaseDialect_POSTGRESQL, 0)
	cPostgreSQL := &conn{parser: pPostgreSQL}
	if g, w := cPostgreSQL.DatabaseDialect(), databasepb.DatabaseDialect_POSTGRESQL; g != w {
		t.Errorf("PostgreSQL DatabaseDialect mismatch\nGot:  %v\nWant: %v", g, w)
	}

	cNil := &conn{}
	if g, w := cNil.DatabaseDialect(), databasepb.DatabaseDialect_DATABASE_DIALECT_UNSPECIFIED; g != w {
		t.Errorf("nil parser DatabaseDialect mismatch\nGot:  %v\nWant: %v", g, w)
	}
}

func TestRegisterTransactionCloseFunc(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name               string
		hasTransaction     bool
		closeFunctions     []func(record func(string))
		triggerResult      txResult
		wantExecutionOrder []string
		wantResult         txResult
	}{
		{
			name:           "NilTransaction",
			hasTransaction: false,
			closeFunctions: []func(record func(string)){
				func(record func(string)) {},
			},
			wantExecutionOrder: nil,
		},
		{
			name:           "NilCloseFunction",
			hasTransaction: true,
			closeFunctions: []func(record func(string)){
				nil,
			},
			triggerResult:      txResultCommit,
			wantExecutionOrder: []string{"original"},
			wantResult:         txResultCommit,
		},
		{
			name:           "SingleRegistration",
			hasTransaction: true,
			closeFunctions: []func(record func(string)){
				func(record func(string)) { record("registered") },
			},
			triggerResult:      txResultCommit,
			wantExecutionOrder: []string{"original", "registered"},
			wantResult:         txResultCommit,
		},
		{
			name:           "ChainedRegistration",
			hasTransaction: true,
			closeFunctions: []func(record func(string)){
				func(record func(string)) { record("first") },
				func(record func(string)) { record("second") },
			},
			triggerResult:      txResultRollback,
			wantExecutionOrder: []string{"original", "first", "second"},
			wantResult:         txResultRollback,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			var executionOrder []string
			var gotResult txResult

			connection := &conn{}
			if testCase.hasTransaction {
				connection.tx = &delegatingTransaction{
					close: func(result txResult) {
						executionOrder = append(executionOrder, "original")
						gotResult = result
					},
				}
			}

			for _, closeFunction := range testCase.closeFunctions {
				if closeFunction == nil {
					connection.registerTransactionCloseFunc(nil)
				} else {
					callback := closeFunction
					connection.registerTransactionCloseFunc(func() {
						callback(func(step string) {
							executionOrder = append(executionOrder, step)
						})
					})
				}
			}

			if connection.tx != nil && connection.tx.close != nil {
				connection.tx.close(testCase.triggerResult)
			}

			if !testCase.hasTransaction {
				if g, w := connection.tx, (*delegatingTransaction)(nil); g != w {
					t.Fatalf("connection.tx mismatch\nGot:  %v\nWant: %v", g, w)
				}
			}

			if diff := cmp.Diff(testCase.wantExecutionOrder, executionOrder); diff != "" {
				t.Errorf("execution order mismatch (-want +got):\n%s", diff)
			}
			if g, w := gotResult, testCase.wantResult; g != w {
				t.Errorf("result mismatch\nGot:  %v\nWant: %v", g, w)
			}
		})
	}
}

func TestConn_BeginTransactions_Error(t *testing.T) {
	t.Parallel()

	canceledContext, cancel := context.WithCancel(context.Background())
	cancel()

	testCases := []struct {
		name      string
		beginFunc func(connection *conn) (driver.Tx, error)
	}{
		{
			name: "ReadOnlyTransaction",
			beginFunc: func(connection *conn) (driver.Tx, error) {
				return connection.BeginReadOnlyTransaction(canceledContext, &ReadOnlyTransactionOptions{}, nil)
			},
		},
		{
			name: "ReadWriteTransaction",
			beginFunc: func(connection *conn) (driver.Tx, error) {
				return connection.BeginReadWriteTransaction(canceledContext, &ReadWriteTransactionOptions{}, nil)
			},
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			connection := &conn{}
			transaction, err := testCase.beginFunc(connection)
			if transaction != nil {
				_ = transaction.Rollback()
				t.Fatalf("expected nil transaction, got: %v", transaction)
			}
			if g, w := errors.Is(err, context.Canceled), true; g != w {
				t.Fatalf("errors.Is(err, context.Canceled) mismatch\nGot:  %v\nWant: %v", g, w)
			}
		})
	}
}
