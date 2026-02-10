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
	"fmt"
	"testing"
	"time"

	"cloud.google.com/go/longrunning/autogen/longrunningpb"
	"cloud.google.com/go/spanner/admin/database/apiv1/databasepb"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
)

func TestDDLExecutionModeAsync(t *testing.T) {
	t.Parallel()

	db, server, teardown := setupTestDBConnectionWithParams(t, "ddl_execution_mode=ASYNC")
	defer teardown()
	ctx := context.Background()

	// Mock an operation that is NOT done.
	opName := "projects/p/instances/i/databases/d/operations/op1"
	server.TestDatabaseAdmin.SetResps([]proto.Message{
		&longrunningpb.Operation{
			Name: opName,
			Done: false, // Operation is not done
		},
	})

	c, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer silentClose(c)

	start := time.Now()
	// Execute DDL via Raw to verify driver.Result behavior (since sql.Result wraps it).
	err = executeDDLAndVerifyOpID(ctx, c, opName)
	if err != nil {
		t.Fatal(err)
	}
	if time.Since(start) > 1*time.Second {
		t.Fatalf("ExecContext took too long for ASYNC mode")
	}

	// Double check that the internal operation ID is also set (legacy check)
	err = c.Raw(func(driverConn interface{}) error {
		sc, ok := driverConn.(*conn)
		if !ok {
			return fmt.Errorf("expected *conn, got %T", driverConn)
		}
		if sc.lastDDLOperationID != opName {
			return fmt.Errorf("expected operation ID %q, got %q", opName, sc.lastDDLOperationID)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

  verifyConnectionPropertyValue(t, c, "ddl_execution_mode", "ASYNC")
}

func TestDDLExecutionModeAsyncWait_Success(t *testing.T) {
	t.Parallel()

	db, server, teardown := setupTestDBConnectionWithParams(t, "ddl_execution_mode=ASYNC_WAIT;ddl_async_wait_timeout=100ms")
	defer teardown()
	ctx := context.Background()

	// Mock an operation that completes immediately.
	opName := "projects/p/instances/i/databases/d/operations/op2"
	var expectedResponse = &databasepb.Database{}
	anyMsg, _ := anypb.New(expectedResponse)
	server.TestDatabaseAdmin.SetResps([]proto.Message{
		&longrunningpb.Operation{
			Name:   opName,
			Done:   true,
			Result: &longrunningpb.Operation_Response{Response: anyMsg},
		},
	})

	c, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer silentClose(c)

	// Verify operation ID is returned in result.
	err = executeDDLAndVerifyOpID(ctx, c, opName)
	if err != nil {
		t.Fatal(err)
	}
}

func TestDDLExecutionModeAsyncWait_Timeout(t *testing.T) {
	t.Parallel()

	tests := []struct {
		timeoutVal         string
		expectedTimeoutVal string
		waitMin            time.Duration
		wantErr            bool
	}{
		{"100ms", "100ms", 100 * time.Millisecond, false},
		{"50ms", "50ms", 50 * time.Millisecond, false},
		{"NULL", "0s", 0, false},
		{"0ms", "0s", 0, false},
		{"-100ms", "", 0, true},
		{"Xms", "", 0, true},
	}

	for _, tc := range tests {
		t.Run(tc.timeoutVal, func(t *testing.T) {
			server, _, serverTeardown := setupMockedTestServerWithDialect(t, databasepb.DatabaseDialect_GOOGLE_STANDARD_SQL)
			defer serverTeardown()
			db, err := sql.Open(
				"spanner",
				fmt.Sprintf("%s/projects/p/instances/i/databases/d?useplaintext=true;ddl_execution_mode=ASYNC_WAIT;ddl_async_wait_timeout=%s", server.Address, tc.timeoutVal))

			if err != nil {
				if tc.wantErr {
					return
				}
				t.Fatalf("unexpected error opening db: %v", err)
			}
			defer func() { _ = db.Close() }()

			ctx := context.Background()

			// Mock an operation that is NOT done and will not finish within the timeout.
			opName := "projects/p/instances/i/databases/d/operations/op-timeout"
			server.TestDatabaseAdmin.SetResps([]proto.Message{
				&longrunningpb.Operation{
					Name: opName,
					Done: false, // Operation is not done
				},
			})

			c, err := db.Conn(ctx)
			if err != nil {
				if tc.wantErr {
					return
				}
				t.Fatalf("unexpected error getting conn: %v", err)
			}
			if tc.wantErr {
				t.Fatal("expected error, got none")
			}
			defer silentClose(c)

			start := time.Now()
			// Execute DDL
			err = executeDDLAndVerifyOpID(ctx, c, opName)
			if err != nil {
				t.Fatal(err)
			}

			// Ensure it waited at least the timeout duration
			if time.Since(start) < tc.waitMin {
				t.Errorf("expected to wait at least %v for timeout", tc.waitMin)
			}

			verifyConnectionPropertyValue(t, c, "ddl_execution_mode", "ASYNC_WAIT")
			verifyConnectionPropertyValue(t, c, "ddl_async_wait_timeout", tc.expectedTimeoutVal)
		})
	}
}

func TestDDLExecutionModeSync(t *testing.T) {
	t.Parallel()

	db, server, teardown := setupTestDBConnectionWithParams(t, "ddl_execution_mode=SYNC")
	defer teardown()
	ctx := context.Background()

	opName := "projects/p/instances/i/databases/d/operations/op4"
	var expectedResponse = &databasepb.Database{}
	anyMsg, _ := anypb.New(expectedResponse)
	server.TestDatabaseAdmin.SetResps([]proto.Message{
		&longrunningpb.Operation{
			Name:   opName,
			Done:   true,
			Result: &longrunningpb.Operation_Response{Response: anyMsg},
		},
	})

	c, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer silentClose(c)

	err = executeDDLAndVerifyOpID(ctx, c, opName)
	if err != nil {
		t.Fatal(err)
	}

	verifyConnectionPropertyValue(t, c, "ddl_execution_mode", "SYNC")
}

func TestDDLExecutionModeDefault(t *testing.T) {
	t.Parallel()

	// Default should be SYNC.
	db, server, teardown := setupTestDBConnection(t)
	defer teardown()
	ctx := context.Background()

	opName := "projects/p/instances/i/databases/d/operations/op5"
	var expectedResponse = &databasepb.Database{}
	anyMsg, _ := anypb.New(expectedResponse)
	server.TestDatabaseAdmin.SetResps([]proto.Message{
		&longrunningpb.Operation{
			Name:   opName,
			Done:   true,
			Result: &longrunningpb.Operation_Response{Response: anyMsg},
		},
	})

	c, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer silentClose(c)

	err = executeDDLAndVerifyOpID(ctx, c, opName)
	if err != nil {
		t.Fatal(err)
	}

	verifyConnectionPropertyValue(t, c, "ddl_execution_mode", "SYNC")
}

func TestDDLBatchAsyncRunViaQuery(t *testing.T) {
	t.Parallel()

	db, server, teardown := setupTestDBConnectionWithParams(t, "ddl_execution_mode=ASYNC")
	defer teardown()
	ctx := context.Background()

	// Mock an operation that is NOT done.
	opName := "projects/p/instances/i/databases/d/operations/op-batch"
	server.TestDatabaseAdmin.SetResps([]proto.Message{
		&longrunningpb.Operation{
			Name: opName,
			Done: false,
		},
	})

	c, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer silentClose(c)

	// Start Batch DDL
	if _, err := c.ExecContext(ctx, "START BATCH DDL"); err != nil {
		t.Fatalf("failed to start batch: %v", err)
	}

	// Buffer a DDL statement
	if _, err := c.ExecContext(ctx, "CREATE TABLE Foo (Id INT64) PRIMARY KEY (Id)"); err != nil {
		t.Fatalf("failed to buffer DDL: %v", err)
	}

	// Run Batch via Query
	rows, err := c.QueryContext(ctx, "RUN BATCH")
	if err != nil {
		t.Fatalf("failed to run batch via Query: %v", err)
	}
	defer rows.Close()

	if !rows.Next() {
		t.Fatal("expected a row with operation ID, got none")
	}

	var gotOpID string
	if err := rows.Scan(&gotOpID); err != nil {
		t.Fatalf("failed to scan operation ID: %v", err)
	}

	if gotOpID != opName {
		t.Errorf("expected operation ID %q, got %q", opName, gotOpID)
	}

	if rows.Next() {
		t.Fatal("expected only one row, got more")
	}
}

func TestDDLAsyncWaitTimeout_Default(t *testing.T) {
	t.Parallel()

	db, _, teardown := setupTestDBConnectionWithParams(t, "ddl_execution_mode=ASYNC_WAIT")
	defer teardown()
	ctx := context.Background()

	c, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer silentClose(c)

	verifyConnectionPropertyValue(t, c, "ddl_async_wait_timeout", "10s")
}

func executeDDLAndVerifyOpID(ctx context.Context, c *sql.Conn, opName string) error {
	return c.Raw(func(driverConn interface{}) error {
		execer, ok := driverConn.(driver.ExecerContext)
		if !ok {
			return fmt.Errorf("driverConn does not implement ExecerContext")
		}

		res, err := execer.ExecContext(ctx, "CREATE TABLE Foo (Id INT64) PRIMARY KEY (Id)", nil)
		if err != nil {
			return fmt.Errorf("failed to execute DDL: %w", err)
		}

		// Verify result implements SpannerResult and has correct OperationID
		spannerRes, ok := res.(SpannerResult)
		if !ok {
			return fmt.Errorf("expected result to implement SpannerResult, got %T", res)
		}
		gotOpID, err := spannerRes.OperationID()
		if err != nil {
			return fmt.Errorf("failed to get operation ID: %w", err)
		}
		if gotOpID != opName {
			return fmt.Errorf("expected operation ID %q, got %q", opName, gotOpID)
		}
		return nil
	})
}
