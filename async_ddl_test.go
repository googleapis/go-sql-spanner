package spannerdriver

import (
	"context"
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

	// Execute DDL via Raw to verify driver.Result behavior (since sql.Result wraps it).
	err = c.Raw(func(driverConn interface{}) error {
		execer, ok := driverConn.(driver.ExecerContext)
		if !ok {
			return fmt.Errorf("driverConn does not implement ExecerContext")
		}
		
		start := time.Now()
		res, err := execer.ExecContext(ctx, "CREATE TABLE Foo (Id INT64) PRIMARY KEY (Id)", nil)
		if err != nil {
			return fmt.Errorf("failed to execute DDL: %w", err)
		}
		if time.Since(start) > 1*time.Second {
			return fmt.Errorf("ExecContext took too long for ASYNC mode")
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
	if err != nil {
		t.Fatal(err)
	}

	// Double check that the internal operation ID is also set (legacy check)
	err = c.Raw(func(driverConn interface{}) error {
		sc, ok := driverConn.(*conn)
		if !ok {
			return fmt.Errorf("expected *conn, got %T", driverConn)
		}
		sc.mu.Lock()
		defer sc.mu.Unlock()
		if sc.lastDDLOperationID != opName {
			return fmt.Errorf("expected operation ID %q, got %q", opName, sc.lastDDLOperationID)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestDDLExecutionModeAsyncWait_Success(t *testing.T) {
	t.Parallel()

	db, server, teardown := setupTestDBConnectionWithParams(t, "ddl_execution_mode=ASYNC_WAIT")
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
	err = c.Raw(func(driverConn interface{}) error {
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
	if err != nil {
		t.Fatal(err)
	}
}

func TestDDLExecutionModeAsyncWait_Timeout(t *testing.T) {
	t.Parallel()
	// TODO: Implement reliable timeout test.
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

	if _, err := c.ExecContext(ctx, "CREATE TABLE Foo (Id INT64) PRIMARY KEY (Id)"); err != nil {
		t.Fatalf("failed to execute DDL: %v", err)
	}
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

	if _, err := c.ExecContext(ctx, "CREATE TABLE Foo (Id INT64) PRIMARY KEY (Id)"); err != nil {
		t.Fatalf("failed to execute DDL: %v", err)
	}

	// Check mode is SYNC
	err = c.Raw(func(driverConn interface{}) error {
		sc, ok := driverConn.(*conn)
		if !ok {
			return fmt.Errorf("expected *conn, got %T", driverConn)
		}
		if mode := propertyDDLExecutionMode.GetValueOrDefault(sc.state); mode != DDLExecutionModeSync {
			return fmt.Errorf("expected default mode SYNC, got %v", mode)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}
