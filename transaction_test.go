package spannerdriver

import (
	"context"
	"database/sql"
	"fmt"
	"reflect"
	"testing"

	"cloud.google.com/go/spanner/admin/database/apiv1/databasepb"
	"cloud.google.com/go/spanner/apiv1/spannerpb"
	"github.com/googleapis/go-sql-spanner/testutil"
)

func TestSetTransactionIsolationLevel(t *testing.T) {
	t.Parallel()

	db, server, teardown := setupTestDBConnection(t)
	defer teardown()
	ctx := context.Background()

	tx, _ := db.BeginTx(ctx, &sql.TxOptions{})
	if _, err := tx.ExecContext(ctx, "set transaction isolation level repeatable read"); err != nil {
		t.Fatal(err)
	}
	_, _ = tx.ExecContext(ctx, testutil.UpdateBarSetFoo)
	_ = tx.Commit()

	requests := server.TestSpanner.DrainRequestsFromServer()
	executeRequests := testutil.RequestsOfType(requests, reflect.TypeOf(&spannerpb.ExecuteSqlRequest{}))
	if g, w := len(executeRequests), 1; g != w {
		t.Fatalf("execute requests count mismatch\n Got: %v\nWant: %v", g, w)
	}
	request := executeRequests[0].(*spannerpb.ExecuteSqlRequest)
	if request.GetTransaction() == nil || request.GetTransaction().GetBegin() == nil {
		t.Fatal("missing begin transaction on ExecuteSqlRequest")
	}
	if g, w := request.GetTransaction().GetBegin().GetIsolationLevel(), spannerpb.TransactionOptions_REPEATABLE_READ; g != w {
		t.Fatalf("begin isolation level mismatch\n Got: %v\nWant: %v", g, w)
	}
}

func TestSetTransactionReadOnly(t *testing.T) {
	t.Parallel()

	db, server, teardown := setupTestDBConnection(t)
	defer teardown()
	ctx := context.Background()

	tx, _ := db.BeginTx(ctx, &sql.TxOptions{})
	if _, err := tx.ExecContext(ctx, "set transaction read only"); err != nil {
		t.Fatal(err)
	}
	row := tx.QueryRowContext(ctx, testutil.SelectFooFromBar, ExecOptions{DirectExecuteQuery: true})
	if err := row.Err(); err != nil {
		t.Fatal(err)
	}
	_ = tx.Commit()

	requests := server.TestSpanner.DrainRequestsFromServer()
	executeRequests := testutil.RequestsOfType(requests, reflect.TypeOf(&spannerpb.ExecuteSqlRequest{}))
	if g, w := len(executeRequests), 1; g != w {
		t.Fatalf("execute requests count mismatch\n Got: %v\nWant: %v", g, w)
	}
	request := executeRequests[0].(*spannerpb.ExecuteSqlRequest)
	if request.GetTransaction() == nil || request.GetTransaction().GetBegin() == nil {
		t.Fatal("missing begin transaction on ExecuteSqlRequest")
	}
	// TODO: Enable once transaction_read_only is picked up by the driver.
	//readOnly := request.GetTransaction().GetBegin().GetReadOnly()
	//if readOnly == nil {
	//	t.Fatal("missing readOnly on ExecuteSqlRequest")
	//}
}

func TestSetTransactionDeferrable(t *testing.T) {
	t.Parallel()

	// SET TRANSACTION [NOT] DEFERRABLE is only supported for PostgreSQL-dialect databases.
	db, _, teardown := setupTestDBConnectionWithParamsAndDialect(t, "", databasepb.DatabaseDialect_POSTGRESQL)
	defer teardown()
	ctx := context.Background()

	tx, _ := db.BeginTx(ctx, &sql.TxOptions{})
	if _, err := tx.ExecContext(ctx, "set transaction deferrable"); err != nil {
		t.Fatal(err)
	}
	row := tx.QueryRowContext(ctx, testutil.SelectFooFromBar, ExecOptions{DirectExecuteQuery: true})
	if err := row.Err(); err != nil {
		t.Fatal(err)
	}

	// transaction_deferrable is a no-op on Spanner, but the SQL statement is supported for
	// PostgreSQL-dialect databases for compatibility reasons.
	row = tx.QueryRowContext(ctx, "show transaction_deferrable")
	if err := row.Err(); err != nil {
		t.Fatal(err)
	}
	var deferrable bool
	if err := row.Scan(&deferrable); err != nil {
		t.Fatal(err)
	}
	_ = tx.Commit()

	if g, w := deferrable, true; g != w {
		t.Fatalf("deferrable mismatch\n Got: %v\nWant: %v", g, w)
	}
}

func TestBeginTransactionIsolationLevel(t *testing.T) {
	t.Parallel()

	db, server, teardown := setupTestDBConnection(t)
	defer teardown()
	ctx := context.Background()

	conn, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer silentClose(conn)

	if _, err := conn.ExecContext(ctx, "begin transaction isolation level repeatable read"); err != nil {
		t.Fatal(err)
	}
	if _, err := conn.ExecContext(ctx, testutil.UpdateBarSetFoo); err != nil {
		t.Fatal(err)
	}
	if _, err := conn.ExecContext(ctx, "commit"); err != nil {
		t.Fatal(err)
	}

	requests := server.TestSpanner.DrainRequestsFromServer()
	executeRequests := testutil.RequestsOfType(requests, reflect.TypeOf(&spannerpb.ExecuteSqlRequest{}))
	if g, w := len(executeRequests), 1; g != w {
		t.Fatalf("execute requests count mismatch\n Got: %v\nWant: %v", g, w)
	}
	request := executeRequests[0].(*spannerpb.ExecuteSqlRequest)
	if request.GetTransaction() == nil || request.GetTransaction().GetBegin() == nil {
		t.Fatal("missing begin transaction on ExecuteSqlRequest")
	}
	if g, w := request.GetTransaction().GetBegin().GetIsolationLevel(), spannerpb.TransactionOptions_REPEATABLE_READ; g != w {
		t.Fatalf("begin isolation level mismatch\n Got: %v\nWant: %v", g, w)
	}
}

func TestBeginTransactionReadOnly(t *testing.T) {
	t.Parallel()

	db, server, teardown := setupTestDBConnection(t)
	defer teardown()
	ctx := context.Background()

	conn, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer silentClose(conn)

	if _, err := conn.ExecContext(ctx, "begin transaction read write"); err != nil {
		t.Fatal(err)
	}
	row := conn.QueryRowContext(ctx, testutil.SelectFooFromBar, ExecOptions{DirectExecuteQuery: true})
	var c int64
	// If we don't call row.Scan(..), then the underlying Rows object is not closed. That again means that the
	// connection cannot be released.
	if err := row.Scan(&c); err != nil {
		t.Fatal(err)
	}
	if _, err := conn.ExecContext(ctx, "commit"); err != nil {
		t.Fatal(err)
	}

	requests := server.TestSpanner.DrainRequestsFromServer()
	executeRequests := testutil.RequestsOfType(requests, reflect.TypeOf(&spannerpb.ExecuteSqlRequest{}))
	if g, w := len(executeRequests), 1; g != w {
		t.Fatalf("execute requests count mismatch\n Got: %v\nWant: %v", g, w)
	}
	request := executeRequests[0].(*spannerpb.ExecuteSqlRequest)
	if request.GetTransaction() == nil || request.GetTransaction().GetBegin() == nil {
		t.Fatal("missing begin transaction on ExecuteSqlRequest")
	}
	// TODO: Enable once transaction_read_only is picked up by the driver.
	//readOnly := request.GetTransaction().GetBegin().GetReadOnly()
	//if readOnly == nil {
	//	t.Fatal("missing readOnly on ExecuteSqlRequest")
	//}
}

func TestBeginTransactionDeferrable(t *testing.T) {
	t.Parallel()

	// BEGIN TRANSACTION [NOT] DEFERRABLE is only supported for PostgreSQL-dialect databases.
	db, _, teardown := setupTestDBConnectionWithParamsAndDialect(t, "", databasepb.DatabaseDialect_POSTGRESQL)
	defer teardown()
	ctx := context.Background()

	conn, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer silentClose(conn)

	if _, err := conn.ExecContext(ctx, "begin transaction deferrable"); err != nil {
		t.Fatal(err)
	}
	row := conn.QueryRowContext(ctx, testutil.SelectFooFromBar, ExecOptions{DirectExecuteQuery: true})
	var c int64
	if err := row.Scan(&c); err != nil {
		t.Fatal(err)
	}

	// transaction_deferrable is a no-op on Spanner, but the SQL statement is supported for
	// PostgreSQL-dialect databases for compatibility reasons.
	row = conn.QueryRowContext(ctx, "show transaction_deferrable")
	var deferrable bool
	if err := row.Scan(&deferrable); err != nil {
		t.Fatal(err)
	}
	if _, err := conn.ExecContext(ctx, "commit"); err != nil {
		t.Fatal(err)
	}

	if g, w := deferrable, true; g != w {
		t.Fatalf("deferrable mismatch\n Got: %v\nWant: %v", g, w)
	}
}

func TestDmlBatchReturnsBatchUpdateCounts(t *testing.T) {
	t.Parallel()
	db, _, teardown := setupTestDBConnection(t)
	defer teardown()
	ctx := context.Background()

	conn, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer silentClose(conn)

	for _, retry := range []bool{true, false} {
		_, err := conn.ExecContext(ctx, "begin transaction")
		if err != nil {
			t.Fatal(err)
		}
		if _, err := conn.ExecContext(ctx, fmt.Sprintf("set local retry_aborts_internally=%v", retry)); err != nil {
			t.Fatal(err)
		}
		if _, err := conn.ExecContext(ctx, "start batch dml"); err != nil {
			t.Fatal(err)
		}
		_, _ = conn.ExecContext(ctx, testutil.UpdateBarSetFoo)
		_, _ = conn.ExecContext(ctx, testutil.UpdateSingersSetLastName)
		var res SpannerResult
		if err := conn.Raw(func(driverConn interface{}) error {
			spannerConn, _ := driverConn.(SpannerConn)
			res, err = spannerConn.RunDmlBatch(ctx)
			return err
		}); err != nil {
			t.Fatal(err)
		}
		results, err := res.BatchRowsAffected()
		if err != nil {
			t.Fatal(err)
		}
		if g, w := results, []int64{testutil.UpdateBarSetFooRowCount, testutil.UpdateSingersSetLastNameRowCount}; !reflect.DeepEqual(g, w) {
			t.Fatalf("batch affected mismatch\n Got: %v\nWant: %v", g, w)
		}
		if _, err := conn.ExecContext(ctx, "commit"); err != nil {
			t.Fatal(err)
		}
	}
}
