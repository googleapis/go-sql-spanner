package spannerdriver

import (
	"context"
	"database/sql"
	"fmt"
	"reflect"
	"testing"
	"time"

	"cloud.google.com/go/spanner/admin/database/apiv1/databasepb"
	"cloud.google.com/go/spanner/apiv1/spannerpb"
	"github.com/googleapis/go-sql-spanner/testutil"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestSetTransactionTag(t *testing.T) {
	t.Parallel()

	db, server, teardown := setupTestDBConnection(t)
	defer teardown()
	ctx := context.Background()

	c, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer silentClose(c)
	if _, err := c.ExecContext(ctx, "set transaction_tag = 'foo'"); err != nil {
		t.Fatal(err)
	}

	for range 2 {
		tx, err := c.BeginTx(ctx, &sql.TxOptions{})
		if err != nil {
			t.Fatal(err)
		}
		if _, err = tx.ExecContext(ctx, testutil.UpdateBarSetFoo); err != nil {
			t.Fatal(err)
		}
		if err := tx.Commit(); err != nil {
			t.Fatal(err)
		}
	}

	requests := server.TestSpanner.DrainRequestsFromServer()
	executeRequests := testutil.RequestsOfType(requests, reflect.TypeOf(&spannerpb.ExecuteSqlRequest{}))
	if g, w := len(executeRequests), 2; g != w {
		t.Fatalf("execute requests count mismatch\n Got: %v\nWant: %v", g, w)
	}
	request := executeRequests[0].(*spannerpb.ExecuteSqlRequest)
	if g, w := request.RequestOptions.TransactionTag, "foo"; g != w {
		t.Fatalf("transaction tag mismatch\n Got: %v\nWant: %v", g, w)
	}
	// The transaction_tag should not be sticky.
	request = executeRequests[1].(*spannerpb.ExecuteSqlRequest)
	if g, w := request.RequestOptions.TransactionTag, ""; g != w {
		t.Fatalf("transaction tag mismatch\n Got: %v\nWant: %v", g, w)
	}
}

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
	readOnly := request.GetTransaction().GetBegin().GetReadOnly()
	if readOnly == nil {
		t.Fatal("missing readOnly on ExecuteSqlRequest")
	}
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

	if _, err := conn.ExecContext(ctx, "begin transaction read only"); err != nil {
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
	readOnly := request.GetTransaction().GetBegin().GetReadOnly()
	if readOnly == nil {
		t.Fatal("missing readOnly on ExecuteSqlRequest")
	}
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

func TestTransactionTimeout(t *testing.T) {
	t.Parallel()

	db, server, teardown := setupTestDBConnection(t)
	defer teardown()
	ctx := context.Background()

	server.TestSpanner.PutExecutionTime(testutil.MethodExecuteStreamingSql, testutil.SimulatedExecutionTime{MinimumExecutionTime: 50 * time.Millisecond})
	tx, _ := db.BeginTx(ctx, &sql.TxOptions{})
	if _, err := tx.ExecContext(ctx, "set local transaction_timeout=10ms"); err != nil {
		t.Fatal(err)
	}
	_, err := tx.ExecContext(ctx, testutil.UpdateBarSetFoo)
	if g, w := status.Code(err), codes.DeadlineExceeded; g != w {
		t.Fatalf("error code mismatch\n Got: %v\nWant: %v", g, w)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}

	requests := server.TestSpanner.DrainRequestsFromServer()
	executeRequests := testutil.RequestsOfType(requests, reflect.TypeOf(&spannerpb.ExecuteSqlRequest{}))
	if g, w := len(executeRequests), 1; g > w {
		t.Fatalf("execute requests count mismatch\n Got: %v\nWant at most: %v", g, w)
	}
	beginRequests := testutil.RequestsOfType(requests, reflect.TypeOf(&spannerpb.BeginTransactionRequest{}))
	if g, w := len(beginRequests), 0; g != w {
		t.Fatalf("begin requests count mismatch\n Got: %v\nWant: %v", g, w)
	}
	commitRequests := testutil.RequestsOfType(requests, reflect.TypeOf(&spannerpb.CommitRequest{}))
	if g, w := len(commitRequests), 0; g != w {
		t.Fatalf("commit requests count mismatch\n Got: %v\nWant: %v", g, w)
	}
	rollbackRequests := testutil.RequestsOfType(requests, reflect.TypeOf(&spannerpb.RollbackRequest{}))
	// There should be no rollback requests on the server, because the initial ExecuteSqlRequest timed out.
	// That means that no transaction ID was returned to the client, so there is nothing to rollback.
	if g, w := len(rollbackRequests), 0; g != w {
		t.Fatalf("rollback requests count mismatch\n Got: %v\nWant: %v", g, w)
	}
}

func TestTransactionTimeoutSecondStatement(t *testing.T) {
	t.Parallel()

	db, server, teardown := setupTestDBConnection(t)
	defer teardown()
	ctx := context.Background()

	tx, _ := db.BeginTx(ctx, &sql.TxOptions{})
	if _, err := tx.ExecContext(ctx, "set local transaction_timeout=40ms"); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.ExecContext(ctx, testutil.UpdateBarSetFoo); err != nil {
		t.Fatal(err)
	}

	server.TestSpanner.PutExecutionTime(testutil.MethodExecuteStreamingSql, testutil.SimulatedExecutionTime{MinimumExecutionTime: 60 * time.Millisecond})
	rows, err := tx.QueryContext(ctx, testutil.SelectFooFromBar, ExecOptions{DirectExecuteQuery: true})
	if rows != nil {
		_ = rows.Close()
	}
	if g, w := status.Code(err), codes.DeadlineExceeded; g != w {
		t.Fatalf("error code mismatch\n Got: %v\nWant: %v", g, w)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}

	requests := server.TestSpanner.DrainRequestsFromServer()
	executeRequests := testutil.RequestsOfType(requests, reflect.TypeOf(&spannerpb.ExecuteSqlRequest{}))
	// The server should receive 1 or 2 requests, depending on exactly when the deadline exceeded error happens.
	if g, w1, w2 := len(executeRequests), 1, 2; g != w1 && g != w2 {
		t.Fatalf("execute requests count mismatch\n Got: %v\nWant: %v\n  Or: %v", g, w1, w2)
	}
	commitRequests := testutil.RequestsOfType(requests, reflect.TypeOf(&spannerpb.CommitRequest{}))
	if g, w := len(commitRequests), 0; g != w {
		t.Fatalf("commit requests count mismatch\n Got: %v\nWant: %v", g, w)
	}
	rollbackRequests := testutil.RequestsOfType(requests, reflect.TypeOf(&spannerpb.RollbackRequest{}))
	if g, w := len(rollbackRequests), 1; g != w {
		t.Fatalf("rollback requests count mismatch\n Got: %v\nWant: %v", g, w)
	}
}

func BenchmarkReadWriteTransaction(b *testing.B) {
	db, server, teardown := setupTestDBConnection(b)
	defer teardown()
	ctx := context.Background()
	query := "select * from random_table"

	for _, numRows := range []int{1, 10, 100, 1000, 10000} {
		resultSet := testutil.CreateRandomResultSet(numRows, databasepb.DatabaseDialect_GOOGLE_STANDARD_SQL)
		_ = server.TestSpanner.PutStatementResult(query, &testutil.StatementResult{
			Type:      testutil.StatementResultResultSet,
			ResultSet: resultSet,
		})

		b.Run(fmt.Sprintf("num-rows-%d", numRows), func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				tx, err := db.BeginTx(ctx, &sql.TxOptions{})
				if err != nil {
					b.Fatal(err)
				}
				//if _, err := tx.ExecContext(ctx, "set local retry_aborts_internally = false"); err != nil {
				//	b.Fatal(err)
				//}
				if _, err := tx.ExecContext(ctx, "set local transaction_tag = 'my_tag'"); err != nil {
					b.Fatal(err)
				}
				rows, err := tx.QueryContext(ctx, query)
				if err != nil {
					b.Fatal(err)
				}
				for rows.Next() {
					// Just iterate through the results
				}
				if rows.Err() != nil {
					b.Fatal(rows.Err())
				}
				for range 10 {
					if _, err = tx.ExecContext(ctx, testutil.UpdateBarSetFoo); err != nil {
						b.Fatal(err)
					}
				}
				if _, err := tx.ExecContext(ctx, "start batch dml"); err != nil {
					b.Fatal(err)
				}
				for range 10 {
					if _, err = tx.ExecContext(ctx, testutil.UpdateBarSetFoo); err != nil {
						b.Fatal(err)
					}
				}
				if _, err := tx.ExecContext(ctx, "run batch"); err != nil {
					b.Fatal(err)
				}
				if err := tx.Commit(); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
