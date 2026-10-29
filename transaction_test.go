package spannerdriver

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"reflect"
	"testing"
	"time"

	"cloud.google.com/go/spanner"
	"cloud.google.com/go/spanner/admin/database/apiv1/databasepb"
	"cloud.google.com/go/spanner/apiv1/spannerpb"
	"github.com/googleapis/go-sql-spanner/connectionstate"
	"github.com/googleapis/go-sql-spanner/parser"
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

	server.TestSpanner.PutExecutionTime(testutil.MethodExecuteStreamingSql, testutil.SimulatedExecutionTime{MinimumExecutionTime: 200 * time.Millisecond})
	tx, _ := db.BeginTx(ctx, &sql.TxOptions{})
	if _, err := tx.ExecContext(ctx, "set local transaction_timeout=100ms"); err != nil {
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
	if _, err := tx.ExecContext(ctx, "set local transaction_timeout=200ms"); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.ExecContext(ctx, testutil.UpdateBarSetFoo); err != nil {
		t.Fatal(err)
	}

	server.TestSpanner.PutExecutionTime(testutil.MethodExecuteStreamingSql, testutil.SimulatedExecutionTime{MinimumExecutionTime: 300 * time.Millisecond})
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

func TestReadOnlyTransactionTimestamp(t *testing.T) {
	t.Parallel()

	db, _, teardown := setupTestDBConnection(t)
	defer teardown()
	ctx := context.Background()

	conn, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer silentClose(conn)

	err = conn.Raw(func(driverConn any) error {
		spannerConn := driverConn.(SpannerConn)
		_, err := spannerConn.ReadOnlyTransactionTimestamp()
		return err
	})
	if err == nil {
		t.Error("expected error retrieving read timestamp when no transaction is active")
	} else if g, w := status.Code(err), codes.FailedPrecondition; g != w {
		t.Errorf("error code mismatch\nGot:  %v\nWant: %v", g, w)
	}

	if _, err := conn.ExecContext(ctx, "begin transaction read only"); err != nil {
		t.Fatal(err)
	}

	rows, err := conn.QueryContext(ctx, testutil.SelectFooFromBar)
	if err != nil {
		t.Fatal(err)
	}
	if rows.Next() {
		var foo int64
		_ = rows.Scan(&foo)
	}
	_ = rows.Close()

	var readTimestamp time.Time
	err = conn.Raw(func(driverConn any) error {
		spannerConn := driverConn.(SpannerConn)
		var err error
		readTimestamp, err = spannerConn.ReadOnlyTransactionTimestamp()
		return err
	})
	if err != nil {
		t.Fatalf("unexpected error retrieving read timestamp: %v", err)
	}
	if readTimestamp.IsZero() {
		t.Error("expected non-zero read timestamp")
	}

	if _, err := conn.ExecContext(ctx, "rollback"); err != nil {
		t.Fatal(err)
	}

	err = conn.Raw(func(driverConn any) error {
		spannerConn := driverConn.(SpannerConn)
		_, err := spannerConn.ReadOnlyTransactionTimestamp()
		return err
	})
	if err == nil {
		t.Error("expected error retrieving read timestamp after transaction is closed")
	} else if g, w := status.Code(err), codes.FailedPrecondition; g != w {
		t.Errorf("error code mismatch\nGot:  %v\nWant: %v", g, w)
	}
}

func TestReadOnlyTransactionTimestamp_VariousOptions(t *testing.T) {
	t.Parallel()

	t.Run("BatchReadOnly", func(t *testing.T) {
		db, server, teardown := setupTestDBConnection(t)
		defer teardown()
		ctx := context.Background()

		conn, err := db.Conn(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer silentClose(conn)

		if _, err := conn.ExecContext(ctx, "SET transaction_batch_read_only = true"); err != nil {
			t.Fatal(err)
		}
		if _, err := conn.ExecContext(ctx, "begin transaction read only"); err != nil {
			t.Fatal(err)
		}

		// Drain any requests that might have been sent during setup/connection initialization
		_ = server.TestSpanner.DrainRequestsFromServer()

		// Retrieve the timestamp immediately without executing a query
		var readTimestamp time.Time
		err = conn.Raw(func(driverConn any) error {
			spannerConn := driverConn.(SpannerConn)
			var err error
			readTimestamp, err = spannerConn.ReadOnlyTransactionTimestamp()
			return err
		})
		if err != nil {
			t.Fatalf("unexpected error retrieving read timestamp: %v", err)
		}
		if readTimestamp.IsZero() {
			t.Error("expected non-zero read timestamp")
		}

		// Verify that exactly one BeginTransaction request was sent (triggered by activation when querying timestamp)
		requests := server.TestSpanner.DrainRequestsFromServer()
		beginRequests := testutil.RequestsOfType(requests, reflect.TypeOf(&spannerpb.BeginTransactionRequest{}))
		if g, w := len(beginRequests), 1; g != w {
			t.Errorf("num begin requests mismatch\nGot:  %v\nWant: %v", g, w)
		}

		if _, err := conn.ExecContext(ctx, "rollback"); err != nil {
			t.Fatal(err)
		}
	})

	for _, option := range []string{"InlinedBeginTransaction", "ExplicitBeginTransaction"} {
		t.Run(option, func(t *testing.T) {
			db, server, teardown := setupTestDBConnection(t)
			defer teardown()
			ctx := context.Background()

			conn, err := db.Conn(ctx)
			if err != nil {
				t.Fatal(err)
			}
			defer silentClose(conn)

			if _, err := conn.ExecContext(ctx, fmt.Sprintf("SET begin_transaction_option = '%s'", option)); err != nil {
				t.Fatal(err)
			}
			if _, err := conn.ExecContext(ctx, "begin transaction read only"); err != nil {
				t.Fatal(err)
			}

			// Run a query to start the transaction on the Spanner server and select a read timestamp
			rows, err := conn.QueryContext(ctx, testutil.SelectFooFromBar)
			if err != nil {
				t.Fatal(err)
			}
			for rows.Next() {
			}
			_ = rows.Close()

			// Retrieve the timestamp after running a query
			var readTimestamp time.Time
			err = conn.Raw(func(driverConn any) error {
				spannerConn := driverConn.(SpannerConn)
				var err error
				readTimestamp, err = spannerConn.ReadOnlyTransactionTimestamp()
				return err
			})
			if err != nil {
				t.Fatalf("unexpected error retrieving read timestamp: %v", err)
			}
			if readTimestamp.IsZero() {
				t.Error("expected non-zero read timestamp")
			}

			// Verify that the correct number of BeginTransaction requests were sent (0 for inline, 1 for explicit)
			requests := server.TestSpanner.DrainRequestsFromServer()
			beginRequests := testutil.RequestsOfType(requests, reflect.TypeOf(&spannerpb.BeginTransactionRequest{}))
			expectedRequests := 0
			if option == "ExplicitBeginTransaction" {
				expectedRequests = 1
			}
			if g, w := len(beginRequests), expectedRequests; g != w {
				t.Errorf("num begin requests mismatch\nGot:  %v\nWant: %v", g, w)
			}

			if _, err := conn.ExecContext(ctx, "rollback"); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestNilBackendTransactionSafety(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	selectStmt := spanner.Statement{SQL: testutil.SelectFooFromBar}
	updateStmt := spanner.Statement{SQL: testutil.UpdateBarSetFoo}

	t.Run("readWriteTransaction_nil_rwTx", func(t *testing.T) {
		rwTx := &readWriteTransaction{
			rwTx:   nil,
			logger: noopLogger,
		}
		_, err := rwTx.Query(ctx, selectStmt, parser.StatementTypeQuery, &ExecOptions{})
		if err == nil || spanner.ErrCode(err) != codes.FailedPrecondition {
			t.Errorf("Query nil rwTx error code mismatch\nGot:  %v\nWant: %v", spanner.ErrCode(err), codes.FailedPrecondition)
		}

		_, err = rwTx.ExecContext(ctx, updateStmt, &parser.StatementInfo{}, spanner.QueryOptions{})
		if err == nil || spanner.ErrCode(err) != codes.FailedPrecondition {
			t.Errorf("ExecContext nil rwTx error code mismatch\nGot:  %v\nWant: %v", spanner.ErrCode(err), codes.FailedPrecondition)
		}

		_, err = rwTx.runDmlBatch(ctx)
		if err == nil || spanner.ErrCode(err) != codes.FailedPrecondition {
			t.Errorf("runDmlBatch nil rwTx error code mismatch\nGot:  %v\nWant: %v", spanner.ErrCode(err), codes.FailedPrecondition)
		}

		err = rwTx.BufferWrite(nil)
		if g, w := spanner.ErrCode(err), codes.FailedPrecondition; g != w {
			t.Errorf("BufferWrite nil rwTx error code mismatch\nGot:  %v\nWant: %v", g, w)
		}

		dRw := &delegatingTransaction{
			contextTransaction: rwTx,
		}
		err = dRw.Savepoint("s1")
		if g, w := spanner.ErrCode(err), codes.FailedPrecondition; g != w {
			t.Errorf("Savepoint nil rwTx error code mismatch\nGot:  %v\nWant: %v", g, w)
		}

		err = dRw.ReleaseSavepoint("s1")
		if g, w := spanner.ErrCode(err), codes.FailedPrecondition; g != w {
			t.Errorf("ReleaseSavepoint nil rwTx error code mismatch\nGot:  %v\nWant: %v", g, w)
		}

		err = dRw.RollbackToSavepoint(ctx, "s1")
		if g, w := spanner.ErrCode(err), codes.FailedPrecondition; g != w {
			t.Errorf("RollbackToSavepoint nil rwTx error code mismatch\nGot:  %v\nWant: %v", g, w)
		}

		_, _, err = rwTx.savepointCheckpoint()
		if g, w := spanner.ErrCode(err), codes.FailedPrecondition; g != w {
			t.Errorf("savepointCheckpoint nil rwTx error code mismatch\nGot:  %v\nWant: %v", g, w)
		}
		err = rwTx.canUseSavepoints("create", false)
		if g, w := spanner.ErrCode(err), codes.FailedPrecondition; g != w {
			t.Errorf("canUseSavepoints nil rwTx error code mismatch\nGot:  %v\nWant: %v", g, w)
		}
		err = rwTx.rollbackToSavepointCheckpoint(ctx, 0, 0)
		if g, w := spanner.ErrCode(err), codes.FailedPrecondition; g != w {
			t.Errorf("rollbackToSavepointCheckpoint nil rwTx error code mismatch\nGot:  %v\nWant: %v", g, w)
		}
		err = rwTx.resetForRetry(ctx)
		if g, w := spanner.ErrCode(err), codes.FailedPrecondition; g != w {
			t.Errorf("resetForRetry nil rwTx error code mismatch\nGot:  %v\nWant: %v", g, w)
		}
	})

	t.Run("readOnlyTransaction_nil_roTx", func(t *testing.T) {
		roTx := &readOnlyTransaction{
			roTx:   nil,
			boTx:   nil,
			logger: noopLogger,
		}
		_, err := roTx.Query(ctx, selectStmt, parser.StatementTypeQuery, &ExecOptions{})
		if g, w := spanner.ErrCode(err), codes.FailedPrecondition; g != w {
			t.Errorf("Query nil roTx error code mismatch\nGot:  %v\nWant: %v", g, w)
		}

		dRo := &delegatingTransaction{
			contextTransaction: roTx,
		}
		err = dRo.Savepoint("s1")
		if g, w := spanner.ErrCode(err), codes.FailedPrecondition; g != w {
			t.Errorf("Savepoint nil roTx error code mismatch\nGot:  %v\nWant: %v", g, w)
		}

		err = dRo.ReleaseSavepoint("s1")
		if g, w := spanner.ErrCode(err), codes.FailedPrecondition; g != w {
			t.Errorf("ReleaseSavepoint nil roTx error code mismatch\nGot:  %v\nWant: %v", g, w)
		}

		err = dRo.RollbackToSavepoint(ctx, "s1")
		if g, w := spanner.ErrCode(err), codes.FailedPrecondition; g != w {
			t.Errorf("RollbackToSavepoint nil roTx error code mismatch\nGot:  %v\nWant: %v", g, w)
		}

		_, _, err = roTx.savepointCheckpoint()
		if g, w := spanner.ErrCode(err), codes.FailedPrecondition; g != w {
			t.Errorf("savepointCheckpoint nil roTx error code mismatch\nGot:  %v\nWant: %v", g, w)
		}
		err = roTx.canUseSavepoints("create", false)
		if g, w := spanner.ErrCode(err), codes.FailedPrecondition; g != w {
			t.Errorf("canUseSavepoints nil roTx error code mismatch\nGot:  %v\nWant: %v", g, w)
		}
		err = roTx.rollbackToSavepointCheckpoint(ctx, 0, 0)
		if g, w := spanner.ErrCode(err), codes.FailedPrecondition; g != w {
			t.Errorf("rollbackToSavepointCheckpoint nil roTx error code mismatch\nGot:  %v\nWant: %v", g, w)
		}
	})
}

func TestFailedTransactionCommit_SessionStateRollback(t *testing.T) {
	t.Parallel()

	db, server, teardown := setupTestDBConnectionWithParamsAndDialect(t, "", databasepb.DatabaseDialect_POSTGRESQL)
	defer teardown()
	ctx := context.Background()

	conn, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer silentClose(conn)

	// Set in_error_tx_behavior to enforce_in_error_state
	setPGInErrorTxBehavior(t, conn, connectionstate.InErrorTxBehaviorEnforceInErrorState)

	tx, err := conn.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}

	// Change a session property inside the transaction
	if _, err := tx.ExecContext(ctx, "SET READ_ONLY_STALENESS = 'EXACT_STALENESS 10s'"); err != nil {
		t.Fatal(err)
	}

	// Cause an error so the transaction enters state 'E'
	_ = server.TestSpanner.PutStatementResult(testutil.UpdateBarSetFoo, &testutil.StatementResult{Err: status.Error(codes.InvalidArgument, "server error")})
	if _, err := tx.ExecContext(ctx, testutil.UpdateBarSetFoo); err == nil {
		t.Fatal("expected error, got nil")
	}

	// Commit on the failed transaction
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}

	// Session property setting should have been rolled back to default (Strong) instead of committed (ExactStaleness)
	var staleness spanner.TimestampBound
	err = conn.Raw(func(driverConn any) error {
		if sc, ok := driverConn.(SpannerConn); ok {
			staleness = sc.ReadOnlyStaleness()
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if g, w := staleness.String(), "(strong)"; g != w {
		t.Errorf("read only staleness was committed during failed transaction rollback\nGot:  %v\nWant: %v", g, w)
	}
}

func TestSavepoint_TransactionReplay(t *testing.T) {
	t.Parallel()

	db, server, teardown := setupTestDBConnection(t)
	defer teardown()
	ctx := context.Background()

	dml1 := "UPDATE Foo SET Val=1 WHERE Id=1"
	dml2 := "UPDATE Foo SET Val=2 WHERE Id=2"
	server.TestSpanner.PutStatementResult(dml1, &testutil.StatementResult{Type: testutil.StatementResultUpdateCount, UpdateCount: 1})
	server.TestSpanner.PutStatementResult(dml2, &testutil.StatementResult{Type: testutil.StatementResultUpdateCount, UpdateCount: 1})

	dbConn, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer silentClose(dbConn)

	tx, err := dbConn.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		t.Fatal(err)
	}

	if _, err := tx.ExecContext(ctx, dml1); err != nil {
		t.Fatalf("dml1 failed: %v", err)
	}

	err = dbConn.Raw(func(driverConn any) error {
		c := driverConn.(*conn)
		return c.tx.Savepoint("s1")
	})
	if err != nil {
		t.Fatalf("savepoint s1 failed: %v", err)
	}

	if _, err := tx.ExecContext(ctx, dml2); err != nil {
		t.Fatalf("dml2 failed: %v", err)
	}

	err = dbConn.Raw(func(driverConn any) error {
		c := driverConn.(*conn)
		return c.tx.RollbackToSavepoint(ctx, "s1")
	})
	if err != nil {
		t.Fatalf("rollback to savepoint s1 failed: %v", err)
	}

	if err := tx.Commit(); err != nil {
		t.Fatalf("commit failed: %v", err)
	}

	requests := server.TestSpanner.DrainRequestsFromServer()
	commitRequests := testutil.RequestsOfType(requests, reflect.TypeOf(&spannerpb.CommitRequest{}))
	if g, w := len(commitRequests), 1; g != w {
		t.Fatalf("commit requests count mismatch\nGot:  %v\nWant: %v", g, w)
	}

	rollbackRequests := testutil.RequestsOfType(requests, reflect.TypeOf(&spannerpb.RollbackRequest{}))
	if g, w := len(rollbackRequests), 1; g != w {
		t.Fatalf("rollback requests count mismatch\nGot:  %v\nWant: %v", g, w)
	}

	executeRequests := testutil.RequestsOfType(requests, reflect.TypeOf(&spannerpb.ExecuteSqlRequest{}))
	if g, w := len(executeRequests), 3; g != w {
		t.Fatalf("execute requests count mismatch\nGot:  %v\nWant: %v", g, w)
	}

	req1 := executeRequests[0].(*spannerpb.ExecuteSqlRequest)
	req2 := executeRequests[1].(*spannerpb.ExecuteSqlRequest)
	req3 := executeRequests[2].(*spannerpb.ExecuteSqlRequest)

	if g, w := req1.Sql, dml1; g != w {
		t.Errorf("req1 sql mismatch\nGot:  %v\nWant: %v", g, w)
	}
	if g, w := req2.Sql, dml2; g != w {
		t.Errorf("req2 sql mismatch\nGot:  %v\nWant: %v", g, w)
	}
	if g, w := req3.Sql, dml1; g != w {
		t.Errorf("req3 sql mismatch\nGot:  %v\nWant: %v", g, w)
	}

	if req1.Transaction.GetBegin() == nil {
		t.Error("expected req1 to begin a transaction")
	}
	if len(req2.Transaction.GetId()) == 0 || req2.Transaction.GetBegin() != nil {
		t.Error("expected req2 to use an existing transaction ID")
	}
	if req3.Transaction.GetBegin() == nil {
		t.Error("expected req3 (replayed) to begin a new transaction")
	}
}

func TestSavepoint_TransactionReplayWithMutations(t *testing.T) {
	t.Parallel()

	db, server, teardown := setupTestDBConnection(t)
	defer teardown()
	ctx := context.Background()

	dbConn, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer silentClose(dbConn)

	tx, err := dbConn.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		t.Fatal(err)
	}

	mutation1 := spanner.Insert("Foo", []string{"Id", "Val"}, []any{int64(1), "one"})
	mutation2 := spanner.Insert("Foo", []string{"Id", "Val"}, []any{int64(2), "two"})

	err = dbConn.Raw(func(driverConn any) error {
		c := driverConn.(*conn)
		if err := c.tx.BufferWrite([]*spanner.Mutation{mutation1}); err != nil {
			return err
		}
		if err := c.tx.Savepoint("s1"); err != nil {
			return err
		}
		if err := c.tx.BufferWrite([]*spanner.Mutation{mutation2}); err != nil {
			return err
		}
		return c.tx.RollbackToSavepoint(ctx, "s1")
	})
	if err != nil {
		t.Fatalf("savepoint with mutations failed: %v", err)
	}

	if err := tx.Commit(); err != nil {
		t.Fatalf("commit failed: %v", err)
	}

	requests := server.TestSpanner.DrainRequestsFromServer()
	commitRequests := testutil.RequestsOfType(requests, reflect.TypeOf(&spannerpb.CommitRequest{}))
	if g, w := len(commitRequests), 1; g != w {
		t.Fatalf("commit requests count mismatch\nGot:  %v\nWant: %v", g, w)
	}

	commitReq := commitRequests[0].(*spannerpb.CommitRequest)
	if g, w := len(commitReq.Mutations), 1; g != w {
		t.Fatalf("mutations count in commit mismatch\nGot:  %v\nWant: %v", g, w)
	}
	if g, w := commitReq.Mutations[0].GetInsert().Table, "Foo"; g != w {
		t.Errorf("mutation table mismatch\nGot:  %v\nWant: %v", g, w)
	}
	if g, w := commitReq.Mutations[0].GetInsert().Values[0].GetValues()[0].GetStringValue(), "1"; g != w {
		t.Errorf("mutation id mismatch\nGot:  %v\nWant: %v", g, w)
	}
}

func TestSavepoint_NestedAndShadowed(t *testing.T) {
	t.Parallel()

	db, _, teardown := setupTestDBConnection(t)
	defer teardown()
	ctx := context.Background()

	dbConn, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer silentClose(dbConn)

	tx, err := dbConn.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		t.Fatal(err)
	}

	err = dbConn.Raw(func(driverConn any) error {
		c := driverConn.(*conn)

		// Create s1, s2, s3
		if err := c.tx.Savepoint("s1"); err != nil {
			return fmt.Errorf("s1: %w", err)
		}
		if err := c.tx.Savepoint("s2"); err != nil {
			return fmt.Errorf("s2: %w", err)
		}
		if err := c.tx.Savepoint("s3"); err != nil {
			return fmt.Errorf("s3: %w", err)
		}

		// Rolling back to s2 must prune s3
		if err := c.tx.RollbackToSavepoint(ctx, "s2"); err != nil {
			return fmt.Errorf("rollback s2: %w", err)
		}
		if err := c.tx.RollbackToSavepoint(ctx, "s3"); err == nil {
			return fmt.Errorf("expected error rolling back to pruned s3, got nil")
		} else if g, w := ToPGSQLState(err), "3B001"; g != w {
			return fmt.Errorf("sqlstate mismatch for s3\nGot:  %v\nWant: %v", g, w)
		}

		// s2 and s1 should still exist
		if err := c.tx.RollbackToSavepoint(ctx, "s2"); err != nil {
			return fmt.Errorf("second rollback s2: %w", err)
		}
		if err := c.tx.RollbackToSavepoint(ctx, "s1"); err != nil {
			return fmt.Errorf("rollback s1: %w", err)
		}

		// Releasing s1 should remove s1 (and s2 if it still existed)
		if err := c.tx.ReleaseSavepoint("s1"); err != nil {
			return fmt.Errorf("release s1: %w", err)
		}
		if err := c.tx.RollbackToSavepoint(ctx, "s1"); err == nil {
			return fmt.Errorf("expected error rolling back to released s1, got nil")
		} else if g, w := ToPGSQLState(err), "3B001"; g != w {
			return fmt.Errorf("sqlstate mismatch for released s1\nGot:  %v\nWant: %v", g, w)
		}

		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}
}

func TestSavepoint_Errors(t *testing.T) {
	t.Parallel()

	db, _, teardown := setupTestDBConnection(t)
	defer teardown()
	ctx := context.Background()

	dbConn, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer silentClose(dbConn)

	tx, err := dbConn.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		t.Fatal(err)
	}

	err = dbConn.Raw(func(driverConn any) error {
		c := driverConn.(*conn)

		t.Run("rollback_nonexistent", func(t *testing.T) {
			err := c.tx.RollbackToSavepoint(ctx, "s_none")
			if err == nil {
				t.Fatal("expected error, got nil")
			}
			if g, w := status.Code(err), codes.FailedPrecondition; g != w {
				t.Errorf("status code mismatch\nGot:  %v\nWant: %v", g, w)
			}
			if g, w := ToPGSQLState(err), "3B001"; g != w {
				t.Errorf("sqlstate mismatch\nGot:  %v\nWant: %v", g, w)
			}
		})

		t.Run("release_nonexistent", func(t *testing.T) {
			err := c.tx.ReleaseSavepoint("s_none")
			if err == nil {
				t.Fatal("expected error, got nil")
			}
			if g, w := status.Code(err), codes.FailedPrecondition; g != w {
				t.Errorf("status code mismatch\nGot:  %v\nWant: %v", g, w)
			}
			if g, w := ToPGSQLState(err), "3B001"; g != w {
				t.Errorf("sqlstate mismatch\nGot:  %v\nWant: %v", g, w)
			}
		})

		t.Run("empty_name", func(t *testing.T) {
			for _, invalidName := range []string{"", "   ", "\t\n"} {
				if err := c.tx.Savepoint(invalidName); err == nil {
					t.Fatalf("expected error for Savepoint(%q), got nil", invalidName)
				} else if g, w := status.Code(err), codes.InvalidArgument; g != w {
					t.Errorf("status code mismatch for Savepoint(%q)\nGot:  %v\nWant: %v", invalidName, g, w)
				}

				if err := c.tx.RollbackToSavepoint(ctx, invalidName); err == nil {
					t.Fatalf("expected error for RollbackToSavepoint(%q), got nil", invalidName)
				} else if g, w := status.Code(err), codes.InvalidArgument; g != w {
					t.Errorf("status code mismatch for RollbackToSavepoint(%q)\nGot:  %v\nWant: %v", invalidName, g, w)
				}

				if err := c.tx.ReleaseSavepoint(invalidName); err == nil {
					t.Fatalf("expected error for ReleaseSavepoint(%q), got nil", invalidName)
				} else if g, w := status.Code(err), codes.InvalidArgument; g != w {
					t.Errorf("status code mismatch for ReleaseSavepoint(%q)\nGot:  %v\nWant: %v", invalidName, g, w)
				}
			}
		})

		t.Run("savepoint_during_batch", func(t *testing.T) {
			if _, err := c.tx.StartBatchDML(spanner.QueryOptions{}, false); err != nil {
				t.Fatalf("failed to start batch: %v", err)
			}
			defer func() {
				_, _ = c.tx.AbortBatch()
			}()

			if err := c.tx.Savepoint("s_batch"); err == nil {
				t.Fatal("expected error creating savepoint during active batch, got nil")
			} else if g, w := status.Code(err), codes.FailedPrecondition; g != w {
				t.Errorf("status code mismatch\nGot:  %v\nWant: %v", g, w)
			}

			if err := c.tx.RollbackToSavepoint(ctx, "s_batch"); err == nil {
				t.Fatal("expected error rolling back savepoint during active batch, got nil")
			} else if g, w := status.Code(err), codes.FailedPrecondition; g != w {
				t.Errorf("status code mismatch\nGot:  %v\nWant: %v", g, w)
			}

			if err := c.tx.ReleaseSavepoint("s_batch"); err == nil {
				t.Fatal("expected error releasing savepoint during active batch, got nil")
			} else if g, w := status.Code(err), codes.FailedPrecondition; g != w {
				t.Errorf("status code mismatch\nGot:  %v\nWant: %v", g, w)
			}
		})

		t.Run("case_insensitive_lookup_googlesql", func(t *testing.T) {
			if err := c.tx.Savepoint("MyCaseSavepoint"); err != nil {
				t.Fatalf("failed to create savepoint: %v", err)
			}
			if err := c.tx.ReleaseSavepoint("mycasesavepoint"); err != nil {
				t.Errorf("expected case-insensitive release to succeed, got: %v", err)
			}
		})

		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}
}

func TestSavepoint_ReadOnlyTransaction(t *testing.T) {
	t.Parallel()

	db, _, teardown := setupTestDBConnection(t)
	defer teardown()
	ctx := context.Background()

	dbConn, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer silentClose(dbConn)

	tx, err := dbConn.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		t.Fatal(err)
	}

	err = dbConn.Raw(func(driverConn any) error {
		c := driverConn.(*conn)

		// Invalid savepoint names return InvalidArgument.
		if err := c.tx.Savepoint(""); err == nil {
			t.Fatal("expected error creating savepoint with empty name, got nil")
		} else if g, w := status.Code(err), codes.InvalidArgument; g != w {
			t.Errorf("status code mismatch for empty name\nGot:  %v\nWant: %v", g, w)
		}

		// Unknown savepoints return FailedPrecondition / 3B001.
		if err := c.tx.RollbackToSavepoint(ctx, "s_unknown"); err == nil {
			t.Fatal("expected error rolling back to unknown savepoint, got nil")
		} else if g, w := status.Code(err), codes.FailedPrecondition; g != w {
			t.Errorf("status code mismatch for unknown rollback\nGot:  %v\nWant: %v", g, w)
		}
		if err := c.tx.ReleaseSavepoint("s_unknown"); err == nil {
			t.Fatal("expected error releasing unknown savepoint, got nil")
		} else if g, w := status.Code(err), codes.FailedPrecondition; g != w {
			t.Errorf("status code mismatch for unknown release\nGot:  %v\nWant: %v", g, w)
		}

		if err := c.tx.Savepoint("s1"); err != nil {
			t.Fatalf("savepoint in read-only failed: %v", err)
		}
		if err := c.tx.RollbackToSavepoint(ctx, "s1"); err != nil {
			t.Fatalf("rollback in read-only failed: %v", err)
		}
		if err := c.tx.ReleaseSavepoint("s1"); err != nil {
			t.Fatalf("release in read-only failed: %v", err)
		}
		if err := c.tx.RollbackToSavepoint(ctx, "s1"); err == nil {
			t.Fatal("expected error rolling back to released savepoint, got nil")
		} else if g, w := ToPGSQLState(err), "3B001"; g != w {
			t.Errorf("sqlstate mismatch\nGot:  %v\nWant: %v", g, w)
		}

		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}
}

func TestSavepoint_SQLStatementNotExposedYet(t *testing.T) {
	t.Parallel()

	db, _, teardown := setupTestDBConnection(t)
	defer teardown()
	ctx := context.Background()

	statements := []string{
		"SAVEPOINT s1",
		"ROLLBACK TO SAVEPOINT s1",
		"RELEASE SAVEPOINT s1",
	}

	for _, sqlStmt := range statements {
		t.Run(sqlStmt, func(t *testing.T) {
			_, err := db.ExecContext(ctx, sqlStmt)
			if err == nil {
				t.Fatalf("expected error executing %q, got nil", sqlStmt)
			}
			if g, w := status.Code(err), codes.Internal; g != w {
				t.Errorf("status code mismatch for %q\nGot:  %v\nWant: %v", sqlStmt, g, w)
			}
		})
	}
}

func TestSavepoint_ReplayAborted_SuccessOnRetry(t *testing.T) {
	t.Parallel()

	db, server, teardown := setupTestDBConnection(t)
	defer teardown()
	ctx := context.Background()

	dml1 := testutil.UpdateSingersSetLastName
	dml2 := testutil.UpdateBarSetFoo

	dbConn, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer silentClose(dbConn)

	tx, err := dbConn.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		t.Fatal(err)
	}

	if _, err := tx.ExecContext(ctx, dml1); err != nil {
		t.Fatal(err)
	}

	err = dbConn.Raw(func(driverConn any) error {
		c := driverConn.(*conn)
		return c.tx.Savepoint("s1")
	})
	if err != nil {
		t.Fatal(err)
	}

	if _, err := tx.ExecContext(ctx, dml2); err != nil {
		t.Fatal(err)
	}

	// Simulate Aborted on the first replay attempt of dml1.
	server.TestSpanner.PutExecutionTime(testutil.MethodExecuteStreamingSql, testutil.SimulatedExecutionTime{
		Errors: []error{status.Error(codes.Aborted, "Aborted")},
	})

	err = dbConn.Raw(func(driverConn any) error {
		c := driverConn.(*conn)
		return c.tx.RollbackToSavepoint(ctx, "s1")
	})
	if err != nil {
		t.Fatalf("rollback to savepoint failed: %v", err)
	}

	if err := tx.Commit(); err != nil {
		t.Fatalf("commit failed: %v", err)
	}

	requests := server.TestSpanner.DrainRequestsFromServer()
	executeRequests := testutil.RequestsOfType(requests, reflect.TypeOf(&spannerpb.ExecuteSqlRequest{}))
	// Expected executions:
	// 1. Initial dml1
	// 2. Initial dml2
	// 3. Replay dml1 (aborted)
	// 4. Retry replay dml1 (succeeded)
	if g, w := len(executeRequests), 4; g != w {
		t.Fatalf("execute requests count mismatch\nGot:  %v\nWant: %v", g, w)
	}
	req1 := executeRequests[0].(*spannerpb.ExecuteSqlRequest)
	req2 := executeRequests[1].(*spannerpb.ExecuteSqlRequest)
	req3 := executeRequests[2].(*spannerpb.ExecuteSqlRequest)
	req4 := executeRequests[3].(*spannerpb.ExecuteSqlRequest)

	if g, w := req1.Sql, dml1; g != w {
		t.Errorf("req1 sql mismatch\nGot:  %v\nWant: %v", g, w)
	}
	if g, w := req2.Sql, dml2; g != w {
		t.Errorf("req2 sql mismatch\nGot:  %v\nWant: %v", g, w)
	}
	if g, w := req3.Sql, dml1; g != w {
		t.Errorf("req3 sql mismatch\nGot:  %v\nWant: %v", g, w)
	}
	if g, w := req4.Sql, dml1; g != w {
		t.Errorf("req4 sql mismatch\nGot:  %v\nWant: %v", g, w)
	}
}

func TestSavepoint_ReplayAborted_MultipleAbortsThenSuccess(t *testing.T) {
	t.Parallel()

	db, server, teardown := setupTestDBConnection(t)
	defer teardown()
	ctx := context.Background()

	dml1 := testutil.UpdateSingersSetLastName
	dml2 := testutil.UpdateBarSetFoo

	dbConn, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer silentClose(dbConn)

	tx, err := dbConn.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		t.Fatal(err)
	}

	if _, err := tx.ExecContext(ctx, dml1); err != nil {
		t.Fatal(err)
	}

	err = dbConn.Raw(func(driverConn any) error {
		c := driverConn.(*conn)
		return c.tx.Savepoint("s1")
	})
	if err != nil {
		t.Fatal(err)
	}

	if _, err := tx.ExecContext(ctx, dml2); err != nil {
		t.Fatal(err)
	}

	// Simulate Aborted twice consecutively during replay.
	server.TestSpanner.PutExecutionTime(testutil.MethodExecuteStreamingSql, testutil.SimulatedExecutionTime{
		Errors: []error{
			status.Error(codes.Aborted, "Aborted attempt 1"),
			status.Error(codes.Aborted, "Aborted attempt 2"),
		},
	})

	err = dbConn.Raw(func(driverConn any) error {
		c := driverConn.(*conn)
		return c.tx.RollbackToSavepoint(ctx, "s1")
	})
	if err != nil {
		t.Fatalf("rollback to savepoint failed: %v", err)
	}

	if err := tx.Commit(); err != nil {
		t.Fatalf("commit failed: %v", err)
	}

	requests := server.TestSpanner.DrainRequestsFromServer()
	executeRequests := testutil.RequestsOfType(requests, reflect.TypeOf(&spannerpb.ExecuteSqlRequest{}))
	// 1 initial dml1 + 1 initial dml2 + 3 replay dml1 attempts (2 aborted + 1 success) = 5
	if g, w := len(executeRequests), 5; g != w {
		t.Fatalf("execute requests count mismatch\nGot:  %v\nWant: %v", g, w)
	}
}

func TestSavepoint_ReplayFailed_ConcurrentModification(t *testing.T) {
	t.Parallel()

	db, server, teardown := setupTestDBConnection(t)
	defer teardown()
	ctx := context.Background()

	dml1 := testutil.UpdateSingersSetLastName
	dml2 := testutil.UpdateBarSetFoo

	dbConn, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer silentClose(dbConn)

	tx, err := dbConn.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		t.Fatal(err)
	}

	if _, err := tx.ExecContext(ctx, dml1); err != nil {
		t.Fatal(err)
	}

	err = dbConn.Raw(func(driverConn any) error {
		c := driverConn.(*conn)
		return c.tx.Savepoint("s1")
	})
	if err != nil {
		t.Fatal(err)
	}

	if _, err := tx.ExecContext(ctx, dml2); err != nil {
		t.Fatal(err)
	}

	// Change statement result for dml1 so that replay sees a concurrent modification.
	secondResult := &testutil.StatementResult{
		Type:        testutil.StatementResultUpdateCount,
		UpdateCount: 2,
	}
	_ = server.TestSpanner.PutStatementResult(dml1, secondResult)

	err = dbConn.Raw(func(driverConn any) error {
		c := driverConn.(*conn)
		return c.tx.RollbackToSavepoint(ctx, "s1")
	})
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if g, w := err, ErrAbortedDueToConcurrentModification; g != w {
		t.Errorf("error mismatch\nGot:  %v\nWant: %v", g, w)
	}

	// Verify that the underlying transaction was rolled back and closed.
	err = dbConn.Raw(func(driverConn any) error {
		c := driverConn.(*conn)
		rwTx, ok := c.tx.contextTransaction.(*readWriteTransaction)
		if !ok {
			return fmt.Errorf("unexpected transaction type: %T", c.tx.contextTransaction)
		}
		if rwTx.rwTx != nil {
			return fmt.Errorf("expected underlying Spanner transaction to be nil after failed replay")
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	// Clean up by rolling back the public tx.
	_ = tx.Rollback()
}

func TestSavepoint_ReplayAborted_ThenConcurrentModification(t *testing.T) {
	t.Parallel()

	db, server, teardown := setupTestDBConnection(t)
	defer teardown()
	ctx := context.Background()

	dml1 := testutil.UpdateSingersSetLastName
	dml2 := testutil.UpdateBarSetFoo

	dbConn, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer silentClose(dbConn)

	tx, err := dbConn.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		t.Fatal(err)
	}

	if _, err := tx.ExecContext(ctx, dml1); err != nil {
		t.Fatal(err)
	}

	err = dbConn.Raw(func(driverConn any) error {
		c := driverConn.(*conn)
		return c.tx.Savepoint("s1")
	})
	if err != nil {
		t.Fatal(err)
	}

	if _, err := tx.ExecContext(ctx, dml2); err != nil {
		t.Fatal(err)
	}

	// First replay attempt will abort.
	server.TestSpanner.PutExecutionTime(testutil.MethodExecuteStreamingSql, testutil.SimulatedExecutionTime{
		Errors: []error{status.Error(codes.Aborted, "Aborted")},
	})
	// Subsequent retry will see changed results (concurrent modification).
	secondResult := &testutil.StatementResult{
		Type:        testutil.StatementResultUpdateCount,
		UpdateCount: 2,
	}
	_ = server.TestSpanner.PutStatementResult(dml1, secondResult)

	err = dbConn.Raw(func(driverConn any) error {
		c := driverConn.(*conn)
		return c.tx.RollbackToSavepoint(ctx, "s1")
	})
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if g, w := err, ErrAbortedDueToConcurrentModification; g != w {
		t.Errorf("error mismatch\nGot:  %v\nWant: %v", g, w)
	}

	// Verify that the underlying transaction was cleaned up.
	err = dbConn.Raw(func(driverConn any) error {
		c := driverConn.(*conn)
		rwTx, ok := c.tx.contextTransaction.(*readWriteTransaction)
		if !ok {
			return fmt.Errorf("unexpected transaction type: %T", c.tx.contextTransaction)
		}
		if rwTx.rwTx != nil {
			return fmt.Errorf("expected underlying Spanner transaction to be nil after failed replay")
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	_ = tx.Rollback()
}

func TestSavepoint_RetriesDisabled_NotSupported(t *testing.T) {
	t.Parallel()

	db, _, teardown := setupTestDBConnectionWithParams(t, "retryAbortsInternally=false;minSessions=1;maxSessions=1")
	defer teardown()
	ctx := context.Background()

	dbConn, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer silentClose(dbConn)

	tx, err := dbConn.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		t.Fatal(err)
	}

	err = dbConn.Raw(func(driverConn any) error {
		c := driverConn.(*conn)

		if err := c.tx.Savepoint("s1"); err == nil {
			t.Fatal("expected error creating savepoint when retries disabled, got nil")
		} else if g, w := status.Code(err), codes.FailedPrecondition; g != w {
			t.Errorf("status code mismatch for Savepoint\nGot:  %v\nWant: %v", g, w)
		}

		if err := c.tx.RollbackToSavepoint(ctx, "s1"); err == nil {
			t.Fatal("expected error rolling back to savepoint when retries disabled, got nil")
		} else if g, w := status.Code(err), codes.FailedPrecondition; g != w {
			t.Errorf("status code mismatch for RollbackToSavepoint\nGot:  %v\nWant: %v", g, w)
		}

		if err := c.tx.ReleaseSavepoint("s1"); err == nil {
			t.Fatal("expected error releasing savepoint when retries disabled, got nil")
		} else if g, w := status.Code(err), codes.FailedPrecondition; g != w {
			t.Errorf("status code mismatch for ReleaseSavepoint\nGot:  %v\nWant: %v", g, w)
		}

		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	_ = tx.Rollback()
}

func TestSavepoint_RollbackWhenBackendTxNil(t *testing.T) {
	t.Parallel()

	db, server, teardown := setupTestDBConnection(t)
	defer teardown()
	ctx := context.Background()

	dbConn, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer silentClose(dbConn)

	tx, err := dbConn.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		_ = tx.Rollback()
	}()

	if _, err := tx.ExecContext(ctx, testutil.UpdateSingersSetLastName); err != nil {
		t.Fatal(err)
	}

	err = dbConn.Raw(func(driverConn any) error {
		c := driverConn.(*conn)
		return c.tx.Savepoint("s1")
	})
	if err != nil {
		t.Fatalf("failed to create savepoint: %v", err)
	}

	if _, err := tx.ExecContext(ctx, testutil.UpdateBarSetFoo); err != nil {
		t.Fatalf("failed to execute second statement: %v", err)
	}

	err = dbConn.Raw(func(driverConn any) error {
		c := driverConn.(*conn)
		// Simulate PostgreSQL error state where a statement failure invoked
		// rollbackBackendTransaction(), closing and nilling out the backend Spanner transaction.
		c.tx.rollbackBackendTransaction()

		// RollbackToSavepoint must not fail with "underlying transaction is closed or aborted",
		// but instead must recreate the backend transaction and replay statements up to s1.
		return c.tx.RollbackToSavepoint(ctx, "s1")
	})
	if err != nil {
		t.Fatalf("failed to rollback to savepoint after backend rollback: %v", err)
	}

	if err := tx.Commit(); err != nil {
		t.Fatalf("failed to commit transaction after savepoint rollback: %v", err)
	}

	// Verify executed statements on mock server:
	// Initial attempt: UpdateSingersSetLastName (1), UpdateBarSetFoo (2)
	// Replay during rollback to s1: UpdateSingersSetLastName (3)
	// Total: 3 ExecuteSqlRequests
	requests := server.TestSpanner.DrainRequestsFromServer()
	executeRequests := testutil.RequestsOfType(requests, reflect.TypeOf(&spannerpb.ExecuteSqlRequest{}))
	if g, w := len(executeRequests), 3; g != w {
		t.Fatalf("executeRequests count mismatch\nGot:  %v\nWant: %v", g, w)
	}
	if g, w := executeRequests[2].(*spannerpb.ExecuteSqlRequest).Sql, testutil.UpdateSingersSetLastName; g != w {
		t.Errorf("replayed statement mismatch\nGot:  %v\nWant: %v", g, w)
	}
}

func TestSavepoint_PreservesTransactionTag(t *testing.T) {
	t.Parallel()

	db, server, teardown := setupTestDBConnection(t)
	defer teardown()
	ctx := context.Background()

	dbConn, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer silentClose(dbConn)

	expectedTag := "my_savepoint_tag"
	if _, err := dbConn.ExecContext(ctx, fmt.Sprintf("SET TRANSACTION_TAG = '%s'", expectedTag)); err != nil {
		t.Fatal(err)
	}

	tx, err := dbConn.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		_ = tx.Rollback()
	}()

	if _, err := tx.ExecContext(ctx, testutil.UpdateSingersSetLastName); err != nil {
		t.Fatal(err)
	}

	err = dbConn.Raw(func(driverConn any) error {
		c := driverConn.(*conn)
		return c.tx.Savepoint("s1")
	})
	if err != nil {
		t.Fatalf("failed to create savepoint: %v", err)
	}

	if _, err := tx.ExecContext(ctx, testutil.UpdateBarSetFoo); err != nil {
		t.Fatalf("failed to execute second statement: %v", err)
	}

	err = dbConn.Raw(func(driverConn any) error {
		c := driverConn.(*conn)
		return c.tx.RollbackToSavepoint(ctx, "s1")
	})
	if err != nil {
		t.Fatalf("failed to rollback to savepoint: %v", err)
	}

	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}

	// Verify that replayed ExecuteSqlRequest and CommitRequest both carry the transaction tag.
	requests := server.TestSpanner.DrainRequestsFromServer()
	executeRequests := testutil.RequestsOfType(requests, reflect.TypeOf(&spannerpb.ExecuteSqlRequest{}))
	if g, w := len(executeRequests), 3; g != w {
		t.Fatalf("executeRequests count mismatch\nGot:  %v\nWant: %v", g, w)
	}
	// Index 2 is the replayed statement
	if g, w := executeRequests[2].(*spannerpb.ExecuteSqlRequest).RequestOptions.TransactionTag, expectedTag; g != w {
		t.Errorf("replayed statement transaction tag mismatch\nGot:  %v\nWant: %v", g, w)
	}

	commitRequests := testutil.RequestsOfType(requests, reflect.TypeOf(&spannerpb.CommitRequest{}))
	if g, w := len(commitRequests), 1; g != w {
		t.Fatalf("commitRequests count mismatch\nGot:  %v\nWant: %v", g, w)
	}
	if g, w := commitRequests[0].(*spannerpb.CommitRequest).RequestOptions.TransactionTag, expectedTag; g != w {
		t.Errorf("commit request transaction tag mismatch\nGot:  %v\nWant: %v", g, w)
	}
}

func TestSavepoint_AtStartOfTransaction(t *testing.T) {
	t.Parallel()

	db, server, teardown := setupTestDBConnection(t)
	defer teardown()
	ctx := context.Background()

	dbConn, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer silentClose(dbConn)

	tx, err := dbConn.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		_ = tx.Rollback()
	}()

	// Create savepoint at the very start of the transaction before any statement has run.
	err = dbConn.Raw(func(driverConn any) error {
		c := driverConn.(*conn)
		return c.tx.Savepoint("s1")
	})
	if err != nil {
		t.Fatalf("failed to create savepoint at start: %v", err)
	}

	if _, err := tx.ExecContext(ctx, testutil.UpdateSingersSetLastName); err != nil {
		t.Fatalf("failed to execute first statement: %v", err)
	}

	// Rollback to s1 (statementCount == 0, so no statements replayed).
	err = dbConn.Raw(func(driverConn any) error {
		c := driverConn.(*conn)
		return c.tx.RollbackToSavepoint(ctx, "s1")
	})
	if err != nil {
		t.Fatalf("failed to rollback to savepoint at start: %v", err)
	}

	// Execute a new statement on the recreated transaction.
	if _, err := tx.ExecContext(ctx, testutil.UpdateBarSetFoo); err != nil {
		t.Fatalf("failed to execute statement after rollback: %v", err)
	}

	if err := tx.Commit(); err != nil {
		t.Fatalf("failed to commit: %v", err)
	}

	// On the server, we should see UpdateSingersSetLastName (initial), then UpdateBarSetFoo (new tx),
	// followed by a CommitRequest containing only UpdateBarSetFoo.
	requests := server.TestSpanner.DrainRequestsFromServer()
	commitRequests := testutil.RequestsOfType(requests, reflect.TypeOf(&spannerpb.CommitRequest{}))
	if g, w := len(commitRequests), 1; g != w {
		t.Fatalf("commit requests count mismatch\nGot:  %v\nWant: %v", g, w)
	}
}

func TestSavepoint_DuplicateName_GoogleSql(t *testing.T) {
	t.Parallel()

	db, _, teardown := setupTestDBConnection(t)
	defer teardown()
	ctx := context.Background()

	dbConn, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer silentClose(dbConn)

	tx, err := dbConn.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		_ = tx.Rollback()
	}()

	err = dbConn.Raw(func(driverConn any) error {
		c := driverConn.(*conn)

		if err := c.tx.Savepoint("s1"); err != nil {
			t.Fatalf("first savepoint failed: %v", err)
		}
		if err := c.tx.Savepoint("s2"); err != nil {
			t.Fatalf("second savepoint failed: %v", err)
		}

		// GoogleSQL disallows duplicate savepoint names.
		if err := c.tx.Savepoint("s1"); err == nil {
			t.Fatal("expected error creating duplicate savepoint 's1', got nil")
		} else if g, w := status.Code(err), codes.InvalidArgument; g != w {
			t.Errorf("status code mismatch for duplicate s1\nGot:  %v\nWant: %v", g, w)
		}

		// In GoogleSQL, savepoint names are case-insensitive.
		if err := c.tx.Savepoint("S1"); err == nil {
			t.Fatal("expected error creating case-insensitive duplicate savepoint 'S1', got nil")
		} else if g, w := status.Code(err), codes.InvalidArgument; g != w {
			t.Errorf("status code mismatch for duplicate S1\nGot:  %v\nWant: %v", g, w)
		}

		// Release s2. Now creating s2 should succeed again.
		if err := c.tx.ReleaseSavepoint("s2"); err != nil {
			t.Fatalf("release s2 failed: %v", err)
		}
		if err := c.tx.Savepoint("s2"); err != nil {
			t.Fatalf("savepoint s2 after release failed: %v", err)
		}

		// Rolling back to s1 invalidates s2.
		if err := c.tx.RollbackToSavepoint(ctx, "s1"); err != nil {
			t.Fatalf("rollback to s1 failed: %v", err)
		}

		// Creating s2 again should succeed since s2 was pruned by rolling back to s1.
		if err := c.tx.Savepoint("s2"); err != nil {
			t.Fatalf("savepoint s2 after rollback failed: %v", err)
		}

		// s1 still exists, so creating s1 still fails.
		if err := c.tx.Savepoint("s1"); err == nil {
			t.Fatal("expected error creating duplicate s1 after rollback, got nil")
		} else if g, w := status.Code(err), codes.InvalidArgument; g != w {
			t.Errorf("status code mismatch for duplicate s1 after rollback\nGot:  %v\nWant: %v", g, w)
		}

		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

// TestSavepoint_ShadowedName verifies that the PostgreSQL dialect supports
// duplicate/shadowed savepoint names, matching PostgreSQL wire protocol semantics.
func TestSavepoint_ShadowedName(t *testing.T) {
	t.Parallel()

	db, server, teardown := setupTestDBConnectionWithParamsAndDialect(t, "", databasepb.DatabaseDialect_POSTGRESQL)
	defer teardown()
	ctx := context.Background()

	dbConn, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer silentClose(dbConn)

	tx, err := dbConn.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		_ = tx.Rollback()
	}()

	// Execute DML 1
	if _, err := tx.ExecContext(ctx, testutil.UpdateSingersSetLastName); err != nil {
		t.Fatal(err)
	}

	// Create outer savepoint s1
	err = dbConn.Raw(func(driverConn any) error {
		c := driverConn.(*conn)
		return c.tx.Savepoint("s1")
	})
	if err != nil {
		t.Fatal(err)
	}

	// Execute DML 2
	if _, err := tx.ExecContext(ctx, testutil.UpdateBarSetFoo); err != nil {
		t.Fatal(err)
	}

	// Create inner savepoint s1 (shadowing outer s1)
	err = dbConn.Raw(func(driverConn any) error {
		c := driverConn.(*conn)
		return c.tx.Savepoint("s1")
	})
	if err != nil {
		t.Fatal(err)
	}

	// Execute DML 3 (UpdateSingersSetLastName again)
	if _, err := tx.ExecContext(ctx, testutil.UpdateSingersSetLastName); err != nil {
		t.Fatal(err)
	}

	// Rollback to s1 (should roll back to inner s1, so DML 3 is dropped, DML 1 and DML 2 remain)
	err = dbConn.Raw(func(driverConn any) error {
		c := driverConn.(*conn)
		return c.tx.RollbackToSavepoint(ctx, "s1")
	})
	if err != nil {
		t.Fatalf("failed to rollback to inner s1: %v", err)
	}

	// Release s1 (releases inner s1, unmasking outer s1)
	err = dbConn.Raw(func(driverConn any) error {
		c := driverConn.(*conn)
		return c.tx.ReleaseSavepoint("s1")
	})
	if err != nil {
		t.Fatalf("failed to release inner s1: %v", err)
	}

	// Rollback to s1 (should now roll back to outer s1, so DML 2 is dropped, only DML 1 remains)
	err = dbConn.Raw(func(driverConn any) error {
		c := driverConn.(*conn)
		return c.tx.RollbackToSavepoint(ctx, "s1")
	})
	if err != nil {
		t.Fatalf("failed to rollback to outer s1: %v", err)
	}

	if err := tx.Commit(); err != nil {
		t.Fatalf("failed to commit: %v", err)
	}

	requests := server.TestSpanner.DrainRequestsFromServer()
	commitRequests := testutil.RequestsOfType(requests, reflect.TypeOf(&spannerpb.CommitRequest{}))
	if g, w := len(commitRequests), 1; g != w {
		t.Fatalf("commitRequests count mismatch\nGot:  %v\nWant: %v", g, w)
	}
}

func TestSavepoint_QueryReplay(t *testing.T) {
	t.Parallel()

	db, server, teardown := setupTestDBConnection(t)
	defer teardown()
	ctx := context.Background()

	dbConn, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer silentClose(dbConn)

	tx, err := dbConn.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		_ = tx.Rollback()
	}()

	// Execute a SELECT query before savepoint.
	rows, err := tx.QueryContext(ctx, testutil.SelectFooFromBar)
	if err != nil {
		t.Fatalf("query failed: %v", err)
	}
	rowCount := 0
	for rows.Next() {
		rowCount++
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows err: %v", err)
	}
	rows.Close()

	// Create savepoint s1.
	err = dbConn.Raw(func(driverConn any) error {
		c := driverConn.(*conn)
		return c.tx.Savepoint("s1")
	})
	if err != nil {
		t.Fatalf("failed to create savepoint: %v", err)
	}

	// Execute DML after savepoint.
	if _, err := tx.ExecContext(ctx, testutil.UpdateBarSetFoo); err != nil {
		t.Fatalf("dml failed: %v", err)
	}

	// Rollback to savepoint s1. The query must be replayed and checksum-verified on the new transaction.
	err = dbConn.Raw(func(driverConn any) error {
		c := driverConn.(*conn)
		return c.tx.RollbackToSavepoint(ctx, "s1")
	})
	if err != nil {
		t.Fatalf("failed to rollback to savepoint: %v", err)
	}

	if err := tx.Commit(); err != nil {
		t.Fatalf("failed to commit: %v", err)
	}

	requests := server.TestSpanner.DrainRequestsFromServer()
	executeRequests := testutil.RequestsOfType(requests, reflect.TypeOf(&spannerpb.ExecuteSqlRequest{}))
	// 1: initial SELECT, 2: UpdateBarSetFoo, 3: replayed SELECT
	if g, w := len(executeRequests), 3; g != w {
		t.Fatalf("executeRequests count mismatch\nGot:  %v\nWant: %v", g, w)
	}
	if g, w := executeRequests[2].(*spannerpb.ExecuteSqlRequest).Sql, testutil.SelectFooFromBar; g != w {
		t.Errorf("replayed query mismatch\nGot:  %v\nWant: %v", g, w)
	}
}

func TestSavepoint_PostgreSQLErrorStateRecovery(t *testing.T) {
	t.Parallel()

	db, _, teardown := setupTestDBConnectionWithParamsAndDialect(t, "", databasepb.DatabaseDialect_POSTGRESQL)
	defer teardown()
	ctx := context.Background()

	dbConn, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer silentClose(dbConn)

	// Explicitly enable EnforceInErrorState.
	err = dbConn.Raw(func(driverConn any) error {
		c := driverConn.(*conn)
		return c.SetInErrorTxBehavior(connectionstate.InErrorTxBehaviorEnforceInErrorState)
	})
	if err != nil {
		t.Fatal(err)
	}

	tx, err := dbConn.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		_ = tx.Rollback()
	}()

	if _, err := tx.ExecContext(ctx, testutil.UpdateSingersSetLastName); err != nil {
		t.Fatal(err)
	}

	err = dbConn.Raw(func(driverConn any) error {
		c := driverConn.(*conn)
		return c.tx.Savepoint("s1")
	})
	if err != nil {
		t.Fatal(err)
	}

	// Trigger a statement failure by simulating backend rollback and error state
	err = dbConn.Raw(func(driverConn any) error {
		c := driverConn.(*conn)
		c.state.SetTransactionFailed()
		c.tx.rollbackBackendTransaction()
		if g, w := c.state.TransactionState(), connectionstate.TransactionStateFailed; g != w {
			t.Fatalf("transaction state before rollback mismatch\nGot:  %v\nWant: %v", g, w)
		}
		return c.tx.RollbackToSavepoint(ctx, "s1")
	})
	if err != nil {
		t.Fatalf("failed to rollback to savepoint from error state: %v", err)
	}

	// Verify that state was restored to active ('T')
	err = dbConn.Raw(func(driverConn any) error {
		c := driverConn.(*conn)
		if g, w := c.state.TransactionState(), connectionstate.TransactionStateTransaction; g != w {
			t.Fatalf("transaction state after savepoint rollback mismatch\nGot:  %v\nWant: %v", g, w)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	// Subsequent statement execution must succeed (not fail with error state).
	if _, err := tx.ExecContext(ctx, testutil.UpdateBarSetFoo); err != nil {
		t.Fatalf("statement after error recovery failed: %v", err)
	}

	// Commit must succeed (not be rolled back by failed state handler).
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit after error recovery failed: %v", err)
	}
}

func TestSavepoint_PostgreSQLCaseSensitive(t *testing.T) {
	t.Parallel()

	db, _, teardown := setupTestDBConnectionWithParamsAndDialect(t, "", databasepb.DatabaseDialect_POSTGRESQL)
	defer teardown()
	ctx := context.Background()

	dbConn, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer silentClose(dbConn)

	tx, err := dbConn.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		_ = tx.Rollback()
	}()

	err = dbConn.Raw(func(driverConn any) error {
		c := driverConn.(*conn)
		if err := c.tx.Savepoint("my_sp"); err != nil {
			return err
		}

		// In PostgreSQL, unquoted identifiers match exact case as stored; MY_SP must not match my_sp.
		if err := c.tx.RollbackToSavepoint(ctx, "MY_SP"); err == nil {
			t.Fatal("expected error rolling back to uppercase savepoint in PG, got nil")
		} else if g, w := ToPGSQLState(err), "3B001"; g != w {
			t.Errorf("sqlstate mismatch\nGot:  %v\nWant: %v", g, w)
		}

		if err := c.tx.ReleaseSavepoint("MY_SP"); err == nil {
			t.Fatal("expected error releasing uppercase savepoint in PG, got nil")
		} else if g, w := ToPGSQLState(err), "3B001"; g != w {
			t.Errorf("sqlstate mismatch\nGot:  %v\nWant: %v", g, w)
		}

		// Correct case succeeds.
		return c.tx.RollbackToSavepoint(ctx, "my_sp")
	})
	if err != nil {
		t.Fatalf("savepoint operation failed: %v", err)
	}

	if err := tx.Commit(); err != nil {
		t.Fatalf("commit failed: %v", err)
	}
}

func TestSavepoint_UnactivatedTransactionClosedSafety(t *testing.T) {
	t.Parallel()

	db, _, teardown := setupTestDBConnection(t)
	defer teardown()
	ctx := context.Background()

	dbConn, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer silentClose(dbConn)

	tx, err := dbConn.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		t.Fatal(err)
	}

	// Commit before activating.
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}

	err = dbConn.Raw(func(driverConn any) error {
		c := driverConn.(*conn)
		// c.tx was closed by tx.Commit(). Calling Savepoint on an unactivated closed transaction
		// must return codes.FailedPrecondition and not panic.
		d := &delegatingTransaction{
			conn:   c,
			closed: true,
		}
		if g, w := spanner.ErrCode(d.Savepoint("s1")), codes.FailedPrecondition; g != w {
			t.Errorf("Savepoint on closed unactivated tx mismatch\nGot:  %v\nWant: %v", g, w)
		}
		if g, w := spanner.ErrCode(d.RollbackToSavepoint(ctx, "s1")), codes.FailedPrecondition; g != w {
			t.Errorf("RollbackToSavepoint on closed unactivated tx mismatch\nGot:  %v\nWant: %v", g, w)
		}
		if g, w := spanner.ErrCode(d.ReleaseSavepoint("s1")), codes.FailedPrecondition; g != w {
			t.Errorf("ReleaseSavepoint on closed unactivated tx mismatch\nGot:  %v\nWant: %v", g, w)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestSavepoint_DiscardedQueryCallsStop(t *testing.T) {
	t.Parallel()

	db, server, teardown := setupTestDBConnection(t)
	defer teardown()
	ctx := context.Background()

	dbConn, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer silentClose(dbConn)

	tx, err := dbConn.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback() }()

	// DML before savepoint.
	if _, err := tx.ExecContext(ctx, testutil.UpdateBarSetFoo); err != nil {
		t.Fatalf("failed to execute DML before savepoint: %v", err)
	}

	err = dbConn.Raw(func(driverConn any) error {
		c := driverConn.(*conn)
		return c.tx.Savepoint("s1")
	})
	if err != nil {
		t.Fatalf("failed to create savepoint: %v", err)
	}

	// Query after savepoint (this adds a checksumRowIterator with Stop() to tx.statements).
	rows, err := tx.QueryContext(ctx, testutil.SelectFooFromBar)
	if err != nil {
		t.Fatalf("failed to query after savepoint: %v", err)
	}
	for rows.Next() {
	}
	if err := rows.Close(); err != nil {
		t.Fatalf("failed to close rows: %v", err)
	}

	// Rollback to savepoint: this calls Stop() on the discarded checksumRowIterator.
	err = dbConn.Raw(func(driverConn any) error {
		c := driverConn.(*conn)
		return c.tx.RollbackToSavepoint(ctx, "s1")
	})
	if err != nil {
		t.Fatalf("failed to rollback to savepoint: %v", err)
	}

	// Execute another DML after rollback to savepoint.
	if _, err := tx.ExecContext(ctx, testutil.UpdateBarSetFoo); err != nil {
		t.Fatalf("failed to execute DML after rollback: %v", err)
	}

	if err := tx.Commit(); err != nil {
		t.Fatalf("failed to commit transaction: %v", err)
	}

	requests := server.TestSpanner.DrainRequestsFromServer()
	commitRequests := testutil.RequestsOfType(requests, reflect.TypeOf(&spannerpb.CommitRequest{}))
	if g, w := len(commitRequests), 1; g != w {
		t.Errorf("CommitRequest count mismatch\nGot:  %v\nWant: %v", g, w)
	}
}

func TestSavepoint_StartOfTransaction_ImmediateRollbackAndRelease(t *testing.T) {
	t.Parallel()

	db, server, teardown := setupTestDBConnection(t)
	defer teardown()
	ctx := context.Background()

	t.Run("ImmediateRollbackAtStart", func(t *testing.T) {
		dbConn, err := db.Conn(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer silentClose(dbConn)

		tx, err := dbConn.BeginTx(ctx, &sql.TxOptions{})
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = tx.Rollback() }()

		// Create savepoint at start with 0 statements.
		err = dbConn.Raw(func(driverConn any) error {
			c := driverConn.(*conn)
			return c.tx.Savepoint("s_start")
		})
		if err != nil {
			t.Fatalf("failed to create savepoint at start: %v", err)
		}

		// Immediate rollback with 0 statements.
		err = dbConn.Raw(func(driverConn any) error {
			c := driverConn.(*conn)
			return c.tx.RollbackToSavepoint(ctx, "s_start")
		})
		if err != nil {
			t.Fatalf("failed to rollback to savepoint at start: %v", err)
		}

		if _, err := tx.ExecContext(ctx, testutil.UpdateBarSetFoo); err != nil {
			t.Fatalf("failed to execute DML after start rollback: %v", err)
		}

		if err := tx.Commit(); err != nil {
			t.Fatalf("failed to commit: %v", err)
		}
	})

	t.Run("ImmediateReleaseAtStart", func(t *testing.T) {
		dbConn, err := db.Conn(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer silentClose(dbConn)

		tx, err := dbConn.BeginTx(ctx, &sql.TxOptions{})
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = tx.Rollback() }()

		// Create savepoint at start with 0 statements.
		err = dbConn.Raw(func(driverConn any) error {
			c := driverConn.(*conn)
			return c.tx.Savepoint("s_start")
		})
		if err != nil {
			t.Fatalf("failed to create savepoint at start: %v", err)
		}

		// Immediate release with 0 statements.
		err = dbConn.Raw(func(driverConn any) error {
			c := driverConn.(*conn)
			return c.tx.ReleaseSavepoint("s_start")
		})
		if err != nil {
			t.Fatalf("failed to release savepoint at start: %v", err)
		}

		if _, err := tx.ExecContext(ctx, testutil.UpdateBarSetFoo); err != nil {
			t.Fatalf("failed to execute DML after start release: %v", err)
		}

		if err := tx.Commit(); err != nil {
			t.Fatalf("failed to commit: %v", err)
		}
	})
	server.TestSpanner.DrainRequestsFromServer()
}

func TestSavepoint_EndOfTransaction(t *testing.T) {
	t.Parallel()

	db, server, teardown := setupTestDBConnection(t)
	defer teardown()
	ctx := context.Background()

	t.Run("SavepointAtEndThenCommit", func(t *testing.T) {
		dbConn, err := db.Conn(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer silentClose(dbConn)

		tx, err := dbConn.BeginTx(ctx, &sql.TxOptions{})
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = tx.Rollback() }()

		if _, err := tx.ExecContext(ctx, testutil.UpdateBarSetFoo); err != nil {
			t.Fatalf("failed to execute DML 1: %v", err)
		}
		if _, err := tx.ExecContext(ctx, testutil.UpdateBarSetFoo); err != nil {
			t.Fatalf("failed to execute DML 2: %v", err)
		}

		err = dbConn.Raw(func(driverConn any) error {
			c := driverConn.(*conn)
			return c.tx.Savepoint("s_end")
		})
		if err != nil {
			t.Fatalf("failed to create savepoint at end: %v", err)
		}

		if err := tx.Commit(); err != nil {
			t.Fatalf("failed to commit: %v", err)
		}
	})

	t.Run("SavepointAtEndThenReleaseThenCommit", func(t *testing.T) {
		dbConn, err := db.Conn(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer silentClose(dbConn)

		tx, err := dbConn.BeginTx(ctx, &sql.TxOptions{})
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = tx.Rollback() }()

		if _, err := tx.ExecContext(ctx, testutil.UpdateBarSetFoo); err != nil {
			t.Fatalf("failed to execute DML: %v", err)
		}

		err = dbConn.Raw(func(driverConn any) error {
			c := driverConn.(*conn)
			if err := c.tx.Savepoint("s_end"); err != nil {
				return err
			}
			return c.tx.ReleaseSavepoint("s_end")
		})
		if err != nil {
			t.Fatalf("failed savepoint end and release: %v", err)
		}

		if err := tx.Commit(); err != nil {
			t.Fatalf("failed to commit: %v", err)
		}
	})

	t.Run("SavepointAtEndThenRollbackToEnd", func(t *testing.T) {
		dbConn, err := db.Conn(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer silentClose(dbConn)

		tx, err := dbConn.BeginTx(ctx, &sql.TxOptions{})
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = tx.Rollback() }()

		if _, err := tx.ExecContext(ctx, testutil.UpdateBarSetFoo); err != nil {
			t.Fatalf("failed to execute DML: %v", err)
		}

		err = dbConn.Raw(func(driverConn any) error {
			c := driverConn.(*conn)
			if err := c.tx.Savepoint("s_end"); err != nil {
				return err
			}
			// Rollback to end: statementCount == len(statements). Replays all statements.
			return c.tx.RollbackToSavepoint(ctx, "s_end")
		})
		if err != nil {
			t.Fatalf("failed rollback to savepoint at end: %v", err)
		}

		if err := tx.Commit(); err != nil {
			t.Fatalf("failed to commit: %v", err)
		}
	})

	t.Run("RiskyStatementFailureAtEndRolledBack", func(t *testing.T) {
		dbConn, err := db.Conn(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer silentClose(dbConn)

		tx, err := dbConn.BeginTx(ctx, &sql.TxOptions{})
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = tx.Rollback() }()

		if _, err := tx.ExecContext(ctx, testutil.UpdateBarSetFoo); err != nil {
			t.Fatalf("failed to execute DML 1: %v", err)
		}

		err = dbConn.Raw(func(driverConn any) error {
			c := driverConn.(*conn)
			return c.tx.Savepoint("s_checkpoint")
		})
		if err != nil {
			t.Fatalf("failed to create savepoint: %v", err)
		}

		// Execute risky statement that fails.
		server.TestSpanner.PutExecutionTime(testutil.MethodExecuteSql, testutil.SimulatedExecutionTime{
			Errors: []error{status.Error(codes.InvalidArgument, "invalid statement syntax")},
		})
		_, err = tx.ExecContext(ctx, "UPDATE TableWithSyntaxError")
		if err == nil {
			t.Fatal("expected risky statement to fail, but got nil")
		}

		// Rollback risky statement to savepoint.
		err = dbConn.Raw(func(driverConn any) error {
			c := driverConn.(*conn)
			return c.tx.RollbackToSavepoint(ctx, "s_checkpoint")
		})
		if err != nil {
			t.Fatalf("failed to rollback risky statement: %v", err)
		}

		if err := tx.Commit(); err != nil {
			t.Fatalf("failed to commit: %v", err)
		}
	})
	server.TestSpanner.DrainRequestsFromServer()
}

func TestSavepoint_NestedMultiLevelRollback(t *testing.T) {
	t.Parallel()

	db, server, teardown := setupTestDBConnection(t)
	defer teardown()
	ctx := context.Background()

	dbConn, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer silentClose(dbConn)

	tx, err := dbConn.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback() }()

	// DML 1
	if _, err := tx.ExecContext(ctx, testutil.UpdateBarSetFoo); err != nil {
		t.Fatalf("failed DML 1: %v", err)
	}

	err = dbConn.Raw(func(driverConn any) error {
		c := driverConn.(*conn)
		return c.tx.Savepoint("s1")
	})
	if err != nil {
		t.Fatalf("failed Savepoint s1: %v", err)
	}

	// DML 2
	if _, err := tx.ExecContext(ctx, testutil.UpdateBarSetFoo); err != nil {
		t.Fatalf("failed DML 2: %v", err)
	}

	err = dbConn.Raw(func(driverConn any) error {
		c := driverConn.(*conn)
		return c.tx.Savepoint("s2")
	})
	if err != nil {
		t.Fatalf("failed Savepoint s2: %v", err)
	}

	// DML 3
	if _, err := tx.ExecContext(ctx, testutil.UpdateBarSetFoo); err != nil {
		t.Fatalf("failed DML 3: %v", err)
	}

	// Rollback to s2 (discards DML 3)
	err = dbConn.Raw(func(driverConn any) error {
		c := driverConn.(*conn)
		return c.tx.RollbackToSavepoint(ctx, "s2")
	})
	if err != nil {
		t.Fatalf("failed RollbackToSavepoint s2: %v", err)
	}

	// DML 4
	if _, err := tx.ExecContext(ctx, testutil.UpdateBarSetFoo); err != nil {
		t.Fatalf("failed DML 4: %v", err)
	}

	// Rollback to s1 (discards DML 4, s2, DML 2)
	err = dbConn.Raw(func(driverConn any) error {
		c := driverConn.(*conn)
		return c.tx.RollbackToSavepoint(ctx, "s1")
	})
	if err != nil {
		t.Fatalf("failed RollbackToSavepoint s1: %v", err)
	}

	// DML 5
	if _, err := tx.ExecContext(ctx, testutil.UpdateBarSetFoo); err != nil {
		t.Fatalf("failed DML 5: %v", err)
	}

	if err := tx.Commit(); err != nil {
		t.Fatalf("failed to commit: %v", err)
	}

	requests := server.TestSpanner.DrainRequestsFromServer()
	commitRequests := testutil.RequestsOfType(requests, reflect.TypeOf(&spannerpb.CommitRequest{}))
	if g, w := len(commitRequests), 1; g != w {
		t.Errorf("CommitRequest count mismatch\nGot:  %v\nWant: %v", g, w)
	}
}

func TestSavepoint_SliceBoundsDefensiveRecovery(t *testing.T) {
	t.Parallel()

	db, _, teardown := setupTestDBConnection(t)
	defer teardown()
	ctx := context.Background()

	dbConn, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer silentClose(dbConn)

	tx, err := dbConn.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback() }()

	// DML 1
	if _, err := tx.ExecContext(ctx, testutil.UpdateBarSetFoo); err != nil {
		t.Fatalf("failed DML 1: %v", err)
	}

	err = dbConn.Raw(func(driverConn any) error {
		c := driverConn.(*conn)
		return c.tx.Savepoint("s1")
	})
	if err != nil {
		t.Fatalf("failed Savepoint s1: %v", err)
	}

	// DML 2
	if _, err := tx.ExecContext(ctx, testutil.UpdateBarSetFoo); err != nil {
		t.Fatalf("failed DML 2: %v", err)
	}

	err = dbConn.Raw(func(driverConn any) error {
		c := driverConn.(*conn)
		return c.tx.Savepoint("s2")
	})
	if err != nil {
		t.Fatalf("failed Savepoint s2: %v", err)
	}

	// Verify rollback to s1 invalidates s2 immediately.
	err = dbConn.Raw(func(driverConn any) error {
		c := driverConn.(*conn)
		if err := c.tx.RollbackToSavepoint(ctx, "s1"); err != nil {
			return err
		}
		// Subsequent attempt to rollback to s2 must fail because s2 was invalidated.
		err := c.tx.RollbackToSavepoint(ctx, "s2")
		if g, w := spanner.ErrCode(err), codes.FailedPrecondition; g != w {
			t.Errorf("RollbackToSavepoint s2 after rollback to s1 error code mismatch\nGot:  %v\nWant: %v", g, w)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	// Defensive bounds checking directly on readWriteTransaction.
	err = dbConn.Raw(func(driverConn any) error {
		c := driverConn.(*conn)
		rwTx := c.tx.contextTransaction.(*readWriteTransaction)
		if err := rwTx.rollbackToSavepointCheckpoint(ctx, 9999, 0); spanner.ErrCode(err) != codes.FailedPrecondition {
			t.Errorf("out-of-range statement count error code mismatch\nGot:  %v\nWant: %v", spanner.ErrCode(err), codes.FailedPrecondition)
		}
		if err := rwTx.rollbackToSavepointCheckpoint(ctx, -1, 0); spanner.ErrCode(err) != codes.FailedPrecondition {
			t.Errorf("negative statement count error code mismatch\nGot:  %v\nWant: %v", spanner.ErrCode(err), codes.FailedPrecondition)
		}
		if err := rwTx.rollbackToSavepointCheckpoint(ctx, 0, 9999); spanner.ErrCode(err) != codes.FailedPrecondition {
			t.Errorf("out-of-range mutation count error code mismatch\nGot:  %v\nWant: %v", spanner.ErrCode(err), codes.FailedPrecondition)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestSavepoint_BatchReadOnlyTransaction(t *testing.T) {
	t.Parallel()

	db, _, teardown := setupTestDBConnection(t)
	defer teardown()
	ctx := context.Background()

	dbConn, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer silentClose(dbConn)

	tx, err := dbConn.BeginTx(ctx, &sql.TxOptions{
		ReadOnly:  true,
		Isolation: sql.IsolationLevel(withBatchReadOnly(driver.IsolationLevel(sql.LevelDefault))),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback() }()

	err = dbConn.Raw(func(driverConn any) error {
		c := driverConn.(*conn)

		// Create savepoint on batch read-only transaction.
		if err := c.tx.Savepoint("s1"); err != nil {
			return err
		}

		roTx := c.tx.contextTransaction.(*readOnlyTransaction)
		origBoTx := roTx.boTx
		origRoTx := roTx.roTx
		if origBoTx == nil || origRoTx == nil {
			t.Fatal("expected boTx and roTx to be non-nil on batch read-only transaction")
		}

		// Rollback to savepoint is a no-op; boTx and roTx remain identical (same snapshot).
		if err := c.tx.RollbackToSavepoint(ctx, "s1"); err != nil {
			return err
		}

		if roTx.boTx != origBoTx {
			t.Error("expected boTx to remain unchanged after rollbackToSavepoint on batch read-only transaction")
		}
		if roTx.roTx != origRoTx {
			t.Error("expected roTx to remain unchanged after rollbackToSavepoint on batch read-only transaction")
		}

		// Release savepoint is also a clean no-op.
		if err := c.tx.ReleaseSavepoint("s1"); err != nil {
			return err
		}

		// Simulate backend closure.
		c.tx.rollbackBackendTransaction()

		// Operations after backend closure return FailedPrecondition.
		if g, w := spanner.ErrCode(c.tx.Savepoint("s2")), codes.FailedPrecondition; g != w {
			t.Errorf("Savepoint error code mismatch\nGot:  %v\nWant: %v", g, w)
		}
		if g, w := spanner.ErrCode(c.tx.RollbackToSavepoint(ctx, "s1")), codes.FailedPrecondition; g != w {
			t.Errorf("RollbackToSavepoint error code mismatch\nGot:  %v\nWant: %v", g, w)
		}
		if g, w := spanner.ErrCode(c.tx.ReleaseSavepoint("s1")), codes.FailedPrecondition; g != w {
			t.Errorf("ReleaseSavepoint error code mismatch\nGot:  %v\nWant: %v", g, w)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestSavepoint_AutoDmlBatch(t *testing.T) {
	t.Parallel()

	db, _, teardown := setupTestDBConnection(t)
	defer teardown()
	ctx := context.Background()

	dbConn, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer silentClose(dbConn)

	// Enable AUTO_BATCH_DML on connection.
	if _, err := dbConn.ExecContext(ctx, "SET AUTO_BATCH_DML = true"); err != nil {
		t.Fatalf("failed to enable AUTO_BATCH_DML: %v", err)
	}
	if _, err := dbConn.ExecContext(ctx, "SET AUTO_BATCH_DML_UPDATE_COUNT_VERIFICATION = false"); err != nil {
		t.Fatalf("failed to disable AUTO_BATCH_DML_UPDATE_COUNT_VERIFICATION: %v", err)
	}

	tx, err := dbConn.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback() }()

	// Execute DML: gets buffered into auto-batch.
	if _, err := tx.ExecContext(ctx, testutil.UpdateBarSetFoo); err != nil {
		t.Fatalf("failed to execute DML: %v", err)
	}

	err = dbConn.Raw(func(driverConn any) error {
		c := driverConn.(*conn)

		rwTx := c.tx.contextTransaction.(*readWriteTransaction)
		if rwTx.batch == nil || !rwTx.batch.automatic {
			t.Fatal("expected auto-batch to be active before Savepoint")
		}

		// Calling Savepoint flushes auto-batch.
		if err := c.tx.Savepoint("s1"); err != nil {
			return err
		}

		if rwTx.batch != nil {
			t.Fatal("expected auto-batch to be flushed after Savepoint")
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	// Buffer another DML in auto-batch.
	if _, err := tx.ExecContext(ctx, testutil.UpdateBarSetFoo); err != nil {
		t.Fatalf("failed to execute second DML: %v", err)
	}

	err = dbConn.Raw(func(driverConn any) error {
		c := driverConn.(*conn)

		rwTx := c.tx.contextTransaction.(*readWriteTransaction)
		if rwTx.batch == nil || !rwTx.batch.automatic {
			t.Fatal("expected auto-batch to be active before RollbackToSavepoint")
		}

		// Rollback to savepoint aborts active auto-batch.
		if err := c.tx.RollbackToSavepoint(ctx, "s1"); err != nil {
			return err
		}

		if rwTx.batch != nil {
			t.Fatal("expected auto-batch to be aborted after RollbackToSavepoint")
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	if err := tx.Commit(); err != nil {
		t.Fatalf("failed to commit: %v", err)
	}
}

func TestSavepoint_ActivatedTransactionClosedSafety(t *testing.T) {
	t.Parallel()

	db, _, teardown := setupTestDBConnection(t)
	defer teardown()
	ctx := context.Background()

	t.Run("AfterCommit", func(t *testing.T) {
		dbConn, err := db.Conn(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer silentClose(dbConn)

		tx, err := dbConn.BeginTx(ctx, &sql.TxOptions{})
		if err != nil {
			t.Fatal(err)
		}

		if _, err := tx.ExecContext(ctx, testutil.UpdateBarSetFoo); err != nil {
			t.Fatalf("failed to execute DML: %v", err)
		}

		var d *delegatingTransaction
		err = dbConn.Raw(func(driverConn any) error {
			c := driverConn.(*conn)
			d = c.tx
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}

		if err := tx.Commit(); err != nil {
			t.Fatalf("failed to commit: %v", err)
		}

		// Calling savepoint methods on closed activated transaction must fail with FailedPrecondition.
		if g, w := spanner.ErrCode(d.Savepoint("s1")), codes.FailedPrecondition; g != w {
			t.Errorf("Savepoint error code mismatch\nGot:  %v\nWant: %v", g, w)
		}
		if g, w := spanner.ErrCode(d.RollbackToSavepoint(ctx, "s1")), codes.FailedPrecondition; g != w {
			t.Errorf("RollbackToSavepoint error code mismatch\nGot:  %v\nWant: %v", g, w)
		}
		if g, w := spanner.ErrCode(d.ReleaseSavepoint("s1")), codes.FailedPrecondition; g != w {
			t.Errorf("ReleaseSavepoint error code mismatch\nGot:  %v\nWant: %v", g, w)
		}
	})

	t.Run("AfterRollback", func(t *testing.T) {
		dbConn, err := db.Conn(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer silentClose(dbConn)

		tx, err := dbConn.BeginTx(ctx, &sql.TxOptions{})
		if err != nil {
			t.Fatal(err)
		}

		if _, err := tx.ExecContext(ctx, testutil.UpdateBarSetFoo); err != nil {
			t.Fatalf("failed to execute DML: %v", err)
		}

		var d *delegatingTransaction
		err = dbConn.Raw(func(driverConn any) error {
			c := driverConn.(*conn)
			d = c.tx
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}

		if err := tx.Rollback(); err != nil {
			t.Fatalf("failed to rollback: %v", err)
		}

		if g, w := spanner.ErrCode(d.Savepoint("s1")), codes.FailedPrecondition; g != w {
			t.Errorf("Savepoint error code mismatch\nGot:  %v\nWant: %v", g, w)
		}
		if g, w := spanner.ErrCode(d.RollbackToSavepoint(ctx, "s1")), codes.FailedPrecondition; g != w {
			t.Errorf("RollbackToSavepoint error code mismatch\nGot:  %v\nWant: %v", g, w)
		}
		if g, w := spanner.ErrCode(d.ReleaseSavepoint("s1")), codes.FailedPrecondition; g != w {
			t.Errorf("ReleaseSavepoint error code mismatch\nGot:  %v\nWant: %v", g, w)
		}
	})
}

func TestSavepoint_RollbackManualBatchDML(t *testing.T) {
	t.Parallel()

	db, server, teardown := setupTestDBConnection(t)
	defer teardown()
	ctx := context.Background()

	dbConn, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer silentClose(dbConn)

	tx, err := dbConn.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		_ = tx.Rollback()
	}()

	// 1. Initial DML
	if _, err := tx.ExecContext(ctx, testutil.UpdateSingersSetLastName); err != nil {
		t.Fatalf("failed to execute initial DML: %v", err)
	}

	// 2. Savepoint s1
	err = dbConn.Raw(func(driverConn any) error {
		return driverConn.(*conn).tx.Savepoint("s1")
	})
	if err != nil {
		t.Fatalf("failed to create savepoint s1: %v", err)
	}

	// 3. Batch DML 1 (two statements)
	if _, err := tx.ExecContext(ctx, "START BATCH DML"); err != nil {
		t.Fatalf("failed to start batch 1: %v", err)
	}
	if _, err := tx.ExecContext(ctx, testutil.UpdateBarSetFoo); err != nil {
		t.Fatalf("failed to execute batch 1 stmt 1: %v", err)
	}
	if _, err := tx.ExecContext(ctx, testutil.UpdateBarSetFoo); err != nil {
		t.Fatalf("failed to execute batch 1 stmt 2: %v", err)
	}
	if _, err := tx.ExecContext(ctx, "RUN BATCH"); err != nil {
		t.Fatalf("failed to run batch 1: %v", err)
	}

	// 4. Savepoint s2
	err = dbConn.Raw(func(driverConn any) error {
		return driverConn.(*conn).tx.Savepoint("s2")
	})
	if err != nil {
		t.Fatalf("failed to create savepoint s2: %v", err)
	}

	// 5. Intermediate DML
	if _, err := tx.ExecContext(ctx, testutil.UpdateSingersSetLastName); err != nil {
		t.Fatalf("failed to execute intermediate DML: %v", err)
	}

	// 6. Savepoint s3
	err = dbConn.Raw(func(driverConn any) error {
		return driverConn.(*conn).tx.Savepoint("s3")
	})
	if err != nil {
		t.Fatalf("failed to create savepoint s3: %v", err)
	}

	// 7. Batch DML 2 (two statements)
	if _, err := tx.ExecContext(ctx, "START BATCH DML"); err != nil {
		t.Fatalf("failed to start batch 2: %v", err)
	}
	if _, err := tx.ExecContext(ctx, testutil.UpdateBarSetFoo); err != nil {
		t.Fatalf("failed to execute batch 2 stmt 1: %v", err)
	}
	if _, err := tx.ExecContext(ctx, testutil.UpdateBarSetFoo); err != nil {
		t.Fatalf("failed to execute batch 2 stmt 2: %v", err)
	}
	if _, err := tx.ExecContext(ctx, "RUN BATCH"); err != nil {
		t.Fatalf("failed to run batch 2: %v", err)
	}

	// 8. Savepoint s4
	err = dbConn.Raw(func(driverConn any) error {
		return driverConn.(*conn).tx.Savepoint("s4")
	})
	if err != nil {
		t.Fatalf("failed to create savepoint s4: %v", err)
	}

	// 9. Rollback to s2 (discards intermediate DML and Batch 2)
	err = dbConn.Raw(func(driverConn any) error {
		return driverConn.(*conn).tx.RollbackToSavepoint(ctx, "s2")
	})
	if err != nil {
		t.Fatalf("failed to rollback to savepoint s2: %v", err)
	}

	// 10. Commit transaction
	if err := tx.Commit(); err != nil {
		t.Fatalf("failed to commit: %v", err)
	}

	requests := server.TestSpanner.DrainRequestsFromServer()
	commitRequests := testutil.RequestsOfType(requests, reflect.TypeOf(&spannerpb.CommitRequest{}))
	if g, w := len(commitRequests), 1; g != w {
		t.Fatalf("commit requests count mismatch\nGot:  %v\nWant: %v", g, w)
	}
	rollbackRequests := testutil.RequestsOfType(requests, reflect.TypeOf(&spannerpb.RollbackRequest{}))
	if g, w := len(rollbackRequests), 1; g != w {
		t.Fatalf("rollback requests count mismatch\nGot:  %v\nWant: %v", g, w)
	}
	// Initial DML (1) + Intermediate DML (1) + Replayed Initial DML (1) = 3
	execSqlRequests := testutil.RequestsOfType(requests, reflect.TypeOf(&spannerpb.ExecuteSqlRequest{}))
	if g, w := len(execSqlRequests), 3; g != w {
		t.Errorf("executeSql requests count mismatch\nGot:  %v\nWant: %v", g, w)
	}
	// Initial Batch 1 (1) + Batch 2 (1) + Replayed Batch 1 (1) = 3
	batchRequests := testutil.RequestsOfType(requests, reflect.TypeOf(&spannerpb.ExecuteBatchDmlRequest{}))
	if g, w := len(batchRequests), 3; g != w {
		t.Errorf("batch requests count mismatch\nGot:  %v\nWant: %v", g, w)
	}
}

func TestSavepoint_InterleavedMutationsAndDml(t *testing.T) {
	t.Parallel()

	db, server, teardown := setupTestDBConnection(t)
	defer teardown()
	ctx := context.Background()

	dbConn, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer silentClose(dbConn)

	tx, err := dbConn.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		_ = tx.Rollback()
	}()

	mutation1 := spanner.Insert("Foo1", []string{"Id", "Val"}, []any{int64(1), "one"})
	mutation2 := spanner.Insert("Foo2", []string{"Id", "Val"}, []any{int64(2), "two"})
	mutation3 := spanner.Insert("Foo3", []string{"Id", "Val"}, []any{int64(3), "three"})

	err = dbConn.Raw(func(driverConn any) error {
		c := driverConn.(*conn)
		if err := c.tx.BufferWrite([]*spanner.Mutation{mutation1}); err != nil {
			return err
		}
		if err := c.tx.Savepoint("s1"); err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	if _, err := tx.ExecContext(ctx, testutil.UpdateSingersSetLastName); err != nil {
		t.Fatalf("DML 1 failed: %v", err)
	}

	err = dbConn.Raw(func(driverConn any) error {
		c := driverConn.(*conn)
		if err := c.tx.BufferWrite([]*spanner.Mutation{mutation2}); err != nil {
			return err
		}
		if err := c.tx.Savepoint("s2"); err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	if _, err := tx.ExecContext(ctx, testutil.UpdateBarSetFoo); err != nil {
		t.Fatalf("DML 2 failed: %v", err)
	}

	err = dbConn.Raw(func(driverConn any) error {
		c := driverConn.(*conn)
		if err := c.tx.BufferWrite([]*spanner.Mutation{mutation3}); err != nil {
			return err
		}
		return c.tx.RollbackToSavepoint(ctx, "s1")
	})
	if err != nil {
		t.Fatalf("rollback to s1 failed: %v", err)
	}

	if err := tx.Commit(); err != nil {
		t.Fatalf("commit failed: %v", err)
	}

	requests := server.TestSpanner.DrainRequestsFromServer()
	commitRequests := testutil.RequestsOfType(requests, reflect.TypeOf(&spannerpb.CommitRequest{}))
	if g, w := len(commitRequests), 1; g != w {
		t.Fatalf("commit requests count mismatch\nGot:  %v\nWant: %v", g, w)
	}

	commitReq := commitRequests[0].(*spannerpb.CommitRequest)
	if g, w := len(commitReq.Mutations), 1; g != w {
		t.Fatalf("mutations count in commit mismatch\nGot:  %v\nWant: %v", g, w)
	}
	if g, w := commitReq.Mutations[0].GetInsert().Table, "Foo1"; g != w {
		t.Errorf("mutation table mismatch\nGot:  %v\nWant: %v", g, w)
	}
	if g, w := commitReq.Mutations[0].GetInsert().Values[0].GetValues()[0].GetStringValue(), "1"; g != w {
		t.Errorf("mutation id mismatch\nGot:  %v\nWant: %v", g, w)
	}

	rollbackRequests := testutil.RequestsOfType(requests, reflect.TypeOf(&spannerpb.RollbackRequest{}))
	if g, w := len(rollbackRequests), 1; g != w {
		t.Fatalf("rollback requests count mismatch\nGot:  %v\nWant: %v", g, w)
	}
	// DML 1 and DML 2 occurred after s1, so they are not replayed on commit.
	execSqlRequests := testutil.RequestsOfType(requests, reflect.TypeOf(&spannerpb.ExecuteSqlRequest{}))
	if g, w := len(execSqlRequests), 2; g != w {
		t.Errorf("executeSql requests count mismatch\nGot:  %v\nWant: %v", g, w)
	}
}

func TestSavepoint_ReadOnlyTransaction_RetriesDisabled(t *testing.T) {
	t.Parallel()

	db, _, teardown := setupTestDBConnectionWithParams(t, "retryAbortsInternally=false;minSessions=1;maxSessions=1")
	defer teardown()
	ctx := context.Background()

	dbConn, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer silentClose(dbConn)

	tx, err := dbConn.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		_ = tx.Rollback()
	}()

	err = dbConn.Raw(func(driverConn any) error {
		c := driverConn.(*conn)
		// Creating savepoint in read-only transaction should succeed even if internal retries are disabled.
		if err := c.tx.Savepoint("s1"); err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		t.Fatalf("savepoint in read-only transaction failed: %v", err)
	}

	rows, err := tx.QueryContext(ctx, testutil.SelectFooFromBar)
	if err != nil {
		t.Fatalf("query failed: %v", err)
	}
	for rows.Next() {
	}
	_ = rows.Close()

	err = dbConn.Raw(func(driverConn any) error {
		c := driverConn.(*conn)
		// Rolling back to savepoint in read-only transaction should succeed even if internal retries are disabled.
		if err := c.tx.RollbackToSavepoint(ctx, "s1"); err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		t.Fatalf("rollback to savepoint in read-only transaction failed: %v", err)
	}

	// Resuming queries after rollback should succeed in read-only transaction.
	rows2, err := tx.QueryContext(ctx, testutil.SelectFooFromBar)
	if err != nil {
		t.Fatalf("query after rollback failed: %v", err)
	}
	for rows2.Next() {
	}
	_ = rows2.Close()

	if err := tx.Commit(); err != nil {
		t.Fatalf("commit failed: %v", err)
	}
}
