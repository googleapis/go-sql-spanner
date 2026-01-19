package server

import (
	"fmt"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"cloud.google.com/go/spanner/admin/database/apiv1/databasepb"
	"cloud.google.com/go/spanner/apiv1/spannerpb"
	"github.com/google/uuid"
	"github.com/googleapis/go-sql-spanner/testutil"
	"spannerlib/socket-server/client"
)

func TestConnect(t *testing.T) {
	t.Parallel()

	server, teardown := setupMockSpannerServer(t)
	defer teardown()
	dsn := fmt.Sprintf("%s/projects/p/instances/i/databases/d?useplaintext=true", server.Address)

	tp, addr, cleanup := startTestSpannerLibServer(t)
	defer cleanup()

	pool := client.CreatePool(tp, addr, dsn)
	conn, err := pool.CreateConnection()
	if err != nil {
		t.Fatal(err)
	}
	if err := conn.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestExecute(t *testing.T) {
	t.Parallel()

	server, teardown := setupMockSpannerServer(t)
	defer teardown()
	dsn := fmt.Sprintf("%s/projects/p/instances/i/databases/d?useplaintext=true", server.Address)

	tp, addr, cleanup := startTestSpannerLibServer(t)
	defer cleanup()

	pool := client.CreatePool(tp, addr, dsn)
	conn, err := pool.CreateConnection()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close() }()
	rows, err := conn.Execute(&spannerpb.ExecuteSqlRequest{Sql: testutil.SelectFooFromBar})
	if err != nil {
		t.Fatal(err)
	}
	c := 0
	for {
		row, err := rows.Next()
		if err != nil {
			t.Fatal(err)
		}
		if row == nil {
			break
		}
		c++
		if g, w := len(row.Values), 1; g != w {
			t.Fatalf("col count mismatch\n Got: %v\n Want: %v", g, w)
		}
	}
	if g, w := c, 2; g != w {
		t.Fatalf("row count mismatch\n Got: %d\n Want: %d", g, w)
	}
}

func TestExecuteBatch(t *testing.T) {
	t.Parallel()

	server, teardown := setupMockSpannerServer(t)
	defer teardown()
	dsn := fmt.Sprintf("%s/projects/p/instances/i/databases/d?useplaintext=true", server.Address)

	tp, addr, cleanup := startTestSpannerLibServer(t)
	defer cleanup()

	pool := client.CreatePool(tp, addr, dsn)
	conn, err := pool.CreateConnection()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close() }()

	result, err := conn.ExecuteBatch(&spannerpb.ExecuteBatchDmlRequest{
		Statements: []*spannerpb.ExecuteBatchDmlRequest_Statement{
			{Sql: testutil.UpdateBarSetFoo},
			{Sql: testutil.UpdateBarSetFoo},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if g, w := len(result.ResultSets), 2; g != w {
		t.Fatalf("result count mismatch\n Got: %v\n Want: %v", g, w)
	}
	for _, result := range result.ResultSets {
		if g, w := result.Stats.GetRowCountExact(), int64(testutil.UpdateBarSetFooRowCount); g != w {
			t.Fatalf("update count mismatch\n Got: %v\n Want: %v", g, w)
		}
	}
}

func TestTransaction(t *testing.T) {
	t.Parallel()

	server, teardown := setupMockSpannerServer(t)
	defer teardown()
	dsn := fmt.Sprintf("%s/projects/p/instances/i/databases/d?useplaintext=true", server.Address)

	tp, addr, cleanup := startTestSpannerLibServer(t)
	defer cleanup()

	pool := client.CreatePool(tp, addr, dsn)
	conn, err := pool.CreateConnection()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close() }()

	for _, commit := range []bool{true, false} {
		if err := conn.Begin(&spannerpb.TransactionOptions{}); err != nil {
			t.Fatal(err)
		}
		rows, err := conn.Execute(&spannerpb.ExecuteSqlRequest{Sql: testutil.UpdateBarSetFoo})
		if err != nil {
			t.Fatal(err)
		}
		row, err := rows.Next()
		if err != nil {
			t.Fatal(err)
		}
		if row != nil {
			t.Fatal("expected nil row")
		}

		if commit {
			resp, err := conn.Commit()
			if err != nil {
				t.Fatal(err)
			}
			if resp == nil || resp.CommitTimestamp == nil {
				t.Fatal("missing commit timestamp")
			}
		} else {
			if err := conn.Rollback(); err != nil {
				t.Fatal(err)
			}
		}
	}
}

func startTestSpannerLibServer(t *testing.T) (tp, addr string, cleanup func()) {
	var name string
	if runtime.GOOS == "windows" {
		tp = "tcp"
		name = "localhost:0"
	} else {
		tp = "unix"
		name = filepath.Join(os.TempDir(), fmt.Sprintf("spannerlib-%s", uuid.NewString()))
	}
	lis, err := net.Listen(tp, name)
	if err != nil {
		t.Fatalf("failed to listen: %v\n", err)
	}
	addr = lis.Addr().String()
	server, err := CreateServer()
	if err != nil {
		t.Fatalf("failed to create server: %v\n", err)
	}
	go func() { _ = server.Serve(lis) }()

	cleanup = func() {
		server.GracefulStop()
		_ = os.Remove(name)
	}

	return
}

func setupMockSpannerServer(t *testing.T) (server *testutil.MockedSpannerInMemTestServer, teardown func()) {
	return setupMockSpannerServerWithDialect(t, databasepb.DatabaseDialect_GOOGLE_STANDARD_SQL)
}

func setupMockSpannerServerWithDialect(t *testing.T, dialect databasepb.DatabaseDialect) (server *testutil.MockedSpannerInMemTestServer, teardown func()) {
	server, _, serverTeardown := testutil.NewMockedSpannerInMemTestServer(t)
	server.SetupSelectDialectResult(dialect)
	return server, serverTeardown
}
