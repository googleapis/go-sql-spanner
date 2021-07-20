package spannerdriver

import (
	"cloud.google.com/go/spanner"
	"cloud.google.com/go/spanner/testutil"
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"github.com/google/go-cmp/cmp"
	"google.golang.org/api/option"
	sppb "google.golang.org/genproto/googleapis/spanner/v1"
	"google.golang.org/grpc/codes"
	gstatus "google.golang.org/grpc/status"
	"reflect"
	"testing"
)

func setupTestDbConnection(t *testing.T) (db *sql.DB, server *testutil.MockedSpannerInMemTestServer, teardown func()) {
	server, _, serverTeardown := setupMockedTestServer(t)
	db, err := sql.Open(
		"spanner",
		fmt.Sprintf("%s/projects/p/instances/i/databases/d?useplaintext=true", server.Address))
	if err != nil {
		serverTeardown()
		t.Error(err)
	}
	return db, server, func() {
		db.Close()
		serverTeardown()
	}
}

func TestPing(t *testing.T) {
	t.Parallel()

	db, _, teardown := setupTestDbConnection(t)
	defer teardown()
	if err := db.Ping(); err != nil {
		t.Errorf("unexpected error for ping: %v", err)
	}
}

func TestPing_Fails(t *testing.T) {
	t.Parallel()

	db, server, teardown := setupTestDbConnection(t)
	defer teardown()
	s := gstatus.Newf(codes.PermissionDenied, "Permission denied for database")
	server.TestSpanner.PutStatementResult("SELECT 1", &testutil.StatementResult{Err: s.Err()})
	if g, w := db.Ping(), driver.ErrBadConn; g != w {
		t.Errorf("ping error mismatch\nGot: %v\nWant: %v", g, w)
	}
}

func TestSimpleQuery(t *testing.T) {
	t.Parallel()

	db, server, teardown := setupTestDbConnection(t)
	defer teardown()
	rows, err := db.Query(testutil.SelectFooFromBar)
	if err != nil {
		t.Error(err)
	}
	defer rows.Close()

	for want := int64(1); rows.Next(); want++ {
		cols, err := rows.Columns()
		if err != nil {
			t.Error(err)
		}
		if !cmp.Equal(cols, []string{"FOO"}) {
			t.Errorf("cols mismatch\nGot: %v\nWant: %v", cols, []string{"FOO"})
		}
		var got int64
		err = rows.Scan(&got)
		if err != nil {
			t.Error(err)
		}
		if got != want {
			t.Errorf("value mismatch\nGot: %v\nWant: %v", got, want)
		}
	}
	requests := drainRequestsFromServer(server.TestSpanner)
	sqlRequests := requestsOfType(requests, reflect.TypeOf(&sppb.ExecuteSqlRequest{}))
	if g, w := len(sqlRequests), 1; g != w {
		t.Errorf("sql requests count mismatch\nGot: %v\nWant: %v", g, w)
	}
	req := sqlRequests[0].(*sppb.ExecuteSqlRequest)
	if req.Transaction == nil {
		t.Errorf("missing transaction for ExecuteSqlRequest")
	}
	if req.Transaction.GetSingleUse() == nil {
		t.Errorf("missing single use selector for ExecuteSqlRequest")
	}
	if req.Transaction.GetSingleUse().GetReadOnly() == nil {
		t.Errorf("missing read-only option for ExecuteSqlRequest")
	}
	if !req.Transaction.GetSingleUse().GetReadOnly().GetStrong() {
		t.Errorf("missing strong timestampbound for ExecuteSqlRequest")
	}
}

func TestSimpleReadOnlyTransaction(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	db, server, teardown := setupTestDbConnection(t)
	defer teardown()
	tx, err := db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		t.Error(err)
	}
	rows, err := tx.Query(testutil.SelectFooFromBar)
	if err != nil {
		t.Error(err)
	}
	defer rows.Close()

	for want := int64(1); rows.Next(); want++ {
		cols, err := rows.Columns()
		if err != nil {
			t.Error(err)
		}
		if !cmp.Equal(cols, []string{"FOO"}) {
			t.Errorf("cols mismatch\nGot: %v\nWant: %v", cols, []string{"FOO"})
		}
		var got int64
		err = rows.Scan(&got)
		if err != nil {
			t.Error(err)
		}
		if got != want {
			t.Errorf("value mismatch\nGot: %v\nWant: %v", got, want)
		}
	}
	err = tx.Commit()
	if err != nil {
		t.Error(err)
	}

	requests := drainRequestsFromServer(server.TestSpanner)
	sqlRequests := requestsOfType(requests, reflect.TypeOf(&sppb.ExecuteSqlRequest{}))
	if g, w := len(sqlRequests), 1; g != w {
		t.Errorf("ExecuteSqlRequests count mismatch\nGot: %v\nWant: %v", g, w)
	}
	req := sqlRequests[0].(*sppb.ExecuteSqlRequest)
	if req.Transaction == nil {
		t.Errorf("missing transaction for ExecuteSqlRequest")
	}
	if req.Transaction.GetId() == nil {
		t.Errorf("missing id selector for ExecuteSqlRequest")
	}
	// Read-only transactions are not really committed on Cloud Spanner, so
	// there should be no commit request on the server.
	commitRequests := requestsOfType(requests, reflect.TypeOf(&sppb.CommitRequest{}))
	if g, w := len(commitRequests), 0; g != w {
		t.Errorf("commit requests count mismatch\nGot: %v\nWant: %v", g, w)
	}
	beginReadOnlyRequests := filterBeginReadOnlyRequests(requestsOfType(requests, reflect.TypeOf(&sppb.BeginTransactionRequest{})))
	if g, w := len(beginReadOnlyRequests), 1; g != w {
		t.Errorf("begin requests count mismatch\nGot: %v\nWant: %v", g, w)
	}
}

func TestSimpleReadWriteTransaction(t *testing.T) {
	t.Parallel()

	db, server, teardown := setupTestDbConnection(t)
	defer teardown()
	tx, err := db.Begin()
	if err != nil {
		t.Error(err)
	}
	rows, err := tx.Query(testutil.SelectFooFromBar)
	if err != nil {
		t.Error(err)
	}
	defer rows.Close()

	for want := int64(1); rows.Next(); want++ {
		cols, err := rows.Columns()
		if err != nil {
			t.Error(err)
		}
		if !cmp.Equal(cols, []string{"FOO"}) {
			t.Errorf("cols mismatch\nGot: %v\nWant: %v", cols, []string{"FOO"})
		}
		var got int64
		err = rows.Scan(&got)
		if err != nil {
			t.Error(err)
		}
		if got != want {
			t.Errorf("value mismatch\nGot: %v\nWant: %v", got, want)
		}
	}
	err = tx.Commit()
	if err != nil {
		t.Error(err)
	}

	requests := drainRequestsFromServer(server.TestSpanner)
	sqlRequests := requestsOfType(requests, reflect.TypeOf(&sppb.ExecuteSqlRequest{}))
	if g, w := len(sqlRequests), 1; g != w {
		t.Errorf("ExecuteSqlRequests count mismatch\nGot: %v\nWant: %v", g, w)
	}
	req := sqlRequests[0].(*sppb.ExecuteSqlRequest)
	if req.Transaction == nil {
		t.Errorf("missing transaction for ExecuteSqlRequest")
	}
	if req.Transaction.GetId() == nil {
		t.Errorf("missing id selector for ExecuteSqlRequest")
	}
	commitRequests := requestsOfType(requests, reflect.TypeOf(&sppb.CommitRequest{}))
	if g, w := len(commitRequests), 1; g != w {
		t.Errorf("commit requests count mismatch\nGot: %v\nWant: %v", g, w)
	}
	commitReq := commitRequests[0].(*sppb.CommitRequest)
	if c, e := commitReq.GetTransactionId(), req.Transaction.GetId(); !cmp.Equal(c, e) {
		t.Errorf("transaction id mismatch\nCommit: %c\nExecute: %v", c, e)
	}
}

func setupMockedTestServer(t *testing.T) (server *testutil.MockedSpannerInMemTestServer, client *spanner.Client, teardown func()) {
	return setupMockedTestServerWithConfig(t, spanner.ClientConfig{})
}

func setupMockedTestServerWithConfig(t *testing.T, config spanner.ClientConfig) (server *testutil.MockedSpannerInMemTestServer, client *spanner.Client, teardown func()) {
	return setupMockedTestServerWithConfigAndClientOptions(t, config, []option.ClientOption{})
}

func setupMockedTestServerWithConfigAndClientOptions(t *testing.T, config spanner.ClientConfig, clientOptions []option.ClientOption) (server *testutil.MockedSpannerInMemTestServer, client *spanner.Client, teardown func()) {
	server, opts, serverTeardown := testutil.NewMockedSpannerInMemTestServer(t)
	opts = append(opts, clientOptions...)
	ctx := context.Background()
	formattedDatabase := fmt.Sprintf("projects/%s/instances/%s/databases/%s", "[PROJECT]", "[INSTANCE]", "[DATABASE]")
	client, err := spanner.NewClientWithConfig(ctx, formattedDatabase, config, opts...)
	if err != nil {
		t.Error(err)
	}
	return server, client, func() {
		client.Close()
		serverTeardown()
	}
}

func filterBeginReadOnlyRequests(requests []interface{}) []*sppb.BeginTransactionRequest {
	res := make([]*sppb.BeginTransactionRequest, 0)
	for _, r := range requests {
		if req, ok := r.(*sppb.BeginTransactionRequest); ok {
			if req.Options != nil && req.Options.GetReadOnly() != nil {
				res = append(res, req)
			}
		}
	}
	return res
}

func requestsOfType(requests []interface{}, t reflect.Type) []interface{} {
	res := make([]interface{}, 0)
	for _, req := range requests {
		if reflect.TypeOf(req) == t {
			res = append(res, req)
		}
	}
	return res
}

func drainRequestsFromServer(server testutil.InMemSpannerServer) []interface{} {
	var reqs []interface{}
loop:
	for {
		select {
		case req := <-server.ReceivedRequests():
			reqs = append(reqs, req)
		default:
			break loop
		}
	}
	return reqs
}
