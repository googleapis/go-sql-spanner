package spannerdriver

import (
	"cloud.google.com/go/spanner"
	"cloud.google.com/go/spanner/testutil"
	"context"
	"database/sql"
	"fmt"
	"github.com/google/go-cmp/cmp"
	"google.golang.org/api/option"
	"testing"
)

func setupTestDbConnection(t *testing.T) (db *sql.DB, server *testutil.MockedSpannerInMemTestServer, teardown func()) {
	server, _, serverTeardown := setupMockedTestServer(t)
	db, err := sql.Open(
		"spanner",
		fmt.Sprintf("%s/projects/p/instances/i/databases/d?useplaintext=true", server.Address))
	if err != nil {
		serverTeardown()
		t.Fatal(err)
	}
	return db, server, func() {
		db.Close()
		serverTeardown()
	}
}

func TestSimpleQuery(t *testing.T) {
	t.Parallel()

	db, server, teardown := setupTestDbConnection(t)
	defer teardown()
	rows, err := db.Query(testutil.SelectFooFromBar)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()

	for want := int64(1); rows.Next(); want++ {
		cols, err := rows.Columns()
		if err != nil {
			t.Fatal(err)
		}
		if !cmp.Equal(cols, []string{"FOO"}) {
			t.Errorf("cols mismatch\nGot: %v\nWant: %v", cols, []string{"FOO"})
		}
		var got int64
		err = rows.Scan(&got)
		if err != nil {
			t.Fatal(err)
		}
		if got != want {
			t.Errorf("value mismatch\nGot: %v\nWant: %v", got, want)
		}
	}
	requests := drainRequestsFromServer(server.TestSpanner)

	fmt.Printf("requests: %v\n", requests)
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
		t.Fatal(err)
	}
	return server, client, func() {
		client.Close()
		serverTeardown()
	}
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
