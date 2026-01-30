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

package api

import (
	"context"
	"fmt"
	"reflect"
	"testing"

	"cloud.google.com/go/spanner/apiv1/spannerpb"
	"github.com/googleapis/go-sql-spanner/testutil"
)

func TestPartitionedDml(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	server, teardown := setupMockServer(t)
	defer teardown()
	dsn := fmt.Sprintf("%s/projects/p/instances/i/databases/d?useplaintext=true", server.Address)

	poolId, err := CreatePool(ctx, "test", dsn)
	if err != nil {
		t.Fatalf("CreatePool returned unexpected error: %v", err)
	}
	connId, err := CreateConnection(ctx, poolId)
	if err != nil {
		t.Fatalf("CreateConnection returned unexpected error: %v", err)
	}

	if err := executeAndDiscard(ctx, poolId, connId, "set autocommit_dml_mode='partitioned_non_atomic'"); err != nil {
		t.Fatalf("set autocommit_dml_mode returned unexpected error: %v", err)
	}
	if affected, err := executeAndReturnUpdateCount(ctx, poolId, connId, testutil.UpdateBarSetFoo); err != nil {
		t.Fatalf("execute returned unexpected error: %v", err)
	} else {
		if g, w := affected, int64(testutil.UpdateBarSetFooRowCount); g != w {
			t.Fatalf("update count mismatch\n Got: %v\nWant: %v", g, w)
		}
	}

	// Verify that the statement used Partitioned DML.
	requests := server.TestSpanner.DrainRequestsFromServer()
	beginRequests := testutil.RequestsOfType(requests, reflect.TypeOf(&spannerpb.BeginTransactionRequest{}))
	if g, w := len(beginRequests), 1; g != w {
		t.Fatalf("BeginTransaction request count mismatch\n Got: %v\nWant: %v", g, w)
	}
	executeRequests := testutil.RequestsOfType(requests, reflect.TypeOf(&spannerpb.ExecuteSqlRequest{}))
	if g, w := len(executeRequests), 1; g != w {
		t.Fatalf("Execute request count mismatch\n Got: %v\nWant: %v", g, w)
	}
	executeRequest := executeRequests[0].(*spannerpb.ExecuteSqlRequest)
	if executeRequest.GetTransaction() == nil || len(executeRequest.GetTransaction().GetId()) == 0 {
		t.Fatalf("missing transaction on request: %v", executeRequest)
	}
	// Partitioned DML is not committed.
	commitRequests := testutil.RequestsOfType(requests, reflect.TypeOf(&spannerpb.CommitRequest{}))
	if g, w := len(commitRequests), 0; g != w {
		t.Fatalf("Commit request count mismatch\n Got: %v\nWant: %v", g, w)
	}

	if err := CloseConnection(ctx, poolId, connId); err != nil {
		t.Fatalf("CloseConnection returned unexpected error: %v", err)
	}
	if err := ClosePool(ctx, poolId); err != nil {
		t.Fatalf("ClosePool returned unexpected error: %v", err)
	}
}

func executeAndDiscard(ctx context.Context, poolId, connId int64, query string) error {
	if rowsId, err := Execute(ctx, poolId, connId, &spannerpb.ExecuteSqlRequest{Sql: query}); err != nil {
		return err
	} else {
		_ = CloseRows(ctx, poolId, connId, rowsId)
	}
	return nil
}

func executeAndReturnUpdateCount(ctx context.Context, poolId, connId int64, query string) (int64, error) {
	if rowsId, err := Execute(ctx, poolId, connId, &spannerpb.ExecuteSqlRequest{Sql: query}); err != nil {
		return 0, err
	} else {
		defer func() { _ = CloseRows(ctx, poolId, connId, rowsId) }()
		if stats, err := ResultSetStats(ctx, poolId, connId, rowsId); err != nil {
			return 0, err
		} else {
			switch stats.GetRowCount().(type) {
			case *spannerpb.ResultSetStats_RowCountExact:
				return stats.GetRowCountExact(), nil
			case *spannerpb.ResultSetStats_RowCountLowerBound:
				return stats.GetRowCountLowerBound(), nil
			}
			return 0, fmt.Errorf("unexpected result set stats type: %v", stats.GetRowCount())
		}
	}
}
