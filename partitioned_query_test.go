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
	"fmt"
	"math/rand"
	"reflect"
	"slices"
	"testing"

	"cloud.google.com/go/spanner"
	sppb "cloud.google.com/go/spanner/apiv1/spannerpb"
	"github.com/googleapis/go-sql-spanner/testutil"
	"google.golang.org/grpc/codes"
)

func TestBeginBatchReadOnlyTransaction(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	db, server, teardown := setupTestDBConnection(t)
	defer teardown()
	db.SetMaxOpenConns(1)

	// Repeat twice to ensure that there are no connection leaks.
	for i := 0; i < 2; i++ {
		tx, err := BeginBatchReadOnlyTransaction(ctx, db, BatchReadOnlyTransactionOptions{})
		if err != nil {
			t.Fatal(err)
		}
		// Verify that we can use the BatchReadOnlyTransaction as any other read-only transaction.
		rows, err := tx.QueryContext(ctx, testutil.SelectFooFromBar)
		if err != nil {
			t.Fatal(err)
		}
		for rows.Next() {
			// Just verify that we can iterate through the results...
		}
		rows.Close()
		if err := tx.Commit(); err != nil {
			t.Fatal(err)
		}

		requests := drainRequestsFromServer(server.TestSpanner)
		beginRequests := requestsOfType(requests, reflect.TypeOf(&sppb.BeginTransactionRequest{}))
		if g, w := len(beginRequests), 1; g != w {
			t.Fatalf("num begin requests mismatch\n Got: %v\nWant: %v", g, w)
		}
		beginRequest := beginRequests[0].(*sppb.BeginTransactionRequest)
		if !beginRequest.Options.GetReadOnly().GetStrong() {
			t.Fatal("missing strong timestamp bound option")
		}
		executeRequests := requestsOfType(requests, reflect.TypeOf(&sppb.ExecuteSqlRequest{}))
		if g, w := len(executeRequests), 1; g != w {
			t.Fatalf("num execute requests mismatch\n Got: %v\nWant: %v", g, w)
		}
		// (Batch) read-only transactions should not be committed.
		commitRequests := requestsOfType(requests, reflect.TypeOf(&sppb.CommitRequest{}))
		if g, w := len(commitRequests), 0; g != w {
			t.Fatalf("num commit requests mismatch\n Got: %v\nWant: %v", g, w)
		}
	}
}

func TestPartitionQuery(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	db, server, teardown := setupTestDBConnection(t)
	defer teardown()

	tx, err := BeginBatchReadOnlyTransaction(ctx, db, BatchReadOnlyTransactionOptions{})
	if err != nil {
		t.Fatal(err)
	}

	// Partition a query using the batch transaction that was started.
	rows, err := tx.QueryContext(ctx, testutil.SelectFooFromBar,
		ExecOptions{
			PartitionedQueryOptions: PartitionedQueryOptions{PartitionQuery: true},
			QueryOptions:            spanner.QueryOptions{DataBoostEnabled: true},
		})
	if err != nil {
		t.Fatal(err)
	}
	var pq PartitionedQuery
	for rows.Next() {
		if pq.Partitions != nil {
			t.Fatal("only one row should be returned")
		}
		if err := rows.Scan(&pq); err != nil {
			t.Fatal(err)
		}
		if pq.Partitions == nil {
			t.Fatal("missing partitions")
		}
	}
	rows.Close()

	// Setup results for each partition.
	for index, partition := range pq.Partitions {
		res := testutil.CreateSingleColumnResultSet([]int64{int64(index)}, "FOO")
		if err := server.TestSpanner.PutPartitionResult(
			partition.GetPartitionToken(),
			&testutil.StatementResult{Type: testutil.StatementResultResultSet, ResultSet: res}); err != nil {
			t.Fatal(err)
		}
	}

	// Execute each of the partitions.
	for index := range pq.Partitions {
		rows, err := pq.Execute(ctx, index, db)
		if err != nil {
			t.Fatal(err)
		}
		count := 0
		for rows.Next() {
			var v int64
			if err := rows.Scan(&v); err != nil {
				t.Fatal(err)
			}
			if g, w := v, int64(index); g != w {
				t.Fatalf("row value mismatch\n Got: %v\nWant: %v", g, w)
			}
			count++
		}
		if g, w := count, 1; g != w {
			t.Fatalf("row count mismatch for partition %v\n Got: %v\nWant: %v", index, g, w)
		}
		if err := rows.Close(); err != nil {
			t.Fatal(err)
		}
	}

	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}

	requests := drainRequestsFromServer(server.TestSpanner)
	beginRequests := requestsOfType(requests, reflect.TypeOf(&sppb.BeginTransactionRequest{}))
	if g, w := len(beginRequests), 1; g != w {
		t.Fatalf("num begin requests mismatch\n Got: %v\nWant: %v", g, w)
	}
	beginRequest := beginRequests[0].(*sppb.BeginTransactionRequest)
	if !beginRequest.Options.GetReadOnly().GetStrong() {
		t.Fatal("missing strong timestamp bound option")
	}
	partitionRequests := requestsOfType(requests, reflect.TypeOf(&sppb.PartitionQueryRequest{}))
	if g, w := len(partitionRequests), 1; g != w {
		t.Fatalf("num partition requests mismatch\n Got: %v\nWant: %v", g, w)
	}
	executeRequests := requestsOfType(requests, reflect.TypeOf(&sppb.ExecuteSqlRequest{}))
	if g, w := len(executeRequests), len(pq.Partitions); g != w {
		t.Fatalf("num execute requests mismatch\n Got: %v\nWant: %v", g, w)
	}
	executeRequest := executeRequests[0].(*sppb.ExecuteSqlRequest)
	if !executeRequest.DataBoostEnabled {
		t.Fatal("missing DataBoostEnabled option")
	}
	// (Batch) read-only transactions should not be committed.
	commitRequests := requestsOfType(requests, reflect.TypeOf(&sppb.CommitRequest{}))
	if g, w := len(commitRequests), 0; g != w {
		t.Fatalf("num commit requests mismatch\n Got: %v\nWant: %v", g, w)
	}
}

func TestPartitionQueryWithoutTx(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	db, _, teardown := setupTestDBConnection(t)
	defer teardown()

	_, err := db.QueryContext(ctx, testutil.SelectFooFromBar, ExecOptions{
		PartitionedQueryOptions: PartitionedQueryOptions{
			PartitionQuery: true,
		},
	})
	if err == nil {
		t.Fatal("missing error for PartitionQuery without transaction")
	}
	if g, w := spanner.ErrCode(err), codes.FailedPrecondition; g != w {
		t.Fatalf("error code mismatch\n Got: %v\nWant: %v", g, w)
	}
}

func TestPartitionQueryInTx(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	db, _, teardown := setupTestDBConnection(t)
	defer teardown()

	for _, readOnly := range []bool{true, false} {
		// PartitionQuery is not supported in normal transactions, only
		// in BatchReadOnlyTransactions.
		tx, err := db.BeginTx(ctx, &sql.TxOptions{ReadOnly: readOnly})
		if err != nil {
			t.Fatal(err)
		}
		_, err = tx.QueryContext(ctx, testutil.SelectFooFromBar, ExecOptions{
			PartitionedQueryOptions: PartitionedQueryOptions{
				PartitionQuery: true,
			},
		})
		tx.Rollback()
		if err == nil {
			t.Fatal("missing error for PartitionQuery in transaction")
		}
		if g, w := spanner.ErrCode(err), codes.FailedPrecondition; g != w {
			t.Fatalf("error code mismatch\n Got: %v\nWant: %v", g, w)
		}
	}
}

func TestAutoPartitionQuery(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	db, server, teardown := setupTestDBConnection(t)
	defer teardown()

	tx, err := BeginBatchReadOnlyTransaction(ctx, db, BatchReadOnlyTransactionOptions{})
	if err != nil {
		t.Fatal(err)
	}

	maxPartitions := rand.Intn(10) + 1
	// Setup results for each partition.
	allResults := make([]int64, 0, 1000)
	for index := 0; index < maxPartitions; index++ {
		numResults := rand.Intn(200)
		partitionResults := make([]int64, numResults, numResults)
		for i := 0; i < numResults; i++ {
			partitionResults[i] = rand.Int63()
		}
		allResults = append(allResults, partitionResults...)
		res := testutil.CreateSingleColumnResultSet(partitionResults, "FOO")
		token := fmt.Sprintf("%s: %v", testutil.SelectFooFromBar, index)
		if err := server.TestSpanner.PutPartitionResult(
			[]byte(token),
			&testutil.StatementResult{Type: testutil.StatementResultResultSet, ResultSet: res}); err != nil {
			t.Fatal(err)
		}
	}

	// Automatically partition and execute a query.
	rows, err := tx.QueryContext(ctx, testutil.SelectFooFromBar,
		ExecOptions{
			PartitionedQueryOptions: PartitionedQueryOptions{
				AutoPartitionQuery: true,
				MaxParallelism:     rand.Intn(10) + 1,
				PartitionOptions: spanner.PartitionOptions{
					MaxPartitions: int64(maxPartitions),
				},
			},
			QueryOptions: spanner.QueryOptions{DataBoostEnabled: true},
		})
	if err != nil {
		t.Fatal(err)
	}

	count := 0
	for rows.Next() {
		var v int64
		if err := rows.Scan(&v); err != nil {
			t.Fatal(err)
		}
		if !slices.Contains(allResults, v) {
			t.Fatalf("unexpected row value: %v", v)
		}
		count++
	}
	if g, w := count, len(allResults); g != w {
		t.Fatalf("row count mismatch\n Got: %v\nWant: %v", g, w)
	}
	if err := rows.Close(); err != nil {
		t.Fatal(err)
	}

	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}

	requests := drainRequestsFromServer(server.TestSpanner)
	beginRequests := requestsOfType(requests, reflect.TypeOf(&sppb.BeginTransactionRequest{}))
	if g, w := len(beginRequests), 1; g != w {
		t.Fatalf("num begin requests mismatch\n Got: %v\nWant: %v", g, w)
	}
	beginRequest := beginRequests[0].(*sppb.BeginTransactionRequest)
	if !beginRequest.Options.GetReadOnly().GetStrong() {
		t.Fatal("missing strong timestamp bound option")
	}
	partitionRequests := requestsOfType(requests, reflect.TypeOf(&sppb.PartitionQueryRequest{}))
	if g, w := len(partitionRequests), 1; g != w {
		t.Fatalf("num partition requests mismatch\n Got: %v\nWant: %v", g, w)
	}
	executeRequests := requestsOfType(requests, reflect.TypeOf(&sppb.ExecuteSqlRequest{}))
	if g, w := len(executeRequests), maxPartitions; g != w {
		t.Fatalf("num execute requests mismatch\n Got: %v\nWant: %v", g, w)
	}
	executeRequest := executeRequests[0].(*sppb.ExecuteSqlRequest)
	if !executeRequest.DataBoostEnabled {
		t.Fatal("missing DataBoostEnabled option")
	}
	// (Batch) read-only transactions should not be committed.
	commitRequests := requestsOfType(requests, reflect.TypeOf(&sppb.CommitRequest{}))
	if g, w := len(commitRequests), 0; g != w {
		t.Fatalf("num commit requests mismatch\n Got: %v\nWant: %v", g, w)
	}
}
