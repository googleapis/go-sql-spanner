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
	"cmp"
	"context"
	"database/sql"
	"fmt"
	"math/rand"
	"reflect"
	"runtime"
	"slices"
	"testing"

	"cloud.google.com/go/spanner"
	"cloud.google.com/go/spanner/admin/database/apiv1/databasepb"
	sppb "cloud.google.com/go/spanner/apiv1/spannerpb"
	"github.com/googleapis/go-sql-spanner/testutil"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
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
		_ = rows.Close()
		if err := tx.Commit(); err != nil {
			t.Fatal(err)
		}

		requests := server.TestSpanner.DrainRequestsFromServer()
		beginRequests := testutil.RequestsOfType(requests, reflect.TypeOf(&sppb.BeginTransactionRequest{}))
		if g, w := len(beginRequests), 1; g != w {
			t.Fatalf("num begin requests mismatch\n Got: %v\nWant: %v", g, w)
		}
		beginRequest := beginRequests[0].(*sppb.BeginTransactionRequest)
		if !beginRequest.Options.GetReadOnly().GetStrong() {
			t.Fatal("missing strong timestamp bound option")
		}
		executeRequests := testutil.RequestsOfType(requests, reflect.TypeOf(&sppb.ExecuteSqlRequest{}))
		if g, w := len(executeRequests), 1; g != w {
			t.Fatalf("num execute requests mismatch\n Got: %v\nWant: %v", g, w)
		}
		// (Batch) read-only transactions should not be committed.
		commitRequests := testutil.RequestsOfType(requests, reflect.TypeOf(&sppb.CommitRequest{}))
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
	_ = rows.Close()

	// Setup results for each partition.
	for index, partition := range pq.Partitions {
		res := testutil.CreateSingleColumnInt64ResultSet([]int64{int64(index)}, "FOO")
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

	requests := server.TestSpanner.DrainRequestsFromServer()
	beginRequests := testutil.RequestsOfType(requests, reflect.TypeOf(&sppb.BeginTransactionRequest{}))
	if g, w := len(beginRequests), 1; g != w {
		t.Fatalf("num begin requests mismatch\n Got: %v\nWant: %v", g, w)
	}
	beginRequest := beginRequests[0].(*sppb.BeginTransactionRequest)
	if !beginRequest.Options.GetReadOnly().GetStrong() {
		t.Fatal("missing strong timestamp bound option")
	}
	partitionRequests := testutil.RequestsOfType(requests, reflect.TypeOf(&sppb.PartitionQueryRequest{}))
	if g, w := len(partitionRequests), 1; g != w {
		t.Fatalf("num partition requests mismatch\n Got: %v\nWant: %v", g, w)
	}
	executeRequests := testutil.RequestsOfType(requests, reflect.TypeOf(&sppb.ExecuteSqlRequest{}))
	if g, w := len(executeRequests), len(pq.Partitions); g != w {
		t.Fatalf("num execute requests mismatch\n Got: %v\nWant: %v", g, w)
	}
	executeRequest := executeRequests[0].(*sppb.ExecuteSqlRequest)
	if !executeRequest.DataBoostEnabled {
		t.Fatal("missing DataBoostEnabled option")
	}
	// (Batch) read-only transactions should not be committed.
	commitRequests := testutil.RequestsOfType(requests, reflect.TypeOf(&sppb.CommitRequest{}))
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
		_ = tx.Rollback()
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
	db.SetMaxOpenConns(1)

	type queryExecutor interface {
		QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
	}
	type autoPartitionTest struct {
		name                   string
		useExecOption          bool
		withTx                 bool
		maxResultsPerPartition int
	}
	tests := make([]autoPartitionTest, 0)
	for _, useExecOption := range []bool{true, false} {
		for _, withTx := range []bool{false} {
			for _, maxResultsPerPartition := range []int{0, 1, 5, 50, 200} {
				tests = append(tests, autoPartitionTest{
					fmt.Sprintf("useExecOption: %v, withTx: %v, maxResultsPerPartition: %v", useExecOption, withTx, maxResultsPerPartition),
					useExecOption,
					withTx,
					maxResultsPerPartition,
				})
			}
		}
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var tx queryExecutor
			var err error
			if test.withTx {
				tx, err = BeginBatchReadOnlyTransaction(ctx, db, BatchReadOnlyTransactionOptions{})
			} else {
				tx, err = db.Conn(ctx)
			}
			if err != nil {
				t.Fatal(err)
			}
			defer func() {
				if tx, ok := tx.(*sql.Tx); ok {
					if err := tx.Commit(); err != nil {
						t.Fatal(err)
					}
				}
				if tx, ok := tx.(*sql.Conn); ok {
					if err := tx.Close(); err != nil {
						t.Fatal(err)
					}
				}
			}()

			// Setup results for each partition.
			maxPartitions, allResults, err := setupRandomPartitionResults(server, testutil.SelectFooFromBar, test.maxResultsPerPartition)
			if err != nil {
				t.Fatalf("failed to set up partition results: %v", err)
			}

			// Automatically partition and execute a query.
			var rows *sql.Rows
			if test.useExecOption {
				rows, err = tx.QueryContext(ctx, testutil.SelectFooFromBar,
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
			} else {
				execOrFail := func(query string) {
					if r, err := tx.QueryContext(ctx, query); err != nil {
						t.Fatal(err)
					} else {
						_ = r.Close()
					}
				}
				set := "set "
				if test.withTx {
					set = set + "local "
				}
				execOrFail(set + "auto_partition_mode = true")
				execOrFail(set + "data_boost_enabled = true")
				execOrFail(fmt.Sprintf(set+"max_partitioned_parallelism = %d", rand.Intn(10)+1))
				execOrFail(fmt.Sprintf(set+"max_partitions = %d", maxPartitions))
				rows, err = tx.QueryContext(ctx, testutil.SelectFooFromBar)
			}
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = rows.Close() }()

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
			if rows.Err() != nil {
				t.Fatal(rows.Err())
			}
			if g, w := count, len(allResults); g != w {
				t.Fatalf("row count mismatch\n Got: %v\nWant: %v", g, w)
			}
			if err := rows.Close(); err != nil {
				t.Fatal(err)
			}

			requests := server.TestSpanner.DrainRequestsFromServer()
			beginRequests := testutil.RequestsOfType(requests, reflect.TypeOf(&sppb.BeginTransactionRequest{}))
			if g, w := len(beginRequests), 1; g != w {
				t.Fatalf("num begin requests mismatch\n Got: %v\nWant: %v", g, w)
			}
			beginRequest := beginRequests[0].(*sppb.BeginTransactionRequest)
			if !beginRequest.Options.GetReadOnly().GetStrong() {
				t.Fatal("missing strong timestamp bound option")
			}
			partitionRequests := testutil.RequestsOfType(requests, reflect.TypeOf(&sppb.PartitionQueryRequest{}))
			if g, w := len(partitionRequests), 1; g != w {
				t.Fatalf("num partition requests mismatch\n Got: %v\nWant: %v", g, w)
			}
			executeRequests := testutil.RequestsOfType(requests, reflect.TypeOf(&sppb.ExecuteSqlRequest{}))
			if g, w := len(executeRequests), maxPartitions; g != w {
				t.Fatalf("num execute requests mismatch\n Got: %v\nWant: %v", g, w)
			}
			executeRequest := executeRequests[0].(*sppb.ExecuteSqlRequest)
			if !executeRequest.DataBoostEnabled {
				t.Fatal("missing DataBoostEnabled option")
			}
			// (Batch) read-only transactions should not be committed.
			commitRequests := testutil.RequestsOfType(requests, reflect.TypeOf(&sppb.CommitRequest{}))
			if g, w := len(commitRequests), 0; g != w {
				t.Fatalf("num commit requests mismatch\n Got: %v\nWant: %v", g, w)
			}
		})
	}
}

func TestAutoPartitionQuery_ExecuteError(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	db, server, teardown := setupTestDBConnection(t)
	defer teardown()
	db.SetMaxOpenConns(1)

	for _, maxResultsPerPartition := range []int{0, 1, 5, 50, 200} {
		tx, err := BeginBatchReadOnlyTransaction(ctx, db, BatchReadOnlyTransactionOptions{})
		if err != nil {
			t.Fatal(err)
		}

		// Add an error for ExecuteStreamingSql.
		server.TestSpanner.PutExecutionTime(testutil.MethodExecuteStreamingSql, testutil.SimulatedExecutionTime{
			Errors: []error{status.Error(codes.NotFound, "Partition not found")},
		})
		// Also setup results for the partitioned query.
		maxPartitions, allResults, err := setupRandomPartitionResults(server, testutil.SelectFooFromBar, maxResultsPerPartition)
		if err != nil {
			t.Fatalf("failed to set up partition results: %v", err)
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
		// We should get an error either already when the query is executed,
		// or while iterating through the rows.
		if err != nil {
			if g, w := spanner.ErrCode(err), codes.NotFound; g != w {
				t.Fatalf("error code mismatch\n Got: %v\nWant: %v", g, w)
			}
		} else {
			// Iterate over the rows until we get an error or the end of the results.
			var v int64
			count := 0
			for rows.Next() {
				if err := rows.Scan(&v); err != nil {
					t.Fatal(err)
				}
				count++
			}
			if g, w := spanner.ErrCode(rows.Err()), codes.NotFound; g != w {
				t.Fatalf("error code mismatch\n Got: %v\nWant: %v\nRows: %v\n Tot: %v", g, w, count, len(allResults))
			}
			if err := rows.Close(); err != nil {
				t.Fatal(err)
			}
		}
		if err := tx.Commit(); err != nil {
			t.Fatal(err)
		}
	}
}

func TestRunPartitionedQuery(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	db, server, teardown := setupTestDBConnection(t)
	defer teardown()
	db.SetMaxOpenConns(1)

	query := "select * from random where tenant=@tenant"
	type queryExecutor interface {
		QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
	}
	type runPartitionedQueryTest struct {
		name                   string
		withTx                 bool
		maxResultsPerPartition int
	}
	tests := make([]runPartitionedQueryTest, 0)
	for _, withTx := range []bool{false} {
		for _, maxResultsPerPartition := range []int{0, 1, 5, 50, 200} {
			tests = append(tests, runPartitionedQueryTest{
				fmt.Sprintf("withTx: %v, maxResultsPerPartition: %v", withTx, maxResultsPerPartition),
				withTx,
				maxResultsPerPartition,
			})
		}
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var tx queryExecutor
			var err error
			if test.withTx {
				tx, err = BeginBatchReadOnlyTransaction(ctx, db, BatchReadOnlyTransactionOptions{})
			} else {
				tx, err = db.Conn(ctx)
			}
			if err != nil {
				t.Fatal(err)
			}
			defer func() {
				if tx, ok := tx.(*sql.Tx); ok {
					if err := tx.Commit(); err != nil {
						t.Fatal(err)
					}
				}
				if tx, ok := tx.(*sql.Conn); ok {
					if err := tx.Close(); err != nil {
						t.Fatal(err)
					}
				}
			}()

			// Setup results for each partition.
			server.TestSpanner.ClearPartitionResults()
			maxPartitions, allResults, err := setupRandomPartitionResults(server, " "+query, test.maxResultsPerPartition)
			if err != nil {
				t.Fatalf("failed to set up partition results: %v", err)
			}

			rows, err := tx.QueryContext(ctx, "run partitioned query "+query, ExecOptions{
				QueryOptions: spanner.QueryOptions{DataBoostEnabled: true},
			}, "test-tenant-id")
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = rows.Close() }()

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
			if rows.Err() != nil {
				t.Fatal(rows.Err())
			}
			if g, w := count, len(allResults); g != w {
				t.Fatalf("row count mismatch\n Got: %v\nWant: %v", g, w)
			}
			if err := rows.Close(); err != nil {
				t.Fatal(err)
			}

			requests := server.TestSpanner.DrainRequestsFromServer()
			beginRequests := testutil.RequestsOfType(requests, reflect.TypeOf(&sppb.BeginTransactionRequest{}))
			if g, w := len(beginRequests), 1; g != w {
				t.Fatalf("num begin requests mismatch\n Got: %v\nWant: %v", g, w)
			}
			beginRequest := beginRequests[0].(*sppb.BeginTransactionRequest)
			if !beginRequest.Options.GetReadOnly().GetStrong() {
				t.Fatal("missing strong timestamp bound option")
			}
			partitionRequests := testutil.RequestsOfType(requests, reflect.TypeOf(&sppb.PartitionQueryRequest{}))
			if g, w := len(partitionRequests), 1; g != w {
				t.Fatalf("num partition requests mismatch\n Got: %v\nWant: %v", g, w)
			}
			executeRequests := testutil.RequestsOfType(requests, reflect.TypeOf(&sppb.ExecuteSqlRequest{}))
			if g, w := len(executeRequests), maxPartitions; g != w {
				t.Fatalf("num execute requests mismatch\n Got: %v\nWant: %v", g, w)
			}
			executeRequest := executeRequests[0].(*sppb.ExecuteSqlRequest)
			if !executeRequest.DataBoostEnabled {
				t.Fatal("missing DataBoostEnabled option")
			}
			// (Batch) read-only transactions should not be committed.
			commitRequests := testutil.RequestsOfType(requests, reflect.TypeOf(&sppb.CommitRequest{}))
			if g, w := len(commitRequests), 0; g != w {
				t.Fatalf("num commit requests mismatch\n Got: %v\nWant: %v", g, w)
			}
		})
	}
}

func TestRunPartitionedQueryWithRandomResultSet(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	db, server, teardown := setupTestDBConnection(t)
	defer teardown()
	db.SetMaxOpenConns(1)

	query := "select * from random"
	type runPartitionedQueryTest struct {
		name    string
		numRows int
	}
	tests := make([]runPartitionedQueryTest, 0)
	for _, numRows := range []int{0, 1, 100, 1000, runtime.NumCPU(), rand.Intn(50)} {
		tests = append(tests, runPartitionedQueryTest{
			fmt.Sprintf("numRows: %v", numRows),
			numRows,
		})
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			// Setup results for each partition.
			results := testutil.CreateRandomResultSetWithUuidNullOption(test.numRows /* allowNullUuid = */, false, databasepb.DatabaseDialect_GOOGLE_STANDARD_SQL)
			server.TestSpanner.ClearPartitionResults()
			if err := server.TestSpanner.AddResultAsPartitionedResults(" "+query, results); err != nil {
				t.Fatal(err)
			}
			// Set up a regular result for the same query.
			if err := server.TestSpanner.PutStatementResult(query, &testutil.StatementResult{Type: testutil.StatementResultResultSet, ResultSet: results}); err != nil {
				t.Fatal(err)
			}

			partitionedRows, err := db.QueryContext(ctx, "run partitioned query "+query, ExecOptions{DecodeOption: DecodeOptionProto})
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = partitionedRows.Close() }()
			partitionedCols, partitionedValues, err := consumeResultsAndReturnResults(partitionedRows)
			if err != nil {
				t.Fatal(err)
			}

			regularRows, err := db.QueryContext(ctx, query, ExecOptions{DecodeOption: DecodeOptionProto})
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = regularRows.Close() }()
			regularCols, regularValues, err := consumeResultsAndReturnResults(regularRows)
			if err != nil {
				t.Fatal(err)
			}

			if g, w := partitionedCols, regularCols; !reflect.DeepEqual(g, w) {
				t.Fatalf("cols mismatch\n Got: %v\nWant: %v", g, w)
			}
			if g, w := partitionedValues, regularValues; !reflect.DeepEqual(g, w) {
				t.Fatalf("rows mismatch\n Got: %v\nWant: %v", g, w)
			}
			if err := partitionedRows.Close(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func consumeResultsAndReturnResults(r *sql.Rows) ([]string, [][]*spanner.GenericColumnValue, error) {
	cols, err := r.Columns()
	if err != nil {
		return nil, nil, err
	}
	rowCount := 0
	values := make([]any, len(cols))
	results := make([][]*spanner.GenericColumnValue, 0)
	for i := range values {
		values[i] = &spanner.GenericColumnValue{}
	}
	for r.Next() {
		if err := r.Scan(values...); err != nil {
			return nil, nil, err
		}
		results = append(results, make([]*spanner.GenericColumnValue, len(cols)))
		for i, v := range values {
			gcv := v.(*spanner.GenericColumnValue)
			results[rowCount][i] = &spanner.GenericColumnValue{Type: gcv.Type, Value: gcv.Value}
		}
		rowCount++
	}
	if err := r.Err(); err != nil {
		return nil, nil, err
	}
	// Order the results by the UUID column, as that column is guaranteed to be unique.
	idx := slices.Index(cols, "ColUuid")
	if idx == -1 {
		return nil, nil, fmt.Errorf("column uuid not found")
	}
	slices.SortFunc(results, func(a, b []*spanner.GenericColumnValue) int {
		uuidA := a[idx].Value.GetStringValue()
		uuidB := b[idx].Value.GetStringValue()
		if uuidA == uuidB || uuidA == "" || uuidB == "" {
			panic("Same UUID value found")
		}
		if uuidA == "" || uuidB == "" {
			panic("Null UUID value found")
		}
		return cmp.Compare(uuidA, uuidB)
	})
	return cols, results, nil
}

func setupRandomPartitionResults(server *testutil.MockedSpannerInMemTestServer, sql string, maxResultsPerPartition int) (maxPartitions int, allResults []int64, err error) {
	maxPartitions = rand.Intn(10) + 1
	// Setup results for each partition.
	allResults = make([]int64, 0, 1000)
	for index := 0; index < maxPartitions; index++ {
		var numResults int
		if maxResultsPerPartition > 0 {
			numResults = rand.Intn(maxResultsPerPartition)
		} else {
			numResults = 0
		}
		partitionResults := make([]int64, numResults)
		for i := 0; i < numResults; i++ {
			partitionResults[i] = rand.Int63()
		}
		allResults = append(allResults, partitionResults...)
		res := testutil.CreateSingleColumnInt64ResultSet(partitionResults, "FOO")
		token := fmt.Sprintf("%s: %v", sql, index)
		if err := server.TestSpanner.PutPartitionResult(
			[]byte(token),
			&testutil.StatementResult{Type: testutil.StatementResultResultSet, ResultSet: res}); err != nil {
			return 0, nil, err
		}
	}
	return
}
