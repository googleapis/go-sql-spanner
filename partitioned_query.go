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
	"database/sql/driver"
	"errors"
	"io"

	"cloud.google.com/go/spanner"
	"github.com/googleapis/go-sql-spanner/parser"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type BatchReadOnlyTransactionOptions struct {
	TimestampBound spanner.TimestampBound
}

// PartitionedQueryOptions are used for queries that use the AutoPartitionQuery
// option, or for calls to PartitionQuery. These options are ignored for all
// other statements.
type PartitionedQueryOptions struct {
	// AutoPartitionQuery instructs the Spanner driver to automatically
	// partition the query, execute each partition, and return the results
	// as a single set of sql.Rows. This option can be used for queries
	// that are executed in a spanner.BatchReadOnlyTransaction and for
	// ad-hoc queries outside a transaction.
	AutoPartitionQuery bool
	// MaxParallelism is the maximum number of goroutines that will be
	// used to read data from an auto-partitioned query. This option
	// is only used if AutoPartitionQuery has been set to true.
	// Defaults to runtime.NumCPU.
	MaxParallelism int

	// PartitionQuery instructs the driver to only partition the query that
	// is being executed, instead of actually executing the query. The returned
	// rows will contain only one row with one column. This value should be
	// scanned into a spannerdriver.PartitionedQuery value.
	// This option can only be for queries that are executed using a transaction
	// that was started by spannerdriver.BeginBatchReadOnlyTransaction.
	//
	// Example:
	//   tx, _ := spannerdriver.BeginBatchReadOnlyTransaction(ctx, db, BatchReadOnlyTransactionOptions{})
	//   row := tx.QueryRowContext(ctx, testutil.SelectFooFromBar, spannerdriver.ExecOptions{
	//	     PartitionedQueryOptions: spannerdriver.PartitionedQueryOptions{
	//	         PartitionQuery: true,
	//       }})
	//   var pq spannerdriver.PartitionedQuery
	//   _ = row.Scan(&pq)
	//   // Execute each of the partitions.
	//   for index := range pq.Partitions {
	//	     rows, _ := pq.Execute(ctx, index, db)
	//       for rows.Next() {
	//           // Read data ...
	//       }
	//       rows.Close()
	//   }
	//   _ = tx.Commit()
	PartitionQuery bool

	// PartitionOptions are used to partition the given query. These
	// options are only used when one of AutoPartitionQuery or PartitionQuery
	// are set to true.
	PartitionOptions spanner.PartitionOptions

	// ExecutePartition instructs the driver to execute a specific spanner.Partition
	// that has previously been returned by a PartitionQuery call.
	ExecutePartition ExecutePartition
}

func (dest *PartitionedQueryOptions) merge(src *PartitionedQueryOptions) {
	if dest == nil || src == nil {
		return
	}
	if src.AutoPartitionQuery {
		dest.AutoPartitionQuery = src.AutoPartitionQuery
	}
	if src.PartitionQuery {
		dest.PartitionQuery = src.PartitionQuery
	}
	if src.MaxParallelism > 0 {
		dest.MaxParallelism = src.MaxParallelism
	}
	(&dest.ExecutePartition).merge(&src.ExecutePartition)
	if src.PartitionOptions.MaxPartitions > 0 {
		dest.PartitionOptions.MaxPartitions = src.PartitionOptions.MaxPartitions
	}
	if src.PartitionOptions.PartitionBytes > 0 {
		dest.PartitionOptions.PartitionBytes = src.PartitionOptions.PartitionBytes
	}
}

// PartitionedQuery is returned by the driver when a query is executed on a
// BatchReadOnlyTransaction with the PartitionedQueryOptions.PartitionQuery
// option set to true.
type PartitionedQuery struct {
	stmt        spanner.Statement
	execOptions *ExecOptions

	tx         *spanner.BatchReadOnlyTransaction
	Partitions []*spanner.Partition
}

// ExecutePartition is used to instruct the driver to execute one of the partitions
// that was returned by a previous call to PartitionQuery.
type ExecutePartition struct {
	PartitionedQuery *PartitionedQuery
	Index            int
}

func (dest *ExecutePartition) merge(src *ExecutePartition) {
	if src.PartitionedQuery != nil {
		dest.PartitionedQuery = src.PartitionedQuery
	}
	if src.Index > 0 {
		dest.Index = src.Index
	}
}

func (pq *PartitionedQuery) Scan(value any) error {
	if pqVal, ok := value.(*PartitionedQuery); ok {
		*pq = *pqVal
	}
	return nil
}

var _ driver.Rows = &partitionedQueryRows{}

type partitionedQueryRows struct {
	partitionedQuery *PartitionedQuery
	pos              int
}

func (p *partitionedQueryRows) Columns() []string {
	return []string{"PartitionedQuery"}
}

func (p *partitionedQueryRows) Close() error {
	p.partitionedQuery = nil
	return nil
}

func (p *partitionedQueryRows) Next(dest []driver.Value) error {
	if p.pos == 1 {
		return io.EOF
	}
	if p.partitionedQuery == nil {
		return errors.New("cursor is closed")
	}
	p.pos = 1
	dest[0] = p.partitionedQuery
	return nil
}

// BeginBatchReadOnlyTransaction begins a batch read-only transaction on a Spanner
// database. The underlying spanner.BatchReadOnlyTransaction can be used to partition
// queries and execute the individual partitions that are returned. It can also be
// used with the PartitionedQueryOptions.AutoPartitionQuery option to automatically
// partition and execute a query.
//
// NOTE: You *MUST* end the transaction by calling either Commit or Rollback on
// the transaction. Failure to do so will cause the connection that is used for
// the transaction to be leaked.
func BeginBatchReadOnlyTransaction(ctx context.Context, db *sql.DB, options BatchReadOnlyTransactionOptions) (*sql.Tx, error) {
	conn, err := db.Conn(ctx)
	if err != nil {
		return nil, err
	}
	if err := withTransactionCloseFunc(conn, func() {
		// Close the connection asynchronously, as the transaction will still
		// be active when we hit this point.
		go conn.Close()
	}); err != nil {
		return nil, err
	}
	tx, err := conn.BeginTx(ctx, &sql.TxOptions{ReadOnly: true, Isolation: WithBatchReadOnly(sql.LevelDefault)})
	if err := withTempBatchReadOnlyTransactionOptions(conn, &options); err != nil {
		return nil, err
	}
	if err != nil {
		clearTempBatchReadOnlyTransactionOptions(conn)
		return nil, err
	}
	return tx, nil
}

func withTempBatchReadOnlyTransactionOptions(conn *sql.Conn, options *BatchReadOnlyTransactionOptions) error {
	return conn.Raw(func(driverConn any) error {
		spannerConn, ok := driverConn.(SpannerConn)
		if !ok {
			// It is not a Spanner connection.
			return spanner.ToSpannerError(status.Error(codes.FailedPrecondition, "This function can only be used with a Spanner connection"))
		}
		spannerConn.setBatchReadOnlyTransactionOptions(options)
		return nil
	})
}

func clearTempBatchReadOnlyTransactionOptions(conn *sql.Conn) {
	_ = withTempBatchReadOnlyTransactionOptions(conn, nil)
	_ = conn.Close()
}

func (pq *PartitionedQuery) Execute(ctx context.Context, index int, db *sql.DB) (*sql.Rows, error) {
	return db.QueryContext(ctx, pq.stmt.SQL, ExecOptions{
		PartitionedQueryOptions: PartitionedQueryOptions{
			ExecutePartition: ExecutePartition{
				PartitionedQuery: pq,
				Index:            index,
			},
		},
	})
}

func (pq *PartitionedQuery) execute(ctx context.Context, index int) (*rows, error) {
	if index < 0 || index >= len(pq.Partitions) {
		return nil, spanner.ToSpannerError(status.Errorf(codes.InvalidArgument, "invalid partition index: %d", index))
	}
	spannerIter := pq.tx.Execute(ctx, pq.Partitions[index])
	iter := &readOnlyRowIterator{spannerIter, parser.StatementTypeQuery}
	return &rows{it: iter, decodeOption: pq.execOptions.DecodeOption}, nil
}

func (pq *PartitionedQuery) Close() {
	pq.tx.Cleanup(context.Background())
}
