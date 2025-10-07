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
	"database/sql"
	"database/sql/driver"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"

	"cloud.google.com/go/spanner"
	"cloud.google.com/go/spanner/apiv1/spannerpb"
	spannerdriver "github.com/googleapis/go-sql-spanner"
	"github.com/googleapis/go-sql-spanner/parser"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// CloseConnection looks up the connection with the given poolId and connId and closes it.
func CloseConnection(ctx context.Context, poolId, connId int64) error {
	pool, err := findPool(poolId)
	if err != nil {
		return err
	}
	c, ok := pool.connections.LoadAndDelete(connId)
	if !ok {
		// Closing an unknown connection or a connection that has previously been closed is a no-op.
		return nil
	}
	conn := c.(*Connection)
	return conn.close(ctx)
}

// WriteMutations writes an array of mutations to Spanner. The mutations are buffered in
// the current read/write transaction if the connection currently has a read/write transaction.
// The mutations are applied to the database in a new read/write transaction that is automatically
// committed if the connection currently does not have a transaction.
//
// The function returns an error if the connection is currently in a read-only transaction.
//
// The mutationsBytes must be an encoded BatchWriteRequest_MutationGroup protobuf object.
func WriteMutations(ctx context.Context, poolId, connId int64, mutations *spannerpb.BatchWriteRequest_MutationGroup) (*spannerpb.CommitResponse, error) {
	conn, err := findConnection(poolId, connId)
	if err != nil {
		return nil, err
	}
	return conn.writeMutations(ctx, mutations)
}

// BeginTransaction starts a new transaction on the given connection.
// A connection can have at most one transaction at any time. This function therefore returns an error if the
// connection has an active transaction.
func BeginTransaction(ctx context.Context, poolId, connId int64, txOpts *spannerpb.TransactionOptions) error {
	conn, err := findConnection(poolId, connId)
	if err != nil {
		return err
	}
	return conn.BeginTransaction(ctx, txOpts)
}

// Commit commits the current transaction on the given connection.
func Commit(ctx context.Context, poolId, connId int64) (*spannerpb.CommitResponse, error) {
	conn, err := findConnection(poolId, connId)
	if err != nil {
		return nil, err
	}
	return conn.commit(ctx)
}

// Rollback rollbacks the current transaction on the given connection.
func Rollback(ctx context.Context, poolId, connId int64) error {
	conn, err := findConnection(poolId, connId)
	if err != nil {
		return err
	}
	return conn.rollback(ctx)
}

func Execute(ctx context.Context, poolId, connId int64, executeSqlRequest *spannerpb.ExecuteSqlRequest) (int64, error) {
	conn, err := findConnection(poolId, connId)
	if err != nil {
		return 0, err
	}
	return conn.Execute(ctx, executeSqlRequest)
}

func ExecuteBatch(ctx context.Context, poolId, connId int64, statements *spannerpb.ExecuteBatchDmlRequest) (*spannerpb.ExecuteBatchDmlResponse, error) {
	conn, err := findConnection(poolId, connId)
	if err != nil {
		return nil, err
	}
	return conn.ExecuteBatch(ctx, statements.Statements)
}

type Connection struct {
	// results contains the open query results for this connection.
	results    *sync.Map
	resultsIdx atomic.Int64

	// backend is the database/sql connection of this connection.
	backend *sql.Conn
}

// spannerConn is an internal interface that contains the internal functions that are used by this API.
// It is implemented by the spannerdriver.conn struct.
type spannerConn interface {
	WriteMutations(ctx context.Context, ms []*spanner.Mutation) (*spanner.CommitResponse, error)
	BeginReadOnlyTransaction(ctx context.Context, options *spannerdriver.ReadOnlyTransactionOptions, close func()) (driver.Tx, error)
	BeginReadWriteTransaction(ctx context.Context, options *spannerdriver.ReadWriteTransactionOptions, close func()) (driver.Tx, error)
	Commit(ctx context.Context) (*spanner.CommitResponse, error)
	Rollback(ctx context.Context) error
}

type queryExecutor interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
}

func (conn *Connection) close(ctx context.Context) error {
	conn.closeResults(ctx)
	// Rollback any open transactions on the connection.
	_ = conn.rollback(ctx)

	err := conn.backend.Close()
	if err != nil {
		return err
	}
	return nil
}

func (conn *Connection) writeMutations(ctx context.Context, mutation *spannerpb.BatchWriteRequest_MutationGroup) (*spannerpb.CommitResponse, error) {
	mutations := make([]*spanner.Mutation, 0, len(mutation.Mutations))
	for _, m := range mutation.Mutations {
		spannerMutation, err := spanner.WrapMutation(m)
		if err != nil {
			return nil, err
		}
		mutations = append(mutations, spannerMutation)
	}
	var commitResponse *spanner.CommitResponse
	if err := conn.backend.Raw(func(driverConn any) (err error) {
		sc, ok := driverConn.(spannerConn)
		if !ok {
			return status.Error(codes.Internal, "spanner driver connection does not implement spannerConn")
		}
		commitResponse, err = sc.WriteMutations(ctx, mutations)
		return err
	}); err != nil {
		return nil, err
	}

	// The commit response is nil if the connection is currently in a transaction.
	if commitResponse == nil {
		return nil, nil
	}
	response := spannerpb.CommitResponse{
		CommitTimestamp: timestamppb.New(commitResponse.CommitTs),
	}
	return &response, nil
}

func (conn *Connection) BeginTransaction(ctx context.Context, txOpts *spannerpb.TransactionOptions) error {
	var err error
	if txOpts.GetReadOnly() != nil {
		return conn.beginReadOnlyTransaction(ctx, convertToReadOnlyOpts(txOpts))
	} else if txOpts.GetPartitionedDml() != nil {
		err = spanner.ToSpannerError(status.Error(codes.InvalidArgument, "transaction type not supported"))
	} else {
		return conn.beginReadWriteTransaction(ctx, convertToReadWriteTransactionOptions(txOpts))
	}
	if err != nil {
		return err
	}
	return nil
}

func (conn *Connection) beginReadOnlyTransaction(ctx context.Context, opts *spannerdriver.ReadOnlyTransactionOptions) error {
	return conn.backend.Raw(func(driverConn any) (err error) {
		sc, ok := driverConn.(spannerConn)
		if !ok {
			return status.Error(codes.Internal, "driver connection does not implement spannerConn")
		}
		_, err = sc.BeginReadOnlyTransaction(ctx, opts, func() {})
		return err
	})
}

func (conn *Connection) beginReadWriteTransaction(ctx context.Context, opts *spannerdriver.ReadWriteTransactionOptions) error {
	return conn.backend.Raw(func(driverConn any) (err error) {
		sc, ok := driverConn.(spannerConn)
		if !ok {
			return status.Error(codes.Internal, "driver connection does not implement spannerConn")
		}
		_, err = sc.BeginReadWriteTransaction(ctx, opts, func() {})
		return err
	})
}

func (conn *Connection) commit(ctx context.Context) (*spannerpb.CommitResponse, error) {
	var response *spanner.CommitResponse
	if err := conn.backend.Raw(func(driverConn any) (err error) {
		sc, ok := driverConn.(spannerConn)
		if !ok {
			return status.Error(codes.Internal, "driver connection does not implement spannerConn")
		}
		response, err = sc.Commit(ctx)
		if err != nil {
			return err
		}
		return nil
	}); err != nil {
		return nil, err
	}

	// The commit response is nil for read-only transactions.
	if response == nil {
		return nil, nil
	}
	// TODO: Include commit stats
	return &spannerpb.CommitResponse{CommitTimestamp: timestamppb.New(response.CommitTs)}, nil
}

func (conn *Connection) rollback(ctx context.Context) error {
	return conn.backend.Raw(func(driverConn any) (err error) {
		sc, ok := driverConn.(spannerConn)
		if !ok {
			return status.Error(codes.Internal, "driver connection does not implement spannerConn")
		}
		return sc.Rollback(ctx)
	})
}

func convertToReadOnlyOpts(txOpts *spannerpb.TransactionOptions) *spannerdriver.ReadOnlyTransactionOptions {
	return &spannerdriver.ReadOnlyTransactionOptions{
		TimestampBound: convertTimestampBound(txOpts),
	}
}

func convertTimestampBound(txOpts *spannerpb.TransactionOptions) spanner.TimestampBound {
	ro := txOpts.GetReadOnly()
	if ro.GetStrong() {
		return spanner.StrongRead()
	} else if ro.GetReadTimestamp() != nil {
		return spanner.ReadTimestamp(ro.GetReadTimestamp().AsTime())
	} else if ro.GetMinReadTimestamp() != nil {
		return spanner.ReadTimestamp(ro.GetMinReadTimestamp().AsTime())
	} else if ro.GetExactStaleness() != nil {
		return spanner.ExactStaleness(ro.GetExactStaleness().AsDuration())
	} else if ro.GetMaxStaleness() != nil {
		return spanner.MaxStaleness(ro.GetMaxStaleness().AsDuration())
	}
	return spanner.TimestampBound{}
}

func convertToReadWriteTransactionOptions(txOpts *spannerpb.TransactionOptions) *spannerdriver.ReadWriteTransactionOptions {
	readLockMode := spannerpb.TransactionOptions_ReadWrite_READ_LOCK_MODE_UNSPECIFIED
	if txOpts.GetReadWrite() != nil {
		readLockMode = txOpts.GetReadWrite().GetReadLockMode()
	}
	return &spannerdriver.ReadWriteTransactionOptions{
		TransactionOptions: spanner.TransactionOptions{
			IsolationLevel: txOpts.GetIsolationLevel(),
			ReadLockMode:   readLockMode,
		},
	}
}

func convertIsolationLevel(level spannerpb.TransactionOptions_IsolationLevel) sql.IsolationLevel {
	switch level {
	case spannerpb.TransactionOptions_SERIALIZABLE:
		return sql.LevelSerializable
	case spannerpb.TransactionOptions_REPEATABLE_READ:
		return sql.LevelRepeatableRead
	}
	return sql.LevelDefault
}

func (conn *Connection) closeResults(ctx context.Context) {
	conn.results.Range(func(key, value interface{}) bool {
		if r, ok := value.(*rows); ok {
			_ = r.Close(ctx)
		}
		return true
	})
}

func (conn *Connection) Execute(ctx context.Context, statement *spannerpb.ExecuteSqlRequest) (int64, error) {
	return execute(ctx, conn, conn.backend, statement)
}

func (conn *Connection) ExecuteBatch(ctx context.Context, statements []*spannerpb.ExecuteBatchDmlRequest_Statement) (*spannerpb.ExecuteBatchDmlResponse, error) {
	return executeBatch(ctx, conn, conn.backend, statements)
}

func execute(ctx context.Context, conn *Connection, executor queryExecutor, statement *spannerpb.ExecuteSqlRequest) (int64, error) {
	params := extractParams(statement)
	it, err := executor.QueryContext(ctx, statement.Sql, params...)
	if err != nil {
		return 0, err
	}
	// The first result set should contain the metadata.
	if !it.Next() {
		return 0, fmt.Errorf("query returned no metadata")
	}
	metadata := &spannerpb.ResultSetMetadata{}
	if err := it.Scan(&metadata); err != nil {
		return 0, err
	}
	// Move to the next result set, which contains the normal data.
	if !it.NextResultSet() {
		return 0, fmt.Errorf("no results found after metadata")
	}
	id := conn.resultsIdx.Add(1)
	res := &rows{
		backend:  it,
		metadata: metadata,
	}
	if len(metadata.RowType.Fields) == 0 {
		// No rows returned. Read the stats now.
		res.readStats(ctx)
	}
	conn.results.Store(id, res)
	return id, nil
}

func executeBatch(ctx context.Context, conn *Connection, executor queryExecutor, statements []*spannerpb.ExecuteBatchDmlRequest_Statement) (*spannerpb.ExecuteBatchDmlResponse, error) {
	// Determine the type of batch that should be executed based on the type of statements.
	batchType, err := determineBatchType(conn, statements)
	if err != nil {
		return nil, err
	}
	switch batchType {
	case parser.BatchTypeDml:
		return executeBatchDml(ctx, conn, executor, statements)
	case parser.BatchTypeDdl:
		return executeBatchDdl(ctx, conn, executor, statements)
	default:
		return nil, status.Errorf(codes.InvalidArgument, "unsupported batch type: %v", batchType)
	}
}

func executeBatchDdl(ctx context.Context, conn *Connection, executor queryExecutor, statements []*spannerpb.ExecuteBatchDmlRequest_Statement) (*spannerpb.ExecuteBatchDmlResponse, error) {
	if err := conn.backend.Raw(func(driverConn any) error {
		spannerConn, _ := driverConn.(spannerdriver.SpannerConn)
		return spannerConn.StartBatchDDL()
	}); err != nil {
		return nil, err
	}
	for _, statement := range statements {
		_, err := executor.ExecContext(ctx, statement.Sql)
		if err != nil {
			return nil, err
		}
	}
	// TODO: Add support for getting the actual Batch DDL response.
	if err := conn.backend.Raw(func(driverConn any) (err error) {
		spannerConn, _ := driverConn.(spannerdriver.SpannerConn)
		return spannerConn.RunBatch(ctx)
	}); err != nil {
		return nil, err
	}

	response := spannerpb.ExecuteBatchDmlResponse{}
	response.ResultSets = make([]*spannerpb.ResultSet, len(statements))
	for i := range statements {
		response.ResultSets[i] = &spannerpb.ResultSet{Stats: &spannerpb.ResultSetStats{}}
	}
	return &response, nil
}

func executeBatchDml(ctx context.Context, conn *Connection, executor queryExecutor, statements []*spannerpb.ExecuteBatchDmlRequest_Statement) (*spannerpb.ExecuteBatchDmlResponse, error) {
	if err := conn.backend.Raw(func(driverConn any) error {
		spannerConn, _ := driverConn.(spannerdriver.SpannerConn)
		return spannerConn.StartBatchDML()
	}); err != nil {
		return nil, err
	}
	for _, statement := range statements {
		request := &spannerpb.ExecuteSqlRequest{
			Sql:        statement.Sql,
			Params:     statement.Params,
			ParamTypes: statement.ParamTypes,
		}
		params := extractParams(request)
		_, err := executor.ExecContext(ctx, statement.Sql, params...)
		if err != nil {
			return nil, err
		}
	}
	var spannerResult spannerdriver.SpannerResult
	if err := conn.backend.Raw(func(driverConn any) (err error) {
		spannerConn, _ := driverConn.(spannerdriver.SpannerConn)
		spannerResult, err = spannerConn.RunDmlBatch(ctx)
		return err
	}); err != nil {
		return nil, err
	}
	affected, err := spannerResult.BatchRowsAffected()
	if err != nil {
		return nil, err
	}
	response := spannerpb.ExecuteBatchDmlResponse{}
	response.ResultSets = make([]*spannerpb.ResultSet, len(affected))
	for i, aff := range affected {
		response.ResultSets[i] = &spannerpb.ResultSet{Stats: &spannerpb.ResultSetStats{RowCount: &spannerpb.ResultSetStats_RowCountExact{RowCountExact: aff}}}
	}
	return &response, nil
}

func extractParams(statement *spannerpb.ExecuteSqlRequest) []any {
	paramsLen := 1
	if statement.Params != nil {
		paramsLen = 1 + len(statement.Params.Fields)
	}
	params := make([]any, paramsLen)
	params = append(params, spannerdriver.ExecOptions{
		DecodeOption: spannerdriver.DecodeOptionProto,
		// TODO: Implement support for passing in stale query options
		// TimestampBound:          extractTimestampBound(statement),
		ReturnResultSetMetadata: true,
		ReturnResultSetStats:    true,
		DirectExecuteQuery:      true,
	})
	if statement.Params != nil {
		if statement.ParamTypes == nil {
			statement.ParamTypes = make(map[string]*spannerpb.Type)
		}
		for param, value := range statement.Params.Fields {
			genericValue := spanner.GenericColumnValue{
				Value: value,
				Type:  statement.ParamTypes[param],
			}
			if strings.HasPrefix(param, "_") {
				// Prefix the parameter name with a 'p' to work around the fact that database/sql does not allow
				// named arguments to start with anything else than a letter.
				params = append(params, sql.Named("p"+param, spannerdriver.SpannerNamedArg{NameInQuery: param, Value: genericValue}))
			} else {
				params = append(params, sql.Named(param, genericValue))
			}
		}
	}
	return params
}

func determineBatchType(conn *Connection, statements []*spannerpb.ExecuteBatchDmlRequest_Statement) (parser.BatchType, error) {
	if len(statements) == 0 {
		return parser.BatchTypeDdl, status.Errorf(codes.InvalidArgument, "cannot determine type of an empty batch")
	}
	var batchType parser.BatchType
	if err := conn.backend.Raw(func(driverConn any) error {
		spannerConn, _ := driverConn.(spannerdriver.SpannerConn)
		firstStatementType := spannerConn.DetectStatementType(statements[0].Sql)
		if firstStatementType == parser.StatementTypeDml {
			batchType = parser.BatchTypeDml
		} else if firstStatementType == parser.StatementTypeDdl {
			batchType = parser.BatchTypeDdl
		} else {
			return status.Errorf(codes.InvalidArgument, "unsupported statement type for batching: %v", firstStatementType)
		}
		for i, statement := range statements {
			if i > 0 {
				tp := spannerConn.DetectStatementType(statement.Sql)
				if tp != firstStatementType {
					return status.Errorf(codes.InvalidArgument, "Batches may not contain different types of statements. The first statement is of type %v. The statement on position %d is of type %v.", firstStatementType, i, tp)
				}
			}
		}
		return nil
	}); err != nil {
		return parser.BatchTypeDdl, err
	}

	return batchType, nil
}
