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
	"strings"
	"sync"
	"sync/atomic"

	"cloud.google.com/go/spanner"
	"cloud.google.com/go/spanner/apiv1/spannerpb"
	spannerdriver "github.com/googleapis/go-sql-spanner"
	"github.com/googleapis/go-sql-spanner/parser"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// CloseConnection looks up the connection with the given poolId and connId and closes it.
func CloseConnection(ctx context.Context, poolId, connId int64) error {
	pool, err := findPool(poolId)
	if err != nil {
		// Ignore NotFound errors to ensure that closing a non-existing connection is a no-op.
		if status.Code(err) == codes.NotFound {
			return nil
		}
		return err
	}
	c, ok := pool.connections.LoadAndDelete(connId)
	if !ok {
		// Closing an unknown connection or a connection that has previously been closed is a no-op.
		return nil
	}
	conn := c.(*Connection)
	ch := make(chan error, 1)
	go func() {
		ch <- conn.close(context.Background())
	}()
	select {
	case err := <-ch:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
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
	return WriteMutationsWithOptions(ctx, poolId, connId, mutations, nil)
}

// WriteMutationsWithOptions writes an array of mutations to Spanner with the given request options.
// The mutations are buffered in the current read/write transaction if the connection currently has
// a read/write transaction. The mutations are applied to the database in a new read/write transaction
// that is automatically committed if the connection currently does not have a transaction.
//
// The function returns an error if the connection is currently in a read-only transaction.
func WriteMutationsWithOptions(ctx context.Context, poolId, connId int64, mutations *spannerpb.BatchWriteRequest_MutationGroup, requestOptions *spannerpb.RequestOptions) (*spannerpb.CommitResponse, error) {
	if mutations == nil {
		return nil, status.Error(codes.InvalidArgument, "mutations must not be nil")
	}
	conn, err := findConnection(poolId, connId)
	if err != nil {
		return nil, err
	}
	return conn.writeMutations(ctx, mutations, requestOptions)
}

// BeginTransaction starts a new transaction on the given connection.
// A connection can have at most one transaction at any time. This function therefore returns an error if the
// connection has an active transaction.
//
// NOTE: The context that is passed in to this function is registered as the transaction context. The transaction is
// invalidated if the context is cancelled. The context that is passed in to this function should therefore not be a
// context that is cancelled right after calling this function.
func BeginTransaction(ctx context.Context, poolId, connId int64, txOpts *spannerpb.TransactionOptions) error {
	return BeginTransactionWithOptions(ctx, poolId, connId, txOpts, nil)
}

// BeginTransactionWithOptions starts a new transaction on the given connection with the given request options.
func BeginTransactionWithOptions(ctx context.Context, poolId, connId int64, txOpts *spannerpb.TransactionOptions, requestOpts *spannerpb.RequestOptions) error {
	conn, err := findConnection(poolId, connId)
	if err != nil {
		return err
	}
	return conn.BeginTransactionWithOptions(ctx, txOpts, requestOpts)
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
	return ExecuteWithDirectExecuteContext(ctx, nil, poolId, connId, executeSqlRequest)
}

func ExecuteWithDirectExecuteContext(ctx, directExecuteContext context.Context, poolId, connId int64, executeSqlRequest *spannerpb.ExecuteSqlRequest) (int64, error) {
	if executeSqlRequest == nil {
		return 0, status.Error(codes.InvalidArgument, "execute_sql_request must not be nil")
	}
	conn, err := findConnection(poolId, connId)
	if err != nil {
		return 0, err
	}
	return conn.Execute(ctx, directExecuteContext, executeSqlRequest)
}

func ExecuteBatch(ctx context.Context, poolId, connId int64, statements *spannerpb.ExecuteBatchDmlRequest) (*spannerpb.ExecuteBatchDmlResponse, error) {
	if statements == nil {
		return nil, status.Error(codes.InvalidArgument, "execute_batch_dml_request must not be nil")
	}
	conn, err := findConnection(poolId, connId)
	if err != nil {
		return nil, err
	}
	return conn.ExecuteBatch(ctx, statements)
}

type Connection struct {
	pool *Pool

	// results contains the open query results for this connection.
	results    *sync.Map
	resultsIdx atomic.Int64

	// backend is the database/sql connection of this connection.
	backend *sql.Conn

	// ctx is the lifecycle context of this Connection, used to cancel any long-running
	// background operations or queries when the connection is closed.
	ctx    context.Context
	cancel context.CancelFunc
}

// spannerConn is an internal interface that contains the internal functions that are used by this API.
// It is implemented by the spannerdriver.conn struct.
type spannerConn interface {
	WriteMutations(ctx context.Context, ms []*spanner.Mutation) (*spanner.CommitResponse, error)
	WriteMutationsWithOptions(ctx context.Context, ms []*spanner.Mutation, requestOptions *spannerpb.RequestOptions) (*spanner.CommitResponse, error)
	StartBatchDMLWithOptions(options *spannerdriver.ExecOptions) error
	BeginReadOnlyTransaction(ctx context.Context, options *spannerdriver.ReadOnlyTransactionOptions, close func()) (driver.Tx, error)
	BeginReadWriteTransaction(ctx context.Context, options *spannerdriver.ReadWriteTransactionOptions, close func()) (driver.Tx, error)
	Commit(ctx context.Context) (*spanner.CommitResponse, error)
	Rollback(ctx context.Context) error
	spannerdriver.SpannerConn
}

type queryExecutor interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
}

func (conn *Connection) close(ctx context.Context) error {
	if conn.cancel != nil {
		conn.cancel()
	}
	conn.closeResults(ctx)
	// Rollback any open transactions on the connection.
	_ = conn.rollback(ctx)

	err := conn.backend.Close()
	if err != nil {
		return err
	}
	return nil
}

func (conn *Connection) writeMutations(ctx context.Context, mutation *spannerpb.BatchWriteRequest_MutationGroup, requestOptions *spannerpb.RequestOptions) (*spannerpb.CommitResponse, error) {
	mutations := make([]*spanner.Mutation, 0, len(mutation.Mutations))
	for _, m := range mutation.Mutations {
		if m == nil {
			return nil, status.Error(codes.InvalidArgument, "mutation must not be nil")
		}
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
		commitResponse, err = sc.WriteMutationsWithOptions(ctx, mutations, requestOptions)
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

func (conn *Connection) BeginTransactionWithOptions(ctx context.Context, txOpts *spannerpb.TransactionOptions, requestOpts *spannerpb.RequestOptions) error {
	var err error
	if txOpts.GetReadOnly() != nil {
		return conn.beginReadOnlyTransaction(ctx, convertToReadOnlyOpts(txOpts))
	} else if txOpts.GetPartitionedDml() != nil {
		err = spanner.ToSpannerError(status.Error(codes.InvalidArgument, "transaction type not supported"))
	} else {
		return conn.beginReadWriteTransaction(ctx, convertToReadWriteTransactionOptions(txOpts, requestOpts))
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

func convertToReadWriteTransactionOptions(txOpts *spannerpb.TransactionOptions, requestOpts *spannerpb.RequestOptions) *spannerdriver.ReadWriteTransactionOptions {
	readLockMode := spannerpb.TransactionOptions_ReadWrite_READ_LOCK_MODE_UNSPECIFIED
	if txOpts.GetReadWrite() != nil {
		readLockMode = txOpts.GetReadWrite().GetReadLockMode()
	}
	opts := &spannerdriver.ReadWriteTransactionOptions{
		TransactionOptions: spanner.TransactionOptions{
			IsolationLevel: txOpts.GetIsolationLevel(),
			ReadLockMode:   readLockMode,
		},
	}
	if requestOpts != nil {
		opts.TransactionOptions.TransactionTag = requestOpts.TransactionTag
		opts.TransactionOptions.CommitPriority = requestOpts.Priority
	}
	return opts
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

func (conn *Connection) Execute(ctx, directExecuteContext context.Context, statement *spannerpb.ExecuteSqlRequest) (int64, error) {
	mergedCtx, cancel := mergeContexts(ctx, conn.ctx)
	return execute(mergedCtx, directExecuteContext, conn, conn.backend, statement, cancel)
}

func (conn *Connection) ExecuteBatch(ctx context.Context, batchRequest *spannerpb.ExecuteBatchDmlRequest) (*spannerpb.ExecuteBatchDmlResponse, error) {
	mergedCtx, cancel := mergeContexts(ctx, conn.ctx)
	defer cancel()
	return executeBatch(mergedCtx, conn, conn.backend, batchRequest)
}

func execute(ctx, directExecuteContext context.Context, conn *Connection, executor queryExecutor, statement *spannerpb.ExecuteSqlRequest, cancel context.CancelFunc) (int64, error) {
	meta := &spannerdriver.QueryMetadata{}
	params := extractParams(directExecuteContext, statement, meta)
	it, err := executor.QueryContext(ctx, statement.Sql, params...)
	if err != nil {
		if cancel != nil {
			cancel()
		}
		return 0, err
	}
	isMulti := meta.IsMulti
	res := &rows{
		backend: it,
		cancel:  cancel,
		isMulti: isMulti,
	}
	if err := res.readMetadata(ctx); err != nil {
		_ = it.Close()
		if cancel != nil {
			cancel()
		}
		return 0, err
	}
	id := conn.resultsIdx.Add(1)
	if !hasFields(res.metadata) {
		// No rows returned. Read the stats now.
		_ = res.readStats(ctx)
		if !isMulti {
			_ = it.Close()
			if cancel != nil {
				cancel()
			}
		}
	}
	conn.results.Store(id, res)
	return id, nil
}

func hasFields(metadata *spannerpb.ResultSetMetadata) bool {
	return metadata != nil && metadata.RowType != nil && metadata.RowType.Fields != nil && len(metadata.RowType.Fields) > 0
}

func executeBatch(ctx context.Context, conn *Connection, executor queryExecutor, batchRequest *spannerpb.ExecuteBatchDmlRequest) (*spannerpb.ExecuteBatchDmlResponse, error) {
	if batchRequest == nil {
		return nil, status.Error(codes.InvalidArgument, "batch request must not be nil")
	}
	// Determine the type of batch that should be executed based on the type of statements.
	batchType, err := determineBatchType(conn, batchRequest.Statements)
	if err != nil {
		return nil, err
	}
	switch batchType {
	case parser.BatchTypeDml:
		return executeBatchDml(ctx, conn, executor, batchRequest)
	case parser.BatchTypeDdl:
		return executeBatchDdl(ctx, conn, executor, batchRequest.Statements)
	default:
		return nil, status.Errorf(codes.InvalidArgument, "unsupported batch type: %v", batchType)
	}
}

func withSpannerConn(conn *Connection, fn func(spannerConn) error) error {
	return conn.backend.Raw(func(driverConn any) error {
		sc, ok := driverConn.(spannerConn)
		if !ok {
			return status.Error(codes.Internal, "spanner driver connection does not implement spannerConn")
		}
		return fn(sc)
	})
}

func abortBatchQuietly(conn *Connection) {
	_ = withSpannerConn(conn, func(sc spannerConn) error {
		_ = sc.AbortBatch()
		return nil
	})
}

func executeBatchDdl(ctx context.Context, conn *Connection, executor queryExecutor, statements []*spannerpb.ExecuteBatchDmlRequest_Statement) (*spannerpb.ExecuteBatchDmlResponse, error) {
	if err := withSpannerConn(conn, func(sc spannerConn) error {
		return sc.StartBatchDDL()
	}); err != nil {
		return nil, err
	}
	for _, statement := range statements {
		_, err := executor.ExecContext(ctx, statement.Sql)
		if err != nil {
			abortBatchQuietly(conn)
			return nil, err
		}
	}
	// TODO: Add support for getting the actual Batch DDL response.
	if err := withSpannerConn(conn, func(sc spannerConn) error {
		return sc.RunBatch(ctx)
	}); err != nil {
		abortBatchQuietly(conn)
		return nil, err
	}

	response := spannerpb.ExecuteBatchDmlResponse{}
	response.ResultSets = make([]*spannerpb.ResultSet, len(statements))
	for i := range statements {
		response.ResultSets[i] = &spannerpb.ResultSet{Stats: &spannerpb.ResultSetStats{}}
	}
	return &response, nil
}

func executeBatchDml(ctx context.Context, conn *Connection, executor queryExecutor, batchRequest *spannerpb.ExecuteBatchDmlRequest) (*spannerpb.ExecuteBatchDmlResponse, error) {
	var execOptions *spannerdriver.ExecOptions
	if batchRequest.RequestOptions != nil {
		execOptions = &spannerdriver.ExecOptions{
			QueryOptions: spanner.QueryOptions{
				RequestTag: batchRequest.RequestOptions.RequestTag,
				Priority:   batchRequest.RequestOptions.Priority,
			},
			TransactionOptions: spanner.TransactionOptions{
				TransactionTag: batchRequest.RequestOptions.TransactionTag,
			},
		}
	}
	if err := withSpannerConn(conn, func(sc spannerConn) error {
		return sc.StartBatchDMLWithOptions(execOptions)
	}); err != nil {
		return nil, err
	}
	for _, statement := range batchRequest.Statements {
		params := appendStatementParams(make([]any, 0, len(statement.GetParams().GetFields())), statement.Params, statement.ParamTypes)
		_, err := executor.ExecContext(ctx, statement.Sql, params...)
		if err != nil {
			abortBatchQuietly(conn)
			return nil, err
		}
	}
	var spannerResult spannerdriver.SpannerResult
	if err := withSpannerConn(conn, func(sc spannerConn) (err error) {
		spannerResult, err = sc.RunDmlBatch(ctx)
		return err
	}); err != nil {
		abortBatchQuietly(conn)
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

func extractParams(directExecuteContext context.Context, statement *spannerpb.ExecuteSqlRequest, meta *spannerdriver.QueryMetadata) []any {
	paramsLen := 1
	if statement.Params != nil {
		paramsLen = 1 + len(statement.Params.Fields)
	}
	requestTag := ""
	transactionTag := ""
	priority := spannerpb.RequestOptions_PRIORITY_UNSPECIFIED
	if statement.RequestOptions != nil {
		requestTag = statement.RequestOptions.RequestTag
		transactionTag = statement.RequestOptions.TransactionTag
		priority = statement.RequestOptions.Priority
	}
	params := make([]any, paramsLen)
	execOptions := spannerdriver.ExecOptions{
		DecodeOption:            spannerdriver.DecodeOptionProto,
		TimestampBound:          extractTimestampBound(statement),
		ReturnResultSetMetadata: true,
		ReturnResultSetStats:    true,
		DirectExecuteQuery:      true,
		DirectExecuteContext:    directExecuteContext,
		QueryMetadata:           meta,
		QueryOptions: spanner.QueryOptions{
			Mode:       &statement.QueryMode,
			RequestTag: requestTag,
			Priority:   priority,
		},
	}
	if transactionTag != "" {
		execOptions.PropertyValues = []spannerdriver.PropertyValue{{Identifier: parser.Identifier{Parts: []string{"transaction_tag"}}, Value: transactionTag}}
	}
	params = append(params, execOptions)
	return appendStatementParams(params, statement.Params, statement.ParamTypes)
}

func appendStatementParams(params []any, rawParams *structpb.Struct, paramTypes map[string]*spannerpb.Type) []any {
	if rawParams == nil {
		return params
	}
	for param, value := range rawParams.Fields {
		genericValue := spanner.GenericColumnValue{
			Value: value,
			Type:  paramTypes[param],
		}
		if strings.HasPrefix(param, "_") {
			// Prefix the parameter name with a 'p' to work around the fact that database/sql does not allow
			// named arguments to start with anything else than a letter.
			params = append(params, sql.Named("p"+param, spannerdriver.SpannerNamedArg{NameInQuery: param, Value: genericValue}))
		} else {
			params = append(params, sql.Named(param, genericValue))
		}
	}
	return params
}

func extractTimestampBound(statement *spannerpb.ExecuteSqlRequest) *spanner.TimestampBound {
	if statement.Transaction != nil && statement.Transaction.GetSingleUse() != nil && statement.Transaction.GetSingleUse().GetReadOnly() != nil {
		ro := statement.Transaction.GetSingleUse().GetReadOnly()
		var t spanner.TimestampBound
		if ro.GetStrong() {
			t = spanner.StrongRead()
		} else if ro.GetMaxStaleness() != nil {
			t = spanner.MaxStaleness(ro.GetMaxStaleness().AsDuration())
		} else if ro.GetExactStaleness() != nil {
			t = spanner.ExactStaleness(ro.GetExactStaleness().AsDuration())
		} else if ro.GetMinReadTimestamp() != nil {
			t = spanner.MinReadTimestamp(ro.GetMinReadTimestamp().AsTime())
		} else if ro.GetReadTimestamp() != nil {
			t = spanner.ReadTimestamp(ro.GetReadTimestamp().AsTime())
		} else {
			// Default to strong read.
			t = spanner.StrongRead()
		}
		return &t
	}
	return nil
}

func determineBatchType(conn *Connection, statements []*spannerpb.ExecuteBatchDmlRequest_Statement) (parser.BatchType, error) {
	if len(statements) == 0 {
		return parser.BatchTypeDdl, status.Errorf(codes.InvalidArgument, "cannot determine type of an empty batch")
	}
	for i, statement := range statements {
		if statement == nil {
			return parser.BatchTypeDdl, status.Errorf(codes.InvalidArgument, "statement at position %d must not be nil", i+1)
		}
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
			return status.Errorf(codes.InvalidArgument, "unsupported statement for batching: %v", statements[0].Sql)
		}
		for i, statement := range statements {
			if i > 0 {
				tp := spannerConn.DetectStatementType(statement.Sql)
				if tp != firstStatementType {
					return status.Errorf(codes.InvalidArgument, "Batches may not contain different types of statements. The first statement is of type %v. The statement on position %d is of type %v.", firstStatementType, i+1, tp)
				}
			}
		}
		return nil
	}); err != nil {
		return parser.BatchTypeDdl, err
	}

	return batchType, nil
}

func mergeContexts(ctx1, ctx2 context.Context) (context.Context, context.CancelFunc) {
	if ctx1 == nil {
		ctx1 = context.Background()
	}
	mergedCtx, cancel := context.WithCancel(ctx1)
	if ctx2 == nil || ctx2.Done() == nil {
		return mergedCtx, cancel
	}
	if ctx2.Err() != nil {
		cancel()
		return mergedCtx, cancel
	}
	stop := context.AfterFunc(ctx2, func() {
		cancel()
	})
	return mergedCtx, func() {
		stop()
		cancel()
	}
}

// TransactionState returns the current transaction state ('I', 'T', 'E') of the connection.
func TransactionState(poolId, connId int64) (rune, error) {
	conn, err := findConnection(poolId, connId)
	if err != nil {
		return 'I', err
	}
	var txState rune
	err = conn.backend.Raw(func(driverConn any) error {
		if sc, ok := driverConn.(spannerdriver.SpannerConn); ok {
			txState = rune(sc.TransactionState())
		} else {
			txState = 'I'
		}
		return nil
	})
	if err != nil || txState == 0 {
		return 'I', err
	}
	return txState, nil
}
