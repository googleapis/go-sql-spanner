package api

import (
	"context"
	"database/sql"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"cloud.google.com/go/spanner"
	"cloud.google.com/go/spanner/apiv1/spannerpb"
	spannerdriver "github.com/googleapis/go-sql-spanner"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
	"spannerlib/backend"
)

func CloseConnection(poolId, connId int64) error {
	conn, err := findConnection(poolId, connId)
	if err != nil {
		return err
	}
	return conn.close()
}

func Apply(poolId, connId int64, mutations *spannerpb.BatchWriteRequest_MutationGroup) (*spannerpb.CommitResponse, error) {
	conn, err := findConnection(poolId, connId)
	if err != nil {
		return nil, err
	}
	return conn.apply(mutations)
}

func BeginTransaction(poolId, connId int64, txOpts *spannerpb.TransactionOptions) (int64, error) {
	conn, err := findConnection(poolId, connId)
	if err != nil {
		return 0, err
	}
	return conn.BeginTransaction(txOpts)
}

func Execute(poolId, connId int64, executeSqlRequest *spannerpb.ExecuteSqlRequest) (int64, error) {
	conn, err := findConnection(poolId, connId)
	if err != nil {
		return 0, err
	}
	return conn.Execute(executeSqlRequest)
}

func ExecuteBatchDml(poolId, connId int64, statements *spannerpb.ExecuteBatchDmlRequest) (*spannerpb.ExecuteBatchDmlResponse, error) {
	conn, err := findConnection(poolId, connId)
	if err != nil {
		return nil, err
	}
	return conn.ExecuteBatchDml(statements.Statements)
}

type Connection struct {
	results    *sync.Map
	resultsIdx atomic.Int64

	transactions    *sync.Map
	transactionsIdx atomic.Int64

	backend *backend.SpannerConnection
}

type queryExecutor interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
}

func (conn *Connection) close() error {
	conn.results.Range(func(key, value interface{}) bool {
		res := value.(*rows)
		res.Close()
		return true
	})
	conn.transactions.Range(func(key, value interface{}) bool {
		res := value.(*transaction)
		res.Close()
		return true
	})
	err := conn.backend.Close()
	if err != nil {
		return err
	}
	return nil
}

func (conn *Connection) apply(mutation *spannerpb.BatchWriteRequest_MutationGroup) (*spannerpb.CommitResponse, error) {
	ctx := context.Background()
	mutations := make([]*spanner.Mutation, 0, len(mutation.Mutations))
	for _, m := range mutation.Mutations {
		spannerMutation, err := spanner.WrapMutation(m)
		if err != nil {
			return nil, err
		}
		mutations = append(mutations, spannerMutation)
	}
	var commitTimestamp time.Time
	if err := conn.backend.Conn.Raw(func(driverConn any) (err error) {
		spannerConn, _ := driverConn.(spannerdriver.SpannerConn)
		commitTimestamp, err = spannerConn.Apply(ctx, mutations)
		return err
	}); err != nil {
		return nil, err
	}
	response := spannerpb.CommitResponse{
		CommitTimestamp: timestamppb.New(commitTimestamp),
	}
	return &response, nil
}

func (conn *Connection) BeginTransaction(txOpts *spannerpb.TransactionOptions) (int64, error) {
	var tx *sql.Tx
	var err error
	if txOpts.GetReadOnly() != nil {
		tx, err = spannerdriver.BeginReadOnlyTransactionOnConn(
			context.Background(), conn.backend.Conn, convertToReadOnlyOpts(txOpts))
	} else if txOpts.GetPartitionedDml() != nil {
		err = spanner.ToSpannerError(status.Error(codes.InvalidArgument, "transaction type not supported"))
	} else {
		tx, err = spannerdriver.BeginReadWriteTransactionOnConn(
			context.Background(), conn.backend.Conn, convertToReadWriteTransactionOptions(txOpts))
	}
	if err != nil {
		return 0, err
	}
	id := conn.transactionsIdx.Add(1)
	res := &transaction{
		backend: tx,
		conn:    conn,
		txOpts:  txOpts,
	}
	conn.transactions.Store(id, res)
	return id, nil
}

func convertToReadOnlyOpts(txOpts *spannerpb.TransactionOptions) spannerdriver.ReadOnlyTransactionOptions {
	return spannerdriver.ReadOnlyTransactionOptions{
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

func convertToReadWriteTransactionOptions(txOpts *spannerpb.TransactionOptions) spannerdriver.ReadWriteTransactionOptions {
	readLockMode := spannerpb.TransactionOptions_ReadWrite_READ_LOCK_MODE_UNSPECIFIED
	if txOpts.GetReadWrite() != nil {
		readLockMode = txOpts.GetReadWrite().GetReadLockMode()
	}
	return spannerdriver.ReadWriteTransactionOptions{
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

func (conn *Connection) Execute(statement *spannerpb.ExecuteSqlRequest) (int64, error) {
	return execute(conn, conn.backend.Conn, statement)
}

func (conn *Connection) ExecuteBatchDml(statements []*spannerpb.ExecuteBatchDmlRequest_Statement) (*spannerpb.ExecuteBatchDmlResponse, error) {
	return executeBatchDml(conn, conn.backend.Conn, statements)
}

func execute(conn *Connection, executor queryExecutor, statement *spannerpb.ExecuteSqlRequest) (int64, error) {
	params := extractParams(statement)
	it, err := executor.QueryContext(context.Background(), statement.Sql, params...)
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
		res.readStats()
	}
	conn.results.Store(id, res)
	return id, nil
}

func executeBatchDml(conn *Connection, executor queryExecutor, statements []*spannerpb.ExecuteBatchDmlRequest_Statement) (*spannerpb.ExecuteBatchDmlResponse, error) {
	if err := conn.backend.Conn.Raw(func(driverConn any) error {
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
		_, err := executor.ExecContext(context.Background(), statement.Sql, params...)
		if err != nil {
			return nil, err
		}
	}
	var spannerResult spannerdriver.SpannerResult
	if err := conn.backend.Conn.Raw(func(driverConn any) (err error) {
		spannerConn, _ := driverConn.(spannerdriver.SpannerConn)
		spannerResult, err = spannerConn.RunDmlBatch(context.Background())
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
		DecodeOption:            spannerdriver.DecodeOptionProto,
		TimestampBound:          extractTimestampBound(statement),
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
		}
		return &t
	}
	return nil
}
