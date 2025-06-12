package exported

import "C"
import (
	"context"
	"database/sql"
	"sync"
	"sync/atomic"

	"cloud.google.com/go/spanner"
	"cloud.google.com/go/spanner/apiv1/spannerpb"
	spannerdriver "github.com/googleapis/go-sql-spanner"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"spannerlib/backend"
)

func CloseConnection(poolId, connId int64) *Message {
	conn, err := findConnection(poolId, connId)
	if err != nil {
		return errMessage(err)
	}
	return conn.close()
}

func BeginTransaction(poolId, connId int64, txOptsBytes []byte) *Message {
	txOpts := spannerpb.TransactionOptions{}
	if err := proto.Unmarshal(txOptsBytes, &txOpts); err != nil {
		return errMessage(err)
	}
	conn, err := findConnection(poolId, connId)
	if err != nil {
		return errMessage(err)
	}
	return conn.BeginTransaction(&txOpts)
}

func Execute(poolId, connId int64, statementBytes []byte) *Message {
	statement := spannerpb.ExecuteBatchDmlRequest_Statement{}
	if err := proto.Unmarshal(statementBytes, &statement); err != nil {
		return errMessage(err)
	}
	conn, err := findConnection(poolId, connId)
	if err != nil {
		return errMessage(err)
	}
	return conn.Execute(&statement)
}

func ExecuteBatchDml(poolId, connId int64, statementsBytes []byte) *Message {
	statements := spannerpb.ExecuteBatchDmlRequest{}
	if err := proto.Unmarshal(statementsBytes, &statements); err != nil {
		return errMessage(err)
	}
	conn, err := findConnection(poolId, connId)
	if err != nil {
		return errMessage(err)
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

func (conn *Connection) close() *Message {
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
		return errMessage(err)
	}
	return &Message{}
}

func (conn *Connection) BeginTransaction(txOpts *spannerpb.TransactionOptions) *Message {
	var tx *sql.Tx
	var err error
	if txOpts.GetReadOnly() != nil {
		tx, err = spannerdriver.BeginReadOnlyTransactionOnConn(
			context.Background(), conn.backend.Conn, convertToReadOnlyOpts(txOpts))
	} else if txOpts.GetReadWrite() != nil {
		tx, err = spannerdriver.BeginReadWriteTransactionOnConn(
			context.Background(), conn.backend.Conn, convertToReadWriteTransactionOptions(txOpts))
	} else {
		err = spanner.ToSpannerError(status.Error(codes.InvalidArgument, "transaction type not supported"))
	}
	if err != nil {
		return errMessage(err)
	}
	id := conn.transactionsIdx.Add(1)
	res := &transaction{
		backend: tx,
		conn:    conn,
	}
	conn.transactions.Store(id, res)
	return idMessage(id)
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
	return spannerdriver.ReadWriteTransactionOptions{
		TransactionOptions: spanner.TransactionOptions{
			IsolationLevel: txOpts.GetIsolationLevel(),
			ReadLockMode:   txOpts.GetReadWrite().GetReadLockMode(),
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

func (conn *Connection) Execute(statement *spannerpb.ExecuteBatchDmlRequest_Statement) *Message {
	return execute(conn, conn.backend.Conn, statement)
}

func (conn *Connection) ExecuteBatchDml(statements []*spannerpb.ExecuteBatchDmlRequest_Statement) *Message {
	return executeBatchDml(conn, conn.backend.Conn, statements)
}

func execute(conn *Connection, executor queryExecutor, statement *spannerpb.ExecuteBatchDmlRequest_Statement) *Message {
	params := extractParams(statement)
	it, err := executor.QueryContext(context.Background(), statement.Sql, params...)
	if err != nil {
		return errMessage(err)
	}
	id := conn.resultsIdx.Add(1)
	res := &rows{
		backend: it,
	}
	conn.results.Store(id, res)
	return idMessage(id)
}

func executeBatchDml(conn *Connection, executor queryExecutor, statements []*spannerpb.ExecuteBatchDmlRequest_Statement) *Message {
	if err := conn.backend.Conn.Raw(func(driverConn any) error {
		spannerConn, _ := driverConn.(spannerdriver.SpannerConn)
		return spannerConn.StartBatchDML()
	}); err != nil {
		return errMessage(err)
	}
	for _, statement := range statements {
		params := extractParams(statement)
		_, err := executor.ExecContext(context.Background(), statement.Sql, params...)
		if err != nil {
			return errMessage(err)
		}
	}
	var spannerResult spannerdriver.SpannerResult
	if err := conn.backend.Conn.Raw(func(driverConn any) (err error) {
		spannerConn, _ := driverConn.(spannerdriver.SpannerConn)
		spannerResult, err = spannerConn.RunDmlBatch(context.Background())
		return err
	}); err != nil {
		return errMessage(err)
	}
	affected, err := spannerResult.BatchRowsAffected()
	if err != nil {
		return errMessage(err)
	}
	response := spannerpb.ExecuteBatchDmlResponse{}
	response.ResultSets = make([]*spannerpb.ResultSet, len(affected))
	for i, aff := range affected {
		response.ResultSets[i] = &spannerpb.ResultSet{Stats: &spannerpb.ResultSetStats{RowCount: &spannerpb.ResultSetStats_RowCountExact{RowCountExact: aff}}}
	}
	res, err := proto.Marshal(&response)
	if err != nil {
		return errMessage(err)
	}
	return &Message{Res: res}
}

func extractParams(statement *spannerpb.ExecuteBatchDmlRequest_Statement) []any {
	paramsLen := 1
	if statement.Params != nil {
		paramsLen = 1 + len(statement.Params.Fields)
	}
	params := make([]any, paramsLen)
	params = append(params, spannerdriver.ExecOptions{DecodeOption: spannerdriver.DecodeOptionProto})
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
