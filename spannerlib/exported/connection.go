package exported

import "C"
import (
	"context"
	"database/sql"
	"fmt"
	"sync"
	"sync/atomic"

	"cloud.google.com/go/spanner"
	"cloud.google.com/go/spanner/apiv1/spannerpb"
	spannerdriver "github.com/googleapis/go-sql-spanner"
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

func Execute(poolId, connId int64, statementBytes []byte) *Message {
	statement := spannerpb.ExecuteBatchDmlRequest_Statement{}
	if err := proto.Unmarshal(statementBytes, &statement); err != nil {
		return errMessage(err)
	}
	fmt.Printf("Statement: %v\n", statement.Sql)
	fmt.Printf("Params: %v\n", statement.Params)
	conn, err := findConnection(poolId, connId)
	if err != nil {
		return errMessage(err)
	}
	return conn.Execute(&statement)
}

type Connection struct {
	results    *sync.Map
	resultsIdx atomic.Int64

	backend *backend.SpannerConnection
}

func (conn *Connection) close() *Message {
	conn.results.Range(func(key, value interface{}) bool {
		res := value.(*rows)
		res.Close()
		return true
	})
	err := conn.backend.Close()
	if err != nil {
		return errMessage(err)
	}
	return &Message{}
}

func (conn *Connection) Execute(statement *spannerpb.ExecuteBatchDmlRequest_Statement) *Message {
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
	it, err := conn.backend.Conn.QueryContext(context.Background(), statement.Sql, params...)
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
