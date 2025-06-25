package exported

import (
	"database/sql"

	"cloud.google.com/go/spanner"
	"cloud.google.com/go/spanner/apiv1/spannerpb"
	spannerdriver "github.com/googleapis/go-sql-spanner"
	"google.golang.org/protobuf/proto"
)

func BufferWrite(poolId, connId, txId int64, mutationBytes []byte) *Message {
	mutations := spannerpb.BatchWriteRequest_MutationGroup{}
	if err := proto.Unmarshal(mutationBytes, &mutations); err != nil {
		return errMessage(err)
	}
	tx, err := findTx(poolId, connId, txId)
	if err != nil {
		return errMessage(err)
	}
	return tx.bufferWrite(&mutations)
}

func ExecuteTransaction(poolId, connId, txId int64, statementBytes []byte) *Message {
	statement := spannerpb.ExecuteSqlRequest{}
	if err := proto.Unmarshal(statementBytes, &statement); err != nil {
		return errMessage(err)
	}
	tx, err := findTx(poolId, connId, txId)
	if err != nil {
		return errMessage(err)
	}
	return tx.Execute(&statement)
}

func Commit(poolId, connId, txId int64) *Message {
	tx, err := findTx(poolId, connId, txId)
	if err != nil {
		return errMessage(err)
	}
	conn, err := findConnection(poolId, connId)
	if err != nil {
		return errMessage(err)
	}
	conn.transactions.Delete(txId)
	return tx.Commit()
}

func Rollback(poolId, connId, txId int64) *Message {
	tx, err := findTx(poolId, connId, txId)
	if err != nil {
		return errMessage(err)
	}
	conn, err := findConnection(poolId, connId)
	if err != nil {
		return errMessage(err)
	}
	conn.transactions.Delete(txId)
	return tx.Rollback()
}

type transaction struct {
	backend *sql.Tx
	conn    *Connection
	closed  bool
}

func (tx *transaction) Close() *Message {
	if tx.closed {
		return &Message{}
	}
	tx.closed = true
	if err := tx.backend.Rollback(); err != nil {
		return errMessage(err)
	}
	return &Message{}
}

func (tx *transaction) bufferWrite(mutation *spannerpb.BatchWriteRequest_MutationGroup) *Message {
	mutations := make([]*spanner.Mutation, 0, len(mutation.Mutations))
	for _, m := range mutation.Mutations {
		spannerMutation, err := spanner.WrapMutation(m)
		if err != nil {
			return errMessage(err)
		}
		mutations = append(mutations, spannerMutation)
	}
	if err := tx.conn.backend.Conn.Raw(func(driverConn any) error {
		spannerConn, _ := driverConn.(spannerdriver.SpannerConn)
		return spannerConn.BufferWrite(mutations)
	}); err != nil {
		return errMessage(err)
	}
	return &Message{}
}

func (tx *transaction) Execute(statement *spannerpb.ExecuteSqlRequest) *Message {
	return execute(tx.conn, tx.backend, statement)
}

func (tx *transaction) Commit() *Message {
	tx.closed = true
	if err := tx.backend.Commit(); err != nil {
		return errMessage(err)
	}
	return &Message{}
}

func (tx *transaction) Rollback() *Message {
	tx.closed = true
	if err := tx.backend.Rollback(); err != nil {
		return errMessage(err)
	}
	return &Message{}
}
