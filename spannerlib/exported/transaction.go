package exported

import (
	"database/sql"

	"cloud.google.com/go/spanner/apiv1/spannerpb"
	"google.golang.org/protobuf/proto"
)

func ExecuteTransaction(poolId, connId, txId int64, statementBytes []byte) *Message {
	statement := spannerpb.ExecuteBatchDmlRequest_Statement{}
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

func (tx *transaction) Execute(statement *spannerpb.ExecuteBatchDmlRequest_Statement) *Message {
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
