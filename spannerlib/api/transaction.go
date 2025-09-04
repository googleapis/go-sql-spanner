package api

import (
	"database/sql"

	"cloud.google.com/go/spanner"
	"cloud.google.com/go/spanner/apiv1/spannerpb"
	spannerdriver "github.com/googleapis/go-sql-spanner"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func BufferWrite(poolId, connId, txId int64, mutations *spannerpb.BatchWriteRequest_MutationGroup) error {
	tx, err := findTx(poolId, connId, txId)
	if err != nil {
		return err
	}
	return tx.bufferWrite(mutations)
}

func ExecuteTransaction(poolId, connId, txId int64, request *spannerpb.ExecuteSqlRequest) (int64, error) {
	tx, err := findTx(poolId, connId, txId)
	if err != nil {
		return 0, err
	}
	return tx.Execute(request)
}

func Commit(poolId, connId, txId int64) (*spannerpb.CommitResponse, error) {
	tx, err := findTx(poolId, connId, txId)
	if err != nil {
		return nil, err
	}
	conn, err := findConnection(poolId, connId)
	if err != nil {
		return nil, err
	}
	conn.transactions.Delete(txId)
	return tx.Commit()
}

func Rollback(poolId, connId, txId int64) error {
	tx, err := findTx(poolId, connId, txId)
	if err != nil {
		return err
	}
	conn, err := findConnection(poolId, connId)
	if err != nil {
		return err
	}
	conn.transactions.Delete(txId)
	return tx.Rollback()
}

type transaction struct {
	backend *sql.Tx
	conn    *Connection
	txOpts  *spannerpb.TransactionOptions
	closed  bool
}

func (tx *transaction) Close() error {
	if tx.closed {
		return nil
	}
	tx.closed = true
	if err := tx.backend.Rollback(); err != nil {
		return err
	}
	return nil
}

func (tx *transaction) bufferWrite(mutation *spannerpb.BatchWriteRequest_MutationGroup) error {
	mutations := make([]*spanner.Mutation, 0, len(mutation.Mutations))
	for _, m := range mutation.Mutations {
		spannerMutation, err := spanner.WrapMutation(m)
		if err != nil {
			return err
		}
		mutations = append(mutations, spannerMutation)
	}
	if err := tx.conn.backend.Conn.Raw(func(driverConn any) error {
		spannerConn, _ := driverConn.(spannerdriver.SpannerConn)
		return spannerConn.BufferWrite(mutations)
	}); err != nil {
		return err
	}
	return nil
}

func (tx *transaction) Execute(statement *spannerpb.ExecuteSqlRequest) (int64, error) {
	return execute(tx.conn, tx.backend, statement)
}

func (tx *transaction) Commit() (*spannerpb.CommitResponse, error) {
	tx.closed = true
	if err := tx.backend.Commit(); err != nil {
		return nil, err
	}
	var response *spanner.CommitResponse
	if tx.txOpts.GetReadWrite() == nil {
		return &spannerpb.CommitResponse{}, nil
	}
	if err := tx.conn.backend.Conn.Raw(func(driverConn any) (err error) {
		spannerConn, _ := driverConn.(spannerdriver.SpannerConn)
		response, err = spannerConn.CommitResponse()
		if err != nil {
			return err
		}
		return nil
	}); err != nil {
		return nil, err
	}
	// TODO: Include commit stats
	return &spannerpb.CommitResponse{CommitTimestamp: timestamppb.New(response.CommitTs)}, nil
}

func (tx *transaction) Rollback() error {
	tx.closed = true
	if err := tx.backend.Rollback(); err != nil {
		return err
	}
	return nil
}
