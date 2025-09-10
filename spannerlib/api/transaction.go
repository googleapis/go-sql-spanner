package api

import (
	"context"
	"database/sql"

	"cloud.google.com/go/spanner"
	"cloud.google.com/go/spanner/apiv1/spannerpb"
	spannerdriver "github.com/googleapis/go-sql-spanner"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func Commit(poolId, connId int64) (*spannerpb.CommitResponse, error) {
	conn, err := findConnection(poolId, connId)
	if err != nil {
		return nil, err
	}
	return commit(conn)
}

func Rollback(poolId, connId int64) error {
	conn, err := findConnection(poolId, connId)
	if err != nil {
		return err
	}
	return rollback(conn)
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
	if err := tx.conn.backend.Raw(func(driverConn any) error {
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

func commit(conn *Connection) (*spannerpb.CommitResponse, error) {
	var response *spanner.CommitResponse
	if err := conn.backend.Raw(func(driverConn any) (err error) {
		spannerConn, _ := driverConn.(spannerConn)
		response, err = spannerConn.Commit(context.Background())
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

func rollback(conn *Connection) error {
	return conn.backend.Raw(func(driverConn any) (err error) {
		spannerConn, _ := driverConn.(spannerConn)
		return spannerConn.Rollback(context.Background())
	})
}
