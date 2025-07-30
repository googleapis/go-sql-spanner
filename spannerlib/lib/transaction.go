package lib

import (
	"cloud.google.com/go/spanner/apiv1/spannerpb"
	"google.golang.org/protobuf/proto"
	"spannerlib/api"
)

func BufferWrite(poolId, connId, txId int64, mutationBytes []byte) *Message {
	mutations := spannerpb.BatchWriteRequest_MutationGroup{}
	if err := proto.Unmarshal(mutationBytes, &mutations); err != nil {
		return errMessage(err)
	}
	err := api.BufferWrite(poolId, connId, txId, &mutations)
	if err != nil {
		return errMessage(err)
	}
	return &Message{}
}

func ExecuteTransaction(poolId, connId, txId int64, statementBytes []byte) *Message {
	statement := spannerpb.ExecuteSqlRequest{}
	if err := proto.Unmarshal(statementBytes, &statement); err != nil {
		return errMessage(err)
	}
	id, err := api.ExecuteTransaction(poolId, connId, txId, &statement)
	if err != nil {
		return errMessage(err)
	}
	return idMessage(id)
}

func Commit(poolId, connId, txId int64) *Message {
	response, err := api.Commit(poolId, connId, txId)
	if err != nil {
		return errMessage(err)
	}
	res, err := proto.Marshal(response)
	if err != nil {
		return errMessage(err)
	}
	return &Message{Res: res}
}

func Rollback(poolId, connId, txId int64) *Message {
	err := api.Rollback(poolId, connId, txId)
	if err != nil {
		return errMessage(err)
	}
	return &Message{}
}
