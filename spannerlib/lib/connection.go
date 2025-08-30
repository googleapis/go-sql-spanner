package lib

import "C"
import (
	"cloud.google.com/go/spanner/apiv1/spannerpb"
	"google.golang.org/protobuf/proto"
	"spannerlib/api"
)

func CloseConnection(poolId, connId int64) *Message {
	err := api.CloseConnection(poolId, connId)
	if err != nil {
		return errMessage(err)
	}
	return &Message{}
}

func Apply(poolId, connId int64, mutationBytes []byte) *Message {
	mutations := spannerpb.BatchWriteRequest_MutationGroup{}
	if err := proto.Unmarshal(mutationBytes, &mutations); err != nil {
		return errMessage(err)
	}
	response, err := api.Apply(poolId, connId, &mutations)
	if err != nil {
		return errMessage(err)
	}
	res, err := proto.Marshal(response)
	if err != nil {
		return errMessage(err)
	}
	return &Message{Res: res}
}

func BeginTransaction(poolId, connId int64, txOptsBytes []byte) *Message {
	txOpts := spannerpb.TransactionOptions{}
	if err := proto.Unmarshal(txOptsBytes, &txOpts); err != nil {
		return errMessage(err)
	}
	id, err := api.BeginTransaction(poolId, connId, &txOpts)
	if err != nil {
		return errMessage(err)
	}
	return idMessage(id)
}

func Execute(poolId, connId int64, executeSqlRequestBytes []byte) *Message {
	statement := spannerpb.ExecuteSqlRequest{}
	if err := proto.Unmarshal(executeSqlRequestBytes, &statement); err != nil {
		return errMessage(err)
	}
	id, err := api.Execute(poolId, connId, &statement)
	if err != nil {
		return errMessage(err)
	}
	return idMessage(id)
}

func ExecuteBatch(poolId, connId int64, statementsBytes []byte) *Message {
	statements := spannerpb.ExecuteBatchDmlRequest{}
	if err := proto.Unmarshal(statementsBytes, &statements); err != nil {
		return errMessage(err)
	}
	response, err := api.ExecuteBatch(poolId, connId, &statements)
	if err != nil {
		return errMessage(err)
	}
	res, err := proto.Marshal(response)
	if err != nil {
		return errMessage(err)
	}
	return &Message{Res: res}
}
