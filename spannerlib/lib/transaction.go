package lib

import (
	"cloud.google.com/go/spanner/apiv1/spannerpb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"spannerlib/api"
)

func ExecuteTransaction(poolId, connId, txId int64, statementBytes []byte) *Message {
	statement := spannerpb.ExecuteSqlRequest{}
	if err := proto.Unmarshal(statementBytes, &statement); err != nil {
		return errMessage(err)
	}
	//id, err := api.ExecuteTransaction(poolId, connId, txId, &statement)
	err := status.Error(codes.Unimplemented, "not yet implemented")
	if err != nil {
		return errMessage(err)
	}
	return idMessage(0)
}

func Commit(poolId, connId int64) *Message {
	response, err := api.Commit(poolId, connId)
	if err != nil {
		return errMessage(err)
	}
	if response == nil {
		return &Message{}
	}
	res, err := proto.Marshal(response)
	if err != nil {
		return errMessage(err)
	}
	return &Message{Res: res}
}

func Rollback(poolId, connId int64) *Message {
	err := api.Rollback(poolId, connId)
	if err != nil {
		return errMessage(err)
	}
	return &Message{}
}
