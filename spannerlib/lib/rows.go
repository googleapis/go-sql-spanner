package lib

import (
	"google.golang.org/protobuf/proto"
	"spannerlib/api"
)

func Metadata(poolId, connId, rowsId int64) *Message {
	metadata, err := api.Metadata(poolId, connId, rowsId)
	if err != nil {
		return errMessage(err)
	}
	metadataBytes, err := proto.Marshal(metadata)
	if err != nil {
		return errMessage(err)
	}
	return &Message{Res: metadataBytes}
}

func ResultSetStats(poolId, connId, rowsId int64) *Message {
	stats, err := api.ResultSetStats(poolId, connId, rowsId)
	if err != nil {
		return errMessage(err)
	}
	statsBytes, err := proto.Marshal(stats)
	if err != nil {
		return errMessage(err)
	}
	return &Message{Res: statsBytes}
}

func Next(poolId, connId, rowsId int64) *Message {
	valuesBytes, err := api.NextEncoded(poolId, connId, rowsId)
	if err != nil {
		return errMessage(err)
	}
	return &Message{Res: valuesBytes}
}

func CloseRows(poolId, connId, rowsId int64) *Message {
	err := api.CloseRows(poolId, connId, rowsId)
	if err != nil {
		return errMessage(err)
	}
	return &Message{}
}
