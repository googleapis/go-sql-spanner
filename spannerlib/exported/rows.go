package exported

import (
	"database/sql"

	"cloud.google.com/go/spanner"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"
)

func Next(poolId, connId, rowsId int64) *Message {
	res, err := findRows(poolId, connId, rowsId)
	if err != nil {
		return errMessage(err)
	}
	return res.Next()
}

func CloseRows(poolId, connId, rowsId int64) *Message {
	res, err := findRows(poolId, connId, rowsId)
	if err != nil {
		return errMessage(err)
	}
	return res.Close()
}

type rows struct {
	backend *sql.Rows
}

func (rows *rows) Close() *Message {
	err := rows.backend.Close()
	if err != nil {
		return errMessage(err)
	}
	return &Message{}
}

func (rows *rows) Metadata() *Message {
	return &Message{}
}

func (rows *rows) Next() *Message {
	ok := rows.backend.Next()
	if !ok {
		// An empty message indicates no more rows.
		return &Message{}
	}
	cols, err := rows.backend.Columns()
	if err != nil {
		return errMessage(err)
	}
	buffer := make([]any, len(cols))
	for i := range buffer {
		buffer[i] = &spanner.GenericColumnValue{}
	}
	if err := rows.backend.Scan(buffer...); err != nil {
		return errMessage(err)
	}
	row := &structpb.ListValue{
		Values: make([]*structpb.Value, len(buffer)),
	}
	for i := range buffer {
		row.Values[i] = buffer[i].(*spanner.GenericColumnValue).Value
	}
	res, err := proto.Marshal(row)
	if err != nil {
		return errMessage(err)
	}
	return &Message{Res: res}
}
