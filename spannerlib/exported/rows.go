package exported

import (
	"database/sql"
	"encoding/base64"

	"cloud.google.com/go/spanner"
	"cloud.google.com/go/spanner/apiv1/spannerpb"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"
)

func Metadata(poolId, connId, rowsId int64) *Message {
	res, err := findRows(poolId, connId, rowsId)
	if err != nil {
		return errMessage(err)
	}
	return res.Metadata()
}

func UpdateCount(poolId, connId, rowsId int64) *Message {
	res, err := findRows(poolId, connId, rowsId)
	if err != nil {
		return errMessage(err)
	}
	return res.UpdateCount()
}

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
	conn, err := findConnection(poolId, connId)
	if err != nil {
		return errMessage(err)
	}
	conn.results.Delete(rowsId)
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

func (rows *rows) metadata() (*spannerpb.ResultSetMetadata, error) {
	b, err := rows.metadataBytes()
	if err != nil {
		return nil, err
	}
	var res spannerpb.ResultSetMetadata
	err = proto.Unmarshal(b, &res)
	if err != nil {
		return nil, err
	}
	return &res, nil
}

func (rows *rows) metadataBytes() ([]byte, error) {
	colTypes, err := rows.backend.ColumnTypes()
	if err != nil {
		return nil, err
	}
	return base64.StdEncoding.DecodeString(colTypes[0].DatabaseTypeName())
}

func (rows *rows) Metadata() *Message {
	metadataBytes, err := rows.metadataBytes()
	if err != nil {
		return errMessage(err)
	}
	return &Message{Res: metadataBytes}
}

func (rows *rows) UpdateCount() *Message {
	colTypes, err := rows.backend.Columns()
	if err != nil {
		return errMessage(err)
	}
	return &Message{Res: []byte(colTypes[0])}
}

func (rows *rows) Next() *Message {
	ok := rows.backend.Next()
	if !ok {
		// An empty message indicates no more rows.
		return &Message{}
	}
	metadata, err := rows.metadata()
	if err != nil {
		return errMessage(err)
	}
	buffer := make([]any, len(metadata.RowType.Fields))
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
