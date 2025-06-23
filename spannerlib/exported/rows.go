package exported

import (
	"database/sql"
	"encoding/base64"

	"cloud.google.com/go/spanner"
	"cloud.google.com/go/spanner/apiv1/spannerpb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
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

func ResultSetStats(poolId, connId, rowsId int64) *Message {
	res, err := findRows(poolId, connId, rowsId)
	if err != nil {
		return errMessage(err)
	}
	return res.ResultSetStats()
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
	backend  *sql.Rows
	metadata *spannerpb.ResultSetMetadata
	stats    *spannerpb.ResultSetStats
}

func (rows *rows) Close() *Message {
	err := rows.backend.Close()
	if err != nil {
		return errMessage(err)
	}
	return &Message{}
}

func (rows *rows) metadataBytes() ([]byte, error) {
	colTypes, err := rows.backend.ColumnTypes()
	if err != nil {
		return nil, err
	}
	return base64.StdEncoding.DecodeString(colTypes[0].DatabaseTypeName())
}

func (rows *rows) Metadata() *Message {
	metadataBytes, err := proto.Marshal(rows.metadata)
	if err != nil {
		return errMessage(err)
	}
	return &Message{Res: metadataBytes}
}

func (rows *rows) ResultSetStats() *Message {
	if rows.stats == nil {
		rows.readStats()
	}
	statsBytes, err := proto.Marshal(rows.stats)
	if err != nil {
		return errMessage(err)
	}
	return &Message{Res: statsBytes}
}

func (rows *rows) Next() *Message {
	// No columns means no rows, so just return an empty message to indicate that there are no (more) rows.
	if len(rows.metadata.RowType.Fields) == 0 {
		return &Message{}
	}
	if rows.stats != nil {
		return errMessage(spanner.ToSpannerError(status.Error(codes.FailedPrecondition, "cannot read more data after returning stats")))
	}
	ok := rows.backend.Next()
	if !ok {
		// No more rows. Read stats and return an empty message.
		rows.readStats()
		// An empty message indicates no more rows.
		return &Message{}
	}
	buffer := make([]any, len(rows.metadata.RowType.Fields))
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

func (rows *rows) readStats() {
	rows.stats = &spannerpb.ResultSetStats{}
	if !rows.backend.NextResultSet() {
		return
	}
	if rows.backend.Next() {
		_ = rows.backend.Scan(&rows.stats)
	}
}
