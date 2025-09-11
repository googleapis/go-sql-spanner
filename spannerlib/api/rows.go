package api

import (
	"database/sql"
	"encoding/base64"
	"errors"

	"cloud.google.com/go/spanner"
	"cloud.google.com/go/spanner/apiv1/spannerpb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"
)

type EncodeRowOption int32

const (
	EncodeRowOptionProto EncodeRowOption = iota
)

func Metadata(poolId, connId, rowsId int64) (*spannerpb.ResultSetMetadata, error) {
	res, err := findRows(poolId, connId, rowsId)
	if err != nil {
		return nil, err
	}
	return res.Metadata()
}

func ResultSetStats(poolId, connId, rowsId int64) (*spannerpb.ResultSetStats, error) {
	res, err := findRows(poolId, connId, rowsId)
	if err != nil {
		return nil, err
	}
	return res.ResultSetStats()
}

func NextEncoded(poolId, connId, rowsId int64) ([]byte, error) {
	_, bytes, err := next(poolId, connId, rowsId, true)
	if err != nil {
		return nil, err
	}
	return bytes, nil
}

func Next(poolId, connId, rowsId int64) (*structpb.ListValue, error) {
	values, _, err := next(poolId, connId, rowsId, false)
	if err != nil {
		return nil, err
	}
	return values, nil
}

func next(poolId, connId, rowsId int64, marshalResult bool) (*structpb.ListValue, []byte, error) {
	rows, err := findRows(poolId, connId, rowsId)
	if err != nil {
		return nil, nil, err
	}
	values, err := rows.Next()
	if err != nil {
		return nil, nil, err
	}
	if !marshalResult || values == nil {
		return values, nil, nil
	}

	rows.marshalBuffer, err = proto.MarshalOptions{}.MarshalAppend(rows.marshalBuffer[:0], rows.values)
	if err != nil {
		return nil, nil, err
	}
	return values, rows.marshalBuffer, nil
}

func CloseRows(poolId, connId, rowsId int64) error {
	res, err := findRows(poolId, connId, rowsId)
	if err != nil {
		return err
	}
	conn, err := findConnection(poolId, connId)
	if err != nil {
		return err
	}
	conn.results.Delete(rowsId)
	return res.Close()
}

type rows struct {
	backend  *sql.Rows
	metadata *spannerpb.ResultSetMetadata
	stats    *spannerpb.ResultSetStats
	done     bool

	buffer        []any
	values        *structpb.ListValue
	marshalBuffer []byte
}

func (rows *rows) Close() error {
	err := rows.backend.Close()
	if err != nil {
		return err
	}
	return nil
}

func (rows *rows) metadataBytes() ([]byte, error) {
	colTypes, err := rows.backend.ColumnTypes()
	if err != nil {
		return nil, err
	}
	return base64.StdEncoding.DecodeString(colTypes[0].DatabaseTypeName())
}

func (rows *rows) Metadata() (*spannerpb.ResultSetMetadata, error) {
	return rows.metadata, nil
}

func (rows *rows) ResultSetStats() (*spannerpb.ResultSetStats, error) {
	if rows.stats == nil {
		rows.readStats()
	}
	return rows.stats, nil
}

type genericValue struct {
	v *structpb.Value
}

func (gv *genericValue) Scan(src any) error {
	if v, ok := src.(spanner.GenericColumnValue); ok {
		gv.v = v.Value
		return nil
	}
	return errors.New("cannot convert value to generic column value")
}

func (rows *rows) Next() (*structpb.ListValue, error) {
	// No columns means no rows, so just return nil to indicate that there are no (more) rows.
	if len(rows.metadata.RowType.Fields) == 0 || rows.done {
		return nil, nil
	}
	if rows.stats != nil {
		return nil, spanner.ToSpannerError(status.Error(codes.FailedPrecondition, "cannot read more data after returning stats"))
	}
	ok := rows.backend.Next()
	if !ok {
		rows.done = true
		// No more rows. Read stats and return nil.
		rows.readStats()
		// nil indicates no more rows.
		return nil, nil
	}

	if rows.buffer == nil {
		rows.buffer = make([]any, len(rows.metadata.RowType.Fields))
		for i := range rows.buffer {
			rows.buffer[i] = &genericValue{}
		}
		rows.values = &structpb.ListValue{
			Values: make([]*structpb.Value, len(rows.buffer)),
		}
		rows.marshalBuffer = make([]byte, 0)
	}
	if err := rows.backend.Scan(rows.buffer...); err != nil {
		return nil, err
	}
	for i := range rows.buffer {
		rows.values.Values[i] = rows.buffer[i].(*genericValue).v
	}
	return rows.values, nil
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
