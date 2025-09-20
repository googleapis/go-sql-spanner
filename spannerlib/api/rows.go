// Copyright 2025 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package api

import (
	"context"
	"database/sql"
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

// Metadata returns the ResultSetMetadata of the given rows.
// This function can be called for any type of statement (queries, DML, DDL).
func Metadata(_ context.Context, poolId, connId, rowsId int64) (*spannerpb.ResultSetMetadata, error) {
	res, err := findRows(poolId, connId, rowsId)
	if err != nil {
		return nil, err
	}
	return res.Metadata()
}

// ResultSetStats returns the result statistics of the given rows.
// This function can only be called once all data in the rows have been fetched.
// The stats are empty for queries and DDL statements.
func ResultSetStats(ctx context.Context, poolId, connId, rowsId int64) (*spannerpb.ResultSetStats, error) {
	res, err := findRows(poolId, connId, rowsId)
	if err != nil {
		return nil, err
	}
	return res.ResultSetStats(ctx)
}

// NextEncoded returns the next row data in encoded form.
// Using NextEncoded instead of Next can be more efficient for large result sets,
// as it allows the library to re-use the encoding buffer.
// TODO: Add an encoder function as input argument, instead of hardcoding protobuf encoding here.
func NextEncoded(ctx context.Context, poolId, connId, rowsId int64) ([]byte, error) {
	_, bytes, err := next(ctx, poolId, connId, rowsId, true)
	if err != nil {
		return nil, err
	}
	return bytes, nil
}

// Next returns the next row as a protobuf ListValue.
func Next(ctx context.Context, poolId, connId, rowsId int64) (*structpb.ListValue, error) {
	values, _, err := next(ctx, poolId, connId, rowsId, false)
	if err != nil {
		return nil, err
	}
	return values, nil
}

// next returns the next row of data.
// The row is returned as a protobuf ListValue if marshalResult==false.
// The row is returned as a byte slice if marshalResult==true.
// TODO: Add generics to the function and add input arguments for encoding instead of hardcoding it.
func next(ctx context.Context, poolId, connId, rowsId int64, marshalResult bool) (*structpb.ListValue, []byte, error) {
	rows, err := findRows(poolId, connId, rowsId)
	if err != nil {
		return nil, nil, err
	}
	values, err := rows.Next(ctx)
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

// CloseRows closes the given rows. Callers must always call this to clean up any resources
// that are held by the underlying cursor.
func CloseRows(ctx context.Context, poolId, connId, rowsId int64) error {
	conn, err := findConnection(poolId, connId)
	if err != nil {
		return err
	}
	r, ok := conn.results.LoadAndDelete(rowsId)
	if !ok {
		return nil
	}
	res := r.(*rows)
	return res.Close(ctx)
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

func (rows *rows) Close(ctx context.Context) error {
	err := rows.backend.Close()
	if err != nil {
		return err
	}
	return nil
}

func (rows *rows) Metadata() (*spannerpb.ResultSetMetadata, error) {
	return rows.metadata, nil
}

func (rows *rows) ResultSetStats(ctx context.Context) (*spannerpb.ResultSetStats, error) {
	if rows.stats == nil {
		if err := rows.readStats(ctx); err != nil {
			return nil, err
		}
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

func (rows *rows) Next(ctx context.Context) (*structpb.ListValue, error) {
	// No columns means no rows, so just return nil to indicate that there are no (more) rows.
	if len(rows.metadata.RowType.Fields) == 0 || rows.done {
		return nil, nil
	}
	if rows.stats != nil {
		return nil, spanner.ToSpannerError(status.Error(codes.FailedPrecondition, "cannot read more data after returning stats"))
	}
	ok := rows.backend.Next()
	if !ok && rows.backend.Err() != nil {
		return nil, rows.backend.Err()
	}
	if !ok {
		rows.done = true
		// No more rows. Read stats and return nil.
		if err := rows.readStats(ctx); err != nil {
			return nil, err
		}
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

func (rows *rows) readStats(ctx context.Context) error {
	rows.stats = &spannerpb.ResultSetStats{}
	if !rows.backend.NextResultSet() {
		return status.Error(codes.Internal, "stats results not found")
	}
	if rows.backend.Next() {
		if err := rows.backend.Scan(&rows.stats); err != nil {
			return err
		}
	} else {
		if err := rows.backend.Err(); err != nil {
			return err
		}
		return status.Error(codes.Internal, "stats row not found")
	}
	return nil
}
