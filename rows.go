// Copyright 2021 Google LLC
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

package spannerdriver

import (
	"context"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"math/big"
	"reflect"
	"sync"
	"time"

	"cloud.google.com/go/civil"
	"cloud.google.com/go/spanner"
	"cloud.google.com/go/spanner/admin/database/apiv1/databasepb"
	sppb "cloud.google.com/go/spanner/apiv1/spannerpb"
	"github.com/google/uuid"
	"github.com/googleapis/go-sql-spanner/connectionstate"
	"google.golang.org/api/iterator"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/types/known/structpb"
)

type resultSetType int

const (
	resultSetTypeMetadata resultSetType = iota
	resultSetTypeResults
	resultSetTypeStats
	resultSetTypeNoMoreResults
)

var _ driver.RowsNextResultSet = &rows{}

func createRows(state *connectionstate.ConnectionState, it rowIterator, cancel context.CancelFunc, opts *ExecOptions) *rows {
	return &rows{
		state:                   state,
		it:                      it,
		cancel:                  cancel,
		decodeOption:            opts.DecodeOption,
		decodeToNativeArrays:    opts.DecodeToNativeArrays,
		returnResultSetMetadata: opts.ReturnResultSetMetadata,
		returnResultSetStats:    opts.ReturnResultSetStats,
	}
}

type rows struct {
	it     rowIterator
	close  func() error
	cancel context.CancelFunc

	colsOnce sync.Once
	dirtyErr error
	cols     []string
	colTypes []*sppb.Type

	state                *connectionstate.ConnectionState
	decodeOption         DecodeOption
	decodeToNativeArrays bool

	dirtyRow *spanner.Row

	currentResultSetType    resultSetType
	returnResultSetMetadata bool
	returnResultSetStats    bool

	hasReturnedResultSetMetadata bool
	hasReturnedResultSetStats    bool
}

// HasNextResultSet implements [driver.RowsNextResultSet.HasNextResultSet].
func (r *rows) HasNextResultSet() bool {
	if r.currentResultSetType == resultSetTypeMetadata && r.returnResultSetMetadata {
		return true
	}
	if r.currentResultSetType == resultSetTypeResults && r.returnResultSetStats {
		return true
	}
	return false
}

// NextResultSet implements [driver.RowsNextResultSet.NextResultSet].
func (r *rows) NextResultSet() error {
	if !r.HasNextResultSet() {
		return io.EOF
	}
	r.currentResultSetType++
	return nil
}

// Columns returns the names of the columns. The number of
// columns of the result is inferred from the length of the
// slice. If a particular column name isn't known, an empty
// string should be returned for that entry.
func (r *rows) Columns() []string {
	r.getColumns()
	switch r.currentResultSetType {
	case resultSetTypeMetadata:
		return []string{"metadata"}
	case resultSetTypeResults:
		return r.cols
	case resultSetTypeStats:
		return []string{"stats"}
	case resultSetTypeNoMoreResults:
		return []string{}
	}
	return []string{}
}

// Close closes the rows iterator.
func (r *rows) Close() error {
	r.it.Stop()
	if r.close != nil {
		if err := r.close(); err != nil {
			return err
		}
	}
	if r.cancel != nil {
		r.cancel()
	}
	return nil
}

func (r *rows) getColumns() {
	r.colsOnce.Do(func() {
		// Automatically advance the Rows object to the actual query data if we should
		// not return the ResultSetMetadata as a separate result set.
		if r.currentResultSetType == resultSetTypeMetadata && !r.returnResultSetMetadata {
			r.currentResultSetType = resultSetTypeResults
		}
		row, err := r.it.Next()
		if err == nil {
			r.dirtyRow = row
		} else {
			r.dirtyErr = err
			if err != iterator.Done {
				return
			}
		}
		metadata, err := r.it.Metadata()
		if err != nil {
			r.dirtyErr = err
			return
		}
		rowType := metadata.RowType
		r.cols = make([]string, len(rowType.Fields))
		r.colTypes = make([]*sppb.Type, len(rowType.Fields))
		for i, c := range rowType.Fields {
			r.cols[i] = c.Name
			r.colTypes[i] = c.Type
		}
	})
}

// Next is called to populate the next row of data into
// the provided slice. The provided slice will be the same
// size as the Columns() are wide.
//
// Next should return io.EOF when there are no more rows.
//
// The dest should not be written to outside of Next. Care
// should be taken when closing Rows not to modify
// a buffer held in dest.
func (r *rows) Next(dest []driver.Value) error {
	r.getColumns()

	if r.currentResultSetType == resultSetTypeMetadata {
		return r.nextMetadata(dest)
	}
	if r.currentResultSetType == resultSetTypeStats {
		return r.nextStats(dest)
	}

	var row *spanner.Row
	if r.dirtyErr != nil {
		err := r.dirtyErr
		r.dirtyErr = nil
		if err == iterator.Done {
			return io.EOF
		}
		return err
	}
	if r.dirtyRow != nil {
		row = r.dirtyRow
		r.dirtyRow = nil
	} else {
		var err error
		row, err = r.it.Next() // returns io.EOF when there is no next
		if err == iterator.Done {
			return io.EOF
		}
		if err != nil {
			return err
		}
	}

	if r.colTypes == nil {
		return fmt.Errorf("spanner: missing column types metadata")
	}

	for i := 0; i < row.Size(); i++ {
		if r.decodeOption == DecodeOptionProto {
			var col spanner.GenericColumnValue
			if err := row.Column(i, &col); err != nil {
				return err
			}
			dest[i] = col
			continue
		}
		switch r.colTypes[i].Code {
		case sppb.TypeCode_INT64, sppb.TypeCode_ENUM:
			var v spanner.NullInt64
			if err := row.Column(i, &v); err != nil {
				return err
			}
			if v.Valid {
				dest[i] = v.Int64
			} else {
				dest[i] = nil
			}
		case sppb.TypeCode_FLOAT32:
			var v spanner.NullFloat32
			if err := row.Column(i, &v); err != nil {
				return err
			}
			if v.Valid {
				dest[i] = v.Float32
			} else {
				dest[i] = nil
			}
		case sppb.TypeCode_FLOAT64:
			var v spanner.NullFloat64
			if err := row.Column(i, &v); err != nil {
				return err
			}
			if v.Valid {
				dest[i] = v.Float64
			} else {
				dest[i] = nil
			}
		case sppb.TypeCode_NUMERIC:
			if propertyDecodeNumericToString.GetValueOrDefault(r.state) {
				var col spanner.GenericColumnValue
				if err := row.Column(i, &col); err != nil {
					return err
				}
				if _, ok := col.Value.Kind.(*structpb.Value_NullValue); ok {
					dest[i] = nil
				} else {
					dest[i] = col.Value.GetStringValue()
				}
			} else {
				if r.colTypes[i].TypeAnnotation == sppb.TypeAnnotationCode_PG_NUMERIC {
					var v spanner.PGNumeric
					if err := row.Column(i, &v); err != nil {
						return err
					}
					if v.Valid {
						dest[i] = v.Numeric
					} else {
						dest[i] = nil
					}
				} else {
					var v spanner.NullNumeric
					if err := row.Column(i, &v); err != nil {
						return err
					}
					if v.Valid {
						dest[i] = v.Numeric
					} else {
						dest[i] = nil
					}
				}
			}
		case sppb.TypeCode_STRING:
			var v spanner.NullString
			if err := row.Column(i, &v); err != nil {
				return err
			}
			if v.Valid {
				dest[i] = v.StringVal
			} else {
				dest[i] = nil
			}
		case sppb.TypeCode_JSON:
			if r.colTypes[i].TypeAnnotation == sppb.TypeAnnotationCode_PG_JSONB {
				var v spanner.PGJsonB
				if err := row.Column(i, &v); err != nil {
					return err
				}
				dest[i] = v
			} else {
				var v spanner.NullJSON
				if err := row.Column(i, &v); err != nil {
					return err
				}
				// We always assign `v` to dest[i] here because there is no native type
				// for JSON in the Go sql package. That means that instead of returning
				// nil we should return a NullJSON with valid=false.
				dest[i] = v
			}
		case sppb.TypeCode_UUID:
			var v spanner.NullUUID
			if err := row.Column(i, &v); err != nil {
				return err
			}
			if v.Valid {
				dest[i] = v.UUID.String()
			} else {
				dest[i] = nil
			}
		case sppb.TypeCode_BYTES, sppb.TypeCode_PROTO:
			// The column value is a base64 encoded string.
			var v []byte
			if err := row.Column(i, &v); err != nil {
				return err
			}
			dest[i] = v
		case sppb.TypeCode_BOOL:
			var v spanner.NullBool
			if err := row.Column(i, &v); err != nil {
				return err
			}
			if v.Valid {
				dest[i] = v.Bool
			} else {
				dest[i] = nil
			}
		case sppb.TypeCode_DATE:
			var col spanner.GenericColumnValue
			if err := row.Column(i, &col); err != nil {
				return err
			}
			if _, ok := col.Value.Kind.(*structpb.Value_NullValue); ok {
				dest[i] = nil
			} else {
				dest[i] = col.Value.GetStringValue()
			}
		case sppb.TypeCode_TIMESTAMP:
			var v spanner.NullTime
			if err := row.Column(i, &v); err != nil {
				return err
			}
			if v.Valid {
				dest[i] = v.Time
			} else {
				dest[i] = nil
			}
		case sppb.TypeCode_ARRAY:
			switch r.colTypes[i].ArrayElementType.Code {
			case sppb.TypeCode_INT64, sppb.TypeCode_ENUM:
				if r.decodeToNativeArrays {
					var v []int64
					if err := row.Column(i, &v); err != nil {
						return err
					}
					dest[i] = v
				} else {
					var v []spanner.NullInt64
					if err := row.Column(i, &v); err != nil {
						return err
					}
					dest[i] = v
				}
			case sppb.TypeCode_FLOAT32:
				if r.decodeToNativeArrays {
					var v []float32
					if err := row.Column(i, &v); err != nil {
						return err
					}
					dest[i] = v
				} else {
					var v []spanner.NullFloat32
					if err := row.Column(i, &v); err != nil {
						return err
					}
					dest[i] = v
				}
			case sppb.TypeCode_FLOAT64:
				if r.decodeToNativeArrays {
					var v []float64
					if err := row.Column(i, &v); err != nil {
						return err
					}
					dest[i] = v
				} else {
					var v []spanner.NullFloat64
					if err := row.Column(i, &v); err != nil {
						return err
					}
					dest[i] = v
				}
			case sppb.TypeCode_NUMERIC:
				if r.colTypes[i].ArrayElementType.TypeAnnotation == sppb.TypeAnnotationCode_PG_NUMERIC {
					var v []spanner.PGNumeric
					if err := row.Column(i, &v); err != nil {
						return err
					}
					dest[i] = v
				} else {
					var v []spanner.NullNumeric
					if err := row.Column(i, &v); err != nil {
						return err
					}
					dest[i] = v
				}
			case sppb.TypeCode_STRING:
				if r.decodeToNativeArrays {
					var v []string
					if err := row.Column(i, &v); err != nil {
						return err
					}
					dest[i] = v
				} else {
					var v []spanner.NullString
					if err := row.Column(i, &v); err != nil {
						return err
					}
					dest[i] = v
				}
			case sppb.TypeCode_JSON:
				if r.colTypes[i].ArrayElementType.TypeAnnotation == sppb.TypeAnnotationCode_PG_JSONB {
					var v []spanner.PGJsonB
					if err := row.Column(i, &v); err != nil {
						// Workaround for https://github.com/googleapis/google-cloud-go/pull/13602
						if spanner.ErrCode(err) == codes.InvalidArgument && err.Error() == "spanner: code = \"InvalidArgument\", desc = \"type *[]spanner.PGJsonB cannot be used for decoding ARRAY[JSON]\"" {
							var tmp []spanner.NullJSON
							if err := row.Column(i, &tmp); err != nil {
								return err
							}
							v = make([]spanner.PGJsonB, 0, len(tmp))
							for _, j := range tmp {
								v = append(v, spanner.PGJsonB{Value: j.Value, Valid: j.Valid})
							}
						} else {
							return err
						}
					}
					dest[i] = v
				} else {
					var v []spanner.NullJSON
					if err := row.Column(i, &v); err != nil {
						return err
					}
					dest[i] = v
				}
			case sppb.TypeCode_UUID:
				if r.decodeToNativeArrays {
					var v []uuid.UUID
					if err := row.Column(i, &v); err != nil {
						return err
					}
					dest[i] = v
				} else {
					var v []spanner.NullUUID
					if err := row.Column(i, &v); err != nil {
						return err
					}
					dest[i] = v
				}
			case sppb.TypeCode_BYTES, sppb.TypeCode_PROTO:
				var v [][]byte
				if err := row.Column(i, &v); err != nil {
					return err
				}
				dest[i] = v
			case sppb.TypeCode_BOOL:
				if r.decodeToNativeArrays {
					var v []bool
					if err := row.Column(i, &v); err != nil {
						return err
					}
					dest[i] = v
				} else {
					var v []spanner.NullBool
					if err := row.Column(i, &v); err != nil {
						return err
					}
					dest[i] = v
				}
			case sppb.TypeCode_DATE:
				if r.decodeToNativeArrays {
					var v []civil.Date
					if err := row.Column(i, &v); err != nil {
						return err
					}
					dest[i] = v
				} else {
					var v []spanner.NullDate
					if err := row.Column(i, &v); err != nil {
						return err
					}
					dest[i] = v
				}
			case sppb.TypeCode_TIMESTAMP:
				if r.decodeToNativeArrays {
					var v []time.Time
					if err := row.Column(i, &v); err != nil {
						return err
					}
					dest[i] = v
				} else {
					var v []spanner.NullTime
					if err := row.Column(i, &v); err != nil {
						return err
					}
					dest[i] = v
				}
			default:
				return fmt.Errorf("unsupported array element type ARRAY<%v>, "+
					"use spannerdriver.ExecOptions{DecodeOption: spannerdriver.DecodeOptionProto} "+
					"to return the underlying protobuf value", r.colTypes[i].ArrayElementType.Code)
			}
		default:
			return fmt.Errorf("unsupported type %v, "+
				"use spannerdriver.ExecOptions{DecodeOption: spannerdriver.DecodeOptionProto} "+
				"to return the underlying protobuf value", r.colTypes[i].Code)
		}
	}
	return nil
}

func (r *rows) nextMetadata(dest []driver.Value) error {
	if r.dirtyErr != nil && !errors.Is(r.dirtyErr, iterator.Done) {
		return r.dirtyErr
	}
	if r.hasReturnedResultSetMetadata {
		return io.EOF
	}
	r.hasReturnedResultSetMetadata = true
	metadata, err := r.it.Metadata()
	if err != nil {
		return err
	}
	dest[0] = metadata
	return nil
}

func (r *rows) nextStats(dest []driver.Value) error {
	if r.hasReturnedResultSetStats {
		return io.EOF
	}
	r.hasReturnedResultSetStats = true
	dest[0] = r.it.ResultSetStats()
	return nil
}

func (r *rows) ColumnTypeScanType(index int) reflect.Type {
	if index < 0 || index >= len(r.colTypes) {
		return nil
	}
	t := r.colTypes[index]
	if r.decodeOption == DecodeOptionProto {
		return reflect.TypeOf(spanner.GenericColumnValue{})
	}
	return scanType(t, r.decodeToNativeArrays, r.state)
}

func scanType(t *sppb.Type, decodeToNativeArrays bool, state *connectionstate.ConnectionState) reflect.Type {
	switch t.Code {
	case sppb.TypeCode_INT64, sppb.TypeCode_ENUM:
		return reflect.TypeOf(int64(0))
	case sppb.TypeCode_FLOAT32:
		return reflect.TypeOf(float32(0))
	case sppb.TypeCode_FLOAT64:
		return reflect.TypeOf(float64(0))
	case sppb.TypeCode_NUMERIC:
		if propertyDecodeNumericToString.GetValueOrDefault(state) {
			return reflect.TypeOf("")
		}
		return reflect.TypeOf(big.Rat{})
	case sppb.TypeCode_STRING, sppb.TypeCode_DATE, sppb.TypeCode_UUID:
		return reflect.TypeOf("")
	case sppb.TypeCode_BYTES, sppb.TypeCode_PROTO:
		return reflect.TypeOf([]byte{})
	case sppb.TypeCode_BOOL:
		return reflect.TypeOf(true)
	case sppb.TypeCode_TIMESTAMP:
		return reflect.TypeOf(time.Time{})
	case sppb.TypeCode_JSON:
		if t.TypeAnnotation == sppb.TypeAnnotationCode_PG_JSONB {
			return reflect.TypeOf(spanner.PGJsonB{})
		}
		return reflect.TypeOf(spanner.NullJSON{})
	case sppb.TypeCode_ARRAY:
		if t.ArrayElementType == nil {
			return reflect.TypeOf([]any{}).Elem()
		}
		et := scanType(t.ArrayElementType, decodeToNativeArrays, state)
		if decodeToNativeArrays {
			return reflect.SliceOf(et)
		}
		switch t.ArrayElementType.Code {
		case sppb.TypeCode_INT64, sppb.TypeCode_ENUM:
			return reflect.TypeOf([]spanner.NullInt64{})
		case sppb.TypeCode_FLOAT32:
			return reflect.TypeOf([]spanner.NullFloat32{})
		case sppb.TypeCode_FLOAT64:
			return reflect.TypeOf([]spanner.NullFloat64{})
		case sppb.TypeCode_BOOL:
			return reflect.TypeOf([]spanner.NullBool{})
		case sppb.TypeCode_STRING:
			return reflect.TypeOf([]spanner.NullString{})
		case sppb.TypeCode_DATE:
			return reflect.TypeOf([]spanner.NullDate{})
		case sppb.TypeCode_TIMESTAMP:
			return reflect.TypeOf([]spanner.NullTime{})
		case sppb.TypeCode_NUMERIC:
			if t.ArrayElementType.TypeAnnotation == sppb.TypeAnnotationCode_PG_NUMERIC {
				return reflect.TypeOf([]spanner.PGNumeric{})
			}
			return reflect.TypeOf([]spanner.NullNumeric{})
		case sppb.TypeCode_JSON:
			if t.ArrayElementType.TypeAnnotation == sppb.TypeAnnotationCode_PG_JSONB {
				return reflect.TypeOf([]spanner.PGJsonB{})
			}
			return reflect.TypeOf([]spanner.NullJSON{})
		case sppb.TypeCode_UUID:
			return reflect.TypeOf([]spanner.NullUUID{})
		case sppb.TypeCode_BYTES, sppb.TypeCode_PROTO:
			return reflect.TypeOf([][]byte{})
		default:
			return reflect.SliceOf(et)
		}
	default:
		return reflect.TypeOf([]any{}).Elem()
	}
}

func (r *rows) ColumnTypeDatabaseTypeName(index int) string {
	if index < 0 || index >= len(r.colTypes) {
		return ""
	}
	t := r.colTypes[index]
	dialect := propertyDatabaseDialect.GetValueOrDefault(r.state)
	return databaseTypeName(t, dialect)
}

func databaseTypeName(t *sppb.Type, dialect databasepb.DatabaseDialect) string {
	isPG := dialect == databasepb.DatabaseDialect_POSTGRESQL
	switch t.Code {
	case sppb.TypeCode_INT64, sppb.TypeCode_ENUM:
		if isPG {
			return "bigint"
		}
		return "INT64"
	case sppb.TypeCode_FLOAT32:
		if isPG {
			return "real"
		}
		return "FLOAT32"
	case sppb.TypeCode_FLOAT64:
		if isPG {
			return "double precision"
		}
		return "FLOAT64"
	case sppb.TypeCode_NUMERIC:
		if isPG {
			return "numeric"
		}
		return "NUMERIC"
	case sppb.TypeCode_STRING:
		if isPG {
			return "text"
		}
		return "STRING"
	case sppb.TypeCode_DATE:
		if isPG {
			return "date"
		}
		return "DATE"
	case sppb.TypeCode_UUID:
		if isPG {
			return "uuid"
		}
		return "UUID"
	case sppb.TypeCode_BYTES, sppb.TypeCode_PROTO:
		if isPG {
			return "bytea"
		}
		return "BYTES"
	case sppb.TypeCode_BOOL:
		if isPG {
			return "boolean"
		}
		return "BOOL"
	case sppb.TypeCode_TIMESTAMP:
		if isPG {
			return "timestamp with time zone"
		}
		return "TIMESTAMP"
	case sppb.TypeCode_JSON:
		if isPG {
			return "jsonb"
		}
		return "JSON"
	case sppb.TypeCode_ARRAY:
		if t.ArrayElementType == nil {
			return "ARRAY"
		}
		if isPG {
			switch t.ArrayElementType.Code {
			case sppb.TypeCode_STRING:
				return "_text"
			case sppb.TypeCode_INT64:
				return "_int8"
			case sppb.TypeCode_FLOAT64:
				return "_float8"
			case sppb.TypeCode_BOOL:
				return "_bool"
			case sppb.TypeCode_BYTES:
				return "_bytea"
			case sppb.TypeCode_DATE:
				return "_date"
			case sppb.TypeCode_TIMESTAMP:
				return "_timestamptz"
			case sppb.TypeCode_NUMERIC:
				return "_numeric"
			case sppb.TypeCode_JSON:
				return "_jsonb"
			default:
				return "_" + databaseTypeName(t.ArrayElementType, dialect)
			}
		}
		return "ARRAY<" + databaseTypeName(t.ArrayElementType, dialect) + ">"
	default:
		return ""
	}
}

func (r *rows) ColumnTypePrecisionScale(index int) (precision, scale int64, ok bool) {
	if index < 0 || index >= len(r.colTypes) {
		return 0, 0, false
	}
	if r.colTypes[index].Code == sppb.TypeCode_NUMERIC {
		return 38, 9, true
	}
	return 0, 0, false
}

func (r *rows) ColumnTypeLength(index int) (length int64, ok bool) {
	return 0, false
}

func (r *rows) ColumnTypeNullable(index int) (nullable, ok bool) {
	return false, false
}

var _ driver.Rows = (*emptyRows)(nil)
var _ driver.RowsNextResultSet = (*emptyRows)(nil)
var emptyRowsMetadata = &sppb.ResultSetMetadata{RowType: &sppb.StructType{Fields: []*sppb.StructType_Field{}}}
var emptyRowsStats = &sppb.ResultSetStats{}

type emptyRows struct {
	cancel                  context.CancelFunc
	currentResultSetType    resultSetType
	returnResultSetMetadata bool
	returnResultSetStats    bool

	hasReturnedResultSetMetadata bool
	hasReturnedResultSetStats    bool
	stats                        *sppb.ResultSetStats
}

func createDriverResultRows(result driver.Result, isPartitionedDml bool, cancel context.CancelFunc, opts *ExecOptions) *emptyRows {
	stats := emptyRowsStats
	if affected, err := result.RowsAffected(); err == nil {
		if isPartitionedDml {
			stats = &sppb.ResultSetStats{RowCount: &sppb.ResultSetStats_RowCountLowerBound{RowCountLowerBound: affected}}
		} else {
			stats = &sppb.ResultSetStats{RowCount: &sppb.ResultSetStats_RowCountExact{RowCountExact: affected}}
		}
	}
	res := &emptyRows{
		cancel:                  cancel,
		returnResultSetMetadata: opts.ReturnResultSetMetadata,
		returnResultSetStats:    opts.ReturnResultSetStats,
		stats:                   stats,
	}
	if !opts.ReturnResultSetMetadata {
		res.currentResultSetType = resultSetTypeResults
	}
	return res
}

func (e *emptyRows) HasNextResultSet() bool {
	if e.currentResultSetType == resultSetTypeMetadata && e.returnResultSetMetadata {
		return true
	}
	if e.currentResultSetType == resultSetTypeResults && e.returnResultSetStats {
		return true
	}
	return false
}

func (e *emptyRows) NextResultSet() error {
	if !e.HasNextResultSet() {
		return io.EOF
	}
	e.currentResultSetType++
	return nil
}

func (e *emptyRows) Columns() []string {
	switch e.currentResultSetType {
	case resultSetTypeMetadata:
		return []string{"metadata"}
	case resultSetTypeResults:
		return []string{"affected_rows"}
	case resultSetTypeStats:
		return []string{"stats"}
	case resultSetTypeNoMoreResults:
		return []string{}
	}
	return []string{}
}

func (e *emptyRows) Close() error {
	if e.cancel != nil {
		e.cancel()
	}
	return nil
}

func (e *emptyRows) Next(dest []driver.Value) error {
	if e.currentResultSetType == resultSetTypeMetadata {
		return e.nextMetadata(dest)
	}
	if e.currentResultSetType == resultSetTypeStats {
		return e.nextStats(dest)
	}

	return io.EOF
}

func (e *emptyRows) nextMetadata(dest []driver.Value) error {
	if e.hasReturnedResultSetMetadata {
		return io.EOF
	}
	e.hasReturnedResultSetMetadata = true
	dest[0] = emptyRowsMetadata
	return nil
}

func (e *emptyRows) nextStats(dest []driver.Value) error {
	if e.hasReturnedResultSetStats {
		return io.EOF
	}
	e.hasReturnedResultSetStats = true
	if e.stats == nil {
		dest[0] = emptyRowsStats
	} else {
		dest[0] = e.stats
	}
	return nil
}
