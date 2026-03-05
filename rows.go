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
	"sync"
	"time"

	"cloud.google.com/go/civil"
	"cloud.google.com/go/spanner"
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
		for i, c := range rowType.Fields {
			r.cols[i] = c.Name
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

	for i := 0; i < row.Size(); i++ {
		var col spanner.GenericColumnValue
		if err := row.Column(i, &col); err != nil {
			return err
		}
		if r.decodeOption == DecodeOptionProto {
			dest[i] = col
			continue
		}
		switch col.Type.Code {
		case sppb.TypeCode_INT64, sppb.TypeCode_ENUM:
			var v spanner.NullInt64
			if err := col.Decode(&v); err != nil {
				return err
			}
			if v.Valid {
				dest[i] = v.Int64
			} else {
				dest[i] = nil
			}
		case sppb.TypeCode_FLOAT32:
			var v spanner.NullFloat32
			if err := col.Decode(&v); err != nil {
				return err
			}
			if v.Valid {
				dest[i] = v.Float32
			} else {
				dest[i] = nil
			}
		case sppb.TypeCode_FLOAT64:
			var v spanner.NullFloat64
			if err := col.Decode(&v); err != nil {
				return err
			}
			if v.Valid {
				dest[i] = v.Float64
			} else {
				dest[i] = nil
			}
		case sppb.TypeCode_NUMERIC:
			if propertyDecodeNumericToString.GetValueOrDefault(r.state) {
				if _, ok := col.Value.Kind.(*structpb.Value_NullValue); ok {
					dest[i] = nil
				} else {
					dest[i] = col.Value.GetStringValue()
				}
			} else {
				if col.Type.TypeAnnotation == sppb.TypeAnnotationCode_PG_NUMERIC {
					var v spanner.PGNumeric
					if err := col.Decode(&v); err != nil {
						return err
					}
					if v.Valid {
						dest[i] = v.Numeric
					} else {
						dest[i] = nil
					}
				} else {
					var v spanner.NullNumeric
					if err := col.Decode(&v); err != nil {
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
			if err := col.Decode(&v); err != nil {
				return err
			}
			if v.Valid {
				dest[i] = v.StringVal
			} else {
				dest[i] = nil
			}
		case sppb.TypeCode_JSON:
			if col.Type.TypeAnnotation == sppb.TypeAnnotationCode_PG_JSONB {
				var v spanner.PGJsonB
				if err := col.Decode(&v); err != nil {
					return err
				}
				dest[i] = v
			} else {
				var v spanner.NullJSON
				if err := col.Decode(&v); err != nil {
					return err
				}
				// We always assign `v` to dest[i] here because there is no native type
				// for JSON in the Go sql package. That means that instead of returning
				// nil we should return a NullJSON with valid=false.
				dest[i] = v
			}
		case sppb.TypeCode_UUID:
			var v spanner.NullUUID
			if err := col.Decode(&v); err != nil {
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
			if err := col.Decode(&v); err != nil {
				return err
			}
			dest[i] = v
		case sppb.TypeCode_BOOL:
			var v spanner.NullBool
			if err := col.Decode(&v); err != nil {
				return err
			}
			if v.Valid {
				dest[i] = v.Bool
			} else {
				dest[i] = nil
			}
		case sppb.TypeCode_DATE:
			_, isNull := col.Value.Kind.(*structpb.Value_NullValue)
			if isNull {
				dest[i] = nil
			} else {
				dest[i] = col.Value.GetStringValue()
			}
		case sppb.TypeCode_TIMESTAMP:
			var v spanner.NullTime
			if err := col.Decode(&v); err != nil {
				return err
			}
			if v.Valid {
				dest[i] = v.Time
			} else {
				dest[i] = nil
			}
		case sppb.TypeCode_ARRAY:
			switch col.Type.ArrayElementType.Code {
			case sppb.TypeCode_INT64, sppb.TypeCode_ENUM:
				if r.decodeToNativeArrays {
					var v []int64
					if err := col.Decode(&v); err != nil {
						return err
					}
					dest[i] = v
				} else {
					var v []spanner.NullInt64
					if err := col.Decode(&v); err != nil {
						return err
					}
					dest[i] = v
				}
			case sppb.TypeCode_FLOAT32:
				if r.decodeToNativeArrays {
					var v []float32
					if err := col.Decode(&v); err != nil {
						return err
					}
					dest[i] = v
				} else {
					var v []spanner.NullFloat32
					if err := col.Decode(&v); err != nil {
						return err
					}
					dest[i] = v
				}
			case sppb.TypeCode_FLOAT64:
				if r.decodeToNativeArrays {
					var v []float64
					if err := col.Decode(&v); err != nil {
						return err
					}
					dest[i] = v
				} else {
					var v []spanner.NullFloat64
					if err := col.Decode(&v); err != nil {
						return err
					}
					dest[i] = v
				}
			case sppb.TypeCode_NUMERIC:
				if col.Type.ArrayElementType.TypeAnnotation == sppb.TypeAnnotationCode_PG_NUMERIC {
					var v []spanner.PGNumeric
					if err := col.Decode(&v); err != nil {
						return err
					}
					dest[i] = v
				} else {
					var v []spanner.NullNumeric
					if err := col.Decode(&v); err != nil {
						return err
					}
					dest[i] = v
				}
			case sppb.TypeCode_STRING:
				if r.decodeToNativeArrays {
					var v []string
					if err := col.Decode(&v); err != nil {
						return err
					}
					dest[i] = v
				} else {
					var v []spanner.NullString
					if err := col.Decode(&v); err != nil {
						return err
					}
					dest[i] = v
				}
			case sppb.TypeCode_JSON:
				if col.Type.ArrayElementType.TypeAnnotation == sppb.TypeAnnotationCode_PG_JSONB {
					var v []spanner.PGJsonB
					if err := col.Decode(&v); err != nil {
						// Workaround for https://github.com/googleapis/google-cloud-go/pull/13602
						if spanner.ErrCode(err) == codes.InvalidArgument && err.Error() == "spanner: code = \"InvalidArgument\", desc = \"type *[]spanner.PGJsonB cannot be used for decoding ARRAY[JSON]\"" {
							var tmp []spanner.NullJSON
							if err := col.Decode(&tmp); err != nil {
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
					if err := col.Decode(&v); err != nil {
						return err
					}
					dest[i] = v
				}
			case sppb.TypeCode_UUID:
				if r.decodeToNativeArrays {
					var v []uuid.UUID
					if err := col.Decode(&v); err != nil {
						return err
					}
					dest[i] = v
				} else {
					var v []spanner.NullUUID
					if err := col.Decode(&v); err != nil {
						return err
					}
					dest[i] = v
				}
			case sppb.TypeCode_BYTES, sppb.TypeCode_PROTO:
				var v [][]byte
				if err := col.Decode(&v); err != nil {
					return err
				}
				dest[i] = v
			case sppb.TypeCode_BOOL:
				if r.decodeToNativeArrays {
					var v []bool
					if err := col.Decode(&v); err != nil {
						return err
					}
					dest[i] = v
				} else {
					var v []spanner.NullBool
					if err := col.Decode(&v); err != nil {
						return err
					}
					dest[i] = v
				}
			case sppb.TypeCode_DATE:
				if r.decodeToNativeArrays {
					var v []civil.Date
					if err := col.Decode(&v); err != nil {
						return err
					}
					dest[i] = v
				} else {
					var v []spanner.NullDate
					if err := col.Decode(&v); err != nil {
						return err
					}
					dest[i] = v
				}
			case sppb.TypeCode_TIMESTAMP:
				if r.decodeToNativeArrays {
					var v []time.Time
					if err := col.Decode(&v); err != nil {
						return err
					}
					dest[i] = v
				} else {
					var v []spanner.NullTime
					if err := col.Decode(&v); err != nil {
						return err
					}
					dest[i] = v
				}
			default:
				return fmt.Errorf("unsupported array element type ARRAY<%v>, "+
					"use spannerdriver.ExecOptions{DecodeOption: spannerdriver.DecodeOptionProto} "+
					"to return the underlying protobuf value", col.Type.ArrayElementType.Code)
			}
		default:
			return fmt.Errorf("unsupported type %v, "+
				"use spannerdriver.ExecOptions{DecodeOption: spannerdriver.DecodeOptionProto} "+
				"to return the underlying protobuf value", col.Type.Code)
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
