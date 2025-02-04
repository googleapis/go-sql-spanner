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
	"database/sql/driver"
	"fmt"
	"io"
	"sync"
	"time"

	"cloud.google.com/go/civil"
	"cloud.google.com/go/spanner"
	sppb "cloud.google.com/go/spanner/apiv1/spannerpb"
	"google.golang.org/api/iterator"
)

type rows struct {
	it rowIterator

	colsOnce sync.Once
	dirtyErr error
	cols     []string

	decodeOption         DecodeOption
	decodeToNativeArrays bool

	dirtyRow *spanner.Row
}

// Columns returns the names of the columns. The number of
// columns of the result is inferred from the length of the
// slice. If a particular column name isn't known, an empty
// string should be returned for that entry.
func (r *rows) Columns() []string {
	r.getColumns()
	return r.cols
}

// Close closes the rows iterator.
func (r *rows) Close() error {
	r.it.Stop()
	return nil
}

func (r *rows) getColumns() {
	r.colsOnce.Do(func() {
		row, err := r.it.Next()
		if err == nil {
			r.dirtyRow = row
		} else {
			r.dirtyErr = err
			if err != iterator.Done {
				return
			}
		}
		rowType := r.it.Metadata().RowType
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
	var row *spanner.Row
	if r.dirtyRow != nil {
		row = r.dirtyRow
		r.dirtyRow = nil
	} else if r.dirtyErr != nil {
		err := r.dirtyErr
		r.dirtyErr = nil
		if err == iterator.Done {
			return io.EOF
		}
		return err
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
			var v spanner.NullNumeric
			if err := col.Decode(&v); err != nil {
				return err
			}
			if v.Valid {
				dest[i] = v.Numeric
			} else {
				dest[i] = nil
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
			var v spanner.NullJSON
			if err := col.Decode(&v); err != nil {
				return err
			}
			// We always assign `v` to dest[i] here because there is no native type
			// for JSON in the Go sql package. That means that instead of returning
			// nil we should return a NullJSON with valid=false.
			dest[i] = v
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
			var v spanner.NullDate
			if err := col.Decode(&v); err != nil {
				return err
			}
			if v.Valid {
				dest[i] = v.Date
			} else {
				dest[i] = nil
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
				var v []spanner.NullNumeric
				if err := col.Decode(&v); err != nil {
					return err
				}
				dest[i] = v
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
				var v []spanner.NullJSON
				if err := col.Decode(&v); err != nil {
					return err
				}
				dest[i] = v
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
