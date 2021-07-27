// Copyright 2020 Google Inc. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package spannerdriver

import (
	"database/sql/driver"
	"io"
	"sync"
	"time"

	"cloud.google.com/go/spanner"
	"google.golang.org/api/iterator"
	sppb "google.golang.org/genproto/googleapis/spanner/v1"
)

type rows struct {
	it *spanner.RowIterator

	colsOnce sync.Once
	dirtyErr error
	cols     []string

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
		rowType := r.it.Metadata.RowType
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
		switch col.Type.Code {
		case sppb.TypeCode_INT64:
			var v spanner.NullInt64
			if err := col.Decode(&v); err != nil {
				return err
			}
			dest[i] = v.Int64
		case sppb.TypeCode_FLOAT64:
			var v spanner.NullFloat64
			if err := col.Decode(&v); err != nil {
				return err
			}
			dest[i] = v.Float64
		case sppb.TypeCode_NUMERIC:
			var v spanner.NullNumeric
			if err := col.Decode(&v); err != nil {
				return err
			}
			dest[i] = v.Numeric
		case sppb.TypeCode_STRING:
			var v spanner.NullString
			if err := col.Decode(&v); err != nil {
				return err
			}
			dest[i] = v.StringVal
		case sppb.TypeCode_BYTES:
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
			dest[i] = v.Bool
		case sppb.TypeCode_DATE:
			var v spanner.NullDate
			if err := col.Decode(&v); err != nil {
				return err
			}
			if v.IsNull() {
				dest[i] = v.Date // typed nil
			} else {
				dest[i] = v.Date.In(time.UTC) // TODO(jbd): Add note about this.
			}
		case sppb.TypeCode_TIMESTAMP:
			var v spanner.NullTime
			if err := col.Decode(&v); err != nil {
				return err
			}
			dest[i] = v.Time
		}
		// TODO(jbd): Implement other types.
		// How to handle array and struct?
	}
	return nil
}
