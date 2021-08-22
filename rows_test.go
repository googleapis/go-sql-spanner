// Copyright 2021 Google Inc. All Rights Reserved.
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
	"fmt"
	"io"
	"testing"

	"cloud.google.com/go/spanner"
	sppb "google.golang.org/genproto/googleapis/spanner/v1"
)

type testIterator struct {
	metadata *sppb.ResultSetMetadata
	rows     []*spanner.Row
	index    int
}

func (t *testIterator) Next() (*spanner.Row, error) {
	if t.index == len(t.rows) {
		return nil, io.EOF
	}
	row := t.rows[t.index]
	t.index++
	return row, nil
}

func (t *testIterator) Stop() {
}

func (t *testIterator) Metadata() *sppb.ResultSetMetadata {
	return t.metadata
}

func newRow(t *testing.T, cols []string, vals []interface{}) *spanner.Row {
	row, err := spanner.NewRow(cols, vals)
	if err != nil {
		t.Fatalf("failed to create test row: %v", err)
	}
	return row
}

func TestRows_Next(t *testing.T) {
	cols := []string{"COL1", "COL2", "COL3"}
	it := testIterator{
		metadata: &sppb.ResultSetMetadata{
			RowType: &sppb.StructType{
				Fields: []*sppb.StructType_Field{
					{Name: "COL1", Type: &sppb.Type{Code: sppb.TypeCode_INT64}},
					{Name: "COL2", Type: &sppb.Type{Code: sppb.TypeCode_STRING}},
					{Name: "COL3", Type: &sppb.Type{Code: sppb.TypeCode_FLOAT64}},
				},
			},
		},
		rows: []*spanner.Row{
			newRow(t, cols, []interface{}{int64(1), "Test1", 3.14 * 1.0}),
			newRow(t, cols, []interface{}{int64(2), spanner.NullString{}, spanner.NullFloat64{}}),
			newRow(t, cols, []interface{}{int64(3), "Test3", 3.14 * 3.0}),
		},
	}

	rows := rows{it: &it}
	dest := make([]driver.Value, 3)
	values := make([][]driver.Value, 0)
	for {
		err := rows.Next(dest)
		if err == io.EOF {
			break
		}
		row := make([]driver.Value, 3)
		copy(row, dest)
		values = append(values, row)
	}
	if g, w := len(values), 3; g != w {
		t.Fatalf("values length mismatch\nGot: %v\nWant: %v", g, w)
	}
	for i := int64(1); i <= int64(len(values)); i++ {
		if g, w := values[i-1][0], i; g != w {
			t.Fatalf("COL1 value mismatch for row %d\nGot: %v\nWant: %v", i, g, w)
		}
		if i == 1 || i == 3 {
			if g, w := values[i-1][1], fmt.Sprintf("Test%d", i); g != w {
				t.Fatalf("COL2 value mismatch for row %d\nGot: %v\nWant: %v", i, g, w)
			}
			if g, w := values[i-1][2], 3.14*float64(i); g != w {
				t.Fatalf("COL3 value mismatch for row %d\nGot: %v\nWant: %v", i, g, w)
			}
		} else {
			if g, w := values[i-1][1], driver.Value(nil); g != w {
				t.Fatalf("COL2 value mismatch for row %d\nGot: %v\nWant: %v", i, g, w)
			}
			if g, w := values[i-1][2], driver.Value(nil); g != w {
				t.Fatalf("COL3 value mismatch for row %d\nGot: %v\nWant: %v", i, g, w)
			}
		}
	}
}
