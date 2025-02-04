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
	"testing"

	"cloud.google.com/go/spanner"
	sppb "cloud.google.com/go/spanner/apiv1/spannerpb"
	"google.golang.org/protobuf/types/known/structpb"
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
		if err != nil {
			t.Fatal(err)
		}
		row := make([]driver.Value, 3)
		copy(row, dest)
		values = append(values, row)
	}
	if g, w := len(values), 3; g != w {
		t.Fatalf("values length mismatch\nGot: %v\nWant: %v", g, w)
	}
	for i := int64(0); i < int64(len(values)); i++ {
		if g, w := values[i][0], i+1; g != w {
			t.Fatalf("COL1 value mismatch for row %d\nGot: %v\nWant: %v", i, g, w)
		}
		if i == 0 || i == 2 {
			if g, w := values[i][1], fmt.Sprintf("Test%d", i+1); g != w {
				t.Fatalf("COL2 value mismatch for row %d\nGot: %v\nWant: %v", i, g, w)
			}
			if g, w := values[i][2], 3.14*float64(i+1); g != w {
				t.Fatalf("COL3 value mismatch for row %d\nGot: %v\nWant: %v", i, g, w)
			}
		} else {
			if g, w := values[i][1], driver.Value(nil); g != w {
				t.Fatalf("COL2 value mismatch for row %d\nGot: %v\nWant: %v", i, g, w)
			}
			if g, w := values[i][2], driver.Value(nil); g != w {
				t.Fatalf("COL3 value mismatch for row %d\nGot: %v\nWant: %v", i, g, w)
			}
		}
	}
}

func TestRows_Next_Unsupported(t *testing.T) {
	unspecifiedType := &sppb.Type{Code: sppb.TypeCode_TYPE_CODE_UNSPECIFIED}

	it := testIterator{
		metadata: &sppb.ResultSetMetadata{
			RowType: &sppb.StructType{
				Fields: []*sppb.StructType_Field{
					{Name: "COL1", Type: unspecifiedType},
				},
			},
		},
		rows: []*spanner.Row{
			newRow(t, []string{"COL1"}, []any{
				spanner.GenericColumnValue{
					Type:  unspecifiedType,
					Value: &structpb.Value{},
				},
			}),
		},
	}

	rows := rows{it: &it}

	dest := make([]driver.Value, 1)
	err := rows.Next(dest)
	if err == nil {
		t.Fatal("expected an error, but got nil")
	}
	const expectedError = "unsupported type TYPE_CODE_UNSPECIFIED, use spannerdriver.ExecOptions{DecodeOption: spannerdriver.DecodeOptionProto} to return the underlying protobuf value"
	if err.Error() != expectedError {
		t.Fatalf("expected error %q, but got %q", expectedError, err.Error())
	}
}
