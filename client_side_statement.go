// Copyright 2021 Google LLC All Rights Reserved.
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
	"time"

	"cloud.google.com/go/spanner"
	"cloud.google.com/go/spanner/apiv1/spannerpb"
	"google.golang.org/api/iterator"
)

func createEmptyIterator() *clientSideIterator {
	return &clientSideIterator{
		metadata: &spannerpb.ResultSetMetadata{
			RowType: &spannerpb.StructType{
				Fields: []*spannerpb.StructType_Field{},
			},
		},
		rows: []*spanner.Row{},
	}
}

// createBooleanIterator creates a row iterator with a single BOOL column with
// one row. This is used for client side statements that return a result set
// containing a BOOL value.
func createBooleanIterator(column string, value bool) (*clientSideIterator, error) {
	return createSingleValueIterator(column, value, spannerpb.TypeCode_BOOL)
}

// createInt64Iterator creates a row iterator with a single INT64 column with
// one row. This is used for client side statements that return a result set
// containing an INT64 value.
func createInt64Iterator(column string, value int64) (*clientSideIterator, error) {
	return createSingleValueIterator(column, value, spannerpb.TypeCode_INT64)
}

// createStringIterator creates a row iterator with a single STRING column with
// one row. This is used for client side statements that return a result set
// containing a STRING value.
func createStringIterator(column string, value string) (*clientSideIterator, error) {
	return createSingleValueIterator(column, value, spannerpb.TypeCode_STRING)
}

// createTimestampIterator creates a row iterator with a single TIMESTAMP column with
// one row. This is used for client side statements that return a result set
// containing a TIMESTAMP value.
func createTimestampIterator(column string, value *time.Time) (*clientSideIterator, error) {
	return createSingleValueIterator(column, value, spannerpb.TypeCode_TIMESTAMP)
}

func createSingleValueIterator(column string, value interface{}, code spannerpb.TypeCode) (*clientSideIterator, error) {
	row, err := spanner.NewRow([]string{column}, []interface{}{value})
	if err != nil {
		return nil, err
	}
	return &clientSideIterator{
		metadata: &spannerpb.ResultSetMetadata{
			RowType: &spannerpb.StructType{
				Fields: []*spannerpb.StructType_Field{
					{Name: column, Type: &spannerpb.Type{Code: code}},
				},
			},
		},
		rows: []*spanner.Row{row},
	}, nil
}

var _ rowIterator = &clientSideIterator{}

// clientSideIterator implements the rowIterator interface for client side
// statements. All values are created and kept in memory, and this struct
// should only be used for small result sets.
type clientSideIterator struct {
	metadata *spannerpb.ResultSetMetadata
	rows     []*spanner.Row
	index    int
	stopped  bool
}

func (t *clientSideIterator) Next() (*spanner.Row, error) {
	if t.index == len(t.rows) {
		return nil, iterator.Done
	}
	row := t.rows[t.index]
	t.index++
	return row, nil
}

func (t *clientSideIterator) Stop() {
	t.stopped = true
	t.rows = nil
	t.metadata = nil
}

func (t *clientSideIterator) Metadata() (*spannerpb.ResultSetMetadata, error) {
	return t.metadata, nil
}

func (t *clientSideIterator) ResultSetStats() *spannerpb.ResultSetStats {
	return &spannerpb.ResultSetStats{}
}
