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
	"context"
	"database/sql/driver"
	"time"

	"cloud.google.com/go/spanner"
	"cloud.google.com/go/spanner/apiv1/spannerpb"
	"google.golang.org/api/iterator"
)

// statementExecutor is an empty struct that is used to hold the execution methods
// of the different client side statements. This makes it possible to look up the
// methods using reflection, which is not possible if the methods do not belong to
// a struct. The methods all accept the same arguments and return the same types.
// This is to ensure that they can be assigned to a compiled clientSideStatement.
//
// The different methods of statementExecutor are invoked by a connection when one
// of the valid client side statements is executed on a connection. These methods
// are responsible for any argument parsing and translating that might be needed
// before the corresponding method on the connection can be called.
//
// The names of the methods are exactly equal to the naming in the
// client_side_statements.json file. This means that some methods do not adhere
// to the Go style guide, as these method names are equal for all languages that
// implement the Connection API.
type statementExecutor struct {
}

func (s *statementExecutor) StartBatchDdl(_ context.Context, c *conn, _ string, _ *ExecOptions, _ []driver.NamedValue) (driver.Result, error) {
	return c.startBatchDDL()
}

func (s *statementExecutor) StartBatchDml(_ context.Context, c *conn, _ string, _ *ExecOptions, _ []driver.NamedValue) (driver.Result, error) {
	return c.startBatchDML( /* automatic = */ false)
}

func (s *statementExecutor) RunBatch(ctx context.Context, c *conn, _ string, _ *ExecOptions, _ []driver.NamedValue) (driver.Result, error) {
	return c.runBatch(ctx)
}

func (s *statementExecutor) AbortBatch(_ context.Context, c *conn, _ string, _ *ExecOptions, _ []driver.NamedValue) (driver.Result, error) {
	return c.abortBatch()
}

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
