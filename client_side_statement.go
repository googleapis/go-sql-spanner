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
	"fmt"
	"io"
	"strconv"
	"strings"

	"cloud.google.com/go/spanner"
	sppb "google.golang.org/genproto/googleapis/spanner/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// statementExecutor is an empty struct that is used to hold the execution methods
// of the different client side statements. This makes it possible to look up the
// methods using reflection, which is not possible if the methods do not belong to
//  a struct.
// The different methods of statementExecutor are invoked by a connection when one
// of the valid client side statements is executed on a connection.
type statementExecutor struct {
}

func (s *statementExecutor) ShowRetryAbortsInternally(_ context.Context, c *conn, _ string, _ []driver.NamedValue) (driver.Rows, error) {
	it, err := createBooleanIterator("RetryAbortsInternally", c.RetryAbortsInternally())
	if err != nil {
		return nil, err
	}
	return &rows{it: it}, nil
}

func (s *statementExecutor) ShowAutocommitDmlMode(_ context.Context, c *conn, _ string, _ []driver.NamedValue) (driver.Rows, error) {
	it, err := createStringIterator("AutocommitDmlMode", c.AutocommitDmlMode().String())
	if err != nil {
		return nil, err
	}
	return &rows{it: it}, nil
}

func (s *statementExecutor) StartBatchDdl(_ context.Context, c *conn, _ string, _ []driver.NamedValue) (driver.Result, error) {
	return c.startBatchDdl()
}

func (s *statementExecutor) StartBatchDml(_ context.Context, c *conn, _ string, _ []driver.NamedValue) (driver.Result, error) {
	return c.startBatchDml()
}

func (s *statementExecutor) RunBatch(ctx context.Context, c *conn, _ string, _ []driver.NamedValue) (driver.Result, error) {
	return c.runBatch(ctx)
}

func (s *statementExecutor) AbortBatch(_ context.Context, c *conn, _ string, _ []driver.NamedValue) (driver.Result, error) {
	return c.abortBatch()
}

func (s *statementExecutor) SetRetryAbortsInternally(_ context.Context, c *conn, params string, _ []driver.NamedValue) (driver.Result, error) {
	if params == "" {
		return nil, spanner.ToSpannerError(status.Error(codes.InvalidArgument, "no value given for RetryAbortsInternally"))
	}
	retry, err := strconv.ParseBool(params)
	if err != nil {
		return nil, spanner.ToSpannerError(status.Errorf(codes.InvalidArgument, "invalid boolean value: %s", params))
	}
	return c.setRetryAbortsInternally(retry)
}

func (s *statementExecutor) SetAutocommitDmlMode(_ context.Context, c *conn, params string, _ []driver.NamedValue) (driver.Result, error) {
	if params == "" {
		return nil, spanner.ToSpannerError(status.Error(codes.InvalidArgument, "no value given for AutocommitDmlMode"))
	}
	var mode AutocommitDmlMode
	switch strings.ToUpper(params) {
	case fmt.Sprintf("'%s'", strings.ToUpper(Transactional.String())):
		mode = Transactional
	case fmt.Sprintf("'%s'", strings.ToUpper(PartitionedNonAtomic.String())):
		mode = PartitionedNonAtomic
	default:
		return nil, spanner.ToSpannerError(status.Errorf(codes.InvalidArgument, "invalid AutocommitDmlMode value: %s", params))
	}
	return c.setAutocommitDmlMode(mode)
}

// createBooleanIterator creates a row iterator with a single BOOL column with
// one row. This is used for client side statements that return a result set
// containing a BOOL value.
func createBooleanIterator(column string, value bool) (*clientSideIterator, error) {
	return createSingleValueIterator(column, value, sppb.TypeCode_BOOL)
}

// createStringIterator creates a row iterator with a single STRING column with
// one row. This is used for client side statements that return a result set
// containing a STRING value.
func createStringIterator(column string, value string) (*clientSideIterator, error) {
	return createSingleValueIterator(column, value, sppb.TypeCode_STRING)
}

func createSingleValueIterator(column string, value interface{}, code sppb.TypeCode) (*clientSideIterator, error) {
	row, err := spanner.NewRow([]string{column}, []interface{}{value})
	if err != nil {
		return nil, err
	}
	return &clientSideIterator{
		metadata: &sppb.ResultSetMetadata{
			RowType: &sppb.StructType{
				Fields: []*sppb.StructType_Field{
					{Name: column, Type: &sppb.Type{Code: sppb.TypeCode_BOOL}},
				},
			},
		},
		rows: []*spanner.Row{row},
	}, nil
}

// clientSideIterator implements the rowIterator interface for client side
// statements. All values are created and kept in memory, and this struct
// should only be used for small result sets.
type clientSideIterator struct {
	metadata *sppb.ResultSetMetadata
	rows     []*spanner.Row
	index    int
	stopped  bool
}

func (t *clientSideIterator) Next() (*spanner.Row, error) {
	if t.index == len(t.rows) {
		return nil, io.EOF
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

func (t *clientSideIterator) Metadata() *sppb.ResultSetMetadata {
	return t.metadata
}
