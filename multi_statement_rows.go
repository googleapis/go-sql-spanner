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

package spannerdriver

import (
	"context"
	"database/sql/driver"
	"io"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var _ driver.RowsNextResultSet = &multiStatementRows{}
var errRowsClosed = status.Error(codes.FailedPrecondition, "rows already closed")
var errExpectedRowsNextResultSet = status.Error(codes.Internal, "expected RowsNextResultSet")

// multiStatementRows is an implementation of the driver.RowsNextResultSet interface that is used by database/sql
// for multi-statement SQL strings that return multiple results. This implementation is a wrapper on top of the
// existing driver.Rows implementation in the Spanner database/sql driver, and it is only used when the driver
// detects that a SQL string contains more than one statement.
type multiStatementRows struct {
	ctx context.Context

	closed       bool
	closeErr     error
	currentIndex int
	results      []driver.RowsNextResultSet

	errorIndex int
	err        error
}

type statementKey struct {
	index int
}

// queryMultiple executes a multi-statement SQL string and returns a multiStatementRows.
// The function tries to execute all statements on Spanner. It stops execution at the first error (if any).
// Results are returned for all successful statements. If any error occurs, then execution stops at that statement
// and the error is returned as the result of that statement.
//
// The above means that this function returns an error if the first statement fails. It returns a nil error if one
// of the later statements fail.
func queryMultiple(ctx context.Context, conn *conn, statements []string, args []driver.NamedValue) (*multiStatementRows, error) {
	result := &multiStatementRows{
		ctx:        ctx,
		errorIndex: -1,
		results:    make([]driver.RowsNextResultSet, 0, len(statements)),
	}
	for index, statement := range statements {
		// Create a derived context for each query to prevent the main context from being linked to each query.
		// The underlying Spanner RowIterator cancels the context that it is given when it is closed. The RowIterator
		// is automatically closed when all data has been read, meaning that if we were to use the same context for
		// each query, the context will be cancelled once all the data of the first result set have been read. That
		// again can cause reading data from the following result sets to fail.
		queryCtx := context.WithValue(ctx, statementKey{index: index}, statement)
		// TODO: Implement a look-ahead to see if we can use DML/DDL batching.
		// TODO: Implement a look-ahead to see if the following statements are all queries, and whether we can execute
		//       those in parallel (probably needs to be an opt-in).
		r, err := conn.querySingle(queryCtx, statement /* isPartOfMultiStatementString = */, true, args)
		if err != nil {
			if index == 0 {
				return nil, err
			}
			result.errorIndex = index
			result.err = err
			return result, nil
		}
		if rn, ok := r.(driver.RowsNextResultSet); ok {
			result.results = append(result.results, rn)
		} else {
			_ = r.Close()
			if index == 0 {
				return nil, errExpectedRowsNextResultSet
			} else {
				result.errorIndex = index
				result.err = errExpectedRowsNextResultSet
				return result, nil
			}
		}
	}
	return result, nil
}

func (m *multiStatementRows) HasNextResultSet() bool {
	if m.closed {
		return false
	}
	if m.currentHasNextResultSet() {
		return true
	}
	if m.errorIndex > -1 {
		return m.currentIndex < m.errorIndex
	}
	return m.currentIndex < len(m.results)-1
}

func (m *multiStatementRows) currentHasNextResultSet() bool {
	return m.currentIndex < len(m.results) && m.results[m.currentIndex].HasNextResultSet()
}

func (m *multiStatementRows) NextResultSet() error {
	if m.closed {
		return errRowsClosed
	}
	if !m.HasNextResultSet() {
		return io.EOF
	}
	if m.currentHasNextResultSet() {
		return m.results[m.currentIndex].NextResultSet()
	}

	if err := m.results[m.currentIndex].Close(); err != nil {
		return err
	}
	m.currentIndex++
	if m.currentIndex == m.errorIndex {
		return m.err
	}
	return nil
}

func (m *multiStatementRows) Columns() []string {
	if m.currentIndex == m.errorIndex {
		return nil
	}
	return m.results[m.currentIndex].Columns()
}

func (m *multiStatementRows) Close() error {
	if m.closed {
		return m.closeErr
	}

	m.closed = true
	for _, result := range m.results {
		if err := result.Close(); err != nil && m.closeErr == nil {
			m.closeErr = err
		}
	}
	return m.closeErr
}

func (m *multiStatementRows) Next(dest []driver.Value) error {
	if m.currentIndex == m.errorIndex {
		return m.err
	}
	return m.results[m.currentIndex].Next(dest)
}
