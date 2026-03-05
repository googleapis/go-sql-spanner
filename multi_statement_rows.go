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
	"errors"
	"io"

	sppb "cloud.google.com/go/spanner/apiv1/spannerpb"
	"github.com/googleapis/go-sql-spanner/parser"
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
//
// The function uses similar transaction semantics as PostgreSQL for multi-statement SQL strings. That means:
//  1. The statements are wrapped in an implicit transaction block. The implicit transaction is committed
//     if all statements executed successfully, and rolled back if any of the statements fail.
//  2. If the connection already has a transaction, then that transaction is used instead.
//  3. If the SQL string contains a BEGIN statement, then the transaction is changed from implicit to explicit.
//     An explicit transaction does not automatically commit or roll back at the end of the block.
//  4. If the SQL string contains a COMMIT statement, then the current transaction is committed. The next statement
//     starts a new implicit transaction block.
//  5. DDL statements commit the current implicit transaction, as DDL is not supported in transactions. A new
//     implicit transaction block is started after the DDL statement(s). This is different from how PostgreSQL
//     behaves, as PostgreSQL supports DDL in transactions.
func queryMultiple(ctx context.Context, conn *conn, statements []string, args []driver.NamedValue) (res *multiStatementRows, returnedErr error) {
	var currentTransaction *delegatingTransaction
	defer func() {
		if currentTransaction != nil && currentTransaction.implicit {
			if returnedErr != nil || res.err != nil {
				_ = currentTransaction.Rollback()
			} else {
				returnedErr = currentTransaction.Commit()
			}
		}
	}()

	result := &multiStatementRows{
		ctx:        ctx,
		errorIndex: -1,
		results:    make([]driver.RowsNextResultSet, 0, len(statements)),
	}
	var err error
	statementInfo := determineStatementTypes(conn, statements)
	for index := 0; index < len(statements); {
		statement := statements[index]
		// Start an implicit transaction block if the connection currently does not have a transaction and needs one,
		// or commit the current implicit transaction if the next statement is a DDL statement.
		currentTransaction, err = maybeStartOrCommitImplicitTransaction(ctx, conn, index, statements, statementInfo)
		if err != nil {
			return nil, err
		}

		// Create a derived context for each query to prevent the main context from being linked to each query.
		// The underlying Spanner RowIterator cancels the context that it is given when it is closed. The RowIterator
		// is automatically closed when all data has been read, meaning that if we were to use the same context for
		// each query, the context will be cancelled once all the data of the first result set have been read. That
		// again can cause reading data from the following result sets to fail.
		queryCtx := context.WithValue(ctx, statementKey{index: index}, statement)
		// TODO: Implement a look-ahead to see if the following statements are all queries, and whether we can execute
		//       those in parallel (should be an opt-in).
		numExecuted := 0
		if canUseDdlBatch(index, statementInfo) {
			numExecuted, err = queryDdlBatch(queryCtx, conn, index, result, statements, statementInfo, args)
		} else if canUseDmlBatch(index, statementInfo) {
			numExecuted, err = queryDmlBatch(queryCtx, conn, index, result, statements, statementInfo, args)
		} else {
			numExecuted, err = querySingle(queryCtx, conn, index, result, statement, args)
		}

		index += numExecuted
		if err != nil {
			if index == 0 {
				return nil, err
			}
			return result, nil
		}
	}
	return result, nil
}

// Execute a statement from a multi-statement SQL string as a single statement.
// The result is added to the multiStatementRows instance that is passed in to this function.
func querySingle(ctx context.Context, conn *conn, index int, result *multiStatementRows, statement string, args []driver.NamedValue) (int, error) {
	r, err := conn.querySingle(ctx, statement /* isPartOfMultiStatementString = */, true, args)
	if err != nil {
		result.errorIndex = index
		result.err = err
		return 0, err
	}
	if rn, ok := r.(driver.RowsNextResultSet); ok {
		result.results = append(result.results, rn)
	} else {
		// This should not happen.
		return 0, registerUnexpectedResultSetError(r, index, result)
	}
	return 1, nil
}

// Execute a sequence of DDL statements from a multi-statement SQL string as a DDL batch.
func queryDdlBatch(ctx context.Context, conn *conn, index int, result *multiStatementRows, statements []string, statementInfo []*parser.StatementInfo, args []driver.NamedValue) (int, error) {
	startIndex := index
	endIndex := len(statements)
	for i := startIndex + 1; i < len(statementInfo); i++ {
		if statementInfo[i].StatementType != parser.StatementTypeDdl {
			endIndex = i
			break
		}
	}
	if err := conn.StartBatchDDL(); err != nil {
		return 0, registerError(index, result, err)
	}
	for i := startIndex; i < endIndex; i++ {
		if _, err := querySingle(ctx, conn, i, result, statements[i], args); err != nil {
			_ = conn.AbortBatch()
			return index, err
		}
	}
	_, err := conn.runBatch(ctx)
	if err != nil {
		var be *BatchError
		if errors.As(err, &be) {
			return len(be.BatchUpdateCounts), registerBatchError(index, result, be)
		}
		return 0, registerError(index, result, err)
	}
	return endIndex - startIndex, nil
}

// Execute a sequence of DML statements from a multi-statement SQL string as a DML batch.
func queryDmlBatch(ctx context.Context, conn *conn, index int, result *multiStatementRows, statements []string, statementInfo []*parser.StatementInfo, args []driver.NamedValue) (int, error) {
	startIndex := index
	endIndex := len(statements)
	for i := startIndex + 1; i < len(statementInfo); i++ {
		if statementInfo[i].StatementType != parser.StatementTypeDml {
			endIndex = i
			break
		}
	}
	if err := conn.StartBatchDML(); err != nil {
		return 0, registerError(index, result, err)
	}
	execOpts := conn.tempExecOptions
	if execOpts == nil {
		execOpts = &ExecOptions{}
	}
	for i := startIndex; i < endIndex; i++ {
		if res, err := conn.execSingle(ctx, statements[i], args); err != nil {
			_ = conn.AbortBatch()
			return startIndex, err
		} else {
			r := createDriverResultRows(res /* isPartitionedDml = */, false, func() {}, execOpts)
			result.results = append(result.results, r)
		}
	}
	res, err := conn.RunDmlBatch(ctx)
	if err != nil {
		var be *BatchError
		if errors.As(err, &be) {
			return len(be.BatchUpdateCounts), registerBatchError(index, result, be)
		}
		return 0, registerError(index, result, err)
	}
	modified, err := res.BatchRowsAffected()
	if err != nil {
		return 0, registerError(index, result, err)
	}
	for i, m := range modified {
		if noRows, ok := result.results[startIndex+i].(*emptyRows); ok {
			noRows.stats = &sppb.ResultSetStats{RowCount: &sppb.ResultSetStats_RowCountExact{RowCountExact: m}}
		}
	}
	return endIndex - startIndex, nil
}

func registerError(index int, result *multiStatementRows, err error) error {
	result.errorIndex = index
	result.err = err
	return err
}

func registerBatchError(index int, result *multiStatementRows, err *BatchError) error {
	result.errorIndex = index + len(err.BatchUpdateCounts)
	result.err = err
	return err
}

func registerUnexpectedResultSetError(r driver.Rows, index int, result *multiStatementRows) error {
	_ = r.Close()
	result.errorIndex = index
	result.err = errExpectedRowsNextResultSet
	return result.err
}

func maybeStartOrCommitImplicitTransaction(ctx context.Context, conn *conn, index int, statements []string, statementInfo []*parser.StatementInfo) (*delegatingTransaction, error) {
	if shouldStartImplicitTransaction(conn, index, statements, statementInfo) && !conn.inTransaction() {
		var tx driver.Tx
		var err error
		if canUseReadOnlyTransaction(conn, index, statements, statementInfo) {
			tx, err = conn.BeginReadOnlyTransaction(ctx, &ReadOnlyTransactionOptions{
				baseTransactionOptions: baseTransactionOptions{implicit: true},
			}, func() {})
		} else {
			tx, err = conn.BeginReadWriteTransaction(ctx, &ReadWriteTransactionOptions{
				baseTransactionOptions: baseTransactionOptions{implicit: true},
			}, func() {})
		}
		if err != nil {
			return nil, err
		}
		if delegatingTx, ok := tx.(*delegatingTransaction); !ok {
			// This should never happen.
			return nil, status.Error(codes.Internal, "expected a Spanner transaction")
		} else {
			return delegatingTx, nil
		}
	} else if conn.inImplicitTransaction() && statementInfo[index].StatementType == parser.StatementTypeDdl {
		if _, err := conn.Commit(ctx); err != nil {
			return nil, err
		}
	}
	return conn.tx, nil
}

func shouldStartImplicitTransaction(conn *conn, index int, statements []string, statementInfo []*parser.StatementInfo) bool {
	// Do not start a transaction if this is the last statement.
	if index == len(statements)-1 {
		return false
	}
	info := statementInfo[index]
	statement := statements[index]

	if info.StatementType == parser.StatementTypeQuery || info.StatementType == parser.StatementTypeDml || info.StatementType == parser.StatementTypeUnknown {
		// Do not start a transaction if the statement that follows directly is a DDL statement, and that just means
		// that the transaction will be committed right after anyway.
		if statementInfo[index+1].StatementType == parser.StatementTypeDdl {
			return false
		}
		return true
	}
	if info.StatementType == parser.StatementTypeDdl {
		return false
	}
	if info.StatementType == parser.StatementTypeClientSide {
		// It's a client-side statement. Only start an implicit transaction if the client-side statement is not a
		// transaction control statement.
		cs, err := conn.parser.ParseClientSideStatement(statement)
		if err != nil {
			return true
		}
		if _, ok := cs.(*parser.ParsedBeginStatement); ok {
			return false
		}
		if _, ok := cs.(*parser.ParsedCommitStatement); ok {
			return false
		}
		if _, ok := cs.(*parser.ParsedRollbackStatement); ok {
			return false
		}
	}
	// Start an implicit transaction by default.
	return true
}

func determineStatementTypes(conn *conn, statements []string) []*parser.StatementInfo {
	p := conn.parser
	res := make([]*parser.StatementInfo, len(statements))
	for i, statement := range statements {
		res[i] = p.DetectStatementType(statement)
	}
	return res
}

func canUseReadOnlyTransaction(conn *conn, index int, statements []string, statementInfo []*parser.StatementInfo) bool {
	if statementInfo[index].StatementType != parser.StatementTypeQuery {
		return false
	}
	insideExplicitTx := false
	for i := index + 1; i < len(statementInfo); i++ {
		if isCommitOrRollback(conn, statements[i], statementInfo[i]) {
			return true
		}
		if isBegin(conn, statements[i], statementInfo[i]) {
			insideExplicitTx = true
		}
		if statementInfo[i].StatementType == parser.StatementTypeDml || statementInfo[i].StatementType == parser.StatementTypeUnknown {
			return false
		}
	}
	// We cannot use a read-only transaction if we are inside an explicit transaction block, as the transaction will
	// still be active when all the statements have been executed.
	return !insideExplicitTx
}

func canUseDdlBatch(index int, statementInfo []*parser.StatementInfo) bool {
	if index == len(statementInfo)-1 || statementInfo[index].StatementType != parser.StatementTypeDdl {
		return false
	}
	return statementInfo[index+1].StatementType == parser.StatementTypeDdl
}

func canUseDmlBatch(index int, statementInfo []*parser.StatementInfo) bool {
	if index == len(statementInfo)-1 || statementInfo[index].StatementType != parser.StatementTypeDml || statementInfo[index].HasThenReturn {
		return false
	}
	return statementInfo[index+1].StatementType == parser.StatementTypeDml && !statementInfo[index+1].HasThenReturn
}

func isBegin(conn *conn, query string, statementInfo *parser.StatementInfo) bool {
	if statementInfo.StatementType == parser.StatementTypeClientSide {
		cs, err := conn.parser.ParseClientSideStatement(query)
		if err != nil {
			return false
		}
		if _, ok := cs.(*parser.ParsedBeginStatement); ok {
			return true
		}
	}
	return false
}

func isCommitOrRollback(conn *conn, query string, statementInfo *parser.StatementInfo) bool {
	if statementInfo.StatementType == parser.StatementTypeDdl {
		return true
	}
	if statementInfo.StatementType == parser.StatementTypeClientSide {
		cs, err := conn.parser.ParseClientSideStatement(query)
		if err != nil {
			return false
		}
		if _, ok := cs.(*parser.ParsedCommitStatement); ok {
			return true
		}
		if _, ok := cs.(*parser.ParsedRollbackStatement); ok {
			return true
		}
	}
	return false
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
