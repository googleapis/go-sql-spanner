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
	"crypto/sha256"
	"database/sql/driver"
	"fmt"
	"log/slog"
	"math/rand"
	"sync"
	"time"

	"cloud.google.com/go/spanner"
	sppb "cloud.google.com/go/spanner/apiv1/spannerpb"
	"github.com/googleapis/gax-go/v2"
	"github.com/googleapis/go-sql-spanner/parser"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// spannerTransaction is the generic interface for both spanner.ReadWriteTransaction
// and spanner.ReadWriteStmtBasedTransaction.
type spannerTransaction interface {
	QueryWithOptions(ctx context.Context, statement spanner.Statement, opts spanner.QueryOptions) *spanner.RowIterator
}

// contextTransaction is the combination of both read/write and read-only
// transactions.
type contextTransaction interface {
	deadline() (time.Time, bool)
	Commit() error
	Rollback() error
	resetForRetry(ctx context.Context) error
	Query(ctx context.Context, stmt spanner.Statement, stmtType parser.StatementType, execOptions *ExecOptions) (rowIterator, error)
	partitionQuery(ctx context.Context, stmt spanner.Statement, execOptions *ExecOptions) (driver.Rows, error)
	ExecContext(ctx context.Context, stmt spanner.Statement, statementInfo *parser.StatementInfo, options spanner.QueryOptions) (*result, error)

	StartBatchDML(options spanner.QueryOptions, automatic bool) (driver.Result, error)
	RunBatch(ctx context.Context) (driver.Result, error)
	RunDmlBatch(ctx context.Context) (SpannerResult, error)
	AbortBatch() (driver.Result, error)
	IsInBatch() bool

	BufferWrite(ms []*spanner.Mutation) error
}

type rowIterator interface {
	Next() (*spanner.Row, error)
	Stop()
	Metadata() (*sppb.ResultSetMetadata, error)
	ResultSetStats() *sppb.ResultSetStats
}

var _ rowIterator = &readOnlyRowIterator{}

type readOnlyRowIterator struct {
	*spanner.RowIterator
	stmtType parser.StatementType
}

func (ri *readOnlyRowIterator) Next() (*spanner.Row, error) {
	return ri.RowIterator.Next()
}

func (ri *readOnlyRowIterator) Stop() {
	ri.RowIterator.Stop()
}

func (ri *readOnlyRowIterator) Metadata() (*sppb.ResultSetMetadata, error) {
	return ri.RowIterator.Metadata, nil
}

func (ri *readOnlyRowIterator) ResultSetStats() *sppb.ResultSetStats {
	return createResultSetStats(ri.RowIterator, ri.stmtType)
}

func createResultSetStats(it *spanner.RowIterator, stmtType parser.StatementType) *sppb.ResultSetStats {
	// TODO: The Spanner client library should offer an option to get the full
	//       ResultSetStats, instead of only the RowCount and QueryPlan.
	stats := &sppb.ResultSetStats{
		QueryPlan: it.QueryPlan,
	}
	if stmtType == parser.StatementTypeDml {
		stats.RowCount = &sppb.ResultSetStats_RowCountExact{RowCountExact: it.RowCount}
	}
	return stats
}

type txResult int

const (
	txResultCommit txResult = iota
	txResultRollback
)

var _ contextTransaction = &delegatingTransaction{}

// delegatingTransaction wraps a read/write or read-only transaction and delegates
// all calls to the underlying transaction. The underlying transaction is automatically
// created when the first query or DML statement is executed. The type of transaction is
// determined at the moment that the underlying transaction is created. This allows an
// application to execute statements like `set transaction read only` at the start of a
// transaction to set the type of transaction.
type delegatingTransaction struct {
	conn               *conn
	ctx                context.Context
	close              func(result txResult)
	implicit           bool
	contextTransaction contextTransaction
}

func (d *delegatingTransaction) deadline() (time.Time, bool) {
	if d.contextTransaction != nil {
		return d.contextTransaction.deadline()
	}
	return d.ctx.Deadline()
}

func (d *delegatingTransaction) ensureActivated() error {
	if d.contextTransaction != nil {
		return nil
	}
	tx, err := d.conn.activateTransaction()
	if err != nil {
		return err
	}
	d.contextTransaction = tx
	return nil
}

func (d *delegatingTransaction) Commit() error {
	if d.contextTransaction == nil {
		d.close(txResultCommit)
		return nil
	}
	return d.contextTransaction.Commit()
}

func (d *delegatingTransaction) Rollback() error {
	if d.contextTransaction == nil {
		d.close(txResultRollback)
		return nil
	}
	return d.contextTransaction.Rollback()
}

func (d *delegatingTransaction) resetForRetry(ctx context.Context) error {
	if d.contextTransaction == nil {
		return status.Error(codes.FailedPrecondition, "a transaction can only be reset after it has been activated")
	}
	return d.contextTransaction.resetForRetry(ctx)
}

func (d *delegatingTransaction) Query(ctx context.Context, stmt spanner.Statement, stmtType parser.StatementType, execOptions *ExecOptions) (rowIterator, error) {
	if err := d.ensureActivated(); err != nil {
		return nil, err
	}
	return d.contextTransaction.Query(ctx, stmt, stmtType, execOptions)
}

func (d *delegatingTransaction) partitionQuery(ctx context.Context, stmt spanner.Statement, execOptions *ExecOptions) (driver.Rows, error) {
	if err := d.ensureActivated(); err != nil {
		return nil, err
	}
	return d.contextTransaction.partitionQuery(ctx, stmt, execOptions)
}

func (d *delegatingTransaction) ExecContext(ctx context.Context, stmt spanner.Statement, statementInfo *parser.StatementInfo, options spanner.QueryOptions) (*result, error) {
	if err := d.ensureActivated(); err != nil {
		return nil, err
	}
	return d.contextTransaction.ExecContext(ctx, stmt, statementInfo, options)
}

func (d *delegatingTransaction) StartBatchDML(options spanner.QueryOptions, automatic bool) (driver.Result, error) {
	if err := d.ensureActivated(); err != nil {
		return nil, err
	}
	return d.contextTransaction.StartBatchDML(options, automatic)
}

func (d *delegatingTransaction) RunBatch(ctx context.Context) (driver.Result, error) {
	if err := d.ensureActivated(); err != nil {
		return nil, err
	}
	return d.contextTransaction.RunBatch(ctx)
}

func (d *delegatingTransaction) RunDmlBatch(ctx context.Context) (SpannerResult, error) {
	if err := d.ensureActivated(); err != nil {
		return nil, err
	}
	return d.contextTransaction.RunDmlBatch(ctx)
}

func (d *delegatingTransaction) AbortBatch() (driver.Result, error) {
	if err := d.ensureActivated(); err != nil {
		return nil, err
	}
	return d.contextTransaction.AbortBatch()
}

func (d *delegatingTransaction) IsInBatch() bool {
	return d.contextTransaction != nil && d.contextTransaction.IsInBatch()
}

func (d *delegatingTransaction) BufferWrite(ms []*spanner.Mutation) error {
	if err := d.ensureActivated(); err != nil {
		return err
	}
	return d.contextTransaction.BufferWrite(ms)
}

var _ contextTransaction = &readOnlyTransaction{}

type readOnlyTransaction struct {
	roTx   *spanner.ReadOnlyTransaction
	boTx   *spanner.BatchReadOnlyTransaction
	logger *slog.Logger
	close  func(result txResult)

	timestampBoundMu       sync.Mutex
	timestampBoundSet      bool
	timestampBoundCallback func() spanner.TimestampBound
}

func (tx *readOnlyTransaction) deadline() (time.Time, bool) {
	return time.Time{}, false
}

func (tx *readOnlyTransaction) Commit() error {
	tx.logger.Debug("committing transaction")
	// Read-only transactions don't really commit, but closing the transaction
	// will return the session to the pool.
	if tx.boTx != nil {
		tx.boTx.Close()
	} else if tx.roTx != nil {
		tx.roTx.Close()
	}
	tx.close(txResultCommit)
	return nil
}

func (tx *readOnlyTransaction) Rollback() error {
	tx.logger.Debug("rolling back transaction")
	// Read-only transactions don't really roll back, but closing the transaction
	// will return the session to the pool.
	if tx.roTx != nil {
		tx.roTx.Close()
	}
	tx.close(txResultRollback)
	return nil
}

func (tx *readOnlyTransaction) resetForRetry(ctx context.Context) error {
	// no-op
	return nil
}

func (tx *readOnlyTransaction) Query(ctx context.Context, stmt spanner.Statement, stmtType parser.StatementType, execOptions *ExecOptions) (rowIterator, error) {
	tx.logger.DebugContext(ctx, "Query", "stmt", stmt.SQL)
	if execOptions.PartitionedQueryOptions.AutoPartitionQuery {
		if tx.boTx == nil {
			return nil, spanner.ToSpannerError(status.Errorf(codes.FailedPrecondition, "AutoPartitionQuery is only supported for batch read-only transactions"))
		}
		pq, err := tx.createPartitionedQuery(ctx, stmt, execOptions)
		if err != nil {
			return nil, err
		}
		mi := createMergedIterator(tx.logger, pq, execOptions.PartitionedQueryOptions.MaxParallelism)
		if err := mi.run(ctx); err != nil {
			mi.Stop()
			return nil, err
		}
		return mi, nil
	}
	if tx.timestampBoundCallback != nil {
		tx.timestampBoundMu.Lock()
		if !tx.timestampBoundSet {
			tx.roTx.WithTimestampBound(tx.timestampBoundCallback())
			tx.timestampBoundSet = true
		}
		tx.timestampBoundMu.Unlock()
	}
	return &readOnlyRowIterator{tx.roTx.QueryWithOptions(ctx, stmt, execOptions.QueryOptions), stmtType}, nil
}

func (tx *readOnlyTransaction) partitionQuery(ctx context.Context, stmt spanner.Statement, execOptions *ExecOptions) (driver.Rows, error) {
	pq, err := tx.createPartitionedQuery(ctx, stmt, execOptions)
	if err != nil {
		return nil, err
	}
	return &partitionedQueryRows{partitionedQuery: pq}, nil
}

func (tx *readOnlyTransaction) createPartitionedQuery(ctx context.Context, stmt spanner.Statement, execOptions *ExecOptions) (*PartitionedQuery, error) {
	if tx.boTx == nil {
		return nil, spanner.ToSpannerError(status.Errorf(codes.FailedPrecondition, "partitionQuery is only supported for batch read-only transactions"))
	}
	partitions, err := tx.boTx.PartitionQueryWithOptions(ctx, stmt, execOptions.PartitionedQueryOptions.PartitionOptions, execOptions.QueryOptions)
	if err != nil {
		return nil, err
	}
	return &PartitionedQuery{
		stmt:        stmt,
		execOptions: execOptions,
		tx:          tx.boTx,
		Partitions:  partitions,
	}, nil
}

func (tx *readOnlyTransaction) ExecContext(_ context.Context, _ spanner.Statement, _ *parser.StatementInfo, _ spanner.QueryOptions) (*result, error) {
	return nil, spanner.ToSpannerError(status.Errorf(codes.FailedPrecondition, "read-only transactions cannot write"))
}

func (tx *readOnlyTransaction) StartBatchDML(_ spanner.QueryOptions, _ bool) (driver.Result, error) {
	return nil, spanner.ToSpannerError(status.Error(codes.FailedPrecondition, "read-only transactions cannot write"))
}

func (tx *readOnlyTransaction) RunBatch(_ context.Context) (driver.Result, error) {
	return nil, spanner.ToSpannerError(status.Error(codes.FailedPrecondition, "read-only transactions cannot write"))
}

func (tx *readOnlyTransaction) RunDmlBatch(_ context.Context) (SpannerResult, error) {
	return nil, spanner.ToSpannerError(status.Error(codes.FailedPrecondition, "read-only transactions cannot write"))
}

func (tx *readOnlyTransaction) AbortBatch() (driver.Result, error) {
	return driver.ResultNoRows, nil
}

func (tx *readOnlyTransaction) IsInBatch() bool {
	return false
}

func (tx *readOnlyTransaction) BufferWrite([]*spanner.Mutation) error {
	return spanner.ToSpannerError(status.Errorf(codes.FailedPrecondition, "read-only transactions cannot write"))
}

// ErrAbortedDueToConcurrentModification is returned by a read/write transaction
// that was aborted by Cloud Spanner, and where the internal retry attempt
// failed because it detected that the results during the retry were different
// from the initial attempt.
//
// Use the RunTransaction function to execute a read/write transaction in a
// retry loop. This function will never return ErrAbortedDueToConcurrentModification.
var ErrAbortedDueToConcurrentModification = status.Error(codes.Aborted, "Transaction was aborted due to a concurrent modification")

var _ contextTransaction = &readWriteTransaction{}

// readWriteTransaction is the internal structure for go/sql read/write
// transactions. These transactions can automatically be retried if the
// underlying Spanner transaction is aborted. This is done by keeping track
// of all statements that are executed on the transaction. If the transaction
// is aborted, the transaction will be replayed using a new read/write
// transaction on Spanner, and the results of the two will be compared with each
// other. If they are equal, the underlying Spanner read/write transaction is
// replaced with the one that was used for the replay, and the user transaction
// can continue as if nothing happened.
type readWriteTransaction struct {
	ctx    context.Context
	conn   *conn
	logger *slog.Logger
	// rwTx is the underlying Spanner read/write transaction. This transaction
	// will be replaced with a new one if the initial transaction is aborted.
	rwTx *spanner.ReadWriteStmtBasedTransaction
	// active indicates whether at least one statement has been executed on this transaction.
	active bool
	// batch is any DML batch that is active for this transaction.
	batch *batch
	close func(result txResult, commitResponse *spanner.CommitResponse, commitErr error)
	// retryAborts indicates whether this transaction will automatically retry
	// the transaction if it is aborted by Spanner. The default is true.
	retryAborts func() bool

	// statements contains the list of statements that has been executed on this
	// transaction so far. These statements will be replayed on a new read write
	// transaction if the initial attempt is aborted.
	statements []retriableStatement

	// mutations contains the buffered mutations of this transaction. These are
	// added to the next transaction if the transaction executes an internal retry.
	mutations []*spanner.Mutation
}

// retriableStatement is the interface that is used to keep track of statements
// that have been executed on a read/write transaction. These statements must
// implement a retry method that will be executed during a transaction retry.
type retriableStatement interface {
	// retry retries the statement on a new Spanner transaction. The method must
	// return nil if it receives the same result as during the initial attempt,
	// and otherwise return the error ErrAbortedDueToConcurrentModification.
	//
	// Note: This method does not return any error that is returned by Spanner
	// when the statement is executed. Instead, if the statement returns an
	// error, the returned error should be compared with the result during the
	// initial attempt. If the two errors are equal, the retry of the statement
	// should be considered successful and the method should return nil.
	retry(ctx context.Context, tx *spanner.ReadWriteStmtBasedTransaction) error
}

// retriableUpdate implements retriableStatement for update statements.
type retriableUpdate struct {
	// stmt is the statement that was executed on Spanner.
	stmt     spanner.Statement
	stmtInfo *parser.StatementInfo
	options  spanner.QueryOptions
	// res is the record count and other results that were returned by Spanner.
	res result
	// err is the error that was returned by Spanner.
	err error
}

func (ru *retriableUpdate) String() string {
	return ru.stmt.SQL
}

// retry retries an update statement on Spanner. It returns nil if the result
// of the statement during the retry is equal to the result during the initial
// attempt.
func (ru *retriableUpdate) retry(ctx context.Context, tx *spanner.ReadWriteStmtBasedTransaction) error {
	res, err := execTransactionalDML(ctx, tx, ru.stmt, ru.stmtInfo, ru.options)
	if err != nil && spanner.ErrCode(err) == codes.Aborted {
		return err
	}
	if err != nil && errorsEqualForRetry(err, ru.err) {
		return nil
	}
	if res != nil && res.rowsAffected == ru.res.rowsAffected && res.hasLastInsertId == ru.res.hasLastInsertId && res.lastInsertId == ru.res.lastInsertId {
		return nil
	}
	return ErrAbortedDueToConcurrentModification
}

// retriableBatchUpdate implements retriableStatement for Batch DML.
type retriableBatchUpdate struct {
	// statements are the statement that were executed on Spanner.
	statements []spanner.Statement
	options    spanner.QueryOptions
	// c is the record counts that were returned by Spanner.
	c []int64
	// err is the error that was returned by Spanner.
	err error
}

func (ru *retriableBatchUpdate) String() string {

	return fmt.Sprintf("[%s]", ru.statements)
}

// retry retries an BatchDML statement on Spanner. It returns nil if the result
// of the statement during the retry is equal to the result during the initial
// attempt.
func (ru *retriableBatchUpdate) retry(ctx context.Context, tx *spanner.ReadWriteStmtBasedTransaction) error {
	c, err := tx.BatchUpdateWithOptions(ctx, ru.statements, ru.options)
	if err != nil && spanner.ErrCode(err) == codes.Aborted {
		return err
	}
	if !errorsEqualForRetry(err, ru.err) {
		return ErrAbortedDueToConcurrentModification
	}
	if len(c) != len(ru.c) {
		return ErrAbortedDueToConcurrentModification
	}
	for i := range ru.c {
		if c[i] != ru.c[i] {
			return ErrAbortedDueToConcurrentModification
		}
	}
	return nil
}

func (tx *readWriteTransaction) deadline() (time.Time, bool) {
	return tx.ctx.Deadline()
}

// runWithRetry executes a statement on a go/sql read/write transaction and
// automatically retries the entire transaction if the statement returns an
// Aborted error. The method will return ErrAbortedDueToConcurrentModification
// if the transaction is aborted and the retry fails because the retry attempt
// returned different results than the initial attempt.
func (tx *readWriteTransaction) runWithRetry(ctx context.Context, f func(ctx context.Context) error) (err error) {
	for {
		if err == nil {
			err = f(ctx)
		}
		if err == nil {
			return
		}
		if err == ErrAbortedDueToConcurrentModification {
			tx.logger.Log(ctx, LevelNotice, "transaction retry failed due to a concurrent modification")
			return
		}
		if spanner.ErrCode(err) == codes.Aborted {
			delay, ok := spanner.ExtractRetryDelay(err)
			if !ok {
				delay = tx.randomRetryDelay()
			}
			if err := gax.Sleep(ctx, delay); err != nil {
				return err
			}
			err = tx.retry(ctx)
			continue
		}
		return
	}
}

func (tx *readWriteTransaction) randomRetryDelay() time.Duration {
	return time.Millisecond*time.Duration(rand.Int31n(30)) + time.Millisecond
}

// retry retries the entire read/write transaction on a new Spanner transaction.
// It will return ErrAbortedDueToConcurrentModification if the retry fails.
func (tx *readWriteTransaction) retry(ctx context.Context) (err error) {
	tx.logger.Log(ctx, LevelNotice, "starting transaction retry")
	tx.rwTx, err = tx.rwTx.ResetForRetry(ctx)
	if err != nil {
		tx.logger.Log(ctx, LevelNotice, "failed to reset transaction")
		return err
	}
	// Re-apply the mutations from the previous transaction.
	if err := tx.rwTx.BufferWrite(tx.mutations); err != nil {
		return err
	}
	for _, stmt := range tx.statements {
		tx.logger.Log(ctx, slog.LevelDebug, "retrying statement", "stmt", stmt)
		err = stmt.retry(ctx, tx.rwTx)
		if err != nil {
			tx.logger.Log(ctx, slog.LevelDebug, "retrying statement failed", "stmt", stmt)
			return err
		}
	}

	tx.logger.Log(ctx, LevelNotice, "finished transaction retry")
	return err
}

// Commit implements driver.Tx#Commit().
// It will commit the underlying Spanner transaction. If the transaction is
// aborted by Spanner, the entire transaction will automatically be retried,
// unless internal retries have been disabled.
func (tx *readWriteTransaction) Commit() (err error) {
	tx.logger.Debug("committing transaction")
	tx.active = true
	if err := tx.maybeRunAutoDmlBatch(tx.ctx); err != nil {
		_ = tx.rollback()
		return err
	}
	var commitResponse spanner.CommitResponse
	if tx.rwTx != nil {
		if !tx.retryAborts() {
			ts, err := tx.rwTx.CommitWithReturnResp(tx.ctx)
			tx.close(txResultCommit, &ts, err)
			return err
		}

		err = tx.runWithRetry(tx.ctx, func(ctx context.Context) (err error) {
			commitResponse, err = tx.rwTx.CommitWithReturnResp(ctx)
			return err
		})
		if err == ErrAbortedDueToConcurrentModification {
			tx.rwTx.Rollback(context.Background())
		}
	}
	tx.close(txResultCommit, &commitResponse, err)
	return err
}

// Rollback implements driver.Tx#Rollback(). The underlying Spanner transaction
// will be rolled back and the session will be returned to the session pool.
func (tx *readWriteTransaction) Rollback() error {
	tx.logger.Debug("rolling back transaction")
	if tx.batch != nil && tx.batch.automatic {
		_, _ = tx.AbortBatch()
	}
	return tx.rollback()
}

func (tx *readWriteTransaction) rollback() error {
	if tx.rwTx != nil {
		// Always use context.Background() for rollback invocations to allow them
		// to be executed, even if the transaction has timed out or been cancelled.
		tx.rwTx.Rollback(context.Background())
	}
	tx.close(txResultRollback, nil, nil)
	return nil
}

func (tx *readWriteTransaction) resetForRetry(ctx context.Context) error {
	t, err := tx.rwTx.ResetForRetry(ctx)
	if err != nil {
		return err
	}
	tx.rwTx = t
	return nil
}

// Query executes a query using the read/write transaction and returns a
// rowIterator that will automatically retry the read/write transaction if the
// transaction is aborted during the query or while iterating the returned rows.
func (tx *readWriteTransaction) Query(ctx context.Context, stmt spanner.Statement, stmtType parser.StatementType, execOptions *ExecOptions) (rowIterator, error) {
	tx.logger.Debug("Query", "stmt", stmt.SQL)
	tx.active = true
	if err := tx.maybeRunAutoDmlBatch(ctx); err != nil {
		return nil, err
	}
	// If internal retries have been disabled, we don't need to keep track of a
	// running checksum for all results that we have seen.
	if !tx.retryAborts() {
		return &readOnlyRowIterator{tx.rwTx.QueryWithOptions(ctx, stmt, execOptions.QueryOptions), stmtType}, nil
	}

	// If retries are enabled, we need to use a row iterator that will keep
	// track of a running checksum of all the results that we see.
	it := &checksumRowIterator{
		RowIterator: tx.rwTx.QueryWithOptions(ctx, stmt, execOptions.QueryOptions),
		ctx:         ctx,
		tx:          tx,
		stmt:        stmt,
		stmtType:    stmtType,
		options:     execOptions.QueryOptions,
		hash:        sha256.New(),
	}
	tx.statements = append(tx.statements, it)
	return it, nil
}

func (tx *readWriteTransaction) partitionQuery(ctx context.Context, stmt spanner.Statement, execOptions *ExecOptions) (driver.Rows, error) {
	return nil, spanner.ToSpannerError(status.Errorf(codes.FailedPrecondition, "read/write transactions cannot partition queries"))
}

func (tx *readWriteTransaction) ExecContext(ctx context.Context, stmt spanner.Statement, statementInfo *parser.StatementInfo, options spanner.QueryOptions) (res *result, err error) {
	tx.logger.Debug("ExecContext", "stmt", stmt.SQL)
	tx.active = true
	if tx.batch != nil {
		tx.logger.Debug("adding statement to batch")
		tx.batch.statements = append(tx.batch.statements, stmt)
		updateCount := int64(0)
		if tx.batch.automatic {
			updateCount = tx.conn.AutoBatchDmlUpdateCount()
		}
		tx.batch.returnValues = append(tx.batch.returnValues, updateCount)
		return &result{rowsAffected: updateCount}, nil
	}

	if !tx.retryAborts() {
		return execTransactionalDML(ctx, tx.rwTx, stmt, statementInfo, options)
	}

	err = tx.runWithRetry(ctx, func(ctx context.Context) error {
		res, err = execTransactionalDML(ctx, tx.rwTx, stmt, statementInfo, options)
		return err
	})
	retryableStmt := &retriableUpdate{
		stmt:     stmt,
		stmtInfo: statementInfo,
		options:  options,
		err:      err,
	}
	if res != nil {
		retryableStmt.res = *res
	}
	tx.statements = append(tx.statements, retryableStmt)
	return res, err
}

func (tx *readWriteTransaction) StartBatchDML(options spanner.QueryOptions, automatic bool) (driver.Result, error) {
	if tx.batch != nil {
		return nil, spanner.ToSpannerError(status.Errorf(codes.FailedPrecondition, "This transaction already has an active batch."))
	}
	tx.logger.Debug("starting dml batch in transaction", "automatic", automatic)
	tx.active = true
	tx.batch = &batch{tp: parser.BatchTypeDml, options: &ExecOptions{QueryOptions: options}, automatic: automatic}
	return driver.ResultNoRows, nil
}

func (tx *readWriteTransaction) RunBatch(ctx context.Context) (driver.Result, error) {
	if tx.batch == nil {
		return nil, spanner.ToSpannerError(status.Errorf(codes.FailedPrecondition, "This transaction does not have an active batch"))
	}
	switch tx.batch.tp {
	case parser.BatchTypeDml:
		return tx.runDmlBatch(ctx)
	case parser.BatchTypeDdl:
		fallthrough
	default:
		return nil, spanner.ToSpannerError(status.Errorf(codes.InvalidArgument, "Unknown or unsupported batch type: %d", tx.batch.tp))
	}
}

func (tx *readWriteTransaction) RunDmlBatch(ctx context.Context) (SpannerResult, error) {
	if tx.batch == nil {
		return nil, spanner.ToSpannerError(status.Errorf(codes.FailedPrecondition, "this transaction does not have an active batch"))
	}
	if tx.batch.tp != parser.BatchTypeDml {
		return nil, spanner.ToSpannerError(status.Errorf(codes.FailedPrecondition, "batch is not a DML batch"))
	}
	return tx.runDmlBatch(ctx)
}

func (tx *readWriteTransaction) AbortBatch() (driver.Result, error) {
	tx.logger.Debug("aborting batch")
	tx.batch = nil
	return driver.ResultNoRows, nil
}

func (tx *readWriteTransaction) IsInBatch() bool {
	return tx.batch != nil
}

func (tx *readWriteTransaction) maybeRunAutoDmlBatch(ctx context.Context) error {
	if tx.batch == nil || !tx.batch.automatic {
		return nil
	}
	tx.logger.DebugContext(ctx, "running auto-dml-batch")
	batch := tx.batch
	res, err := tx.runDmlBatch(ctx)
	if err != nil {
		return fmt.Errorf("running auto-dml-batch failed: %w", err)
	}
	if !tx.conn.AutoBatchDmlUpdateCountVerification() {
		// Skip verification.
		return nil
	}
	if err := verifyAutoDmlBatch(batch, res.batchUpdateCounts); err != nil {
		return fmt.Errorf("verifying auto-dml-batch failed: %w", err)
	}
	return nil
}

func verifyAutoDmlBatch(batch *batch, batchUpdateCounts []int64) error {
	if len(batch.returnValues) != len(batchUpdateCounts) {
		return spanner.ToSpannerError(status.Error(codes.FailedPrecondition, "batch results length does not match number of update counts"))
	}
	for i := 0; i < len(batch.returnValues); i++ {
		if g, w := batchUpdateCounts[i], batch.returnValues[i]; g != w {
			return spanner.ToSpannerError(status.Errorf(codes.FailedPrecondition, "batch results differ at index %v\n Got: %v\nWant: %v", i, g, w))
		}
	}
	return nil
}

func (tx *readWriteTransaction) runDmlBatch(ctx context.Context) (*result, error) {
	tx.logger.Debug("running dml batch")
	statements := tx.batch.statements
	options := tx.batch.options
	tx.batch = nil

	if !tx.retryAborts() {
		affected, err := tx.rwTx.BatchUpdateWithOptions(ctx, statements, options.QueryOptions)
		res := &result{rowsAffected: sum(affected), batchUpdateCounts: affected}
		ba := toBatchError(res, err)
		return res, ba
	}

	var affected []int64
	var err error
	err = tx.runWithRetry(ctx, func(ctx context.Context) error {
		affected, err = tx.rwTx.BatchUpdateWithOptions(ctx, statements, options.QueryOptions)
		return err
	})
	tx.statements = append(tx.statements, &retriableBatchUpdate{
		statements: statements,
		options:    options.QueryOptions,
		c:          affected,
		err:        err,
	})
	res := &result{rowsAffected: sum(affected), batchUpdateCounts: affected}
	ba := toBatchError(res, err)
	return res, ba
}

func (tx *readWriteTransaction) BufferWrite(ms []*spanner.Mutation) error {
	tx.mutations = append(tx.mutations, ms...)
	return tx.rwTx.BufferWrite(ms)
}

// errorsEqualForRetry returns true if the two errors should be considered equal
// when retrying a transaction. This comparison will return true if:
// - The errors are the same instances
// - Both errors have the same gRPC status code, not being one of the codes OK or Unknown.
func errorsEqualForRetry(err1, err2 error) bool {
	if err1 == err2 {
		return true
	}
	// spanner.ErrCode will return codes.OK for nil errors and codes.Unknown for
	// errors that do not have a gRPC code itself or in one of its wrapped errors.
	code1 := spanner.ErrCode(err1)
	code2 := spanner.ErrCode(err2)
	if code1 == code2 && (code1 != codes.OK && code1 != codes.Unknown) {
		return true
	}
	return false
}
