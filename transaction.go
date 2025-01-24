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
	"bytes"
	"context"
	"database/sql/driver"
	"encoding/gob"
	"fmt"
	"log/slog"
	"time"

	"cloud.google.com/go/spanner"
	sppb "cloud.google.com/go/spanner/apiv1/spannerpb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// contextTransaction is the combination of both read/write and read-only
// transactions.
type contextTransaction interface {
	Commit() error
	Rollback() error
	resetForRetry(ctx context.Context) error
	Query(ctx context.Context, stmt spanner.Statement, execOptions ExecOptions) (rowIterator, error)
	partitionQuery(ctx context.Context, stmt spanner.Statement, execOptions ExecOptions) (driver.Rows, error)
	ExecContext(ctx context.Context, stmt spanner.Statement, options spanner.QueryOptions) (int64, error)

	StartBatchDML(options spanner.QueryOptions) (driver.Result, error)
	RunBatch(ctx context.Context) (driver.Result, error)
	AbortBatch() (driver.Result, error)

	BufferWrite(ms []*spanner.Mutation) error
}

type rowIterator interface {
	Next() (*spanner.Row, error)
	Stop()
	Metadata() (*sppb.ResultSetMetadata, error)
}

type readOnlyRowIterator struct {
	*spanner.RowIterator
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

type readOnlyTransaction struct {
	roTx   *spanner.ReadOnlyTransaction
	boTx   *spanner.BatchReadOnlyTransaction
	logger *slog.Logger
	close  func()
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
	tx.close()
	return nil
}

func (tx *readOnlyTransaction) Rollback() error {
	tx.logger.Debug("rolling back transaction")
	// Read-only transactions don't really roll back, but closing the transaction
	// will return the session to the pool.
	if tx.roTx != nil {
		tx.roTx.Close()
	}
	tx.close()
	return nil
}

func (tx *readOnlyTransaction) resetForRetry(ctx context.Context) error {
	// no-op
	return nil
}

func (tx *readOnlyTransaction) Query(ctx context.Context, stmt spanner.Statement, execOptions ExecOptions) (rowIterator, error) {
	tx.logger.DebugContext(ctx, "Query", "stmt", stmt.SQL)
	if execOptions.PartitionedQueryOptions.AutoPartitionQuery {
		if tx.boTx == nil {
			return nil, spanner.ToSpannerError(status.Errorf(codes.FailedPrecondition, "AutoPartitionQuery is only supported for batch read-only transactions"))
		}
		pq, err := tx.partitionQueryTemp(ctx, stmt, execOptions)
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
	return &readOnlyRowIterator{tx.roTx.QueryWithOptions(ctx, stmt, execOptions.QueryOptions)}, nil
}

func (tx *readOnlyTransaction) partitionQuery(ctx context.Context, stmt spanner.Statement, execOptions ExecOptions) (driver.Rows, error) {
	pq, err := tx.partitionQueryTemp(ctx, stmt, execOptions)
	if err != nil {
		return nil, err
	}
	return &partitionedQueryRows{partitionedQuery: pq}, nil
}

func (tx *readOnlyTransaction) partitionQueryTemp(ctx context.Context, stmt spanner.Statement, execOptions ExecOptions) (*PartitionedQuery, error) {
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

func (tx *readOnlyTransaction) ExecContext(_ context.Context, _ spanner.Statement, _ spanner.QueryOptions) (int64, error) {
	return 0, spanner.ToSpannerError(status.Errorf(codes.FailedPrecondition, "read-only transactions cannot write"))
}

func (tx *readOnlyTransaction) StartBatchDML(_ spanner.QueryOptions) (driver.Result, error) {
	return nil, spanner.ToSpannerError(status.Error(codes.FailedPrecondition, "read-only transactions cannot write"))
}

func (tx *readOnlyTransaction) RunBatch(_ context.Context) (driver.Result, error) {
	return nil, spanner.ToSpannerError(status.Error(codes.FailedPrecondition, "read-only transactions cannot write"))
}

func (tx *readOnlyTransaction) AbortBatch() (driver.Result, error) {
	return driver.ResultNoRows, nil
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
	client *spanner.Client
	logger *slog.Logger
	// rwTx is the underlying Spanner read/write transaction. This transaction
	// will be replaced with a new one if the initial transaction is aborted.
	rwTx *spanner.ReadWriteStmtBasedTransaction
	// batch is any DML batch that is active for this transaction.
	batch *batch
	close func(commitTs *time.Time, commitErr error)
	// retryAborts indicates whether this transaction will automatically retry
	// the transaction if it is aborted by Spanner. The default is true.
	retryAborts bool

	// statements contains the list of statements that has been executed on this
	// transaction so far. These statements will be replayed on a new read write
	// transaction if the initial attempt is aborted.
	statements []retriableStatement
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
	stmt    spanner.Statement
	options spanner.QueryOptions
	// c is the record count that was returned by Spanner.
	c int64
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
	c, err := tx.UpdateWithOptions(ctx, ru.stmt, ru.options)
	if err != nil && spanner.ErrCode(err) == codes.Aborted {
		return err
	}
	if !errorsEqualForRetry(err, ru.err) {
		return ErrAbortedDueToConcurrentModification
	}
	if c != ru.c {
		return ErrAbortedDueToConcurrentModification
	}
	return nil
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
		if err == ErrAbortedDueToConcurrentModification {
			tx.logger.Log(ctx, LevelNotice, "transaction retry failed due to a concurrent modification")
			return
		}
		if spanner.ErrCode(err) == codes.Aborted {
			err = tx.retry(ctx)
			continue
		}
		return
	}
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
	var commitTs time.Time
	if tx.rwTx != nil {
		if !tx.retryAborts {
			ts, err := tx.rwTx.Commit(tx.ctx)
			tx.close(&ts, err)
			return err
		}

		err = tx.runWithRetry(tx.ctx, func(ctx context.Context) (err error) {
			commitTs, err = tx.rwTx.Commit(ctx)
			return err
		})
	}
	tx.close(&commitTs, err)
	return err
}

// Rollback implements driver.Tx#Rollback(). The underlying Spanner transaction
// will be rolled back and the session will be returned to the session pool.
func (tx *readWriteTransaction) Rollback() error {
	tx.logger.Debug("rolling back transaction")
	if tx.rwTx != nil {
		tx.rwTx.Rollback(tx.ctx)
	}
	tx.close(nil, nil)
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
func (tx *readWriteTransaction) Query(ctx context.Context, stmt spanner.Statement, execOptions ExecOptions) (rowIterator, error) {
	tx.logger.Debug("Query", "stmt", stmt.SQL)
	// If internal retries have been disabled, we don't need to keep track of a
	// running checksum for all results that we have seen.
	if !tx.retryAborts {
		return &readOnlyRowIterator{tx.rwTx.QueryWithOptions(ctx, stmt, execOptions.QueryOptions)}, nil
	}

	// If retries are enabled, we need to use a row iterator that will keep
	// track of a running checksum of all the results that we see.
	buffer := &bytes.Buffer{}
	it := &checksumRowIterator{
		RowIterator: tx.rwTx.QueryWithOptions(ctx, stmt, execOptions.QueryOptions),
		ctx:         ctx,
		tx:          tx,
		stmt:        stmt,
		options:     execOptions.QueryOptions,
		buffer:      buffer,
		enc:         gob.NewEncoder(buffer),
	}
	tx.statements = append(tx.statements, it)
	return it, nil
}

func (tx *readWriteTransaction) partitionQuery(ctx context.Context, stmt spanner.Statement, execOptions ExecOptions) (driver.Rows, error) {
	return nil, spanner.ToSpannerError(status.Errorf(codes.FailedPrecondition, "read/write transactions cannot partition queries"))
}

func (tx *readWriteTransaction) ExecContext(ctx context.Context, stmt spanner.Statement, options spanner.QueryOptions) (res int64, err error) {
	tx.logger.Debug("ExecContext", "stmt", stmt.SQL)
	if tx.batch != nil {
		tx.logger.Debug("adding statement to batch")
		tx.batch.statements = append(tx.batch.statements, stmt)
		return 0, nil
	}

	if !tx.retryAborts {
		return tx.rwTx.UpdateWithOptions(ctx, stmt, options)
	}

	err = tx.runWithRetry(ctx, func(ctx context.Context) error {
		res, err = tx.rwTx.UpdateWithOptions(ctx, stmt, options)
		return err
	})
	tx.statements = append(tx.statements, &retriableUpdate{
		stmt:    stmt,
		options: options,
		c:       res,
		err:     err,
	})
	return res, err
}

func (tx *readWriteTransaction) StartBatchDML(options spanner.QueryOptions) (driver.Result, error) {
	if tx.batch != nil {
		return nil, spanner.ToSpannerError(status.Errorf(codes.FailedPrecondition, "This transaction already has an active batch."))
	}
	tx.logger.Debug("starting dml batch")
	tx.batch = &batch{tp: dml, options: ExecOptions{QueryOptions: options}}
	return driver.ResultNoRows, nil
}

func (tx *readWriteTransaction) RunBatch(ctx context.Context) (driver.Result, error) {
	if tx.batch == nil {
		return nil, spanner.ToSpannerError(status.Errorf(codes.FailedPrecondition, "This transaction does not have an active batch"))
	}
	switch tx.batch.tp {
	case dml:
		return tx.runDmlBatch(ctx)
	case ddl:
		fallthrough
	default:
		return nil, spanner.ToSpannerError(status.Errorf(codes.InvalidArgument, "Unknown or unsupported batch type: %d", tx.batch.tp))
	}
}

func (tx *readWriteTransaction) AbortBatch() (driver.Result, error) {
	tx.logger.Debug("aborting batch")
	tx.batch = nil
	return driver.ResultNoRows, nil
}

func (tx *readWriteTransaction) runDmlBatch(ctx context.Context) (driver.Result, error) {
	tx.logger.Debug("running dml batch")
	statements := tx.batch.statements
	options := tx.batch.options
	tx.batch = nil

	if !tx.retryAborts {
		affected, err := tx.rwTx.BatchUpdateWithOptions(ctx, statements, options.QueryOptions)
		return &result{rowsAffected: sum(affected)}, err
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
	return &result{rowsAffected: sum(affected)}, err
}

func (tx *readWriteTransaction) BufferWrite(ms []*spanner.Mutation) error {
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
