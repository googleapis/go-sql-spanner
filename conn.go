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
	"database/sql"
	"database/sql/driver"
	"errors"
	"log/slog"
	"slices"
	"time"

	"cloud.google.com/go/spanner"
	adminapi "cloud.google.com/go/spanner/admin/database/apiv1"
	adminpb "cloud.google.com/go/spanner/admin/database/apiv1/databasepb"
	"cloud.google.com/go/spanner/apiv1/spannerpb"
	"google.golang.org/api/iterator"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// SpannerConn is the public interface for the raw Spanner connection for the
// sql driver. This interface can be used with the db.Conn().Raw() method.
type SpannerConn interface {
	// StartBatchDDL starts a DDL batch on the connection. After calling this
	// method all subsequent DDL statements will be cached locally. Calling
	// RunBatch will send all cached DDL statements to Spanner as one batch.
	// Use DDL batching to speed up the execution of multiple DDL statements.
	// Note that a DDL batch is not atomic. It is possible that some DDL
	// statements are executed successfully and some not.
	// See https://cloud.google.com/spanner/docs/schema-updates#order_of_execution_of_statements_in_batches
	// for more information on how Cloud Spanner handles DDL batches.
	StartBatchDDL() error
	// StartBatchDML starts a DML batch on the connection. After calling this
	// method all subsequent DML statements will be cached locally. Calling
	// RunBatch will send all cached DML statements to Spanner as one batch.
	// Use DML batching to speed up the execution of multiple DML statements.
	// DML batches can be executed both outside of a transaction and during
	// a read/write transaction. If a DML batch is executed outside an active
	// transaction, the batch will be applied atomically to the database if
	// successful and rolled back if one or more of the statements fail.
	// If a DML batch is executed as part of a transaction, the error will
	// be returned to the application, and the application can decide whether
	// to commit or rollback the transaction.
	StartBatchDML() error
	// RunBatch sends all batched DDL or DML statements to Spanner. This is a
	// no-op if no statements have been batched or if there is no active batch.
	RunBatch(ctx context.Context) error
	// AbortBatch aborts the current DDL or DML batch and discards all batched
	// statements.
	AbortBatch() error
	// InDDLBatch returns true if the connection is currently in a DDL batch.
	InDDLBatch() bool
	// InDMLBatch returns true if the connection is currently in a DML batch.
	InDMLBatch() bool
	// GetBatchedStatements returns a copy of the statements that are currently
	// buffered to be executed as a DML or DDL batch. It returns an empty slice
	// if no batch is active, or if there are no statements buffered.
	GetBatchedStatements() []spanner.Statement

	// AutoBatchDml determines whether DML statements should automatically
	// be batched and sent to Spanner when a non-DML statement is encountered.
	// The update count that is returned for DML statements that are buffered
	// is by default 1. This default can be changed by setting the connection
	// variable AutoBatchDmlUpdateCount to a value other than 1.
	// This feature is only used in read/write transactions. DML statements
	// outside transactions are always executed directly.
	AutoBatchDml() bool
	SetAutoBatchDml(autoBatch bool) error
	// AutoBatchDmlUpdateCount determines the update count that is returned for
	// DML statements that are executed when AutoBatchDml is true.
	AutoBatchDmlUpdateCount() int64
	SetAutoBatchDmlUpdateCount(updateCount int64) error
	// AutoBatchDmlUpdateCountVerification enables/disables the verification
	// that the update count that was returned for automatically batched DML
	// statements was correct.
	AutoBatchDmlUpdateCountVerification() bool
	SetAutoBatchDmlUpdateCountVerification(verify bool) error

	// RetryAbortsInternally returns true if the connection automatically
	// retries all aborted transactions.
	RetryAbortsInternally() bool
	// SetRetryAbortsInternally enables/disables the automatic retry of aborted
	// transactions. If disabled, any aborted error from a transaction will be
	// propagated to the application.
	SetRetryAbortsInternally(retry bool) error

	// AutocommitDMLMode returns the current mode that is used for DML
	// statements outside a transaction. The default is Transactional.
	AutocommitDMLMode() AutocommitDMLMode
	// SetAutocommitDMLMode sets the mode to use for DML statements that are
	// executed outside transactions. The default is Transactional. Change to
	// PartitionedNonAtomic to use Partitioned DML instead of Transactional DML.
	// See https://cloud.google.com/spanner/docs/dml-partitioned for more
	// information on Partitioned DML.
	SetAutocommitDMLMode(mode AutocommitDMLMode) error

	// ReadOnlyStaleness returns the current staleness that is used for
	// queries in autocommit mode, and for read-only transactions.
	ReadOnlyStaleness() spanner.TimestampBound
	// SetReadOnlyStaleness sets the staleness to use for queries in autocommit
	// mode and for read-only transaction.
	SetReadOnlyStaleness(staleness spanner.TimestampBound) error

	// IsolationLevel returns the current default isolation level that is
	// used for read/write transactions on this connection.
	IsolationLevel() sql.IsolationLevel
	// SetIsolationLevel sets the default isolation level to use for read/write
	// transactions on this connection.
	SetIsolationLevel(level sql.IsolationLevel) error

	// TransactionTag returns the transaction tag that will be applied to the next
	// read/write transaction on this connection. The transaction tag that is set
	// on the connection is cleared when a read/write transaction is started.
	TransactionTag() string
	// SetTransactionTag sets the transaction tag that should be applied to the
	// next read/write transaction on this connection. The tag is cleared when a
	// read/write transaction is started.
	SetTransactionTag(transactionTag string) error

	// MaxCommitDelay returns the max commit delay that will be applied to read/write
	// transactions on this connection.
	MaxCommitDelay() time.Duration
	// SetMaxCommitDelay sets the max commit delay that will be applied to read/write
	// transactions on this connection.
	SetMaxCommitDelay(delay time.Duration) error

	// ExcludeTxnFromChangeStreams returns true if the next transaction should be excluded from change streams with the
	// DDL option `allow_txn_exclusion=true`.
	ExcludeTxnFromChangeStreams() bool
	// SetExcludeTxnFromChangeStreams sets whether the next transaction should be excluded from change streams with the
	// DDL option `allow_txn_exclusion=true`.
	SetExcludeTxnFromChangeStreams(excludeTxnFromChangeStreams bool) error

	// DecodeToNativeArrays indicates whether arrays with a Go native type
	// should be decoded to those native types instead of the corresponding
	// spanner.NullTypeName (e.g. []bool vs []spanner.NullBool).
	// See ExecOptions.DecodeToNativeArrays for more information.
	DecodeToNativeArrays() bool
	// SetDecodeToNativeArrays sets whether arrays with a Go native type
	// should be decoded to those native types instead of the corresponding
	// spanner.NullTypeName (e.g. []bool vs []spanner.NullBool).
	// See ExecOptions.DecodeToNativeArrays for more information.
	SetDecodeToNativeArrays(decodeToNativeArrays bool) error

	// Apply writes an array of mutations to the database. This method may only be called while the connection
	// is outside a transaction. Use BufferWrite to write mutations in a transaction.
	// See also spanner.Client#Apply
	Apply(ctx context.Context, ms []*spanner.Mutation, opts ...spanner.ApplyOption) (commitTimestamp time.Time, err error)

	// BufferWrite writes an array of mutations to the current transaction. This method may only be called while the
	// connection is in a read/write transaction. Use Apply to write mutations outside a transaction.
	// See also spanner.ReadWriteTransaction#BufferWrite
	BufferWrite(ms []*spanner.Mutation) error

	// CommitTimestamp returns the commit timestamp of the last implicit or explicit read/write transaction that
	// was executed on the connection, or an error if the connection has not executed a read/write transaction
	// that committed successfully. The timestamp is in the local timezone.
	CommitTimestamp() (commitTimestamp time.Time, err error)

	// UnderlyingClient returns the underlying Spanner client for the database.
	// The client cannot be used to access the current transaction or batch on
	// this connection. Executing a transaction or batch using the client that is
	// returned, does not affect this connection.
	// Note that multiple connections share the same Spanner client. Calling
	// this function on different connections to the same database, can
	// return the same Spanner client.
	UnderlyingClient() (client *spanner.Client, err error)

	// resetTransactionForRetry resets the current transaction after it has
	// been aborted by Spanner. Calling this function on a transaction that
	// has not been aborted is not supported and will cause an error to be
	// returned.
	resetTransactionForRetry(ctx context.Context, errDuringCommit bool) error

	// withTempTransactionOptions sets the TransactionOptions that should be used
	// for the next read/write transaction. This method should only be called
	// directly before starting a new read/write transaction.
	withTempTransactionOptions(options *ReadWriteTransactionOptions)

	// withTempReadOnlyTransactionOptions sets the options that should be used
	// for the next read-only transaction. This method should only be called
	// directly before starting a new read-only transaction.
	withTempReadOnlyTransactionOptions(options *ReadOnlyTransactionOptions)

	// withTempBatchReadOnlyTransactionOptions sets the options that should be used
	// for the next batch read-only transaction. This method should only be called
	// directly before starting a new batch read-only transaction.
	withTempBatchReadOnlyTransactionOptions(options *BatchReadOnlyTransactionOptions)
}

var _ SpannerConn = &conn{}

type conn struct {
	parser        *statementParser
	connector     *connector
	closed        bool
	client        *spanner.Client
	adminClient   *adminapi.DatabaseAdminClient
	connId        string
	logger        *slog.Logger
	tx            contextTransaction
	prevTx        contextTransaction
	resetForRetry bool
	commitTs      *time.Time
	database      string
	retryAborts   bool

	execSingleQuery              func(ctx context.Context, c *spanner.Client, statement spanner.Statement, bound spanner.TimestampBound, options ExecOptions) *spanner.RowIterator
	execSingleQueryTransactional func(ctx context.Context, c *spanner.Client, statement spanner.Statement, options ExecOptions) (rowIterator, time.Time, error)
	execSingleDMLTransactional   func(ctx context.Context, c *spanner.Client, statement spanner.Statement, statementInfo *statementInfo, options ExecOptions) (*result, time.Time, error)
	execSingleDMLPartitioned     func(ctx context.Context, c *spanner.Client, statement spanner.Statement, options ExecOptions) (int64, error)

	// batch is the currently active DDL or DML batch on this connection.
	batch *batch
	// autoBatchDml determines whether DML statements should automatically
	// be batched and sent to Spanner when a non-DML statement is encountered.
	autoBatchDml bool
	// autoBatchDmlUpdateCount determines the update count that is returned for
	// DML statements that are executed when autoBatchDml is true.
	autoBatchDmlUpdateCount int64
	// autoBatchDmlUpdateCountVerification enables/disables the verification
	// that the update count that was returned for automatically batched DML
	// statements was correct.
	autoBatchDmlUpdateCountVerification bool

	// autocommitDMLMode determines the type of DML to use when a single DML
	// statement is executed on a connection. The default is Transactional, but
	// it can also be set to PartitionedNonAtomic to execute the statement as
	// Partitioned DML.
	autocommitDMLMode AutocommitDMLMode
	// readOnlyStaleness is used for queries in autocommit mode and for read-only transactions.
	readOnlyStaleness spanner.TimestampBound
	// isolationLevel determines the default isolation level that is used for read/write
	// transactions on this connection. This default is ignored if the BeginTx function is
	// called with an isolation level other than sql.LevelDefault.
	isolationLevel sql.IsolationLevel

	// execOptions are applied to the next statement or transaction that is executed
	// on this connection. It can also be set by passing it in as an argument to
	// ExecContext or QueryContext.
	execOptions ExecOptions

	tempTransactionOptions *ReadWriteTransactionOptions
	// tempReadOnlyTransactionOptions are temporarily set right before a read-only
	// transaction is started on a Spanner connection.
	tempReadOnlyTransactionOptions *ReadOnlyTransactionOptions
	// tempBatchReadOnlyTransactionOptions are temporarily set right before a
	// batch read-only transaction is started on a Spanner connection.
	tempBatchReadOnlyTransactionOptions *BatchReadOnlyTransactionOptions
}

func (c *conn) UnderlyingClient() (*spanner.Client, error) {
	return c.client, nil
}

func (c *conn) CommitTimestamp() (time.Time, error) {
	if c.commitTs == nil {
		return time.Time{}, spanner.ToSpannerError(status.Error(codes.FailedPrecondition, "this connection has not executed a read/write transaction that committed successfully"))
	}
	return *c.commitTs, nil
}

func (c *conn) RetryAbortsInternally() bool {
	return c.retryAborts
}

func (c *conn) SetRetryAbortsInternally(retry bool) error {
	_, err := c.setRetryAbortsInternally(retry)
	return err
}

func (c *conn) setRetryAbortsInternally(retry bool) (driver.Result, error) {
	if c.inTransaction() {
		return nil, spanner.ToSpannerError(status.Error(codes.FailedPrecondition, "cannot change retry mode while a transaction is active"))
	}
	c.retryAborts = retry
	return driver.ResultNoRows, nil
}

func (c *conn) AutocommitDMLMode() AutocommitDMLMode {
	return c.autocommitDMLMode
}

func (c *conn) SetAutocommitDMLMode(mode AutocommitDMLMode) error {
	if mode == Unspecified {
		return spanner.ToSpannerError(status.Error(codes.InvalidArgument, "autocommit dml mode cannot be unspecified"))
	}
	_, err := c.setAutocommitDMLMode(mode)
	return err
}

func (c *conn) setAutocommitDMLMode(mode AutocommitDMLMode) (driver.Result, error) {
	c.autocommitDMLMode = mode
	return driver.ResultNoRows, nil
}

func (c *conn) ReadOnlyStaleness() spanner.TimestampBound {
	return c.readOnlyStaleness
}

func (c *conn) SetReadOnlyStaleness(staleness spanner.TimestampBound) error {
	_, err := c.setReadOnlyStaleness(staleness)
	return err
}

func (c *conn) setReadOnlyStaleness(staleness spanner.TimestampBound) (driver.Result, error) {
	c.readOnlyStaleness = staleness
	return driver.ResultNoRows, nil
}

func (c *conn) IsolationLevel() sql.IsolationLevel {
	return c.isolationLevel
}

func (c *conn) SetIsolationLevel(level sql.IsolationLevel) error {
	c.isolationLevel = level
	return nil
}

func (c *conn) MaxCommitDelay() time.Duration {
	return *c.execOptions.TransactionOptions.CommitOptions.MaxCommitDelay
}

func (c *conn) SetMaxCommitDelay(delay time.Duration) error {
	_, err := c.setMaxCommitDelay(delay)
	return err
}

func (c *conn) setMaxCommitDelay(delay time.Duration) (driver.Result, error) {
	c.execOptions.TransactionOptions.CommitOptions.MaxCommitDelay = &delay
	return driver.ResultNoRows, nil
}

func (c *conn) ExcludeTxnFromChangeStreams() bool {
	return c.execOptions.TransactionOptions.ExcludeTxnFromChangeStreams
}

func (c *conn) SetExcludeTxnFromChangeStreams(excludeTxnFromChangeStreams bool) error {
	_, err := c.setExcludeTxnFromChangeStreams(excludeTxnFromChangeStreams)
	return err
}

func (c *conn) setExcludeTxnFromChangeStreams(excludeTxnFromChangeStreams bool) (driver.Result, error) {
	if c.inTransaction() {
		return nil, spanner.ToSpannerError(status.Error(codes.FailedPrecondition, "cannot set ExcludeTxnFromChangeStreams while a transaction is active"))
	}
	c.execOptions.TransactionOptions.ExcludeTxnFromChangeStreams = excludeTxnFromChangeStreams
	return driver.ResultNoRows, nil
}

func (c *conn) DecodeToNativeArrays() bool {
	return c.execOptions.DecodeToNativeArrays
}

func (c *conn) SetDecodeToNativeArrays(decodeToNativeArrays bool) error {
	c.execOptions.DecodeToNativeArrays = decodeToNativeArrays
	return nil
}

func (c *conn) TransactionTag() string {
	return c.execOptions.TransactionOptions.TransactionTag
}

func (c *conn) SetTransactionTag(transactionTag string) error {
	_, err := c.setTransactionTag(transactionTag)
	return err
}

func (c *conn) setTransactionTag(transactionTag string) (driver.Result, error) {
	if c.inTransaction() {
		return nil, spanner.ToSpannerError(status.Error(codes.FailedPrecondition, "cannot set transaction tag while a transaction is active"))
	}
	c.execOptions.TransactionOptions.TransactionTag = transactionTag
	return driver.ResultNoRows, nil
}

func (c *conn) StatementTag() string {
	return c.execOptions.QueryOptions.RequestTag
}

func (c *conn) SetStatementTag(statementTag string) error {
	_, err := c.setStatementTag(statementTag)
	return err
}

func (c *conn) setStatementTag(statementTag string) (driver.Result, error) {
	c.execOptions.QueryOptions.RequestTag = statementTag
	return driver.ResultNoRows, nil
}

func (c *conn) AutoBatchDml() bool {
	return c.autoBatchDml
}

func (c *conn) SetAutoBatchDml(autoBatch bool) error {
	c.autoBatchDml = autoBatch
	return nil
}

func (c *conn) AutoBatchDmlUpdateCount() int64 {
	return c.autoBatchDmlUpdateCount
}

func (c *conn) SetAutoBatchDmlUpdateCount(updateCount int64) error {
	c.autoBatchDmlUpdateCount = updateCount
	return nil
}

func (c *conn) AutoBatchDmlUpdateCountVerification() bool {
	return c.autoBatchDmlUpdateCountVerification
}

func (c *conn) SetAutoBatchDmlUpdateCountVerification(verify bool) error {
	c.autoBatchDmlUpdateCountVerification = verify
	return nil
}

func (c *conn) StartBatchDDL() error {
	_, err := c.startBatchDDL()
	return err
}

func (c *conn) StartBatchDML() error {
	_, err := c.startBatchDML( /* automatic = */ false)
	return err
}

func (c *conn) RunBatch(ctx context.Context) error {
	_, err := c.runBatch(ctx)
	return err
}

func (c *conn) AbortBatch() error {
	_, err := c.abortBatch()
	return err
}

func (c *conn) InDDLBatch() bool {
	return c.batch != nil && c.batch.tp == ddl
}

func (c *conn) InDMLBatch() bool {
	return (c.batch != nil && c.batch.tp == dml) || (c.inReadWriteTransaction() && c.tx.(*readWriteTransaction).batch != nil)
}

func (c *conn) GetBatchedStatements() []spanner.Statement {
	if c.batch == nil || c.batch.statements == nil {
		return []spanner.Statement{}
	}
	return slices.Clone(c.batch.statements)
}

func (c *conn) inBatch() bool {
	return c.InDDLBatch() || c.InDMLBatch()
}

func (c *conn) startBatchDDL() (driver.Result, error) {
	if c.batch != nil {
		return nil, spanner.ToSpannerError(status.Errorf(codes.FailedPrecondition, "This connection already has an active batch."))
	}
	if c.inTransaction() {
		return nil, spanner.ToSpannerError(status.Errorf(codes.FailedPrecondition, "This connection has an active transaction. DDL batches in transactions are not supported."))
	}
	c.logger.Debug("started ddl batch")
	c.batch = &batch{tp: ddl}
	return driver.ResultNoRows, nil
}

func (c *conn) startBatchDML(automatic bool) (driver.Result, error) {
	execOptions := c.options()

	if c.inTransaction() {
		return c.tx.StartBatchDML(execOptions.QueryOptions, automatic)
	}

	if c.batch != nil {
		return nil, spanner.ToSpannerError(status.Errorf(codes.FailedPrecondition, "This connection already has an active batch."))
	}
	if c.inReadOnlyTransaction() {
		return nil, spanner.ToSpannerError(status.Errorf(codes.FailedPrecondition, "This connection has an active read-only transaction. Read-only transactions cannot execute DML batches."))
	}
	c.logger.Debug("starting dml batch outside transaction")
	c.batch = &batch{tp: dml, options: execOptions}
	return driver.ResultNoRows, nil
}

func (c *conn) runBatch(ctx context.Context) (driver.Result, error) {
	if c.inTransaction() {
		return c.tx.RunBatch(ctx)
	}

	if c.batch == nil {
		return nil, spanner.ToSpannerError(status.Errorf(codes.FailedPrecondition, "This connection does not have an active batch"))
	}
	switch c.batch.tp {
	case ddl:
		return c.runDDLBatch(ctx)
	case dml:
		return c.runDMLBatch(ctx)
	default:
		return nil, spanner.ToSpannerError(status.Errorf(codes.InvalidArgument, "Unknown batch type: %d", c.batch.tp))
	}
}

func (c *conn) runDDLBatch(ctx context.Context) (driver.Result, error) {
	statements := c.batch.statements
	c.batch = nil
	return c.execDDL(ctx, statements...)
}

func (c *conn) runDMLBatch(ctx context.Context) (SpannerResult, error) {
	if c.inTransaction() {
		return c.tx.RunDmlBatch(ctx)
	}

	statements := c.batch.statements
	options := c.batch.options
	options.QueryOptions.LastStatement = true
	c.batch = nil
	return c.execBatchDML(ctx, statements, options)
}

func (c *conn) abortBatch() (driver.Result, error) {
	if c.inTransaction() {
		return c.tx.AbortBatch()
	}

	c.batch = nil
	return driver.ResultNoRows, nil
}

func (c *conn) execDDL(ctx context.Context, statements ...spanner.Statement) (driver.Result, error) {
	if c.batch != nil && c.batch.tp == dml {
		return nil, spanner.ToSpannerError(status.Error(codes.FailedPrecondition, "This connection has an active DML batch"))
	}
	if c.batch != nil && c.batch.tp == ddl {
		c.batch.statements = append(c.batch.statements, statements...)
		return driver.ResultNoRows, nil
	}

	if len(statements) > 0 {
		ddlStatements := make([]string, len(statements))
		for i, s := range statements {
			ddlStatements[i] = s.SQL
		}
		op, err := c.adminClient.UpdateDatabaseDdl(ctx, &adminpb.UpdateDatabaseDdlRequest{
			Database:   c.database,
			Statements: ddlStatements,
		})
		if err != nil {
			return nil, err
		}
		if err := op.Wait(ctx); err != nil {
			return nil, err
		}
	}
	return driver.ResultNoRows, nil
}

func (c *conn) execBatchDML(ctx context.Context, statements []spanner.Statement, options ExecOptions) (SpannerResult, error) {
	if len(statements) == 0 {
		return &result{}, nil
	}

	var affected []int64
	var err error
	if c.inTransaction() {
		tx, ok := c.tx.(*readWriteTransaction)
		if !ok {
			return nil, status.Errorf(codes.FailedPrecondition, "connection is in a transaction that is not a read/write transaction")
		}
		affected, err = tx.rwTx.BatchUpdateWithOptions(ctx, statements, options.QueryOptions)
	} else {
		_, err = c.client.ReadWriteTransactionWithOptions(ctx, func(ctx context.Context, transaction *spanner.ReadWriteTransaction) error {
			affected, err = transaction.BatchUpdateWithOptions(ctx, statements, options.QueryOptions)
			return err
		}, options.TransactionOptions)
	}
	return &result{rowsAffected: sum(affected), batchUpdateCounts: affected}, err
}

func sum(affected []int64) int64 {
	sum := int64(0)
	for _, c := range affected {
		sum += c
	}
	return sum
}

func (c *conn) Apply(ctx context.Context, ms []*spanner.Mutation, opts ...spanner.ApplyOption) (commitTimestamp time.Time, err error) {
	if c.inTransaction() {
		return time.Time{}, spanner.ToSpannerError(
			status.Error(
				codes.FailedPrecondition,
				"Apply may not be called while the connection is in a transaction. Use BufferWrite to write mutations in a transaction."))
	}
	return c.client.Apply(ctx, ms, opts...)
}

func (c *conn) BufferWrite(ms []*spanner.Mutation) error {
	if !c.inTransaction() {
		return spanner.ToSpannerError(
			status.Error(
				codes.FailedPrecondition,
				"BufferWrite may not be called while the connection is not in a transaction. Use Apply to write mutations outside a transaction."))
	}
	return c.tx.BufferWrite(ms)
}

// Ping implements the driver.Pinger interface.
// returns ErrBadConn if the connection is no longer valid.
func (c *conn) Ping(ctx context.Context) error {
	if c.closed {
		return driver.ErrBadConn
	}
	rows, err := c.QueryContext(ctx, "SELECT 1", []driver.NamedValue{})
	if err != nil {
		return driver.ErrBadConn
	}
	defer func() { _ = rows.Close() }()
	values := make([]driver.Value, 1)
	if err := rows.Next(values); err != nil {
		return driver.ErrBadConn
	}
	if values[0] != int64(1) {
		return driver.ErrBadConn
	}
	return nil
}

// ResetSession implements the driver.SessionResetter interface.
// returns ErrBadConn if the connection is no longer valid.
func (c *conn) ResetSession(_ context.Context) error {
	if c.closed {
		return driver.ErrBadConn
	}
	if c.inTransaction() {
		if err := c.tx.Rollback(); err != nil {
			return driver.ErrBadConn
		}
	}
	c.commitTs = nil
	c.batch = nil
	c.autoBatchDml = c.connector.connectorConfig.AutoBatchDml
	c.autoBatchDmlUpdateCount = c.connector.connectorConfig.AutoBatchDmlUpdateCount
	c.autoBatchDmlUpdateCountVerification = !c.connector.connectorConfig.DisableAutoBatchDmlUpdateCountVerification
	c.retryAborts = c.connector.retryAbortsInternally
	c.isolationLevel = c.connector.connectorConfig.IsolationLevel
	// TODO: Reset the following fields to the connector default
	c.autocommitDMLMode = Transactional
	c.readOnlyStaleness = spanner.TimestampBound{}
	c.execOptions = ExecOptions{
		DecodeToNativeArrays: c.connector.connectorConfig.DecodeToNativeArrays,
	}
	return nil
}

// IsValid implements the driver.Validator interface.
func (c *conn) IsValid() bool {
	return !c.closed
}

func (c *conn) CheckNamedValue(value *driver.NamedValue) error {
	if value == nil {
		return nil
	}

	if execOptions, ok := value.Value.(ExecOptions); ok {
		// TODO: This should use a temp value to prevent ExecOptions for one
		//       statement from becoming 'sticky' for all following statements.
		c.execOptions = execOptions
		return driver.ErrRemoveArgument
	}

	if checkIsValidType(value.Value) {
		return nil
	}

	// Convert directly if the value implements driver.Valuer. Although this
	// is also done by the default converter in the sql driver, that conversion
	// requires the value that is returned by Value() to be one of the base
	// types that is supported by database/sql. By doing this check here first,
	// we also support driver.Valuer types that return a Spanner-supported type,
	// such as for example []string.
	if valuer, ok := value.Value.(driver.Valuer); ok {
		if v, err := callValuerValue(valuer); err == nil {
			if checkIsValidType(v) {
				value.Value = v
				return nil
			}
		}
	}

	// Convert the value using the default sql driver. This uses driver.Valuer,
	// if implemented, and falls back to reflection. If the converted value is
	// a supported spanner type, use it. Otherwise, ignore any errors and
	// continue checking other supported spanner specific types.
	if v, err := driver.DefaultParameterConverter.ConvertValue(value.Value); err == nil {
		if checkIsValidType(v) {
			value.Value = v
			return nil
		}
	}

	// google-cloud-go/spanner knows how to deal with these
	if isStructOrArrayOfStructValue(value.Value) || isAnArrayOfProtoColumn(value.Value) {
		return nil
	}

	return spanner.ToSpannerError(status.Errorf(codes.InvalidArgument, "unsupported value type: %T", value.Value))
}

func (c *conn) Prepare(query string) (driver.Stmt, error) {
	return c.PrepareContext(context.Background(), query)
}

func (c *conn) PrepareContext(_ context.Context, query string) (driver.Stmt, error) {
	execOptions := c.options()
	parsedSQL, args, err := c.parser.parseParameters(query)
	if err != nil {
		return nil, err
	}
	return &stmt{conn: c, query: parsedSQL, numArgs: len(args), execOptions: execOptions}, nil
}

func (c *conn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	// Execute client side statement if it is one.
	clientStmt, err := c.parser.parseClientSideStatement(c, query)
	if err != nil {
		return nil, err
	}
	if clientStmt != nil {
		return clientStmt.QueryContext(ctx, args)
	}

	execOptions := c.options()
	return c.queryContext(ctx, query, execOptions, args)
}

func (c *conn) queryContext(ctx context.Context, query string, execOptions ExecOptions, args []driver.NamedValue) (driver.Rows, error) {
	// Clear the commit timestamp of this connection before we execute the query.
	c.commitTs = nil
	// Check if the execution options contains an instruction to execute
	// a specific partition of a PartitionedQuery.
	if pq := execOptions.PartitionedQueryOptions.ExecutePartition.PartitionedQuery; pq != nil {
		return pq.execute(ctx, execOptions.PartitionedQueryOptions.ExecutePartition.Index)
	}

	stmt, err := prepareSpannerStmt(c.parser, query, args)
	if err != nil {
		return nil, err
	}
	statementType := c.parser.detectStatementType(query)
	// DDL statements are not supported in QueryContext so fail early.
	if statementType.statementType == statementTypeDdl {
		return nil, spanner.ToSpannerError(status.Errorf(codes.FailedPrecondition, "QueryContext does not support DDL statements, use ExecContext instead"))
	}
	var iter rowIterator
	if c.tx == nil {
		if statementType.statementType == statementTypeDml {
			// Use a read/write transaction to execute the statement.
			var commitTs time.Time
			iter, commitTs, err = c.execSingleQueryTransactional(ctx, c.client, stmt, execOptions)
			if err != nil {
				return nil, err
			}
			c.commitTs = &commitTs
		} else if execOptions.PartitionedQueryOptions.PartitionQuery {
			return nil, spanner.ToSpannerError(status.Errorf(codes.FailedPrecondition, "PartitionQuery is only supported in batch read-only transactions"))
		} else if execOptions.PartitionedQueryOptions.AutoPartitionQuery {
			return c.executeAutoPartitionedQuery(ctx, query, args)
		} else {
			// The statement was either detected as being a query, or potentially not recognized at all.
			// In that case, just default to using a single-use read-only transaction and let Spanner
			// return an error if the statement is not suited for that type of transaction.
			iter = &readOnlyRowIterator{c.execSingleQuery(ctx, c.client, stmt, c.readOnlyStaleness, execOptions)}
		}
	} else {
		if execOptions.PartitionedQueryOptions.PartitionQuery {
			return c.tx.partitionQuery(ctx, stmt, execOptions)
		}
		iter, err = c.tx.Query(ctx, stmt, execOptions)
		if err != nil {
			return nil, err
		}
	}
	res := &rows{
		it:                      iter,
		decodeOption:            execOptions.DecodeOption,
		decodeToNativeArrays:    execOptions.DecodeToNativeArrays,
		returnResultSetMetadata: execOptions.ReturnResultSetMetadata,
		returnResultSetStats:    execOptions.ReturnResultSetStats,
	}
	if execOptions.DirectExecuteQuery {
		// This call to res.getColumns() triggers the execution of the statement, as it needs to fetch the metadata.
		res.getColumns()
		if res.dirtyErr != nil && !errors.Is(res.dirtyErr, iterator.Done) {
			_ = res.Close()
			return nil, res.dirtyErr
		}
	}
	return res, nil
}

func (c *conn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	// Execute client side statement if it is one.
	stmt, err := c.parser.parseClientSideStatement(c, query)
	if err != nil {
		return nil, err
	}
	if stmt != nil {
		return stmt.ExecContext(ctx, args)
	}
	execOptions := c.options()
	return c.execContext(ctx, query, execOptions, args)
}

func (c *conn) execContext(ctx context.Context, query string, execOptions ExecOptions, args []driver.NamedValue) (driver.Result, error) {
	// Clear the commit timestamp of this connection before we execute the statement.
	c.commitTs = nil

	statementInfo := c.parser.detectStatementType(query)
	// Use admin API if DDL statement is provided.
	if statementInfo.statementType == statementTypeDdl {
		// Spanner does not support DDL in transactions, and although it is technically possible to execute DDL
		// statements while a transaction is active, we return an error to avoid any confusion whether the DDL
		// statement is executed as part of the active transaction or not.
		if c.inTransaction() {
			return nil, spanner.ToSpannerError(status.Errorf(codes.FailedPrecondition, "cannot execute DDL as part of a transaction"))
		}
		return c.execDDL(ctx, spanner.NewStatement(query))
	}

	ss, err := prepareSpannerStmt(c.parser, query, args)
	if err != nil {
		return nil, err
	}

	// Start an automatic DML batch.
	if c.autoBatchDml && !c.inBatch() && c.inReadWriteTransaction() {
		if _, err := c.startBatchDML( /* automatic = */ true); err != nil {
			return nil, err
		}
	}

	var res *result
	var commitTs time.Time
	if c.tx == nil {
		if c.InDMLBatch() {
			c.batch.statements = append(c.batch.statements, ss)
			res = &result{}
		} else {
			dmlMode := c.autocommitDMLMode
			if execOptions.AutocommitDMLMode != Unspecified {
				dmlMode = execOptions.AutocommitDMLMode
			}
			if dmlMode == Transactional {
				res, commitTs, err = c.execSingleDMLTransactional(ctx, c.client, ss, statementInfo, execOptions)
				if err == nil {
					c.commitTs = &commitTs
				}
			} else if dmlMode == PartitionedNonAtomic {
				var rowsAffected int64
				rowsAffected, err = c.execSingleDMLPartitioned(ctx, c.client, ss, execOptions)
				res = &result{rowsAffected: rowsAffected}
			} else {
				return nil, status.Errorf(codes.FailedPrecondition, "invalid dml mode: %s", dmlMode.String())
			}
		}
	} else {
		res, err = c.tx.ExecContext(ctx, ss, statementInfo, execOptions.QueryOptions)
	}
	if err != nil {
		return nil, err
	}
	return res, nil
}

// options returns and resets the ExecOptions for the next statement.
func (c *conn) options() ExecOptions {
	defer func() {
		c.execOptions.TransactionOptions.TransactionTag = ""
		c.execOptions.QueryOptions.RequestTag = ""
	}()
	return c.execOptions
}

func (c *conn) Close() error {
	return c.connector.decreaseConnCount()
}

func noTransaction() error {
	return status.Errorf(codes.FailedPrecondition, "connection does not have a transaction")
}

func (c *conn) resetTransactionForRetry(ctx context.Context, errDuringCommit bool) error {
	if errDuringCommit {
		if c.prevTx == nil {
			return noTransaction()
		}
		c.tx = c.prevTx
		c.resetForRetry = true
	} else if c.tx == nil {
		return noTransaction()
	}
	return c.tx.resetForRetry(ctx)
}

func (c *conn) withTempTransactionOptions(options *ReadWriteTransactionOptions) {
	c.tempTransactionOptions = options
}

func (c *conn) getTransactionOptions() ReadWriteTransactionOptions {
	if c.tempTransactionOptions != nil {
		defer func() { c.tempTransactionOptions = nil }()
		return *c.tempTransactionOptions
	}
	// Clear the transaction tag that has been set on the connection after returning
	// from this function.
	defer func() {
		c.execOptions.TransactionOptions.TransactionTag = ""
	}()
	txOpts := ReadWriteTransactionOptions{
		TransactionOptions:     c.execOptions.TransactionOptions,
		DisableInternalRetries: !c.retryAborts,
	}
	// Only use the default isolation level from the connection if the ExecOptions
	// did not contain a more specific isolation level.
	if txOpts.TransactionOptions.IsolationLevel == spannerpb.TransactionOptions_ISOLATION_LEVEL_UNSPECIFIED {
		// This should never really return an error, but we check just to be absolutely sure.
		level, err := toProtoIsolationLevel(c.isolationLevel)
		if err == nil {
			txOpts.TransactionOptions.IsolationLevel = level
		}
	}
	if txOpts.TransactionOptions.BeginTransactionOption == spanner.DefaultBeginTransaction {
		txOpts.TransactionOptions.BeginTransactionOption = spanner.InlinedBeginTransaction
	}
	return txOpts
}

func (c *conn) withTempReadOnlyTransactionOptions(options *ReadOnlyTransactionOptions) {
	c.tempReadOnlyTransactionOptions = options
}

func (c *conn) getReadOnlyTransactionOptions() ReadOnlyTransactionOptions {
	if c.tempReadOnlyTransactionOptions != nil {
		defer func() { c.tempReadOnlyTransactionOptions = nil }()
		return *c.tempReadOnlyTransactionOptions
	}
	return ReadOnlyTransactionOptions{TimestampBound: c.readOnlyStaleness, BeginTransactionOption: spanner.InlinedBeginTransaction}
}

func (c *conn) withTempBatchReadOnlyTransactionOptions(options *BatchReadOnlyTransactionOptions) {
	c.tempBatchReadOnlyTransactionOptions = options
}

func (c *conn) getBatchReadOnlyTransactionOptions() BatchReadOnlyTransactionOptions {
	if c.tempBatchReadOnlyTransactionOptions != nil {
		defer func() { c.tempBatchReadOnlyTransactionOptions = nil }()
		return *c.tempBatchReadOnlyTransactionOptions
	}
	return BatchReadOnlyTransactionOptions{TimestampBound: c.readOnlyStaleness}
}

func (c *conn) Begin() (driver.Tx, error) {
	return c.BeginTx(context.Background(), driver.TxOptions{})
}

func (c *conn) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	if c.resetForRetry {
		c.resetForRetry = false
		return c.tx, nil
	}
	readOnlyTxOpts := c.getReadOnlyTransactionOptions()
	batchReadOnlyTxOpts := c.getBatchReadOnlyTransactionOptions()
	readWriteTransactionOptions := c.getTransactionOptions()
	if c.inTransaction() {
		return nil, spanner.ToSpannerError(status.Errorf(codes.FailedPrecondition, "already in a transaction"))
	}
	if c.inBatch() {
		return nil, status.Error(codes.FailedPrecondition, "This connection has an active batch. Run or abort the batch before starting a new transaction.")
	}

	// Determine whether internal retries have been disabled using a special
	// value for the transaction isolation level.
	disableRetryAborts := false
	batchReadOnly := false
	sil := opts.Isolation >> 8
	opts.Isolation = opts.Isolation - sil<<8
	if opts.Isolation != driver.IsolationLevel(sql.LevelDefault) {
		level, err := toProtoIsolationLevel(sql.IsolationLevel(opts.Isolation))
		if err != nil {
			return nil, err
		}
		readWriteTransactionOptions.TransactionOptions.IsolationLevel = level
	}
	if sil > 0 {
		switch spannerIsolationLevel(sil) {
		case levelDisableRetryAborts:
			disableRetryAborts = true
		case levelBatchReadOnly:
			batchReadOnly = true
		default:
			// ignore
		}
	}
	if batchReadOnly && !opts.ReadOnly {
		return nil, status.Error(codes.InvalidArgument, "levelBatchReadOnly can only be used for read-only transactions")
	}

	if opts.ReadOnly {
		var logger *slog.Logger
		var ro *spanner.ReadOnlyTransaction
		var bo *spanner.BatchReadOnlyTransaction
		if batchReadOnly {
			logger = c.logger.With("tx", "batchro")
			var err error
			bo, err = c.client.BatchReadOnlyTransaction(ctx, batchReadOnlyTxOpts.TimestampBound)
			if err != nil {
				return nil, err
			}
			ro = &bo.ReadOnlyTransaction
		} else {
			logger = c.logger.With("tx", "ro")
			ro = c.client.ReadOnlyTransaction().WithBeginTransactionOption(readOnlyTxOpts.BeginTransactionOption).WithTimestampBound(readOnlyTxOpts.TimestampBound)
		}
		c.tx = &readOnlyTransaction{
			roTx:   ro,
			boTx:   bo,
			logger: logger,
			close: func() {
				if batchReadOnlyTxOpts.close != nil {
					batchReadOnlyTxOpts.close()
				}
				if readOnlyTxOpts.close != nil {
					readOnlyTxOpts.close()
				}
				c.tx = nil
			},
		}
		return c.tx, nil
	}

	tx, err := spanner.NewReadWriteStmtBasedTransactionWithOptions(ctx, c.client, readWriteTransactionOptions.TransactionOptions)
	if err != nil {
		return nil, err
	}
	logger := c.logger.With("tx", "rw")
	c.tx = &readWriteTransaction{
		ctx:    ctx,
		conn:   c,
		logger: logger,
		rwTx:   tx,
		close: func(commitTs *time.Time, commitErr error) {
			if readWriteTransactionOptions.close != nil {
				readWriteTransactionOptions.close()
			}
			c.prevTx = c.tx
			c.tx = nil
			if commitErr == nil {
				c.commitTs = commitTs
			}
		},
		// Disable internal retries if any of these options have been set.
		retryAborts: !readWriteTransactionOptions.DisableInternalRetries && !disableRetryAborts,
	}
	c.commitTs = nil
	return c.tx, nil
}

func (c *conn) inTransaction() bool {
	return c.tx != nil
}

func (c *conn) inReadOnlyTransaction() bool {
	if c.tx != nil {
		_, ok := c.tx.(*readOnlyTransaction)
		return ok
	}
	return false
}

func (c *conn) inReadWriteTransaction() bool {
	if c.tx != nil {
		_, ok := c.tx.(*readWriteTransaction)
		return ok
	}
	return false
}

func queryInSingleUse(ctx context.Context, c *spanner.Client, statement spanner.Statement, tb spanner.TimestampBound, options ExecOptions) *spanner.RowIterator {
	return c.Single().WithTimestampBound(tb).QueryWithOptions(ctx, statement, options.QueryOptions)
}

func (c *conn) executeAutoPartitionedQuery(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	tx, err := c.BeginTx(ctx, driver.TxOptions{ReadOnly: true, Isolation: withBatchReadOnly(driver.IsolationLevel(sql.LevelDefault))})
	if err != nil {
		return nil, err
	}
	r, err := c.QueryContext(ctx, query, args)
	if err != nil {
		_ = tx.Rollback()
		return nil, err
	}
	if rows, ok := r.(*rows); ok {
		rows.close = func() error {
			return tx.Commit()
		}
	}
	return r, nil
}

func queryInNewRWTransaction(ctx context.Context, c *spanner.Client, statement spanner.Statement, options ExecOptions) (rowIterator, time.Time, error) {
	var result *wrappedRowIterator
	options.QueryOptions.LastStatement = true
	fn := func(ctx context.Context, tx *spanner.ReadWriteTransaction) error {
		it := tx.QueryWithOptions(ctx, statement, options.QueryOptions)
		row, err := it.Next()
		if err == iterator.Done {
			result = &wrappedRowIterator{
				RowIterator: it,
				noRows:      true,
			}
		} else if err != nil {
			it.Stop()
			return err
		} else {
			result = &wrappedRowIterator{
				RowIterator: it,
				firstRow:    row,
			}
		}
		return nil
	}
	resp, err := c.ReadWriteTransactionWithOptions(ctx, fn, options.TransactionOptions)
	if err != nil {
		return nil, time.Time{}, err
	}
	return result, resp.CommitTs, nil
}

var errInvalidDmlForExecContext = spanner.ToSpannerError(status.Error(codes.FailedPrecondition, "Exec and ExecContext can only be used with INSERT statements with a THEN RETURN clause that return exactly one row with one column of type INT64. Use Query or QueryContext for DML statements other than INSERT and/or with THEN RETURN clauses that return other/more data."))

func execInNewRWTransaction(ctx context.Context, c *spanner.Client, statement spanner.Statement, statementInfo *statementInfo, options ExecOptions) (*result, time.Time, error) {
	var res *result
	options.QueryOptions.LastStatement = true
	fn := func(ctx context.Context, tx *spanner.ReadWriteTransaction) error {
		var err error
		res, err = execTransactionalDML(ctx, tx, statement, statementInfo, options.QueryOptions)
		if err != nil {
			return err
		}
		return nil
	}
	resp, err := c.ReadWriteTransactionWithOptions(ctx, fn, options.TransactionOptions)
	if err != nil {
		return &result{}, time.Time{}, err
	}
	return res, resp.CommitTs, nil
}

func execTransactionalDML(ctx context.Context, tx spannerTransaction, statement spanner.Statement, statementInfo *statementInfo, options spanner.QueryOptions) (*result, error) {
	var rowsAffected int64
	var lastInsertId int64
	var hasLastInsertId bool
	it := tx.QueryWithOptions(ctx, statement, options)
	defer it.Stop()
	row, err := it.Next()
	if err != nil && err != iterator.Done {
		return nil, err
	}
	if len(it.Metadata.RowType.Fields) != 0 && !(len(it.Metadata.RowType.Fields) == 1 &&
		it.Metadata.RowType.Fields[0].Type.Code == spannerpb.TypeCode_INT64 &&
		statementInfo.dmlType == dmlTypeInsert) {
		return nil, errInvalidDmlForExecContext
	}
	if err != iterator.Done {
		if err := row.Column(0, &lastInsertId); err != nil {
			return nil, err
		}
		// Verify that the result set only contains one row.
		_, err = it.Next()
		if err == iterator.Done {
			hasLastInsertId = true
		} else {
			// Statement returned more than one row.
			return nil, errInvalidDmlForExecContext
		}
	}
	rowsAffected = it.RowCount
	return &result{rowsAffected: rowsAffected, lastInsertId: lastInsertId, hasLastInsertId: hasLastInsertId}, nil
}

func execAsPartitionedDML(ctx context.Context, c *spanner.Client, statement spanner.Statement, options ExecOptions) (int64, error) {
	queryOptions := options.QueryOptions
	queryOptions.ExcludeTxnFromChangeStreams = options.TransactionOptions.ExcludeTxnFromChangeStreams
	return c.PartitionedUpdateWithOptions(ctx, statement, queryOptions)
}
