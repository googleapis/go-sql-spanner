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
	"sync"
	"time"

	"cloud.google.com/go/spanner"
	adminapi "cloud.google.com/go/spanner/admin/database/apiv1"
	adminpb "cloud.google.com/go/spanner/admin/database/apiv1/databasepb"
	"cloud.google.com/go/spanner/apiv1/spannerpb"
	"github.com/googleapis/go-sql-spanner/connectionstate"
	"github.com/googleapis/go-sql-spanner/parser"
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
	// RunDmlBatch sends all batched DML statements to Spanner. This is a
	// no-op if no statements have been batched or if there is no active DML batch.
	RunDmlBatch(ctx context.Context) (SpannerResult, error)
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

	// ReadLockMode returns the current read lock mode that is used for read/write
	// transactions on this connection.
	ReadLockMode() spannerpb.TransactionOptions_ReadWrite_ReadLockMode
	// SetReadLockMode sets the read lock mode to use for read/write transactions
	// on this connection.
	//
	// The read lock mode option controls the locking behavior for read operations and queries within a
	// read-write transaction. It works in conjunction with the transaction's isolation level.
	//
	// PESSIMISTIC: Read locks are acquired immediately on read. This mode only applies to SERIALIZABLE
	// isolation. This mode prevents concurrent modifications by locking data throughout the transaction.
	// This reduces commit-time aborts due to conflicts but can increase how long transactions wait for
	// locks and the overall contention.
	//
	// OPTIMISTIC: Locks for reads within the transaction are not acquired on read. Instead, the locks
	// are acquired on commit to validate that read/queried data has not changed since the transaction
	// started. If a conflict is detected, the transaction will fail. This mode only applies to SERIALIZABLE
	// isolation. This mode defers locking until commit, which can reduce contention and improve throughput.
	// However, be aware that this increases the risk of transaction aborts if there's significant write
	// competition on the same data.
	//
	// READ_LOCK_MODE_UNSPECIFIED: This is the default if no mode is set. The locking behavior depends on
	// the isolation level:
	//
	// REPEATABLE_READ isolation: Locking semantics default to OPTIMISTIC. However, validation checks at
	// commit are only performed for queries using SELECT FOR UPDATE, statements with LOCK_SCANNED_RANGES
	// hints, and DML statements. Note: It is an error to explicitly set ReadLockMode when the isolation
	// level is REPEATABLE_READ.
	//
	// For all other isolation levels: If the read lock mode is not set, it defaults to PESSIMISTIC locking.
	SetReadLockMode(mode spannerpb.TransactionOptions_ReadWrite_ReadLockMode) error

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
	// CommitResponse returns the commit response of the last implicit or explicit read/write transaction that
	// was executed on the connection, or an error if the connection has not executed a read/write transaction
	// that committed successfully.
	CommitResponse() (commitResponse *spanner.CommitResponse, err error)

	// UnderlyingClient returns the underlying Spanner client for the database.
	// The client cannot be used to access the current transaction or batch on
	// this connection. Executing a transaction or batch using the client that is
	// returned, does not affect this connection.
	// Note that multiple connections share the same Spanner client. Calling
	// this function on different connections to the same database, can
	// return the same Spanner client.
	UnderlyingClient() (client *spanner.Client, err error)

	// DetectStatementType returns the type of SQL statement.
	DetectStatementType(query string) parser.StatementType

	// resetTransactionForRetry resets the current transaction after it has
	// been aborted by Spanner. Calling this function on a transaction that
	// has not been aborted is not supported and will cause an error to be
	// returned.
	resetTransactionForRetry(ctx context.Context, errDuringCommit bool) error

	// withTransactionCloseFunc sets the close function that should be registered
	// on the next transaction on this connection. This method should only be called
	// directly before starting a new transaction.
	withTransactionCloseFunc(close func())

	// setReadWriteTransactionOptions sets the ReadWriteTransactionOptions that should be
	// used for the current read/write transaction. This method should be called right
	// after starting a new read/write transaction.
	setReadWriteTransactionOptions(options *ReadWriteTransactionOptions)

	// setReadOnlyTransactionOptions sets the options that should be used
	// for the current read-only transaction. This method should be called
	// right after starting a new read-only transaction.
	setReadOnlyTransactionOptions(options *ReadOnlyTransactionOptions)

	// setBatchReadOnlyTransactionOptions sets the options that should be used
	// for the current batch read-only transaction. This method should be called
	// right after starting a new batch read-only transaction.
	setBatchReadOnlyTransactionOptions(options *BatchReadOnlyTransactionOptions)
}

var _ SpannerConn = &conn{}

type conn struct {
	parser        *parser.StatementParser
	connector     *connector
	closed        bool
	client        *spanner.Client
	adminClient   *adminapi.DatabaseAdminClient
	connId        string
	logger        *slog.Logger
	tx            *delegatingTransaction
	prevTx        *delegatingTransaction
	resetForRetry bool
	database      string

	execSingleQuery              func(ctx context.Context, c *spanner.Client, statement spanner.Statement, statementInfo *parser.StatementInfo, bound spanner.TimestampBound, options *ExecOptions) *spanner.RowIterator
	execSingleQueryTransactional func(ctx context.Context, c *spanner.Client, statement spanner.Statement, statementInfo *parser.StatementInfo, options *ExecOptions) (rowIterator, *spanner.CommitResponse, error)
	execSingleDMLTransactional   func(ctx context.Context, c *spanner.Client, statement spanner.Statement, statementInfo *parser.StatementInfo, options *ExecOptions) (*result, *spanner.CommitResponse, error)
	execSingleDMLPartitioned     func(ctx context.Context, c *spanner.Client, statement spanner.Statement, options *ExecOptions) (int64, error)

	// state contains the current ConnectionState for this connection.
	state *connectionstate.ConnectionState
	// batch is the currently active DDL or DML batch on this connection.
	batch *batch

	// tempExecOptions can be set by passing it in as an argument to ExecContext or QueryContext
	// and are applied only to that statement.
	tempExecOptions *ExecOptions
	// tempTransactionCloseFunc is set right before a transaction is started, and is set as the
	// close function for that transaction.
	tempTransactionCloseFunc func()
}

func (c *conn) UnderlyingClient() (*spanner.Client, error) {
	return c.client, nil
}

func (c *conn) DetectStatementType(query string) parser.StatementType {
	info := c.parser.DetectStatementType(query)
	return info.StatementType
}

func (c *conn) CommitTimestamp() (time.Time, error) {
	ts := propertyCommitTimestamp.GetValueOrDefault(c.state)
	if ts == nil {
		return time.Time{}, spanner.ToSpannerError(status.Error(codes.FailedPrecondition, "this connection has not executed a read/write transaction that committed successfully"))
	}
	return *ts, nil
}

func (c *conn) CommitResponse() (commitResponse *spanner.CommitResponse, err error) {
	resp := propertyCommitResponse.GetValueOrDefault(c.state)
	if resp == nil {
		return nil, spanner.ToSpannerError(status.Error(codes.FailedPrecondition, "this connection has not executed a read/write transaction that committed successfully"))
	}
	return resp, nil
}

func (c *conn) clearCommitResponse() {
	_ = propertyCommitResponse.SetValue(c.state, nil, connectionstate.ContextUser)
	_ = propertyCommitTimestamp.SetValue(c.state, nil, connectionstate.ContextUser)
}

func (c *conn) setCommitResponse(commitResponse *spanner.CommitResponse) {
	if commitResponse == nil {
		c.clearCommitResponse()
		return
	}
	_ = propertyCommitResponse.SetValue(c.state, commitResponse, connectionstate.ContextUser)
	_ = propertyCommitTimestamp.SetValue(c.state, &commitResponse.CommitTs, connectionstate.ContextUser)
}

func (c *conn) showConnectionVariable(identifier parser.Identifier) (any, bool, error) {
	extension, name, err := toExtensionAndName(identifier)
	if err != nil {
		return nil, false, err
	}
	return c.state.GetValue(extension, name)
}

func (c *conn) setConnectionVariable(identifier parser.Identifier, value string, local bool, transaction bool) error {
	if transaction && !local {
		// When transaction == true, then local must also be true.
		// We should never hit this condition, as this is an indication of a bug in the driver code.
		return status.Errorf(codes.FailedPrecondition, "transaction properties must be set as a local value")
	}
	extension, name, err := toExtensionAndName(identifier)
	if err != nil {
		return err
	}
	if local {
		return c.state.SetLocalValue(extension, name, value, transaction)
	}
	return c.state.SetValue(extension, name, value, connectionstate.ContextUser)
}

func toExtensionAndName(identifier parser.Identifier) (string, string, error) {
	var extension string
	var name string
	if len(identifier.Parts) == 1 {
		extension = ""
		name = identifier.Parts[0]
	} else if len(identifier.Parts) == 2 {
		extension = identifier.Parts[0]
		name = identifier.Parts[1]
	} else {
		return "", "", status.Errorf(codes.InvalidArgument, "invalid variable name: %s", identifier)
	}
	return extension, name, nil
}

func (c *conn) RetryAbortsInternally() bool {
	return propertyRetryAbortsInternally.GetValueOrDefault(c.state)
}

func (c *conn) SetRetryAbortsInternally(retry bool) error {
	_, err := c.setRetryAbortsInternally(retry)
	return err
}

func (c *conn) setRetryAbortsInternally(retry bool) (driver.Result, error) {
	if err := propertyRetryAbortsInternally.SetValue(c.state, retry, connectionstate.ContextUser); err != nil {
		return nil, err
	}
	return driver.ResultNoRows, nil
}

func (c *conn) AutocommitDMLMode() AutocommitDMLMode {
	return propertyAutocommitDmlMode.GetValueOrDefault(c.state)
}

func (c *conn) SetAutocommitDMLMode(mode AutocommitDMLMode) error {
	if mode == Unspecified {
		return spanner.ToSpannerError(status.Error(codes.InvalidArgument, "autocommit dml mode cannot be unspecified"))
	}
	_, err := c.setAutocommitDMLMode(mode)
	return err
}

func (c *conn) setAutocommitDMLMode(mode AutocommitDMLMode) (driver.Result, error) {
	if err := propertyAutocommitDmlMode.SetValue(c.state, mode, connectionstate.ContextUser); err != nil {
		return nil, err
	}
	return driver.ResultNoRows, nil
}

func (c *conn) ReadOnlyStaleness() spanner.TimestampBound {
	return propertyReadOnlyStaleness.GetValueOrDefault(c.state)
}

func (c *conn) SetReadOnlyStaleness(staleness spanner.TimestampBound) error {
	_, err := c.setReadOnlyStaleness(staleness)
	return err
}

func (c *conn) setReadOnlyStaleness(staleness spanner.TimestampBound) (driver.Result, error) {
	if err := propertyReadOnlyStaleness.SetValue(c.state, staleness, connectionstate.ContextUser); err != nil {
		return nil, err
	}
	return driver.ResultNoRows, nil
}

func (c *conn) IsolationLevel() sql.IsolationLevel {
	return propertyIsolationLevel.GetValueOrDefault(c.state)
}

func (c *conn) SetIsolationLevel(level sql.IsolationLevel) error {
	return propertyIsolationLevel.SetValue(c.state, level, connectionstate.ContextUser)
}

func (c *conn) ReadLockMode() spannerpb.TransactionOptions_ReadWrite_ReadLockMode {
	return propertyReadLockMode.GetValueOrDefault(c.state)
}

func (c *conn) SetReadLockMode(mode spannerpb.TransactionOptions_ReadWrite_ReadLockMode) error {
	return propertyReadLockMode.SetValue(c.state, mode, connectionstate.ContextUser)
}

func (c *conn) MaxCommitDelay() time.Duration {
	return propertyMaxCommitDelay.GetValueOrDefault(c.state)
}

func (c *conn) maxCommitDelayPointer() *time.Duration {
	val := propertyMaxCommitDelay.GetConnectionPropertyValue(c.state)
	if val == nil || !val.HasValue() {
		return nil
	}
	maxCommitDelay, _ := val.GetValue()
	duration := maxCommitDelay.(time.Duration)
	return &duration
}

func (c *conn) SetMaxCommitDelay(delay time.Duration) error {
	return propertyMaxCommitDelay.SetValue(c.state, delay, connectionstate.ContextUser)
}

func (c *conn) ExcludeTxnFromChangeStreams() bool {
	return propertyExcludeTxnFromChangeStreams.GetValueOrDefault(c.state)
}

func (c *conn) SetExcludeTxnFromChangeStreams(excludeTxnFromChangeStreams bool) error {
	return propertyExcludeTxnFromChangeStreams.SetValue(c.state, excludeTxnFromChangeStreams, connectionstate.ContextUser)
}

func (c *conn) DecodeToNativeArrays() bool {
	return propertyDecodeToNativeArrays.GetValueOrDefault(c.state)
}

func (c *conn) SetDecodeToNativeArrays(decodeToNativeArrays bool) error {
	return propertyDecodeToNativeArrays.SetValue(c.state, decodeToNativeArrays, connectionstate.ContextUser)
}

func (c *conn) TransactionTag() string {
	return propertyTransactionTag.GetValueOrDefault(c.state)
}

func (c *conn) SetTransactionTag(transactionTag string) error {
	return propertyTransactionTag.SetValue(c.state, transactionTag, connectionstate.ContextUser)
}

func (c *conn) StatementTag() string {
	return propertyStatementTag.GetValueOrDefault(c.state)
}

func (c *conn) SetStatementTag(statementTag string) error {
	return propertyStatementTag.SetValue(c.state, statementTag, connectionstate.ContextUser)
}

func (c *conn) AutoBatchDml() bool {
	return propertyAutoBatchDml.GetValueOrDefault(c.state)
}

func (c *conn) SetAutoBatchDml(autoBatch bool) error {
	return propertyAutoBatchDml.SetValue(c.state, autoBatch, connectionstate.ContextUser)
}

func (c *conn) AutoBatchDmlUpdateCount() int64 {
	return propertyAutoBatchDmlUpdateCount.GetValueOrDefault(c.state)
}

func (c *conn) SetAutoBatchDmlUpdateCount(updateCount int64) error {
	return propertyAutoBatchDmlUpdateCount.SetValue(c.state, updateCount, connectionstate.ContextUser)
}

func (c *conn) AutoBatchDmlUpdateCountVerification() bool {
	return propertyAutoBatchDmlUpdateCountVerification.GetValueOrDefault(c.state)
}

func (c *conn) SetAutoBatchDmlUpdateCountVerification(verify bool) error {
	return propertyAutoBatchDmlUpdateCountVerification.SetValue(c.state, verify, connectionstate.ContextUser)
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

func (c *conn) RunDmlBatch(ctx context.Context) (SpannerResult, error) {
	res, err := c.runBatch(ctx)
	if err != nil {
		return nil, err
	}
	spannerRes, ok := res.(SpannerResult)
	if !ok {
		return nil, spanner.ToSpannerError(status.Errorf(codes.FailedPrecondition, "not a DML batch"))
	}
	return spannerRes, nil
}

func (c *conn) AbortBatch() error {
	_, err := c.abortBatch()
	return err
}

func (c *conn) InDDLBatch() bool {
	return c.batch != nil && c.batch.tp == parser.BatchTypeDdl
}

func (c *conn) InDMLBatch() bool {
	return (c.batch != nil && c.batch.tp == parser.BatchTypeDml) || (c.inTransaction() && c.tx.IsInBatch())
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
	c.batch = &batch{tp: parser.BatchTypeDdl}
	return driver.ResultNoRows, nil
}

func (c *conn) startBatchDML(automatic bool) (driver.Result, error) {
	execOptions := c.options( /*reset = */ true)

	if c.inTransaction() {
		return c.tx.StartBatchDML(execOptions.QueryOptions, automatic)
	}

	if c.batch != nil {
		return nil, spanner.ToSpannerError(status.Errorf(codes.FailedPrecondition, "This connection already has an active batch."))
	}
	c.logger.Debug("starting dml batch outside transaction")
	c.batch = &batch{tp: parser.BatchTypeDml, options: execOptions}
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
	case parser.BatchTypeDdl:
		return c.runDDLBatch(ctx)
	case parser.BatchTypeDml:
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
	if c.batch != nil && c.batch.tp == parser.BatchTypeDml {
		return nil, spanner.ToSpannerError(status.Error(codes.FailedPrecondition, "This connection has an active DML batch"))
	}
	if c.batch != nil && c.batch.tp == parser.BatchTypeDdl {
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

func (c *conn) execBatchDML(ctx context.Context, statements []spanner.Statement, options *ExecOptions) (SpannerResult, error) {
	if len(statements) == 0 {
		return &result{}, nil
	}

	var affected []int64
	var err error
	if c.inTransaction() && c.tx.contextTransaction != nil {
		tx, ok := c.tx.contextTransaction.(*readWriteTransaction)
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

// WriteMutations is not part of the public API of the database/sql driver.
// It is exported for internal reasons, and may receive breaking changes without prior notice.
//
// WriteMutations writes mutations using this connection. The mutations are either buffered in the current transaction,
// or written directly to Spanner using a new read/write transaction if the connection does not have a transaction.
//
// The function returns an error if the connection currently has a read-only transaction.
//
// The returned CommitResponse is nil if the connection currently has a transaction, as the mutations will only be
// applied to Spanner when the transaction commits.
func (c *conn) WriteMutations(ctx context.Context, ms []*spanner.Mutation) (*spanner.CommitResponse, error) {
	if c.inTransaction() {
		return nil, c.BufferWrite(ms)
	}
	ts, err := c.Apply(ctx, ms)
	if err != nil {
		return nil, err
	}
	return &spanner.CommitResponse{CommitTs: ts}, nil
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
	c.batch = nil

	_ = c.state.Reset(connectionstate.ContextUser)
	c.tempExecOptions = nil
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
		c.tempExecOptions = &execOptions
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
	execOptions := c.options( /* reset = */ true)
	parsedSQL, args, err := c.parser.ParseParameters(query)
	if err != nil {
		return nil, err
	}
	return &stmt{conn: c, query: parsedSQL, numArgs: len(args), execOptions: execOptions}, nil
}

func (c *conn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	// Execute client side statement if it is one.
	clientStmt, err := c.parser.ParseClientSideStatement(query)
	if err != nil {
		return nil, err
	}
	execOptions := c.options( /* reset = */ clientStmt == nil)
	if clientStmt != nil {
		execStmt, err := createExecutableStatement(clientStmt)
		if err != nil {
			return nil, err
		}
		return execStmt.queryContext(ctx, c, execOptions)
	}

	return c.queryContext(ctx, query, execOptions, args)
}

func (c *conn) queryContext(ctx context.Context, query string, execOptions *ExecOptions, args []driver.NamedValue) (driver.Rows, error) {
	// Clear the commit timestamp of this connection before we execute the query.
	c.clearCommitResponse()
	// Check if the execution options contains an instruction to execute
	// a specific partition of a PartitionedQuery.
	if pq := execOptions.PartitionedQueryOptions.ExecutePartition.PartitionedQuery; pq != nil {
		return pq.execute(ctx, execOptions.PartitionedQueryOptions.ExecutePartition.Index)
	}

	stmt, err := prepareSpannerStmt(c.parser, query, args)
	if err != nil {
		return nil, err
	}
	statementInfo := c.parser.DetectStatementType(query)
	// DDL statements are not supported in QueryContext so use the execContext method for the execution.
	if statementInfo.StatementType == parser.StatementTypeDdl {
		res, err := c.execContext(ctx, query, execOptions, args)
		if err != nil {
			return nil, err
		}
		return createDriverResultRows(res, execOptions), nil
	}
	var iter rowIterator
	if c.tx == nil {
		if statementInfo.StatementType == parser.StatementTypeDml {
			// Use a read/write transaction to execute the statement.
			var commitResponse *spanner.CommitResponse
			iter, commitResponse, err = c.execSingleQueryTransactional(ctx, c.client, stmt, statementInfo, execOptions)
			if err != nil {
				return nil, err
			}
			c.setCommitResponse(commitResponse)
		} else if execOptions.PartitionedQueryOptions.PartitionQuery {
			return nil, spanner.ToSpannerError(status.Errorf(codes.FailedPrecondition, "PartitionQuery is only supported in batch read-only transactions"))
		} else if execOptions.PartitionedQueryOptions.AutoPartitionQuery {
			return c.executeAutoPartitionedQuery(ctx, query, execOptions, args)
		} else {
			// The statement was either detected as being a query, or potentially not recognized at all.
			// In that case, just default to using a single-use read-only transaction and let Spanner
			// return an error if the statement is not suited for that type of transaction.
			iter = &readOnlyRowIterator{c.execSingleQuery(ctx, c.client, stmt, statementInfo, c.ReadOnlyStaleness(), execOptions), func() {}, statementInfo.StatementType}
		}
	} else {
		if execOptions.PartitionedQueryOptions.PartitionQuery {
			return c.tx.partitionQuery(ctx, stmt, execOptions)
		}
		iter, err = c.tx.Query(ctx, stmt, statementInfo.StatementType, execOptions)
		if err != nil {
			return nil, err
		}
	}
	res := createRows(iter, execOptions)
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
	stmt, err := c.parser.ParseClientSideStatement(query)
	if err != nil {
		return nil, err
	}
	execOptions := c.options( /*reset = */ stmt == nil)
	if stmt != nil {
		execStmt, err := createExecutableStatement(stmt)
		if err != nil {
			return nil, err
		}
		return execStmt.execContext(ctx, c, execOptions)
	}
	return c.execContext(ctx, query, execOptions, args)
}

func (c *conn) execContext(ctx context.Context, query string, execOptions *ExecOptions, args []driver.NamedValue) (driver.Result, error) {
	// Clear the commit timestamp of this connection before we execute the statement.
	c.clearCommitResponse()

	statementInfo := c.parser.DetectStatementType(query)
	// Use admin API if DDL statement is provided.
	if statementInfo.StatementType == parser.StatementTypeDdl {
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
	if c.AutoBatchDml() && !c.inBatch() && c.inTransaction() && statementInfo.StatementType == parser.StatementTypeDml {
		if _, err := c.startBatchDML( /* automatic = */ true); err != nil {
			return nil, err
		}
	}

	var res *result
	var commitResponse *spanner.CommitResponse
	if c.tx == nil {
		if c.InDMLBatch() {
			c.batch.statements = append(c.batch.statements, ss)
			res = &result{}
		} else {
			dmlMode := c.AutocommitDMLMode()
			if execOptions.AutocommitDMLMode != Unspecified {
				dmlMode = execOptions.AutocommitDMLMode
			}
			if dmlMode == Transactional {
				res, commitResponse, err = c.execSingleDMLTransactional(ctx, c.client, ss, statementInfo, execOptions)
				if err == nil {
					c.setCommitResponse(commitResponse)
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

// options returns and optionally resets the ExecOptions for the next statement.
func (c *conn) options(reset bool) *ExecOptions {
	if reset {
		defer func() {
			// Only reset the transaction tag if there is no active transaction on the connection.
			if !c.inTransaction() {
				_ = propertyTransactionTag.ResetValue(c.state, connectionstate.ContextUser)
			}
			_ = propertyStatementTag.ResetValue(c.state, connectionstate.ContextUser)
			c.tempExecOptions = nil
		}()
	}
	effectiveOptions := &ExecOptions{
		AutocommitDMLMode:    c.AutocommitDMLMode(),
		DecodeToNativeArrays: c.DecodeToNativeArrays(),
		QueryOptions: spanner.QueryOptions{
			RequestTag: c.StatementTag(),
		},
		TransactionOptions: spanner.TransactionOptions{
			ExcludeTxnFromChangeStreams: c.ExcludeTxnFromChangeStreams(),
			TransactionTag:              c.TransactionTag(),
			IsolationLevel:              toProtoIsolationLevelOrDefault(c.IsolationLevel()),
			ReadLockMode:                c.ReadLockMode(),
			CommitPriority:              propertyCommitPriority.GetValueOrDefault(c.state),
			CommitOptions: spanner.CommitOptions{
				MaxCommitDelay:    c.maxCommitDelayPointer(),
				ReturnCommitStats: propertyReturnCommitStats.GetValueOrDefault(c.state),
			},
		},
		PartitionedQueryOptions: PartitionedQueryOptions{},
	}
	if c.tempExecOptions != nil {
		effectiveOptions.merge(c.tempExecOptions)
	}
	return effectiveOptions
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

func (c *conn) withTransactionCloseFunc(close func()) {
	c.tempTransactionCloseFunc = close
}

func (c *conn) setReadWriteTransactionOptions(options *ReadWriteTransactionOptions) {
	if options == nil {
		return
	}
	if options.DisableInternalRetries {
		_ = propertyRetryAbortsInternally.SetLocalValue(c.state, !options.DisableInternalRetries)
	}
	if options.TransactionOptions.BeginTransactionOption != spanner.DefaultBeginTransaction {
		_ = propertyBeginTransactionOption.SetLocalValue(c.state, options.TransactionOptions.BeginTransactionOption)
	}
	if options.TransactionOptions.CommitOptions.MaxCommitDelay != nil {
		_ = propertyMaxCommitDelay.SetLocalValue(c.state, *options.TransactionOptions.CommitOptions.MaxCommitDelay)
	}
	if options.TransactionOptions.CommitOptions.ReturnCommitStats {
		_ = propertyReturnCommitStats.SetLocalValue(c.state, options.TransactionOptions.CommitOptions.ReturnCommitStats)
	}
	if options.TransactionOptions.TransactionTag != "" {
		_ = propertyTransactionTag.SetLocalValue(c.state, options.TransactionOptions.TransactionTag)
	}
	if options.TransactionOptions.ReadLockMode != spannerpb.TransactionOptions_ReadWrite_READ_LOCK_MODE_UNSPECIFIED {
		_ = propertyReadLockMode.SetLocalValue(c.state, options.TransactionOptions.ReadLockMode)
	}
	if options.TransactionOptions.IsolationLevel != spannerpb.TransactionOptions_ISOLATION_LEVEL_UNSPECIFIED {
		_ = propertyIsolationLevel.SetLocalValue(c.state, toSqlIsolationLevelOrDefault(options.TransactionOptions.IsolationLevel))
	}
	if options.TransactionOptions.ExcludeTxnFromChangeStreams {
		_ = propertyExcludeTxnFromChangeStreams.SetLocalValue(c.state, options.TransactionOptions.ExcludeTxnFromChangeStreams)
	}
	if options.TransactionOptions.CommitPriority != spannerpb.RequestOptions_PRIORITY_UNSPECIFIED {
		_ = propertyCommitPriority.SetLocalValue(c.state, options.TransactionOptions.CommitPriority)
	}
}

func (c *conn) getTransactionOptions(execOptions *ExecOptions) ReadWriteTransactionOptions {
	txOpts := ReadWriteTransactionOptions{
		TransactionOptions:     execOptions.TransactionOptions,
		DisableInternalRetries: !c.RetryAbortsInternally(),
	}
	// Only use the default isolation level from the connection if the ExecOptions
	// did not contain a more specific isolation level.
	if txOpts.TransactionOptions.IsolationLevel == spannerpb.TransactionOptions_ISOLATION_LEVEL_UNSPECIFIED {
		// This should never really return an error, but we check just to be absolutely sure.
		level, err := toProtoIsolationLevel(c.IsolationLevel())
		if err == nil {
			txOpts.TransactionOptions.IsolationLevel = level
		}
	}
	if txOpts.TransactionOptions.BeginTransactionOption == spanner.DefaultBeginTransaction {
		txOpts.TransactionOptions.BeginTransactionOption = c.convertDefaultBeginTransactionOption(propertyBeginTransactionOption.GetValueOrDefault(c.state))
	}
	return txOpts
}

func (c *conn) setReadOnlyTransactionOptions(options *ReadOnlyTransactionOptions) {
	if options == nil {
		return
	}
	if options.BeginTransactionOption != spanner.DefaultBeginTransaction {
		_ = propertyBeginTransactionOption.SetLocalValue(c.state, options.BeginTransactionOption)
	}
	if options.TimestampBound.String() != "(strong)" {
		_ = propertyReadOnlyStaleness.SetLocalValue(c.state, options.TimestampBound)
	}
}

func (c *conn) getReadOnlyTransactionOptions() ReadOnlyTransactionOptions {
	return ReadOnlyTransactionOptions{TimestampBound: c.ReadOnlyStaleness(), BeginTransactionOption: c.convertDefaultBeginTransactionOption(propertyBeginTransactionOption.GetValueOrDefault(c.state))}
}

func (c *conn) setBatchReadOnlyTransactionOptions(options *BatchReadOnlyTransactionOptions) {
	if options == nil {
		return
	}
	if options.TimestampBound.String() != "(strong)" {
		_ = propertyReadOnlyStaleness.SetLocalValue(c.state, options.TimestampBound)
	}
}

func (c *conn) getBatchReadOnlyTransactionOptions() BatchReadOnlyTransactionOptions {
	return BatchReadOnlyTransactionOptions{TimestampBound: c.ReadOnlyStaleness()}
}

// BeginReadOnlyTransaction is not part of the public API of the database/sql driver.
// It is exported for internal reasons, and may receive breaking changes without prior notice.
//
// BeginReadOnlyTransaction starts a new read-only transaction on this connection.
func (c *conn) BeginReadOnlyTransaction(ctx context.Context, options *ReadOnlyTransactionOptions, close func()) (driver.Tx, error) {
	tx, err := c.beginTx(ctx, driver.TxOptions{ReadOnly: true}, close)
	c.setReadOnlyTransactionOptions(options)
	if err != nil {
		return nil, err
	}
	return tx, nil
}

// BeginReadWriteTransaction is not part of the public API of the database/sql driver.
// It is exported for internal reasons, and may receive breaking changes without prior notice.
//
// BeginReadWriteTransaction starts a new read/write transaction on this connection.
func (c *conn) BeginReadWriteTransaction(ctx context.Context, options *ReadWriteTransactionOptions, close func()) (driver.Tx, error) {
	tx, err := c.beginTx(ctx, driver.TxOptions{}, close)
	c.setReadWriteTransactionOptions(options)
	if err != nil {
		return nil, err
	}
	return tx, nil
}

func (c *conn) Begin() (driver.Tx, error) {
	return c.BeginTx(context.Background(), driver.TxOptions{})
}

func (c *conn) BeginTx(ctx context.Context, driverOpts driver.TxOptions) (driver.Tx, error) {
	defer func() {
		c.tempTransactionCloseFunc = nil
	}()
	return c.beginTx(ctx, driverOpts, c.tempTransactionCloseFunc)
}

func (c *conn) beginTx(ctx context.Context, driverOpts driver.TxOptions, closeFunc func()) (driver.Tx, error) {
	if c.resetForRetry {
		c.resetForRetry = false
		return c.tx, nil
	}
	if c.inTransaction() {
		return nil, spanner.ToSpannerError(status.Errorf(codes.FailedPrecondition, "already in a transaction"))
	}
	if c.inBatch() {
		return nil, status.Error(codes.FailedPrecondition, "This connection has an active batch. Run or abort the batch before starting a new transaction.")
	}

	isolationLevelFromTxOpts := spannerpb.TransactionOptions_ISOLATION_LEVEL_UNSPECIFIED
	// Determine whether internal retries have been disabled using a special
	// value for the transaction isolation level.
	disableRetryAborts := false
	batchReadOnly := false
	sil := driverOpts.Isolation >> 8
	driverOpts.Isolation = driverOpts.Isolation - sil<<8
	if driverOpts.Isolation != driver.IsolationLevel(sql.LevelDefault) {
		level, err := toProtoIsolationLevel(sql.IsolationLevel(driverOpts.Isolation))
		if err != nil {
			return nil, err
		}
		isolationLevelFromTxOpts = level
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
	if batchReadOnly && !driverOpts.ReadOnly {
		return nil, status.Error(codes.InvalidArgument, "levelBatchReadOnly can only be used for read-only transactions")
	}
	if closeFunc == nil {
		closeFunc = func() {}
	}
	if err := c.state.Begin(); err != nil {
		return nil, err
	}
	c.clearCommitResponse()

	if isolationLevelFromTxOpts != spannerpb.TransactionOptions_ISOLATION_LEVEL_UNSPECIFIED {
		_ = propertyIsolationLevel.SetLocalValue(c.state, sql.IsolationLevel(driverOpts.Isolation))
	}
	// TODO: Figure out how to distinguish between 'use the default' and 'use read/write'.
	if driverOpts.ReadOnly {
		_ = propertyTransactionReadOnly.SetLocalValue(c.state, true)
	}
	if batchReadOnly {
		_ = propertyTransactionBatchReadOnly.SetLocalValue(c.state, true)
	}
	if disableRetryAborts {
		_ = propertyRetryAbortsInternally.SetLocalValue(c.state, false)
	}

	c.tx = &delegatingTransaction{
		conn: c,
		ctx:  ctx,
		close: func(result txResult) {
			closeFunc()
			if result == txResultCommit {
				_ = c.state.Commit()
			} else {
				_ = c.state.Rollback()
			}
			c.tx = nil
		},
	}
	return c.tx, nil
}

func (c *conn) addTransactionTimeout(ctx context.Context) (context.Context, context.CancelFunc) {
	timeout := propertyTransactionTimeout.GetValueOrDefault(c.state)
	if timeout == time.Duration(0) {
		return ctx, func() {}
	}
	return context.WithTimeout(ctx, timeout)
}

func (c *conn) activateTransaction() (contextTransaction, error) {
	closeFunc := c.tx.close
	if propertyTransactionReadOnly.GetValueOrDefault(c.state) {
		var logger *slog.Logger
		var ro *spanner.ReadOnlyTransaction
		var bo *spanner.BatchReadOnlyTransaction
		if propertyTransactionBatchReadOnly.GetValueOrDefault(c.state) {
			logger = c.logger.With("tx", "batchro")
			var err error
			// BatchReadOnly transactions (currently) do not support inline-begin.
			// This means that the transaction options must be supplied here, and not through a callback.
			bo, err = c.client.BatchReadOnlyTransaction(c.tx.ctx, propertyReadOnlyStaleness.GetValueOrDefault(c.state))
			if err != nil {
				return nil, err
			}
			ro = &bo.ReadOnlyTransaction
		} else {
			logger = c.logger.With("tx", "ro")
			beginTxOpt := c.convertDefaultBeginTransactionOption(propertyBeginTransactionOption.GetValueOrDefault(c.state))
			ro = c.client.ReadOnlyTransaction().WithBeginTransactionOption(beginTxOpt)
		}
		return &readOnlyTransaction{
			roTx:   ro,
			boTx:   bo,
			logger: logger,
			close:  closeFunc,
			timestampBoundCallback: func() spanner.TimestampBound {
				return propertyReadOnlyStaleness.GetValueOrDefault(c.state)
			},
		}, nil
	}

	opts := spanner.TransactionOptions{}
	opts.BeginTransactionOption = c.convertDefaultBeginTransactionOption(propertyBeginTransactionOption.GetValueOrDefault(c.state))

	ctx, cancel := c.addTransactionTimeout(c.tx.ctx)
	tx, err := spanner.NewReadWriteStmtBasedTransactionWithCallbackForOptions(ctx, c.client, opts, func() spanner.TransactionOptions {
		defer func() {
			// Reset the transaction_tag after starting the transaction.
			_ = propertyTransactionTag.ResetValue(c.state, connectionstate.ContextUser)
		}()
		return c.effectiveTransactionOptions(spannerpb.TransactionOptions_ISOLATION_LEVEL_UNSPECIFIED, c.options( /*reset=*/ true))
	})
	if err != nil {
		cancel()
		return nil, err
	}
	logger := c.logger.With("tx", "rw")
	return &readWriteTransaction{
		ctx:    ctx,
		conn:   c,
		logger: logger,
		rwTx:   tx,
		close: func(result txResult, commitResponse *spanner.CommitResponse, commitErr error) {
			c.prevTx = c.tx
			if commitErr == nil {
				c.setCommitResponse(commitResponse)
				closeFunc(result)
			} else {
				closeFunc(txResultRollback)
			}
			cancel()
		},
		retryAborts: sync.OnceValue(func() bool {
			return c.RetryAbortsInternally()
		}),
	}, nil
}

func (c *conn) effectiveTransactionOptions(isolationLevelFromTxOpts spannerpb.TransactionOptions_IsolationLevel, execOptions *ExecOptions) spanner.TransactionOptions {
	readWriteTransactionOptions := c.getTransactionOptions(execOptions)
	res := readWriteTransactionOptions.TransactionOptions
	if isolationLevelFromTxOpts != spannerpb.TransactionOptions_ISOLATION_LEVEL_UNSPECIFIED {
		res.IsolationLevel = isolationLevelFromTxOpts
	}
	return res
}

func (c *conn) convertDefaultBeginTransactionOption(opt spanner.BeginTransactionOption) spanner.BeginTransactionOption {
	if opt == spanner.DefaultBeginTransaction {
		if propertyBeginTransactionOption.GetValueOrDefault(c.state) == spanner.DefaultBeginTransaction {
			return spanner.InlinedBeginTransaction
		}
		return propertyBeginTransactionOption.GetValueOrDefault(c.state)
	}
	return opt
}

func (c *conn) inTransaction() bool {
	return c.tx != nil
}

// Commit is not part of the public API of the database/sql driver.
// It is exported for internal reasons, and may receive breaking changes without prior notice.
//
// Commit commits the current transaction on this connection.
func (c *conn) Commit(ctx context.Context) (*spanner.CommitResponse, error) {
	if !c.inTransaction() {
		return nil, status.Errorf(codes.FailedPrecondition, "this connection does not have a transaction")
	}
	// TODO: Pass in context to the tx.Commit() function.
	if err := c.tx.Commit(); err != nil {
		return nil, err
	}

	// This will return either the commit response or nil, depending on whether the transaction was a
	// read/write transaction or a read-only transaction.
	return propertyCommitResponse.GetValueOrDefault(c.state), nil
}

// Rollback is not part of the public API of the database/sql driver.
// It is exported for internal reasons, and may receive breaking changes without prior notice.
//
// Rollback rollbacks the current transaction on this connection.
func (c *conn) Rollback(ctx context.Context) error {
	if !c.inTransaction() {
		return status.Errorf(codes.FailedPrecondition, "this connection does not have a transaction")
	}
	// TODO: Pass in context to the tx.Rollback() function.
	return c.tx.Rollback()
}

func queryInSingleUse(ctx context.Context, c *spanner.Client, statement spanner.Statement, statementInfo *parser.StatementInfo, tb spanner.TimestampBound, options *ExecOptions) *spanner.RowIterator {
	return c.Single().WithTimestampBound(tb).QueryWithOptions(ctx, statement, options.QueryOptions)
}

func (c *conn) executeAutoPartitionedQuery(ctx context.Context, query string, execOptions *ExecOptions, args []driver.NamedValue) (driver.Rows, error) {
	tx, err := c.BeginTx(ctx, driver.TxOptions{ReadOnly: true, Isolation: withBatchReadOnly(driver.IsolationLevel(sql.LevelDefault))})
	if err != nil {
		return nil, err
	}
	r, err := c.queryContext(ctx, query, execOptions, args)
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

func queryInNewRWTransaction(ctx context.Context, c *spanner.Client, statement spanner.Statement, statementInfo *parser.StatementInfo, options *ExecOptions) (rowIterator, *spanner.CommitResponse, error) {
	var result *wrappedRowIterator
	options.QueryOptions.LastStatement = true
	fn := func(ctx context.Context, tx *spanner.ReadWriteTransaction) error {
		it := tx.QueryWithOptions(ctx, statement, options.QueryOptions)
		row, err := it.Next()
		if err == iterator.Done {
			result = &wrappedRowIterator{
				RowIterator: it,
				stmtType:    statementInfo.StatementType,
				noRows:      true,
			}
		} else if err != nil {
			it.Stop()
			return err
		} else {
			result = &wrappedRowIterator{
				RowIterator: it,
				stmtType:    statementInfo.StatementType,
				firstRow:    row,
			}
		}
		return nil
	}
	resp, err := c.ReadWriteTransactionWithOptions(ctx, fn, options.TransactionOptions)
	if err != nil {
		return nil, nil, err
	}
	return result, &resp, nil
}

var errInvalidDmlForExecContext = spanner.ToSpannerError(status.Error(codes.FailedPrecondition, "Exec and ExecContext can only be used with INSERT statements with a THEN RETURN clause that return exactly one row with one column of type INT64. Use Query or QueryContext for DML statements other than INSERT and/or with THEN RETURN clauses that return other/more data."))

func execInNewRWTransaction(ctx context.Context, c *spanner.Client, statement spanner.Statement, statementInfo *parser.StatementInfo, options *ExecOptions) (*result, *spanner.CommitResponse, error) {
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
		return &result{}, nil, err
	}
	return res, &resp, nil
}

func execTransactionalDML(ctx context.Context, tx spannerTransaction, statement spanner.Statement, statementInfo *parser.StatementInfo, options spanner.QueryOptions) (*result, error) {
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
		statementInfo.DmlType == parser.DmlTypeInsert) {
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

func execAsPartitionedDML(ctx context.Context, c *spanner.Client, statement spanner.Statement, options *ExecOptions) (int64, error) {
	queryOptions := options.QueryOptions
	queryOptions.ExcludeTxnFromChangeStreams = options.TransactionOptions.ExcludeTxnFromChangeStreams
	return c.PartitionedUpdateWithOptions(ctx, statement, queryOptions)
}
