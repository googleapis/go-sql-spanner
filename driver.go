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
	"database/sql"
	"database/sql/driver"
	"fmt"
	"math/big"
	"regexp"
	"strconv"
	"strings"
	"time"

	"cloud.google.com/go/civil"
	"cloud.google.com/go/spanner"
	adminapi "cloud.google.com/go/spanner/admin/database/apiv1"
	"google.golang.org/api/option"
	adminpb "google.golang.org/genproto/googleapis/spanner/admin/database/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const userAgent = "go-sql-spanner/0.1"

// dsnRegExpString describes the valid values for a dsn (connection name) for
// Google Cloud Spanner. The string consists of the following parts:
// 1. (Optional) Host: The host name and port number to connect to.
// 2. Database name: The database name to connect to in the format `projects/my-project/instances/my-instance/databases/my-database`
// 3. (Optional) Parameters: One or more parameters in the format `name=value`. Multiple entries are separated by `;`.
// Example: `localhost:9010/projects/test-project/instances/test-instance/databases/test-database;usePlainText=true`
var dsnRegExp = regexp.MustCompile("((?P<HOSTGROUP>[\\w.-]+(?:\\.[\\w\\.-]+)*[\\w\\-\\._~:/?#\\[\\]@!\\$&'\\(\\)\\*\\+,;=.]+)/)?projects/(?P<PROJECTGROUP>(([a-z]|[-.:]|[0-9])+|(DEFAULT_PROJECT_ID)))(/instances/(?P<INSTANCEGROUP>([a-z]|[-]|[0-9])+)(/databases/(?P<DATABASEGROUP>([a-z]|[-]|[_]|[0-9])+))?)?(([\\?|;])(?P<PARAMSGROUP>.*))?")

var _ driver.DriverContext = &Driver{}

func init() {
	sql.Register("spanner", &Driver{})
}

// Driver represents a Google Cloud Spanner database/sql driver.
type Driver struct {
}

// Open opens a connection to a Google Cloud Spanner database.
// Use fully qualified string:
//
// Example: projects/$PROJECT/instances/$INSTANCE/databases/$DATABASE
func (d *Driver) Open(name string) (driver.Conn, error) {
	c, err := newConnector(d, name)
	if err != nil {
		return nil, err
	}
	return openDriverConn(context.Background(), c)
}

func (d *Driver) OpenConnector(name string) (driver.Connector, error) {
	return newConnector(d, name)
}

type connectorConfig struct {
	host     string
	project  string
	instance string
	database string
	params   map[string]string
}

func extractConnectorConfig(dsn string) (connectorConfig, error) {
	match := dsnRegExp.FindStringSubmatch(dsn)
	matches := make(map[string]string)
	for i, name := range dsnRegExp.SubexpNames() {
		if i != 0 && name != "" {
			matches[name] = match[i]
		}
	}
	paramsString := matches["PARAMSGROUP"]
	params, err := extractConnectorParams(paramsString)
	if err != nil {
		return connectorConfig{}, err
	}

	return connectorConfig{
		host:     matches["HOSTGROUP"],
		project:  matches["PROJECTGROUP"],
		instance: matches["INSTANCEGROUP"],
		database: matches["DATABASEGROUP"],
		params:   params,
	}, nil
}

func extractConnectorParams(paramsString string) (map[string]string, error) {
	params := make(map[string]string)
	if paramsString == "" {
		return params, nil
	}
	keyValuePairs := strings.Split(paramsString, ";")
	for _, keyValueString := range keyValuePairs {
		if keyValueString == "" {
			// Ignore empty parameter entries in the string, for example if
			// the connection string contains a trailing ';'.
			continue
		}
		keyValue := strings.SplitN(keyValueString, "=", 2)
		if keyValue == nil || len(keyValue) != 2 {
			return nil, spanner.ToSpannerError(status.Errorf(codes.InvalidArgument, "invalid connection property: %s", keyValueString))
		}
		params[strings.ToLower(keyValue[0])] = keyValue[1]
	}
	return params, nil
}

type connector struct {
	driver          *Driver
	connectorConfig connectorConfig

	// spannerClientConfig represents the optional advanced configuration to be used
	// by the Google Cloud Spanner client.
	spannerClientConfig spanner.ClientConfig

	// options represent the optional Google Cloud client options
	// to be passed to the underlying client.
	options []option.ClientOption

	// retryAbortsInternally determines whether Aborted errors will automatically be
	// retried internally (when possible), or whether all aborted errors will be
	// propagated to the caller. This option is enabled by default.
	retryAbortsInternally bool
}

func newConnector(d *Driver, dsn string) (*connector, error) {
	connectorConfig, err := extractConnectorConfig(dsn)
	if err != nil {
		return nil, err
	}
	opts := make([]option.ClientOption, 0)
	if connectorConfig.host != "" {
		opts = append(opts, option.WithEndpoint(connectorConfig.host))
	}
	if strval, ok := connectorConfig.params["useplaintext"]; ok {
		if val, err := strconv.ParseBool(strval); err == nil && val {
			opts = append(opts, option.WithGRPCDialOption(grpc.WithInsecure()), option.WithoutAuthentication())
		}
	}
	retryAbortsInternally := true
	if strval, ok := connectorConfig.params["retryabortsinternally"]; ok {
		if val, err := strconv.ParseBool(strval); err == nil && !val {
			retryAbortsInternally = false
		}
	}
	config := spanner.ClientConfig{
		SessionPoolConfig: spanner.DefaultSessionPoolConfig,
	}
	return &connector{
		driver:                d,
		connectorConfig:       connectorConfig,
		spannerClientConfig:   config,
		options:               opts,
		retryAbortsInternally: retryAbortsInternally,
	}, nil
}

func (c *connector) Connect(ctx context.Context) (driver.Conn, error) {
	return openDriverConn(ctx, c)
}

func openDriverConn(ctx context.Context, c *connector) (driver.Conn, error) {
	opts := append(c.options, option.WithUserAgent(userAgent))
	databaseName := fmt.Sprintf(
		"projects/%s/instances/%s/databases/%s",
		c.connectorConfig.project,
		c.connectorConfig.instance,
		c.connectorConfig.database)
	client, err := spanner.NewClientWithConfig(ctx, databaseName, c.spannerClientConfig, opts...)
	if err != nil {
		return nil, err
	}

	adminClient, err := adminapi.NewDatabaseAdminClient(ctx, opts...)
	if err != nil {
		return nil, err
	}
	return &conn{
		client:                     client,
		adminClient:                adminClient,
		database:                   databaseName,
		retryAborts:                c.retryAbortsInternally,
		execSingleQuery:            queryInSingleUse,
		execSingleDmlTransactional: execInNewRWTransaction,
		execSingleDmlPartitioned:   execAsPartitionedDml,
	}, nil
}

func (c *connector) Driver() driver.Driver {
	return &Driver{}
}

// SpannerConn is the public interface for the raw Spanner connection for the
// sql driver. This interface can be used with the db.Conn().Raw() method.
type SpannerConn interface {
	// StartBatchDdl starts a DDL batch on the connection. After calling this
	// method all subsequent DDL statements will be cached locally. Calling
	// RunBatch will send all cached DDL statements to Spanner as one batch.
	// Use DDL batching to speed up the execution of multiple DDL statements.
	// Note that a DDL batch is not atomic. It is possible that some DDL
	// statements are executed successfully and some not.
	// See https://cloud.google.com/spanner/docs/schema-updates#order_of_execution_of_statements_in_batches
	// for more information on how Cloud Spanner handles DDL batches.
	StartBatchDdl() error
	// StartBatchDml starts a DML batch on the connection. After calling this
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
	StartBatchDml() error
	// RunBatch sends all batched DDL or DML statements to Spanner. This is a
	// no-op if no statements have been batched or if there is no active batch.
	RunBatch(ctx context.Context) error
	// AbortBatch aborts the current DDL or DML batch and discards all batched
	// statements.
	AbortBatch() error
	// InDdlBatch returns true if the connection is currently in a DDL batch.
	InDdlBatch() bool
	// InDmlBatch returns true if the connection is currently in a DML batch.
	InDmlBatch() bool

	// RetryAbortsInternally returns true if the connection automatically
	// retries all aborted transactions.
	RetryAbortsInternally() bool
	// SetRetryAbortsInternally enables/disables the automatic retry of aborted
	// transactions. If disabled, any aborted error from a transaction will be
	// propagated to the application.
	SetRetryAbortsInternally(retry bool) error

	// AutocommitDmlMode returns the current mode that is used for DML
	// statements outside a transaction. The default is Transactional.
	AutocommitDmlMode() AutocommitDmlMode
	// SetAutocommitDmlMode sets the mode to use for DML statements that are
	// executed outside transactions. The default is Transactional. Change to
	// PartitionedNonAtomic to use Partitioned DML instead of Transactional DML.
	// See https://cloud.google.com/spanner/docs/dml-partitioned for more
	// information on Partitioned DML.
	SetAutocommitDmlMode(mode AutocommitDmlMode) error
}

type conn struct {
	closed      bool
	client      *spanner.Client
	adminClient *adminapi.DatabaseAdminClient
	tx          contextTransaction
	database    string
	retryAborts bool

	execSingleQuery            func(ctx context.Context, c *spanner.Client, statement spanner.Statement) *spanner.RowIterator
	execSingleDmlTransactional func(ctx context.Context, c *spanner.Client, statement spanner.Statement) (int64, error)
	execSingleDmlPartitioned   func(ctx context.Context, c *spanner.Client, statement spanner.Statement) (int64, error)

	// batch is the currently active DDL or DML batch on this connection.
	batch *batch

	// autocommitDmlMode determines the type of DML to use when a single DML
	// statement is executed on a connection. The default is Transactional, but
	// it can also be set to PartitionedNonAtomic to execute the statement as
	// Partitioned DML.
	autocommitDmlMode AutocommitDmlMode
}

type batchType int

const (
	ddl batchType = iota
	dml
)

type batch struct {
	tp         batchType
	statements []spanner.Statement
}

// AutocommitDmlMode indicates whether a single DML statement should be executed
// in a normal atomic transaction or as a Partitioned DML statement.
// See https://cloud.google.com/spanner/docs/dml-partitioned for more information.
type AutocommitDmlMode int

func (mode AutocommitDmlMode) String() string {
	switch mode {
	case Transactional:
		return "Transactional"
	case PartitionedNonAtomic:
		return "Partitioned_Non_Atomic"
	}
	return ""
}

const (
	Transactional AutocommitDmlMode = iota
	PartitionedNonAtomic
)

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

func (c *conn) AutocommitDmlMode() AutocommitDmlMode {
	return c.autocommitDmlMode
}

func (c *conn) SetAutocommitDmlMode(mode AutocommitDmlMode) error {
	_, err := c.setAutocommitDmlMode(mode)
	return err
}

func (c *conn) setAutocommitDmlMode(mode AutocommitDmlMode) (driver.Result, error) {
	c.autocommitDmlMode = mode
	return driver.ResultNoRows, nil
}

func (c *conn) StartBatchDdl() error {
	_, err := c.startBatchDdl()
	return err
}

func (c *conn) StartBatchDml() error {
	_, err := c.startBatchDml()
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

func (c *conn) InDdlBatch() bool {
	return c.batch != nil && c.batch.tp == ddl
}

func (c *conn) InDmlBatch() bool {
	return (c.batch != nil && c.batch.tp == dml) || (c.inReadWriteTransaction() && c.tx.(*readWriteTransaction).batch != nil)
}

func (c *conn) inBatch() bool {
	return c.InDdlBatch() || c.InDmlBatch()
}

func (c *conn) startBatchDdl() (driver.Result, error) {
	if c.batch != nil {
		return nil, spanner.ToSpannerError(status.Errorf(codes.FailedPrecondition, "This connection already has an active batch."))
	}
	if c.inTransaction() {
		return nil, spanner.ToSpannerError(status.Errorf(codes.FailedPrecondition, "This connection has an active transaction. DDL batches in transactions are not supported."))
	}
	c.batch = &batch{tp: ddl}
	return driver.ResultNoRows, nil
}

func (c *conn) startBatchDml() (driver.Result, error) {
	if c.inTransaction() {
		return c.tx.StartBatchDml()
	}

	if c.batch != nil {
		return nil, spanner.ToSpannerError(status.Errorf(codes.FailedPrecondition, "This connection already has an active batch."))
	}
	if c.inReadOnlyTransaction() {
		return nil, spanner.ToSpannerError(status.Errorf(codes.FailedPrecondition, "This connection has an active read-only transaction. Read-only transactions cannot execute DML batches."))
	}
	c.batch = &batch{tp: dml}
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
		return c.runDdlBatch(ctx)
	case dml:
		return c.runDmlBatch(ctx)
	default:
		return nil, spanner.ToSpannerError(status.Errorf(codes.InvalidArgument, "Unknown batch type: %d", c.batch.tp))
	}
}

func (c *conn) runDdlBatch(ctx context.Context) (driver.Result, error) {
	statements := c.batch.statements
	c.batch = nil
	return c.execDdl(ctx, statements...)
}

func (c *conn) runDmlBatch(ctx context.Context) (driver.Result, error) {
	statements := c.batch.statements
	c.batch = nil
	return c.execBatchDml(ctx, statements)
}

func (c *conn) abortBatch() (driver.Result, error) {
	if c.inTransaction() {
		return c.tx.AbortBatch()
	}

	c.batch = nil
	return driver.ResultNoRows, nil
}

func (c *conn) execDdl(ctx context.Context, statements ...spanner.Statement) (driver.Result, error) {
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

func (c *conn) execBatchDml(ctx context.Context, statements []spanner.Statement) (driver.Result, error) {
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
		affected, err = tx.rwTx.BatchUpdate(ctx, statements)
	} else {
		_, err = c.client.ReadWriteTransaction(ctx, func(ctx context.Context, transaction *spanner.ReadWriteTransaction) error {
			affected, err = transaction.BatchUpdate(ctx, statements)
			return err
		})
	}
	return &result{rowsAffected: sum(affected)}, err
}

func sum(affected []int64) int64 {
	sum := int64(0)
	for _, c := range affected {
		sum += c
	}
	return sum
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
	defer rows.Close()
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
	c.retryAborts = true
	c.autocommitDmlMode = Transactional
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
	switch t := value.Value.(type) {
	default:
		// Default is to fail, unless it is one of the following supported types.
		return spanner.ToSpannerError(status.Errorf(codes.InvalidArgument, "unsupported value type: %v", t))
	case nil:
	case sql.NullInt64:
	case sql.NullTime:
	case sql.NullString:
	case sql.NullFloat64:
	case sql.NullBool:
	case sql.NullInt32:
	case string:
	case spanner.NullString:
	case []string:
	case []spanner.NullString:
	case *string:
	case []*string:
	case []byte:
	case [][]byte:
	case int:
	case []int:
	case int64:
	case []int64:
	case spanner.NullInt64:
	case []spanner.NullInt64:
	case *int64:
	case []*int64:
	case bool:
	case []bool:
	case spanner.NullBool:
	case []spanner.NullBool:
	case *bool:
	case []*bool:
	case float64:
	case []float64:
	case spanner.NullFloat64:
	case []spanner.NullFloat64:
	case *float64:
	case []*float64:
	case big.Rat:
	case []big.Rat:
	case spanner.NullNumeric:
	case []spanner.NullNumeric:
	case *big.Rat:
	case []*big.Rat:
	case time.Time:
	case []time.Time:
	case spanner.NullTime:
	case []spanner.NullTime:
	case *time.Time:
	case []*time.Time:
	case civil.Date:
	case []civil.Date:
	case spanner.NullDate:
	case []spanner.NullDate:
	case *civil.Date:
	case []*civil.Date:
	case spanner.NullJSON:
	case []spanner.NullJSON:
	case spanner.GenericColumnValue:
	}
	return nil
}

func (c *conn) Prepare(query string) (driver.Stmt, error) {
	return c.PrepareContext(context.Background(), query)
}

func (c *conn) PrepareContext(ctx context.Context, query string) (driver.Stmt, error) {
	args, err := parseNamedParameters(query)
	if err != nil {
		return nil, err
	}
	return &stmt{conn: c, query: query, numArgs: len(args)}, nil
}

func (c *conn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	// Execute client side statement if it is one.
	clientStmt, err := parseClientSideStatement(c, query)
	if err != nil {
		return nil, err
	}
	if clientStmt != nil {
		return clientStmt.QueryContext(ctx, args)
	}

	stmt, err := prepareSpannerStmt(query, args)
	if err != nil {
		return nil, err
	}
	var iter rowIterator
	if c.tx == nil {
		iter = &readOnlyRowIterator{c.execSingleQuery(ctx, c.client, stmt)}
	} else {
		iter = c.tx.Query(ctx, stmt)
	}
	return &rows{it: iter}, nil
}

func (c *conn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	// Execute client side statement if it is one.
	stmt, err := parseClientSideStatement(c, query)
	if err != nil {
		return nil, err
	}
	if stmt != nil {
		return stmt.ExecContext(ctx, args)
	}

	// Use admin API if DDL statement is provided.
	isDdl, err := isDdl(query)
	if err != nil {
		return nil, err
	}
	if isDdl {
		// Spanner does not support DDL in transactions, and although it is technically possible to execute DDL
		// statements while a transaction is active, we return an error to avoid any confusion whether the DDL
		// statement is executed as part of the active transaction or not.
		if c.inTransaction() {
			return nil, spanner.ToSpannerError(status.Errorf(codes.FailedPrecondition, "cannot execute DDL as part of a transaction"))
		}
		return c.execDdl(ctx, spanner.NewStatement(query))
	}

	ss, err := prepareSpannerStmt(query, args)
	if err != nil {
		return nil, err
	}

	var rowsAffected int64
	if c.tx == nil {
		if c.InDmlBatch() {
			c.batch.statements = append(c.batch.statements, ss)
		} else {
			if c.autocommitDmlMode == Transactional {
				rowsAffected, err = c.execSingleDmlTransactional(ctx, c.client, ss)
			} else if c.autocommitDmlMode == PartitionedNonAtomic {
				rowsAffected, err = c.execSingleDmlPartitioned(ctx, c.client, ss)
			} else {
				return nil, status.Errorf(codes.FailedPrecondition, "connection in invalid state for DML statements: %s", c.autocommitDmlMode.String())
			}
		}
	} else {
		rowsAffected, err = c.tx.ExecContext(ctx, ss)
	}
	if err != nil {
		return nil, err
	}
	return &result{rowsAffected: rowsAffected}, nil
}

func (c *conn) Close() error {
	c.client.Close()
	return nil
}

func (c *conn) Begin() (driver.Tx, error) {
	return c.BeginTx(context.Background(), driver.TxOptions{})
}

func (c *conn) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	if c.inTransaction() {
		return nil, spanner.ToSpannerError(status.Errorf(codes.FailedPrecondition, "already in a transaction"))
	}
	if c.inBatch() {
		return nil, status.Error(codes.FailedPrecondition, "This connection has an active batch. Run or abort the batch before starting a new transaction.")
	}

	if opts.ReadOnly {
		ro := c.client.ReadOnlyTransaction().WithTimestampBound(spanner.StrongRead())
		c.tx = &readOnlyTransaction{
			roTx: ro,
			close: func() {
				c.tx = nil
			},
		}
		return c.tx, nil
	}

	tx, err := spanner.NewReadWriteStmtBasedTransaction(ctx, c.client)
	if err != nil {
		return nil, err
	}
	c.tx = &readWriteTransaction{
		ctx:    ctx,
		client: c.client,
		rwTx:   tx,
		close: func() {
			c.tx = nil
		},
		retryAborts: c.retryAborts,
	}
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

func queryInSingleUse(ctx context.Context, c *spanner.Client, statement spanner.Statement) *spanner.RowIterator {
	return c.Single().Query(ctx, statement)
}

func execInNewRWTransaction(ctx context.Context, c *spanner.Client, statement spanner.Statement) (int64, error) {
	var rowsAffected int64
	fn := func(ctx context.Context, tx *spanner.ReadWriteTransaction) error {
		count, err := tx.Update(ctx, statement)
		rowsAffected = count
		return err
	}
	_, err := c.ReadWriteTransaction(ctx, fn)
	if err != nil {
		return 0, err
	}
	return rowsAffected, nil
}

func execAsPartitionedDml(ctx context.Context, c *spanner.Client, statement spanner.Statement) (int64, error) {
	return c.PartitionedUpdate(ctx, statement)
}
