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
	"io"
	"log/slog"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"cloud.google.com/go/spanner"
	adminapi "cloud.google.com/go/spanner/admin/database/apiv1"
	"cloud.google.com/go/spanner/apiv1/spannerpb"
	"github.com/google/uuid"
	"github.com/googleapis/gax-go/v2"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

const userAgent = "go-sql-spanner/1.9.0" // x-release-please-version

// LevelNotice is the default logging level that the Spanner database/sql driver
// uses for informational logs. This level is deliberately chosen to be one level
// lower than the default log level, which is slog.LevelInfo. This prevents the
// driver from adding noise to any default logger that has been set for the
// application.
const LevelNotice = slog.LevelInfo - 1

// Logger that discards everything and skips (almost) all logs.
var noopLogger = slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelError + 1}))

// dsnRegExpString describes the valid values for a dsn (connection name) for
// Google Cloud Spanner. The string consists of the following parts:
//  1. (Optional) Host: The host name and port number to connect to.
//  2. Database name: The database name to connect to in the format `projects/my-project/instances/my-instance/databases/my-database`
//  3. (Optional) Parameters: One or more parameters in the format `name=value`. Multiple entries are separated by `;`.
//     The supported parameters are:
//     - credentials: File name for the credentials to use. The connection will use the default credentials of the
//     environment if no credentials file is specified in the connection string.
//     - usePlainText: Boolean that indicates whether the connection should use plain text communication or not. Set this
//     to true to connect to local mock servers that do not use SSL.
//     - retryAbortsInternally: Boolean that indicates whether the connection should automatically retry aborted errors.
//     The default is true.
//     - disableRouteToLeader: Boolean that indicates if all the requests of type read-write and PDML
//     need to be routed to the leader region.
//     The default is false
//     - enableEndToEndTracing: Boolean that indicates if end-to-end tracing is enabled
//     The default is false
//     - minSessions: The minimum number of sessions in the backing session pool. The default is 100.
//     - maxSessions: The maximum number of sessions in the backing session pool. The default is 400.
//     - numChannels: The number of gRPC channels to use to communicate with Cloud Spanner. The default is 4.
//     - optimizerVersion: Sets the default query optimizer version to use for this connection.
//     - optimizerStatisticsPackage: Sets the default query optimizer statistic package to use for this connection.
//     - rpcPriority: Sets the priority for all RPC invocations from this connection (HIGH/MEDIUM/LOW). The default is HIGH.
//
// Example: `localhost:9010/projects/test-project/instances/test-instance/databases/test-database;usePlainText=true;disableRouteToLeader=true;enableEndToEndTracing=true`
var dsnRegExp = regexp.MustCompile(`((?P<HOSTGROUP>[\w.-]+(?:\.[\w\.-]+)*[\w\-\._~:/?#\[\]@!\$&'\(\)\*\+,;=.]+)/)?projects/(?P<PROJECTGROUP>(([a-z]|[-.:]|[0-9])+|(DEFAULT_PROJECT_ID)))(/instances/(?P<INSTANCEGROUP>([a-z]|[-]|[0-9])+)(/databases/(?P<DATABASEGROUP>([a-z]|[-]|[_]|[0-9])+))?)?(([\?|;])(?P<PARAMSGROUP>.*))?`)

var _ driver.DriverContext = &Driver{}

func init() {
	sql.Register("spanner", &Driver{connectors: make(map[string]*connector)})
}

// Driver represents a Google Cloud Spanner database/sql driver.
type Driver struct {
	mu         sync.Mutex
	connectors map[string]*connector
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

	logger *slog.Logger
	name   string
}

func (cc *connectorConfig) String() string {
	return cc.name
}

func extractConnectorConfig(dsn string) (connectorConfig, error) {
	match := dsnRegExp.FindStringSubmatch(dsn)
	if match == nil {
		return connectorConfig{}, spanner.ToSpannerError(status.Errorf(codes.InvalidArgument, "invalid connection string: %s", dsn))
	}
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
		name:     dsn,
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
	dsn             string
	connectorConfig connectorConfig
	logger          *slog.Logger

	closerMu sync.RWMutex
	closed   bool

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

	clientMu       sync.Mutex
	client         *spanner.Client
	clientErr      error
	adminClient    *adminapi.DatabaseAdminClient
	adminClientErr error
	connCount      int32
}

func newConnector(d *Driver, dsn string) (*connector, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.connectors == nil {
		d.connectors = make(map[string]*connector)
	}
	if c, ok := d.connectors[dsn]; ok {
		return c, nil
	}

	connectorConfig, err := extractConnectorConfig(dsn)
	if err != nil {
		return nil, err
	}
	opts := make([]option.ClientOption, 0)
	if connectorConfig.host != "" {
		opts = append(opts, option.WithEndpoint(connectorConfig.host))
	}
	if strval, ok := connectorConfig.params["credentials"]; ok {
		opts = append(opts, option.WithCredentialsFile(strval))
	}
	if strval, ok := connectorConfig.params["credentialsjson"]; ok {
		opts = append(opts, option.WithCredentialsJSON([]byte(strval)))
	}
	config := spanner.ClientConfig{
		SessionPoolConfig: spanner.DefaultSessionPoolConfig,
	}
	if strval, ok := connectorConfig.params["useplaintext"]; ok {
		if val, err := strconv.ParseBool(strval); err == nil && val {
			opts = append(opts,
				option.WithGRPCDialOption(grpc.WithTransportCredentials(insecure.NewCredentials())),
				option.WithoutAuthentication())
			// TODO: Add connection string property for disabling native metrics.
			config.DisableNativeMetrics = true
		}
	}
	retryAbortsInternally := true
	if strval, ok := connectorConfig.params["retryabortsinternally"]; ok {
		if val, err := strconv.ParseBool(strval); err == nil && !val {
			retryAbortsInternally = false
		}
	}
	if strval, ok := connectorConfig.params["minsessions"]; ok {
		if val, err := strconv.ParseUint(strval, 10, 64); err == nil {
			config.MinOpened = val
		}
	}
	if strval, ok := connectorConfig.params["maxsessions"]; ok {
		if val, err := strconv.ParseUint(strval, 10, 64); err == nil {
			config.MaxOpened = val
		}
	}
	if strval, ok := connectorConfig.params["numchannels"]; ok {
		if val, err := strconv.Atoi(strval); err == nil && val > 0 {
			opts = append(opts, option.WithGRPCConnectionPool(val))
		}
	}
	if strval, ok := connectorConfig.params["rpcpriority"]; ok {
		var priority spannerpb.RequestOptions_Priority
		switch strings.ToUpper(strval) {
		case "LOW":
			priority = spannerpb.RequestOptions_PRIORITY_LOW
		case "MEDIUM":
			priority = spannerpb.RequestOptions_PRIORITY_MEDIUM
		case "HIGH":
			priority = spannerpb.RequestOptions_PRIORITY_HIGH
		default:
			priority = spannerpb.RequestOptions_PRIORITY_UNSPECIFIED
		}
		config.ReadOptions.Priority = priority
		config.TransactionOptions.CommitPriority = priority
		config.QueryOptions.Priority = priority
	}
	if strval, ok := connectorConfig.params["optimizerversion"]; ok {
		if config.QueryOptions.Options == nil {
			config.QueryOptions.Options = &spannerpb.ExecuteSqlRequest_QueryOptions{}
		}
		config.QueryOptions.Options.OptimizerVersion = strval
	}
	if strval, ok := connectorConfig.params["optimizerstatisticspackage"]; ok {
		if config.QueryOptions.Options == nil {
			config.QueryOptions.Options = &spannerpb.ExecuteSqlRequest_QueryOptions{}
		}
		config.QueryOptions.Options.OptimizerStatisticsPackage = strval
	}
	if strval, ok := connectorConfig.params["databaserole"]; ok {
		config.DatabaseRole = strval
	}
	if strval, ok := connectorConfig.params["disableroutetoleader"]; ok {
		if val, err := strconv.ParseBool(strval); err == nil {
			config.DisableRouteToLeader = val
		}
	}
	if strval, ok := connectorConfig.params["enableendtoendtracing"]; ok {
		if val, err := strconv.ParseBool(strval); err == nil {
			config.EnableEndToEndTracing = val
		}
	}
	if strval, ok := connectorConfig.params["disablenativemetrics"]; ok {
		if val, err := strconv.ParseBool(strval); err == nil {
			config.DisableNativeMetrics = val
		}
	}
	config.UserAgent = userAgent
	var logger *slog.Logger
	if connectorConfig.logger == nil {
		d := slog.Default()
		if d == nil {
			logger = noopLogger
		} else {
			logger = d
		}
	} else {
		logger = connectorConfig.logger
	}
	logger = logger.With("config", &connectorConfig)

	c := &connector{
		driver:                d,
		dsn:                   dsn,
		connectorConfig:       connectorConfig,
		logger:                logger,
		spannerClientConfig:   config,
		options:               opts,
		retryAbortsInternally: retryAbortsInternally,
	}
	d.connectors[dsn] = c
	return c, nil
}

func (c *connector) Connect(ctx context.Context) (driver.Conn, error) {
	c.closerMu.RLock()
	defer c.closerMu.RUnlock()
	if c.closed {
		return nil, fmt.Errorf("connector has been closed")
	}
	return openDriverConn(ctx, c)
}

func openDriverConn(ctx context.Context, c *connector) (driver.Conn, error) {
	c.logger.Log(ctx, LevelNotice, "opening connection")

	opts := append(c.options, option.WithUserAgent(userAgent))
	databaseName := fmt.Sprintf(
		"projects/%s/instances/%s/databases/%s",
		c.connectorConfig.project,
		c.connectorConfig.instance,
		c.connectorConfig.database)

	if err := c.increaseConnCount(ctx, databaseName, opts); err != nil {
		return nil, err
	}

	connId := uuid.New().String()
	logger := c.logger.With("connId", connId)
	return &conn{
		connector:                    c,
		client:                       c.client,
		adminClient:                  c.adminClient,
		connId:                       connId,
		logger:                       logger,
		database:                     databaseName,
		retryAborts:                  c.retryAbortsInternally,
		execSingleQuery:              queryInSingleUse,
		execSingleQueryTransactional: queryInNewRWTransaction,
		execSingleDMLTransactional:   execInNewRWTransaction,
		execSingleDMLPartitioned:     execAsPartitionedDML,
	}, nil
}

// increaseConnCount initializes the client and increases the number of connections that are active.
func (c *connector) increaseConnCount(ctx context.Context, databaseName string, opts []option.ClientOption) error {
	c.clientMu.Lock()
	defer c.clientMu.Unlock()

	if c.clientErr != nil {
		return c.clientErr
	}
	if c.adminClientErr != nil {
		return c.adminClientErr
	}

	if c.client == nil {
		c.logger.Log(ctx, LevelNotice, "creating Spanner client")
		c.client, c.clientErr = spanner.NewClientWithConfig(ctx, databaseName, c.spannerClientConfig, opts...)
		if c.clientErr != nil {
			return c.clientErr
		}

		c.logger.Log(ctx, LevelNotice, "creating Spanner Admin client")
		c.adminClient, c.adminClientErr = adminapi.NewDatabaseAdminClient(ctx, opts...)
		if c.adminClientErr != nil {
			c.client = nil
			c.client.Close()
			c.adminClient = nil
			return c.adminClientErr
		}
	}

	c.connCount++
	c.logger.DebugContext(ctx, "increased conn count", "connCount", c.connCount)
	return nil
}

// decreaseConnCount decreases the number of connections that are active and closes the underlying clients if it was the
// last connection.
func (c *connector) decreaseConnCount() error {
	c.clientMu.Lock()
	defer c.clientMu.Unlock()

	c.connCount--
	c.logger.Debug("decreased conn count", "connCount", c.connCount)
	if c.connCount > 0 {
		return nil
	}

	return c.closeClients()
}

func (c *connector) Driver() driver.Driver {
	return c.driver
}

func (c *connector) Close() error {
	c.logger.Debug("closing connector")
	c.closerMu.Lock()
	c.closed = true
	c.closerMu.Unlock()

	c.driver.mu.Lock()
	delete(c.driver.connectors, c.dsn)
	c.driver.mu.Unlock()

	c.clientMu.Lock()
	defer c.clientMu.Unlock()
	return c.closeClients()
}

// Closes the underlying clients.
func (c *connector) closeClients() (err error) {
	c.logger.Debug("closing clients")
	if c.client != nil {
		c.client.Close()
		c.client = nil
	}
	if c.adminClient != nil {
		err = c.adminClient.Close()
		c.adminClient = nil
	}
	return err
}

// RunTransaction runs the given function in a transaction on the given database.
// If the connection is a connection to a Spanner database, the transaction will
// automatically be retried if the transaction is aborted by Spanner. Any other
// errors will be propagated to the caller and the transaction will be rolled
// back. The transaction will be committed if the supplied function did not
// return an error.
//
// If the connection is to a non-Spanner database, no retries will be attempted,
// and any error that occurs during the transaction will be propagated to the
// caller.
//
// The application should *NOT* call tx.Commit() or tx.Rollback(). This is done
// automatically by this function, depending on whether the transaction function
// returned an error or not.
//
// This function will never return ErrAbortedDueToConcurrentModification.
func RunTransaction(ctx context.Context, db *sql.DB, opts *sql.TxOptions, f func(ctx context.Context, tx *sql.Tx) error) error {
	return runTransactionWithOptions(ctx, db, opts, f, spanner.TransactionOptions{})
}

// RunTransactionWithOptions runs the given function in a transaction on the given database.
// If the connection is a connection to a Spanner database, the transaction will
// automatically be retried if the transaction is aborted by Spanner. Any other
// errors will be propagated to the caller and the transaction will be rolled
// back. The transaction will be committed if the supplied function did not
// return an error.
//
// If the connection is to a non-Spanner database, no retries will be attempted,
// and any error that occurs during the transaction will be propagated to the
// caller.
//
// The application should *NOT* call tx.Commit() or tx.Rollback(). This is done
// automatically by this function, depending on whether the transaction function
// returned an error or not.
//
// The given spanner.TransactionOptions will be used for the transaction.
//
// This function will never return ErrAbortedDueToConcurrentModification.
func RunTransactionWithOptions(ctx context.Context, db *sql.DB, opts *sql.TxOptions, f func(ctx context.Context, tx *sql.Tx) error, spannerOptions spanner.TransactionOptions) error {
	return runTransactionWithOptions(ctx, db, opts, f, spannerOptions)
}

func runTransactionWithOptions(ctx context.Context, db *sql.DB, opts *sql.TxOptions, f func(ctx context.Context, tx *sql.Tx) error, spannerOptions spanner.TransactionOptions) error {
	// Get a connection from the pool that we can use to run a transaction.
	// Getting a connection here already makes sure that we can reserve this
	// connection exclusively for the duration of this method. That again
	// allows us to temporarily change the state of the connection (e.g. set
	// the retryAborts flag to false).
	conn, err := db.Conn(ctx)
	if err != nil {
		return err
	}
	defer func() {
		_ = conn.Close()
	}()

	// We don't need to keep track of a running checksum for retries when using
	// this method, so we disable internal retries.
	// Retries will instead be handled by the loop below.
	origRetryAborts := false
	var spannerConn SpannerConn
	if err := conn.Raw(func(driverConn any) error {
		var ok bool
		spannerConn, ok = driverConn.(SpannerConn)
		if !ok {
			// It is not a Spanner connection, so just ignore and continue without any special handling.
			return nil
		}
		spannerConn.withTransactionOptions(spannerOptions)
		origRetryAborts = spannerConn.RetryAbortsInternally()
		return spannerConn.SetRetryAbortsInternally(false)
	}); err != nil {
		return err
	}
	// Reset the flag for internal retries after the transaction (if applicable).
	if origRetryAborts {
		defer func() {
			_ = spannerConn.SetRetryAbortsInternally(origRetryAborts)
		}()
	}

	tx, err := conn.BeginTx(ctx, opts)
	if err != nil {
		return err
	}
	for {
		err = f(ctx, tx)
		errDuringCommit := false
		if err == nil {
			err = tx.Commit()
			if err == nil {
				return nil
			}
			errDuringCommit = true
		}
		// Rollback and return the error if:
		// 1. The connection is not a Spanner connection.
		// 2. Or the error code is not Aborted.
		if spannerConn == nil || spanner.ErrCode(err) != codes.Aborted {
			// We don't really need to call Rollback here if the error happened
			// during the Commit. However, the SQL package treats this as a no-op
			// and just returns an ErrTxDone if we do, so this is simpler than
			// keeping track of where the error happened.
			_ = tx.Rollback()
			return err
		}

		// The transaction was aborted by Spanner.
		// Back off and retry the entire transaction.
		if delay, ok := spanner.ExtractRetryDelay(err); ok {
			err = gax.Sleep(ctx, delay)
			if err != nil {
				// We need to 'roll back' the transaction here to tell the sql
				// package that there is no active transaction on the connection
				// anymore. It does not actually roll back the transaction, as it
				// has already been aborted by Spanner.
				_ = tx.Rollback()
				return err
			}
		}

		// Reset the transaction after it was aborted.
		err = spannerConn.resetTransactionForRetry(ctx, errDuringCommit)
		if err != nil {
			_ = tx.Rollback()
			return err
		}
		// This does not actually start a new transaction, instead it
		// continues with the previous transaction that was already reset.
		// We need to do this, because the sql package registers the
		// transaction as 'done' when Commit has been called, also if the
		// commit fails.
		if errDuringCommit {
			tx, err = conn.BeginTx(ctx, opts)
			if err != nil {
				return err
			}
		}
	}
}

func queryInNewRWTransaction(ctx context.Context, c *spanner.Client, statement spanner.Statement, options ExecOptions) (rowIterator, time.Time, error) {
	var result *wrappedRowIterator
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

func execInNewRWTransaction(ctx context.Context, c *spanner.Client, statement spanner.Statement, options ExecOptions) (int64, time.Time, error) {
	var rowsAffected int64
	fn := func(ctx context.Context, tx *spanner.ReadWriteTransaction) error {
		count, err := tx.UpdateWithOptions(ctx, statement, options.QueryOptions)
		rowsAffected = count
		return err
	}
	resp, err := c.ReadWriteTransactionWithOptions(ctx, fn, options.TransactionOptions)
	if err != nil {
		return 0, time.Time{}, err
	}
	return rowsAffected, resp.CommitTs, nil
}

func execAsPartitionedDML(ctx context.Context, c *spanner.Client, statement spanner.Statement, options ExecOptions) (int64, error) {
	queryOptions := options.QueryOptions
	queryOptions.ExcludeTxnFromChangeStreams = options.TransactionOptions.ExcludeTxnFromChangeStreams
	return c.PartitionedUpdateWithOptions(ctx, statement, queryOptions)
}
