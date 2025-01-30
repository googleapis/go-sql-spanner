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
	"math/big"
	"reflect"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"cloud.google.com/go/civil"
	"cloud.google.com/go/spanner"
	adminapi "cloud.google.com/go/spanner/admin/database/apiv1"
	adminpb "cloud.google.com/go/spanner/admin/database/apiv1/databasepb"
	"cloud.google.com/go/spanner/apiv1/spannerpb"
	"github.com/google/uuid"
	"github.com/googleapis/gax-go/v2"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

const userAgent = "go-sql-spanner/1.10.0" // x-release-please-version

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
var spannerDriver *Driver

func init() {
	spannerDriver = &Driver{connectors: make(map[string]*connector)}
	sql.Register("spanner", spannerDriver)
}

// ExecOptions can be passed in as an argument to the Query, QueryContext,
// Exec, and ExecContext functions to specify additional execution options
// for a statement.
type ExecOptions struct {
	// DecodeOption indicates how the returned rows should be decoded.
	DecodeOption DecodeOption

	// TransactionOptions are the transaction options that will be used for
	// the transaction that is started by the statement.
	TransactionOptions spanner.TransactionOptions
	// QueryOptions are the query options that will be used for the statement.
	QueryOptions spanner.QueryOptions

	// PartitionedQueryOptions are used for partitioned queries, and ignored
	// for all other statements.
	PartitionedQueryOptions PartitionedQueryOptions
}

type DecodeOption int

const (
	// DecodeOptionNormal decodes into idiomatic Go types (e.g. bool, string, int64, etc.)
	DecodeOptionNormal DecodeOption = iota

	// DecodeOptionProto does not decode the returned rows at all, and instead just returns
	// the underlying protobuf objects. Use this for advanced use-cases where you want
	// direct access to the underlying values.
	// All values should be scanned into an instance of spanner.GenericColumnValue like this:
	//
	// 	var v spanner.GenericColumnValue
	// 	row.Scan(&v)
	DecodeOptionProto
)

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
	c, err := newOrCachedConnector(d, name)
	if err != nil {
		return nil, err
	}
	return openDriverConn(context.Background(), c)
}

func (d *Driver) OpenConnector(name string) (driver.Connector, error) {
	return newOrCachedConnector(d, name)
}

// CreateConnector creates a new driver.Connector for Spanner.
// A connector can be passed in to sql.OpenDB to obtain a sql.DB.
//
// Use this method if you want to supply custom configuration for your Spanner
// connections, and cache the connector that is returned in your application.
// The same connector should be used to create all connections that should share
// the same configuration and the same underlying Spanner client.
//
// Note: This function always creates a new connector, even if one with the same
// configuration has been created previously.
func CreateConnector(config ConnectorConfig) (driver.Connector, error) {
	return createConnector(spannerDriver, config)
}

// ConnectorConfig contains the configuration for a Spanner driver.Connector.
type ConnectorConfig struct {
	// Host is the Spanner host that the connector should connect to.
	// Leave this empty to use the standard Spanner API endpoint.
	Host string

	// Project, Instance, and Database identify the database that the connector
	// should create connections for.
	Project  string
	Instance string
	Database string

	// Params contains key/value pairs for commonly used configuration parameters
	// for connections. The valid values are the same as the parameters that can
	// be added to a connection string.
	Params map[string]string

	logger *slog.Logger
	name   string

	// Configurator is called with the spanner.ClientConfig and []option.ClientOption
	// that will be used to create connections by the driver.Connector. Use this
	// function to set any further advanced configuration options that cannot be set
	// with a standard key/value pair in the Params map.
	Configurator func(config *spanner.ClientConfig, opts *[]option.ClientOption)
}

func (cc *ConnectorConfig) String() string {
	return cc.name
}

// ExtractConnectorConfig extracts a ConnectorConfig for Spanner from the given
// data source name.
func ExtractConnectorConfig(dsn string) (ConnectorConfig, error) {
	match := dsnRegExp.FindStringSubmatch(dsn)
	if match == nil {
		return ConnectorConfig{}, spanner.ToSpannerError(status.Errorf(codes.InvalidArgument, "invalid connection string: %s", dsn))
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
		return ConnectorConfig{}, err
	}

	return ConnectorConfig{
		Host:     matches["HOSTGROUP"],
		Project:  matches["PROJECTGROUP"],
		Instance: matches["INSTANCEGROUP"],
		Database: matches["DATABASEGROUP"],
		Params:   params,
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
	connectorConfig ConnectorConfig
	cacheKey        string
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

func newOrCachedConnector(d *Driver, dsn string) (*connector, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.connectors == nil {
		d.connectors = make(map[string]*connector)
	}
	if c, ok := d.connectors[dsn]; ok {
		return c, nil
	}

	connectorConfig, err := ExtractConnectorConfig(dsn)
	if err != nil {
		return nil, err
	}
	c, err := createConnector(d, connectorConfig)
	if err != nil {
		return nil, err
	}
	c.cacheKey = dsn
	d.connectors[dsn] = c
	return c, nil
}

func createConnector(d *Driver, connectorConfig ConnectorConfig) (*connector, error) {
	opts := make([]option.ClientOption, 0)
	if connectorConfig.Host != "" {
		opts = append(opts, option.WithEndpoint(connectorConfig.Host))
	}
	if strval, ok := connectorConfig.Params["credentials"]; ok {
		opts = append(opts, option.WithCredentialsFile(strval))
	}
	if strval, ok := connectorConfig.Params["credentialsjson"]; ok {
		opts = append(opts, option.WithCredentialsJSON([]byte(strval)))
	}
	config := spanner.ClientConfig{
		SessionPoolConfig: spanner.DefaultSessionPoolConfig,
	}
	if strval, ok := connectorConfig.Params["useplaintext"]; ok {
		if val, err := strconv.ParseBool(strval); err == nil && val {
			opts = append(opts,
				option.WithGRPCDialOption(grpc.WithTransportCredentials(insecure.NewCredentials())),
				option.WithoutAuthentication())
			// TODO: Add connection string property for disabling native metrics.
			config.DisableNativeMetrics = true
		}
	}
	retryAbortsInternally := true
	if strval, ok := connectorConfig.Params["retryabortsinternally"]; ok {
		if val, err := strconv.ParseBool(strval); err == nil && !val {
			retryAbortsInternally = false
		}
	}
	if strval, ok := connectorConfig.Params["minsessions"]; ok {
		if val, err := strconv.ParseUint(strval, 10, 64); err == nil {
			config.MinOpened = val
		}
	}
	if strval, ok := connectorConfig.Params["maxsessions"]; ok {
		if val, err := strconv.ParseUint(strval, 10, 64); err == nil {
			config.MaxOpened = val
		}
	}
	if strval, ok := connectorConfig.Params["numchannels"]; ok {
		if val, err := strconv.Atoi(strval); err == nil && val > 0 {
			opts = append(opts, option.WithGRPCConnectionPool(val))
		}
	}
	if strval, ok := connectorConfig.Params["rpcpriority"]; ok {
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
	if strval, ok := connectorConfig.Params["optimizerversion"]; ok {
		if config.QueryOptions.Options == nil {
			config.QueryOptions.Options = &spannerpb.ExecuteSqlRequest_QueryOptions{}
		}
		config.QueryOptions.Options.OptimizerVersion = strval
	}
	if strval, ok := connectorConfig.Params["optimizerstatisticspackage"]; ok {
		if config.QueryOptions.Options == nil {
			config.QueryOptions.Options = &spannerpb.ExecuteSqlRequest_QueryOptions{}
		}
		config.QueryOptions.Options.OptimizerStatisticsPackage = strval
	}
	if strval, ok := connectorConfig.Params["databaserole"]; ok {
		config.DatabaseRole = strval
	}
	if strval, ok := connectorConfig.Params["disableroutetoleader"]; ok {
		if val, err := strconv.ParseBool(strval); err == nil {
			config.DisableRouteToLeader = val
		}
	}
	if strval, ok := connectorConfig.Params["enableendtoendtracing"]; ok {
		if val, err := strconv.ParseBool(strval); err == nil {
			config.EnableEndToEndTracing = val
		}
	}
	if strval, ok := connectorConfig.Params["disablenativemetrics"]; ok {
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
	if connectorConfig.Configurator != nil {
		connectorConfig.Configurator(&config, &opts)
	}

	c := &connector{
		driver:                d,
		connectorConfig:       connectorConfig,
		logger:                logger,
		spannerClientConfig:   config,
		options:               opts,
		retryAbortsInternally: retryAbortsInternally,
	}
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
	opts := c.options
	c.logger.Log(ctx, LevelNotice, "opening connection")
	databaseName := fmt.Sprintf(
		"projects/%s/instances/%s/databases/%s",
		c.connectorConfig.Project,
		c.connectorConfig.Instance,
		c.connectorConfig.Database)

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

	if c.cacheKey != "" {
		c.driver.mu.Lock()
		delete(c.driver.connectors, c.cacheKey)
		c.driver.mu.Unlock()
	}

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
	transactionOptions := &ReadWriteTransactionOptions{
		TransactionOptions:     spannerOptions,
		DisableInternalRetries: true,
	}
	isSpannerConn := false
	if err := conn.Raw(func(driverConn any) error {
		var spannerConn SpannerConn
		spannerConn, isSpannerConn = driverConn.(SpannerConn)
		if !isSpannerConn {
			// It is not a Spanner connection, so just ignore and continue without any special handling.
			return nil
		}
		spannerConn.withTempTransactionOptions(transactionOptions)
		return nil
	}); err != nil {
		return err
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
		if !isSpannerConn || spanner.ErrCode(err) != codes.Aborted {
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
		err = resetTransactionForRetry(ctx, conn, errDuringCommit)
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

func resetTransactionForRetry(ctx context.Context, conn *sql.Conn, errDuringCommit bool) error {
	return conn.Raw(func(driverConn any) error {
		spannerConn, ok := driverConn.(SpannerConn)
		if !ok {
			return spanner.ToSpannerError(status.Error(codes.InvalidArgument, "not a Spanner connection"))
		}
		return spannerConn.resetTransactionForRetry(ctx, errDuringCommit)
	})
}

type ReadWriteTransactionOptions struct {
	// TransactionOptions are passed through to the Spanner client to use for
	// the read/write transaction.
	TransactionOptions spanner.TransactionOptions
	// DisableInternalRetries disables checksum-based retries of Aborted errors
	// for this transaction. By default, read/write transactions keep track of
	// a running checksum of all results it receives from Spanner. If Spanner
	// aborts the transaction, the transaction is retried by the driver and the
	// checksums of the initial attempt and the retry are compared. If the
	// checksums differ, the transaction fails with an Aborted error.
	//
	// If DisableInternalRetries is set to true, checksum-based retries are
	// disabled, and any Aborted error from Spanner is propagated to the
	// application.
	DisableInternalRetries bool

	close func()
}

// BeginReadWriteTransaction begins a read/write transaction on a Spanner database.
// This function allows more options to be passed in that the generic sql.DB.BeginTx
// function.
//
// NOTE: You *MUST* end the transaction by calling either Commit or Rollback on
// the transaction. Failure to do so will cause the connection that is used for
// the transaction to be leaked.
func BeginReadWriteTransaction(ctx context.Context, db *sql.DB, options ReadWriteTransactionOptions) (*sql.Tx, error) {
	conn, err := db.Conn(ctx)
	if err != nil {
		return nil, err
	}
	options.close = func() {
		// Close the connection asynchronously, as the transaction will still
		// be active when we hit this point.
		go conn.Close()
	}
	if err := withTempReadWriteTransactionOptions(conn, &options); err != nil {
		return nil, err
	}
	tx, err := conn.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		clearTempReadWriteTransactionOptions(conn)
		return nil, err
	}
	return tx, nil
}

func withTempReadWriteTransactionOptions(conn *sql.Conn, options *ReadWriteTransactionOptions) error {
	return conn.Raw(func(driverConn any) error {
		spannerConn, ok := driverConn.(SpannerConn)
		if !ok {
			// It is not a Spanner connection.
			return spanner.ToSpannerError(status.Error(codes.FailedPrecondition, "This function can only be used with a Spanner connection"))
		}
		spannerConn.withTempTransactionOptions(options)
		return nil
	})
}

func clearTempReadWriteTransactionOptions(conn *sql.Conn) {
	_ = withTempReadWriteTransactionOptions(conn, nil)
	_ = conn.Close()
}

// ReadOnlyTransactionOptions can be used to create a read-only transaction
// on a Spanner connection.
type ReadOnlyTransactionOptions struct {
	TimestampBound spanner.TimestampBound

	close func()
}

// BeginReadOnlyTransaction begins a read-only transaction on a Spanner database.
//
// NOTE: You *MUST* end the transaction by calling either Commit or Rollback on
// the transaction. Failure to do so will cause the connection that is used for
// the transaction to be leaked.
func BeginReadOnlyTransaction(ctx context.Context, db *sql.DB, options ReadOnlyTransactionOptions) (*sql.Tx, error) {
	conn, err := db.Conn(ctx)
	if err != nil {
		return nil, err
	}
	options.close = func() {
		// Close the connection asynchronously, as the transaction will still
		// be active when we hit this point.
		go conn.Close()
	}
	if err := withTempReadOnlyTransactionOptions(conn, &options); err != nil {
		return nil, err
	}
	tx, err := conn.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		clearTempReadOnlyTransactionOptions(conn)
		return nil, err
	}
	return tx, nil
}

func withTempReadOnlyTransactionOptions(conn *sql.Conn, options *ReadOnlyTransactionOptions) error {
	return conn.Raw(func(driverConn any) error {
		spannerConn, ok := driverConn.(SpannerConn)
		if !ok {
			// It is not a Spanner connection.
			return spanner.ToSpannerError(status.Error(codes.FailedPrecondition, "This function can only be used with a Spanner connection"))
		}
		spannerConn.withTempReadOnlyTransactionOptions(options)
		return nil
	})
}

func clearTempReadOnlyTransactionOptions(conn *sql.Conn) {
	_ = withTempReadOnlyTransactionOptions(conn, nil)
	_ = conn.Close()
}

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
	execSingleDMLTransactional   func(ctx context.Context, c *spanner.Client, statement spanner.Statement, options ExecOptions) (int64, time.Time, error)
	execSingleDMLPartitioned     func(ctx context.Context, c *spanner.Client, statement spanner.Statement, options ExecOptions) (int64, error)

	// batch is the currently active DDL or DML batch on this connection.
	batch *batch

	// autocommitDMLMode determines the type of DML to use when a single DML
	// statement is executed on a connection. The default is Transactional, but
	// it can also be set to PartitionedNonAtomic to execute the statement as
	// Partitioned DML.
	autocommitDMLMode AutocommitDMLMode
	// readOnlyStaleness is used for queries in autocommit mode and for read-only transactions.
	readOnlyStaleness spanner.TimestampBound

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

type batchType int

const (
	ddl batchType = iota
	dml
)

type batch struct {
	tp         batchType
	statements []spanner.Statement
	options    ExecOptions
}

// AutocommitDMLMode indicates whether a single DML statement should be executed
// in a normal atomic transaction or as a Partitioned DML statement.
// See https://cloud.google.com/spanner/docs/dml-partitioned for more information.
type AutocommitDMLMode int

func (mode AutocommitDMLMode) String() string {
	switch mode {
	case Transactional:
		return "Transactional"
	case PartitionedNonAtomic:
		return "Partitioned_Non_Atomic"
	}
	return ""
}

const (
	Transactional AutocommitDMLMode = iota
	PartitionedNonAtomic
)

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

func (c *conn) StartBatchDDL() error {
	_, err := c.startBatchDDL()
	return err
}

func (c *conn) StartBatchDML() error {
	_, err := c.startBatchDML()
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

func (c *conn) startBatchDML() (driver.Result, error) {
	execOptions := c.options()

	if c.inTransaction() {
		return c.tx.StartBatchDML(execOptions.QueryOptions)
	}

	if c.batch != nil {
		return nil, spanner.ToSpannerError(status.Errorf(codes.FailedPrecondition, "This connection already has an active batch."))
	}
	if c.inReadOnlyTransaction() {
		return nil, spanner.ToSpannerError(status.Errorf(codes.FailedPrecondition, "This connection has an active read-only transaction. Read-only transactions cannot execute DML batches."))
	}
	c.logger.Debug("starting dml batch")
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

func (c *conn) runDMLBatch(ctx context.Context) (driver.Result, error) {
	statements := c.batch.statements
	options := c.batch.options
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

func (c *conn) execBatchDML(ctx context.Context, statements []spanner.Statement, options ExecOptions) (driver.Result, error) {
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
	return &result{rowsAffected: sum(affected)}, err
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
	c.commitTs = nil
	c.batch = nil
	c.retryAborts = true
	c.autocommitDMLMode = Transactional
	c.readOnlyStaleness = spanner.TimestampBound{}
	c.execOptions = ExecOptions{}
	return nil
}

// IsValid implements the driver.Validator interface.
func (c *conn) IsValid() bool {
	return !c.closed
}

// checkIsValidType returns true for types that do not need extra conversion
// for spanner.
func checkIsValidType(v driver.Value) bool {
	switch v.(type) {
	default:
		return false
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
	case uint:
	case []uint:
	case *uint:
	case []*uint:
	case *[]uint:
	case int:
	case []int:
	case *int:
	case []*int:
	case *[]int:
	case int64:
	case []int64:
	case spanner.NullInt64:
	case []spanner.NullInt64:
	case *int64:
	case []*int64:
	case uint64:
	case []uint64:
	case *uint64:
	case []*uint64:
	case bool:
	case []bool:
	case spanner.NullBool:
	case []spanner.NullBool:
	case *bool:
	case []*bool:
	case float32:
	case []float32:
	case spanner.NullFloat32:
	case []spanner.NullFloat32:
	case *float32:
	case []*float32:
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
	return true
}

func (c *conn) CheckNamedValue(value *driver.NamedValue) error {
	if value == nil {
		return nil
	}

	if execOptions, ok := value.Value.(ExecOptions); ok {
		c.execOptions = execOptions
		return driver.ErrRemoveArgument
	}

	if checkIsValidType(value.Value) {
		return nil
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

func (c *conn) PrepareContext(ctx context.Context, query string) (driver.Stmt, error) {
	execOptions := c.options()
	parsedSQL, args, err := parseParameters(query)
	if err != nil {
		return nil, err
	}
	return &stmt{conn: c, query: parsedSQL, numArgs: len(args), execOptions: execOptions}, nil
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

	execOptions := c.options()
	return c.queryContext(ctx, query, execOptions, args)
}

func (c *conn) queryContext(ctx context.Context, query string, execOptions ExecOptions, args []driver.NamedValue) (driver.Rows, error) {
	// Clear the commit timestamp of this connection before we execute the query.
	c.commitTs = nil
	if pq := execOptions.PartitionedQueryOptions.ExecutePartition.PartitionedQuery; pq != nil {
		return pq.execute(ctx, execOptions.PartitionedQueryOptions.ExecutePartition.Index)
	}

	stmt, err := prepareSpannerStmt(query, args)
	if err != nil {
		return nil, err
	}
	var iter rowIterator
	if c.tx == nil {
		statementType := detectStatementType(query)
		if statementType == statementTypeDml {
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
			// TODO: Implement
			return nil, spanner.ToSpannerError(status.Errorf(codes.Unimplemented, "auto-partitioning queries not yet implemented"))
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
		iter = c.tx.Query(ctx, stmt, execOptions.QueryOptions)
	}
	return &rows{it: iter, decodeOption: execOptions.DecodeOption}, nil
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
	execOptions := c.options()
	return c.execContext(ctx, query, execOptions, args)
}

func (c *conn) execContext(ctx context.Context, query string, execOptions ExecOptions, args []driver.NamedValue) (driver.Result, error) {
	// Clear the commit timestamp of this connection before we execute the statement.
	c.commitTs = nil

	statementType := detectStatementType(query)
	// Use admin API if DDL statement is provided.
	if statementType == statementTypeDdl {
		// Spanner does not support DDL in transactions, and although it is technically possible to execute DDL
		// statements while a transaction is active, we return an error to avoid any confusion whether the DDL
		// statement is executed as part of the active transaction or not.
		if c.inTransaction() {
			return nil, spanner.ToSpannerError(status.Errorf(codes.FailedPrecondition, "cannot execute DDL as part of a transaction"))
		}
		return c.execDDL(ctx, spanner.NewStatement(query))
	}

	ss, err := prepareSpannerStmt(query, args)
	if err != nil {
		return nil, err
	}

	var rowsAffected int64
	var commitTs time.Time
	if c.tx == nil {
		if c.InDMLBatch() {
			c.batch.statements = append(c.batch.statements, ss)
		} else {
			if c.autocommitDMLMode == Transactional {
				rowsAffected, commitTs, err = c.execSingleDMLTransactional(ctx, c.client, ss, execOptions)
				if err == nil {
					c.commitTs = &commitTs
				}
			} else if c.autocommitDMLMode == PartitionedNonAtomic {
				rowsAffected, err = c.execSingleDMLPartitioned(ctx, c.client, ss, execOptions)
			} else {
				return nil, status.Errorf(codes.FailedPrecondition, "connection in invalid state for DML statements: %s", c.autocommitDMLMode.String())
			}
		}
	} else {
		rowsAffected, err = c.tx.ExecContext(ctx, ss, execOptions.QueryOptions)
	}
	if err != nil {
		return nil, err
	}
	return &result{rowsAffected: rowsAffected}, nil
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
	return ReadWriteTransactionOptions{
		TransactionOptions:     c.execOptions.TransactionOptions,
		DisableInternalRetries: !c.retryAborts,
	}
}

func (c *conn) withTempReadOnlyTransactionOptions(options *ReadOnlyTransactionOptions) {
	c.tempReadOnlyTransactionOptions = options
}

func (c *conn) getReadOnlyTransactionOptions() ReadOnlyTransactionOptions {
	if c.tempReadOnlyTransactionOptions != nil {
		defer func() { c.tempReadOnlyTransactionOptions = nil }()
		return *c.tempReadOnlyTransactionOptions
	}
	return ReadOnlyTransactionOptions{TimestampBound: c.readOnlyStaleness}
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

type spannerIsolationLevel sql.IsolationLevel

const (
	levelNone spannerIsolationLevel = iota
	levelDisableRetryAborts
	levelBatchReadOnly
)

// WithDisableRetryAborts returns a specific Spanner isolation level that contains
// both the given standard isolation level and a custom Spanner isolation level that
// disables internal retries for aborted transactions for a single transaction.
func WithDisableRetryAborts(level sql.IsolationLevel) sql.IsolationLevel {
	return sql.IsolationLevel(levelDisableRetryAborts)<<8 + level
}

// WithBatchReadOnly returns a specific Spanner isolation level that contains
// both the given standard isolation level and a custom Spanner isolation level that
// instructs Spanner to use a spanner.BatchReadOnlyTransaction. This isolation level
// should only be used for read-only transactions.
func WithBatchReadOnly(level sql.IsolationLevel) sql.IsolationLevel {
	return sql.IsolationLevel(levelBatchReadOnly)<<8 + level
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
	// TODO: Fix this, the original isolation level is not correctly restored.
	opts.Isolation = opts.Isolation - sil
	if sil > 0 {
		switch spannerIsolationLevel(sil) {
		case levelDisableRetryAborts:
			disableRetryAborts = true
		case levelBatchReadOnly:
			batchReadOnly = true
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
			ro = c.client.ReadOnlyTransaction().WithTimestampBound(readOnlyTxOpts.TimestampBound)
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
		client: c.client,
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

type wrappedRowIterator struct {
	*spanner.RowIterator

	noRows   bool
	firstRow *spanner.Row
}

func (ri *wrappedRowIterator) Next() (*spanner.Row, error) {
	if ri.noRows {
		return nil, iterator.Done
	}
	if ri.firstRow != nil {
		defer func() { ri.firstRow = nil }()
		return ri.firstRow, nil
	}
	return ri.RowIterator.Next()
}

func (ri *wrappedRowIterator) Stop() {
	ri.RowIterator.Stop()
}

func (ri *wrappedRowIterator) Metadata() *spannerpb.ResultSetMetadata {
	return ri.RowIterator.Metadata
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

/* The following is the same implementation as in google-cloud-go/spanner */

func isStructOrArrayOfStructValue(v interface{}) bool {
	typ := reflect.TypeOf(v)
	if typ.Kind() == reflect.Slice {
		typ = typ.Elem()
	}
	if typ.Kind() == reflect.Ptr {
		typ = typ.Elem()
	}
	return typ.Kind() == reflect.Struct
}

func isAnArrayOfProtoColumn(v interface{}) bool {
	typ := reflect.TypeOf(v)
	if typ.Kind() == reflect.Ptr {
		typ = typ.Elem()
	}
	if typ.Kind() == reflect.Slice {
		typ = typ.Elem()
	}
	return typ.Implements(protoMsgReflectType) || typ.Implements(protoEnumReflectType)
}

var (
	protoMsgReflectType  = reflect.TypeOf((*proto.Message)(nil)).Elem()
	protoEnumReflectType = reflect.TypeOf((*protoreflect.Enum)(nil)).Elem()
)
