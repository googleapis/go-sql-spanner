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
	"regexp"
	"runtime"
	"runtime/debug"
	"strconv"
	"strings"
	"sync"
	"time"

	"cloud.google.com/go/civil"
	"cloud.google.com/go/spanner"
	adminapi "cloud.google.com/go/spanner/admin/database/apiv1"
	"cloud.google.com/go/spanner/apiv1/spannerpb"
	"github.com/google/uuid"
	"github.com/googleapis/gax-go/v2"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

const userAgent = "go-sql-spanner/1.11.2" // x-release-please-version

const gormModule = "github.com/googleapis/go-gorm-spanner"
const gormUserAgent = "go-gorm-spanner"

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
	// DecodeToNativeArrays determines whether arrays that have a Go
	// native type should use that. This option has an effect on arrays
	// that contain:
	//   * BOOL
	//   * INT64 and ENUM
	//   * STRING
	//   * FLOAT32
	//   * FLOAT64
	//   * DATE
	//   * TIMESTAMP
	// These arrays will by default be decoded to the following types:
	//   * []spanner.NullBool
	//   * []spanner.NullInt64
	//   * []spanner.NullString
	//   * []spanner.NullFloat32
	//   * []spanner.NullFloat64
	//   * []spanner.NullDate
	//   * []spanner.NullTime
	// By setting DecodeToNativeArrays, these arrays will instead be
	// decoded to:
	//   * []bool
	//   * []int64
	//   * []string
	//   * []float32
	//   * []float64
	//   * []civil.Date
	//   * []time.Time
	// If this option is used with rows that contains an array with
	// at least one NULL element, the decoding will fail.
	// This option has no effect on arrays of type JSON, NUMERIC and BYTES.
	DecodeToNativeArrays bool

	// TransactionOptions are the transaction options that will be used for
	// the transaction that is started by the statement.
	TransactionOptions spanner.TransactionOptions
	// QueryOptions are the query options that will be used for the statement.
	QueryOptions spanner.QueryOptions

	// PartitionedQueryOptions are used for partitioned queries, and ignored
	// for all other statements.
	PartitionedQueryOptions PartitionedQueryOptions

	// AutoCommitDMLMode determines the type of transaction that DML statements
	// that are executed outside explicit transactions use.
	AutocommitDMLMode AutocommitDMLMode
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

	// AutoConfigEmulator automatically creates a connection for the emulator
	// and also automatically creates the Instance and Database on the emulator.
	// Setting this option to true will:
	// 1. Set the SPANNER_EMULATOR_HOST environment variable to either Host or
	//    'localhost:9010' if no other host has been set.
	// 2. Use plain text communication and NoCredentials.
	// 3. Automatically create the Instance and the Database on the emulator if
	//    any of those do not yet exist.
	AutoConfigEmulator bool

	// Params contains key/value pairs for commonly used configuration parameters
	// for connections. The valid values are the same as the parameters that can
	// be added to a connection string.
	Params map[string]string

	// The initial values for automatic DML batching.
	// The values in the Params map take precedence above these.
	AutoBatchDml                               bool
	AutoBatchDmlUpdateCount                    int64
	DisableAutoBatchDmlUpdateCountVerification bool

	// DecodeToNativeArrays determines whether arrays that have a Go native
	// type should be decoded to those types rather than the corresponding
	// spanner.NullTypeName type.
	// Enabling this option will for example decode ARRAY<BOOL> to []bool instead
	// of []spanner.NullBool.
	//
	// See ExecOptions.DecodeToNativeArrays for more information.
	DecodeToNativeArrays bool

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
	if strval, ok := connectorConfig.Params[strings.ToLower("AutoBatchDml")]; ok {
		if val, err := strconv.ParseBool(strval); err == nil {
			connectorConfig.AutoBatchDml = val
		}
	}
	if strval, ok := connectorConfig.Params[strings.ToLower("AutoBatchDmlUpdateCount")]; ok {
		if val, err := strconv.ParseInt(strval, 10, 64); err == nil {
			connectorConfig.AutoBatchDmlUpdateCount = val
		}
	}
	if strval, ok := connectorConfig.Params[strings.ToLower("AutoBatchDmlUpdateCountVerification")]; ok {
		if val, err := strconv.ParseBool(strval); err == nil {
			connectorConfig.DisableAutoBatchDmlUpdateCountVerification = !val
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
	if strval, ok := connectorConfig.Params[strings.ToLower("DecodeToNativeArrays")]; ok {
		if val, err := strconv.ParseBool(strval); err == nil {
			connectorConfig.DecodeToNativeArrays = val
		}
	}
	if strval, ok := connectorConfig.Params[strings.ToLower("AutoConfigEmulator")]; ok {
		if val, err := strconv.ParseBool(strval); err == nil {
			connectorConfig.AutoConfigEmulator = val
		}
	}

	// Check if it is Spanner gorm that is creating the connection.
	// If so, we should set a different user-agent header than the
	// default go-sql-spanner header.
	callers := make([]uintptr, 20)
	length := runtime.Callers(0, callers)
	frames := runtime.CallersFrames(callers[0:length])
	gorm := false
	for frame, more := frames.Next(); more; {
		if strings.HasPrefix(frame.Function, gormModule) {
			gorm = true
			break
		}
		frame, more = frames.Next()
	}
	if gorm {
		config.UserAgent = spannerGormHeader()
	} else {
		config.UserAgent = userAgent
	}
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
	if connectorConfig.AutoConfigEmulator {
		if err := autoConfigEmulator(context.Background(), connectorConfig.Host, connectorConfig.Project, connectorConfig.Instance, connectorConfig.Database); err != nil {
			return nil, err
		}
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

func spannerGormHeader() string {
	info, ok := debug.ReadBuildInfo()
	if !ok {
		return gormUserAgent
	}
	for _, module := range info.Deps {
		if module.Path == gormModule {
			return fmt.Sprintf("%s/%s", gormUserAgent, module.Version)
		}
	}
	return gormUserAgent
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
	connection := &conn{
		connector:   c,
		client:      c.client,
		adminClient: c.adminClient,
		connId:      connId,
		logger:      logger,
		database:    databaseName,
		retryAborts: c.retryAbortsInternally,

		autoBatchDml:                        c.connectorConfig.AutoBatchDml,
		autoBatchDmlUpdateCount:             c.connectorConfig.AutoBatchDmlUpdateCount,
		autoBatchDmlUpdateCountVerification: !c.connectorConfig.DisableAutoBatchDmlUpdateCountVerification,

		execSingleQuery:              queryInSingleUse,
		execSingleQueryTransactional: queryInNewRWTransaction,
		execSingleDMLTransactional:   execInNewRWTransaction,
		execSingleDMLPartitioned:     execAsPartitionedDML,
	}
	// Initialize the session.
	_ = connection.ResetSession(context.Background())
	return connection, nil
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
		err = protected(ctx, tx, f)
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
func protected(ctx context.Context, tx *sql.Tx, f func(ctx context.Context, tx *sql.Tx) error) (err error) {
	defer func() {
		if x := recover(); x != nil {
			err = spanner.ToSpannerError(status.Errorf(codes.Unknown, "transaction function panic: %v", x))
		}
	}()
	return f(ctx, tx)
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

// AutocommitDMLMode indicates whether a single DML statement should be executed
// in a normal atomic transaction or as a Partitioned DML statement.
// See https://cloud.google.com/spanner/docs/dml-partitioned for more information.
type AutocommitDMLMode int

func (mode AutocommitDMLMode) String() string {
	switch mode {
	case Unspecified:
		return "Unspecified"
	case Transactional:
		return "Transactional"
	case PartitionedNonAtomic:
		return "Partitioned_Non_Atomic"
	}
	return ""
}

const (
	// Unspecified DML mode uses the default of the current connection.
	Unspecified AutocommitDMLMode = iota

	// Transactional DML mode uses a regular, atomic read/write transaction to
	// execute the DML statement.
	Transactional

	// PartitionedNonAtomic mode uses a Partitioned DML transaction to execute
	// the DML statement. Partitioned DML transactions are not guaranteed to be
	// atomic, but allow the statement to exceed the transactional mutation limit.
	PartitionedNonAtomic
)

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

func withBatchReadOnly(level driver.IsolationLevel) driver.IsolationLevel {
	return driver.IsolationLevel(levelBatchReadOnly)<<8 + level
}
