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
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/big"
	"os"
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
	"cloud.google.com/go/spanner/admin/database/apiv1/databasepb"
	"cloud.google.com/go/spanner/apiv1/spannerpb"
	"github.com/google/uuid"
	"github.com/googleapis/gax-go/v2"
	"github.com/googleapis/go-sql-spanner/connectionstate"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
	"google.golang.org/api/option/internaloption"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

const userAgent = "go-sql-spanner/1.17.0" // x-release-please-version

const gormModule = "github.com/googleapis/go-gorm-spanner"
const gormUserAgent = "go-gorm-spanner"

const DefaultStatementCacheSize = 1000

var defaultStatementCacheSize int

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
//     - minSessions (DEPRECATED): The minimum number of sessions in the backing session pool. The default is 100. This option is deprecated, as the driver by default uses a single multiplexed session for all operations.
//     - maxSessions (DEPRECATED): The maximum number of sessions in the backing session pool. The default is 400. This option is deprecated, as the driver by default uses a single multiplexed session for all operations.
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
	determineDefaultStatementCacheSize()
}

func determineDefaultStatementCacheSize() {
	if defaultCacheSizeString, ok := os.LookupEnv("SPANNER_DEFAULT_STATEMENT_CACHE_SIZE"); ok {
		if defaultCacheSize, err := strconv.Atoi(defaultCacheSizeString); err == nil {
			defaultStatementCacheSize = defaultCacheSize
		} else {
			defaultStatementCacheSize = DefaultStatementCacheSize
		}
	} else {
		defaultStatementCacheSize = DefaultStatementCacheSize
	}
}

// SpannerResult is the result type returned by Spanner connections for
// DML batches. This interface extends the standard sql.Result interface
// and adds a BatchRowsAffected function that returns the affected rows
// per statement.
type SpannerResult interface {
	driver.Result

	// BatchRowsAffected returns the affected rows per statement in a DML batch.
	// It returns an error if the statement was not a DML batch.
	BatchRowsAffected() ([]int64, error)
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
	// TimestampBound is the timestamp bound that will be used for the statement
	// if it is a query outside a transaction. Setting this option will override
	// the default TimestampBound that is set on the connection.
	TimestampBound *spanner.TimestampBound

	// PartitionedQueryOptions are used for partitioned queries, and ignored
	// for all other statements.
	PartitionedQueryOptions PartitionedQueryOptions

	// AutoCommitDMLMode determines the type of transaction that DML statements
	// that are executed outside explicit transactions use.
	AutocommitDMLMode AutocommitDMLMode

	// ReturnResultSetMetadata instructs the driver to return an additional result
	// set with the full spannerpb.ResultSetMetadata of the query. This result set
	// contains one row and one column, and the value in that cell is the
	// spannerpb.ResultSetMetadata that was returned by Spanner when executing the
	// query. This result set will be the first result set in the sql.Rows object
	// that is returned.
	//
	// You have to call [sql.Rows.NextResultSet] to move to the result set that
	// contains the actual query data.
	ReturnResultSetMetadata bool

	// ReturnResultSetStats instructs the driver to return an additional result
	// set with the full spannerpb.ResultSetStats of the query. This result set
	// contains one row and one column, and the value in that cell is the
	// spannerpb.ResultSetStats that was returned by Spanner when executing the
	// query. This result set will be the last result set in the sql.Rows object
	// that is returned.
	//
	// You have to call [sql.Rows.NextResultSet] after fetching all query data in
	// order to move to the result set that contains the spannerpb.ResultSetStats.
	ReturnResultSetStats bool

	// DirectExecute determines whether a query is executed directly when the
	// [sql.DB.QueryContext] method is called, or whether the actual query execution
	// is delayed until the first call to [sql.Rows.Next]. The default is to delay
	// the execution. Set this flag to true to execute the query directly when
	// [sql.DB.QueryContext] is called.
	DirectExecuteQuery bool
}

func (dest *ExecOptions) merge(src *ExecOptions) {
	if src == nil || dest == nil {
		return
	}
	if src.DecodeOption != DecodeOptionNormal {
		dest.DecodeOption = src.DecodeOption
	}
	if src.DecodeToNativeArrays {
		dest.DecodeToNativeArrays = src.DecodeToNativeArrays
	}
	if src.ReturnResultSetStats {
		dest.ReturnResultSetStats = src.ReturnResultSetStats
	}
	if src.ReturnResultSetMetadata {
		dest.ReturnResultSetMetadata = src.ReturnResultSetMetadata
	}
	if src.DirectExecuteQuery {
		dest.DirectExecuteQuery = src.DirectExecuteQuery
	}
	if src.AutocommitDMLMode != Unspecified {
		dest.AutocommitDMLMode = src.AutocommitDMLMode
	}
	if src.TimestampBound != nil {
		dest.TimestampBound = src.TimestampBound
	}
	(&dest.PartitionedQueryOptions).merge(&src.PartitionedQueryOptions)
	mergeQueryOptions(&dest.QueryOptions, &src.QueryOptions)
	mergeTransactionOptions(&dest.TransactionOptions, &src.TransactionOptions)
}

func mergeTransactionOptions(dest *spanner.TransactionOptions, src *spanner.TransactionOptions) {
	if src == nil || dest == nil {
		return
	}
	if src.ExcludeTxnFromChangeStreams {
		dest.ExcludeTxnFromChangeStreams = src.ExcludeTxnFromChangeStreams
	}
	if src.TransactionTag != "" {
		dest.TransactionTag = src.TransactionTag
	}
	if src.ReadLockMode != spannerpb.TransactionOptions_ReadWrite_READ_LOCK_MODE_UNSPECIFIED {
		dest.ReadLockMode = src.ReadLockMode
	}
	if src.IsolationLevel != spannerpb.TransactionOptions_ISOLATION_LEVEL_UNSPECIFIED {
		dest.IsolationLevel = src.IsolationLevel
	}
	if src.CommitPriority != spannerpb.RequestOptions_PRIORITY_UNSPECIFIED {
		dest.CommitPriority = src.CommitPriority
	}
	if src.BeginTransactionOption != spanner.DefaultBeginTransaction {
		dest.BeginTransactionOption = src.BeginTransactionOption
	}
	mergeCommitOptions(&dest.CommitOptions, &src.CommitOptions)
}

func mergeCommitOptions(dest *spanner.CommitOptions, src *spanner.CommitOptions) {
	if src == nil || dest == nil {
		return
	}
	if src.ReturnCommitStats {
		dest.ReturnCommitStats = src.ReturnCommitStats
	}
	if src.MaxCommitDelay != nil {
		dest.MaxCommitDelay = src.MaxCommitDelay
	}
}

func mergeQueryOptions(dest *spanner.QueryOptions, src *spanner.QueryOptions) {
	if src == nil || dest == nil {
		return
	}
	if src.ExcludeTxnFromChangeStreams {
		dest.ExcludeTxnFromChangeStreams = src.ExcludeTxnFromChangeStreams
	}
	if src.DataBoostEnabled {
		dest.DataBoostEnabled = src.DataBoostEnabled
	}
	if src.LastStatement {
		dest.LastStatement = src.LastStatement
	}
	if src.Options != nil {
		if dest.Options != nil {
			proto.Merge(dest.Options, src.Options)
		} else {
			dest.Options = src.Options
		}
	}
	if src.DirectedReadOptions != nil {
		if dest.DirectedReadOptions == nil {
			dest.DirectedReadOptions = src.DirectedReadOptions
		} else {
			proto.Merge(dest.DirectedReadOptions, src.DirectedReadOptions)
		}
	}
	if src.Mode != nil {
		dest.Mode = src.Mode
	}
	if src.Priority != spannerpb.RequestOptions_PRIORITY_UNSPECIFIED {
		dest.Priority = src.Priority
	}
	if src.RequestTag != "" {
		dest.RequestTag = src.RequestTag
	}
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

	// StatementCacheSize is the size of the internal cache that is used for
	// connectors that are created from this ConnectorConfig. This cache stores
	// the result of parsing SQL statements for query parameters and the type of
	// statement (Query / DML / DDL).
	// The default size is 1000. This default can also be overridden by setting
	// the environment variable SPANNER_DEFAULT_STATEMENT_CACHE_SIZE.
	StatementCacheSize int
	// DisableStatementCache disables the use of a statement cache.
	DisableStatementCache bool

	// AutoConfigEmulator automatically creates a connection for the emulator
	// and also automatically creates the Instance and Database on the emulator.
	// Setting this option to true will:
	// 1. Set the SPANNER_EMULATOR_HOST environment variable to either Host or
	//    'localhost:9010' if no other host has been set.
	// 2. Use plain text communication and NoCredentials.
	// 3. Automatically create the Instance and the Database on the emulator if
	//    any of those do not yet exist.
	AutoConfigEmulator bool

	// ConnectionStateType determines the behavior of changes to connection state
	// during a transaction.
	// connectionstate.TypeTransactional means that changes during a transaction
	// are only persisted if the transaction is committed. If the transaction is
	// rolled back, any changes to the connection state during the transaction
	// will be lost.
	// connectionstate.TypeNonTransactional means that changes to the connection
	// state during a transaction are persisted directly, and are always visible
	// after the transaction, regardless whether the transaction committed or
	// rolled back.
	ConnectionStateType connectionstate.Type
	// Params contains key/value pairs for commonly used configuration parameters
	// for connections. The valid values are the same as the parameters that can
	// be added to a connection string.
	Params map[string]string

	// The initial values for automatic DML batching.
	// The values in the Params map take precedence above these.
	AutoBatchDml                               bool
	AutoBatchDmlUpdateCount                    int64
	DisableAutoBatchDmlUpdateCountVerification bool

	// IsolationLevel is the default isolation level for read/write transactions.
	IsolationLevel sql.IsolationLevel

	// BeginTransactionOption determines the default for how to begin transactions.
	// The Spanner database/sql driver uses spanner.InlinedBeginTransaction by default
	// for both read-only and read/write transactions.
	BeginTransactionOption spanner.BeginTransactionOption

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
	Configurator func(config *spanner.ClientConfig, opts *[]option.ClientOption) `json:"-"`
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
	driver                *Driver
	connectorConfig       ConnectorConfig
	initialPropertyValues map[string]connectionstate.ConnectionPropertyValue
	cacheKey              string
	logger                *slog.Logger

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
	parser         *statementParser
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
	initialPropertyValues, err := connectionstate.ExtractValues(connectionProperties, connectorConfig.Params)
	if err != nil {
		return nil, err
	}
	state := createConfiguredConnectionState(initialPropertyValues)

	assignPropertyValueIfExists(state, propertyEndpoint, &connectorConfig.Host)
	if connectorConfig.Host != "" {
		opts = append(opts, option.WithEndpoint(connectorConfig.Host))
	}
	if val := propertyCredentials.GetValueOrDefault(state); val != "" {
		opts = append(opts, option.WithCredentialsFile(val))
	}
	if val := propertyCredentialsJson.GetValueOrDefault(state); val != "" {
		opts = append(opts, option.WithCredentialsJSON([]byte(val)))
	}
	config := spanner.ClientConfig{
		SessionPoolConfig: spanner.DefaultSessionPoolConfig,
	}
	if propertyUsePlainText.GetValueOrDefault(state) {
		opts = append(opts,
			option.WithGRPCDialOption(grpc.WithTransportCredentials(insecure.NewCredentials())),
			option.WithoutAuthentication())
		config.DisableNativeMetrics = true
	}
	assignPropertyValueIfExists(state, propertyAutoBatchDml, &connectorConfig.AutoBatchDml)
	assignPropertyValueIfExists(state, propertyAutoBatchDmlUpdateCount, &connectorConfig.AutoBatchDmlUpdateCount)
	assignNegatedPropertyValueIfExists(state, propertyAutoBatchDmlUpdateCountVerification, &connectorConfig.DisableAutoBatchDmlUpdateCountVerification)
	assignPropertyValueIfExists(state, propertyMinSessions, &config.MinOpened)
	assignPropertyValueIfExists(state, propertyMaxSessions, &config.MaxOpened)
	if val := propertyNumChannels.GetValueOrDefault(state); val > 0 {
		opts = append(opts, option.WithGRPCConnectionPool(val))
	}
	assignPropertyValueIfExists(state, propertyRpcPriority, &config.ReadOptions.Priority)
	assignPropertyValueIfExists(state, propertyRpcPriority, &config.TransactionOptions.CommitPriority)
	assignPropertyValueIfExists(state, propertyRpcPriority, &config.QueryOptions.Priority)

	config.QueryOptions.Options = &spannerpb.ExecuteSqlRequest_QueryOptions{}
	assignPropertyValueIfExists(state, propertyOptimizerVersion, &config.QueryOptions.Options.OptimizerVersion)
	assignPropertyValueIfExists(state, propertyOptimizerStatisticsPackage, &config.QueryOptions.Options.OptimizerStatisticsPackage)
	if config.QueryOptions.Options.OptimizerVersion == "" && config.QueryOptions.Options.OptimizerStatisticsPackage == "" {
		config.QueryOptions.Options = nil
	}

	assignPropertyValueIfExists(state, propertyDatabaseRole, &config.DatabaseRole)
	assignPropertyValueIfExists(state, propertyDisableRouteToLeader, &config.DisableRouteToLeader)
	assignPropertyValueIfExists(state, propertyEnableEndToEndTracing, &config.EnableEndToEndTracing)
	assignPropertyValueIfExists(state, propertyDisableNativeMetrics, &config.DisableNativeMetrics)
	assignPropertyValueIfExists(state, propertyDecodeToNativeArrays, &connectorConfig.DecodeToNativeArrays)
	assignPropertyValueIfExists(state, propertyAutoConfigEmulator, &connectorConfig.AutoConfigEmulator)
	assignPropertyValueIfExists(state, propertyIsolationLevel, &connectorConfig.IsolationLevel)
	assignPropertyValueIfExists(state, propertyBeginTransactionOption, &connectorConfig.BeginTransactionOption)
	assignPropertyValueIfExists(state, propertyStatementCacheSize, &connectorConfig.StatementCacheSize)
	assignPropertyValueIfExists(state, propertyDisableStatementCache, &connectorConfig.DisableStatementCache)

	// Check if it is Spanner gorm that is creating the connection.
	// If so, we should set a different user-agent header than the
	// default go-sql-spanner header.
	if isConnectionFromGorm() {
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
		if connectorConfig.Host == "" {
			connectorConfig.Host = "localhost:9010"
		}
		schemeRemoved := regexp.MustCompile("^(http://|https://|passthrough:///)").ReplaceAllString(connectorConfig.Host, "")
		emulatorOpts := []option.ClientOption{
			option.WithEndpoint("passthrough:///" + schemeRemoved),
			option.WithGRPCDialOption(grpc.WithTransportCredentials(insecure.NewCredentials())),
			option.WithoutAuthentication(),
			internaloption.SkipDialSettingsValidation(),
		}
		opts = append(emulatorOpts, opts...)
		if err := autoConfigEmulator(context.Background(), connectorConfig.Host, connectorConfig.Project, connectorConfig.Instance, connectorConfig.Database, opts); err != nil {
			return nil, err
		}
	}

	c := &connector{
		driver:                d,
		connectorConfig:       connectorConfig,
		initialPropertyValues: initialPropertyValues,
		logger:                logger,
		spannerClientConfig:   config,
		options:               opts,
		// TODO: Remove retryAbortsInternally from this struct
		retryAbortsInternally: propertyRetryAbortsInternally.GetValueOrDefault(state),
	}
	addStateFromConnectorConfig(c, c.initialPropertyValues)
	return c, nil
}

func assignPropertyValueIfExists[T comparable](state *connectionstate.ConnectionState, property *connectionstate.TypedConnectionProperty[T], field *T) {
	if val, _, err := property.GetValueOrError(state); err == nil {
		*field = val
	}
}

func assignNegatedPropertyValueIfExists(state *connectionstate.ConnectionState, property *connectionstate.TypedConnectionProperty[bool], field *bool) {
	if val, _, err := property.GetValueOrError(state); err == nil {
		*field = !val
	}
}

func isConnectionFromGorm() bool {
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
	return gorm
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
	connectionStateType := c.connectorConfig.ConnectionStateType
	if connectionStateType == connectionstate.TypeDefault {
		if c.parser.dialect == databasepb.DatabaseDialect_POSTGRESQL {
			connectionStateType = connectionstate.TypeTransactional
		} else {
			connectionStateType = connectionstate.TypeNonTransactional
		}
	}
	connection := &conn{
		parser:      c.parser,
		connector:   c,
		client:      c.client,
		adminClient: c.adminClient,
		connId:      connId,
		logger:      logger,
		database:    databaseName,
		state:       createInitialConnectionState(connectionStateType, c.initialPropertyValues),

		execSingleQuery:              queryInSingleUse,
		execSingleQueryTransactional: queryInNewRWTransaction,
		execSingleDMLTransactional:   execInNewRWTransaction,
		execSingleDMLPartitioned:     execAsPartitionedDML,
	}
	// Initialize the session.
	_ = connection.ResetSession(context.Background())
	return connection, nil
}

func addStateFromConnectorConfig(c *connector, values map[string]connectionstate.ConnectionPropertyValue) {
	updateConnectionPropertyValueIfNotExists(propertyIsolationLevel, values, c.connectorConfig.IsolationLevel)
	updateConnectionPropertyValueIfNotExists(propertyBeginTransactionOption, values, c.connectorConfig.BeginTransactionOption)
	updateConnectionPropertyValueIfNotExists(propertyRetryAbortsInternally, values, c.retryAbortsInternally)
	updateConnectionPropertyValueIfNotExists(propertyAutoBatchDml, values, c.connectorConfig.AutoBatchDml)
	updateConnectionPropertyValueIfNotExists(propertyAutoBatchDmlUpdateCount, values, c.connectorConfig.AutoBatchDmlUpdateCount)
	updateConnectionPropertyValueIfNotExists(propertyAutoBatchDmlUpdateCountVerification, values, !c.connectorConfig.DisableAutoBatchDmlUpdateCountVerification)
}

func updateConnectionPropertyValueIfNotExists[T comparable](property *connectionstate.TypedConnectionProperty[T], values map[string]connectionstate.ConnectionPropertyValue, value T) {
	if _, ok := values[property.Key()]; !ok {
		values[property.Key()] = property.CreateTypedInitialValue(value)
	}
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
		c.logger.Log(ctx, LevelNotice, "fetching database dialect")
		closeClient := func() {
			c.client.Close()
			c.client = nil
		}
		if dialect, err := determineDialect(ctx, c.client); err != nil {
			closeClient()
			return err
		} else {
			// Create a separate statement parser and cache per connector.
			cacheSize := c.connectorConfig.StatementCacheSize
			if c.connectorConfig.DisableStatementCache {
				cacheSize = 0
			} else if c.connectorConfig.StatementCacheSize == 0 {
				cacheSize = defaultStatementCacheSize
			}
			c.parser, err = newStatementParser(dialect, cacheSize)
			if err != nil {
				closeClient()
				return err
			}
		}

		c.logger.Log(ctx, LevelNotice, "creating Spanner Admin client")
		c.adminClient, c.adminClientErr = adminapi.NewDatabaseAdminClient(ctx, opts...)
		if c.adminClientErr != nil {
			closeClient()
			c.adminClient = nil
			return c.adminClientErr
		}
	}

	c.connCount++
	c.logger.DebugContext(ctx, "increased conn count", "connCount", c.connCount)
	return nil
}

func determineDialect(ctx context.Context, client *spanner.Client) (databasepb.DatabaseDialect, error) {
	it := client.Single().Query(ctx, spanner.Statement{SQL: "select option_value from information_schema.database_options where option_name='database_dialect'"})
	defer it.Stop()
	for {
		if row, err := it.Next(); err != nil {
			return databasepb.DatabaseDialect_DATABASE_DIALECT_UNSPECIFIED, err
		} else {
			var dialectName string
			if err := row.Columns(&dialectName); err != nil {
				return databasepb.DatabaseDialect_DATABASE_DIALECT_UNSPECIFIED, err
			}
			if _, err := it.Next(); !errors.Is(err, iterator.Done) {
				if err == nil {
					return databasepb.DatabaseDialect_DATABASE_DIALECT_UNSPECIFIED, fmt.Errorf("more than one dialect result returned")
				} else {
					return databasepb.DatabaseDialect_DATABASE_DIALECT_UNSPECIFIED, err
				}
			}
			if dialect, ok := databasepb.DatabaseDialect_value[dialectName]; ok {
				return databasepb.DatabaseDialect(dialect), nil
			} else {
				return databasepb.DatabaseDialect_DATABASE_DIALECT_UNSPECIFIED, fmt.Errorf("unknown database dialect: %s", dialectName)
			}
		}
	}
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
	_, err := runTransactionWithOptions(ctx, db, opts, f, spanner.TransactionOptions{})
	return err
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
	_, err := runTransactionWithOptions(ctx, db, opts, f, spannerOptions)
	return err
}

// RunTransactionWithCommitResponse runs the given function in a transaction on
// the given database. If the connection is a connection to a Spanner database,
// the transaction will automatically be retried if the transaction is aborted
// by Spanner. Any other errors will be propagated to the caller and the
// transaction will be rolled back. The transaction will be committed if the
// supplied function did not return an error.
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
// This function returns a spanner.CommitResponse if the transaction committed
// successfully.
//
// This function will never return ErrAbortedDueToConcurrentModification.
func RunTransactionWithCommitResponse(ctx context.Context, db *sql.DB, opts *sql.TxOptions, f func(ctx context.Context, tx *sql.Tx) error, spannerOptions spanner.TransactionOptions) (*spanner.CommitResponse, error) {
	return runTransactionWithOptions(ctx, db, opts, f, spannerOptions)
}

func runTransactionWithOptions(ctx context.Context, db *sql.DB, opts *sql.TxOptions, f func(ctx context.Context, tx *sql.Tx) error, spannerOptions spanner.TransactionOptions) (*spanner.CommitResponse, error) {
	// Get a connection from the pool that we can use to run a transaction.
	// Getting a connection here already makes sure that we can reserve this
	// connection exclusively for the duration of this method. That again
	// allows us to temporarily change the state of the connection (e.g. set
	// the retryAborts flag to false).
	conn, err := db.Conn(ctx)
	if err != nil {
		return nil, err
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
		return nil, err
	}

	tx, err := conn.BeginTx(ctx, opts)
	if err != nil {
		return nil, err
	}
	for {
		err = protected(ctx, tx, f)
		errDuringCommit := false
		if err == nil {
			err = tx.Commit()
			if err == nil {
				resp, err := getCommitResponse(conn)
				if err != nil {
					return nil, err
				}
				return resp, nil
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
			return nil, err
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
				return nil, err
			}
		}

		// Reset the transaction after it was aborted.
		err = resetTransactionForRetry(ctx, conn, errDuringCommit)
		if err != nil {
			_ = tx.Rollback()
			return nil, err
		}
		// This does not actually start a new transaction, instead it
		// continues with the previous transaction that was already reset.
		// We need to do this, because the sql package registers the
		// transaction as 'done' when Commit has been called, also if the
		// commit fails.
		if errDuringCommit {
			tx, err = conn.BeginTx(ctx, opts)
			if err != nil {
				return nil, err
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

func getCommitResponse(conn *sql.Conn) (resp *spanner.CommitResponse, err error) {
	if err := conn.Raw(func(driverConn any) error {
		spannerConn, ok := driverConn.(SpannerConn)
		if !ok {
			return spanner.ToSpannerError(status.Error(codes.InvalidArgument, "not a Spanner connection"))
		}
		resp, err = spannerConn.CommitResponse()
		return err
	}); err != nil {
		return nil, err
	}
	return resp, nil
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
	return BeginReadWriteTransactionOnConn(ctx, conn, options)
}

func BeginReadWriteTransactionOnConn(ctx context.Context, conn *sql.Conn, options ReadWriteTransactionOptions) (*sql.Tx, error) {
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
	TimestampBound         spanner.TimestampBound
	BeginTransactionOption spanner.BeginTransactionOption

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
	return BeginReadOnlyTransactionOnConn(ctx, conn, options)
}

func BeginReadOnlyTransactionOnConn(ctx context.Context, conn *sql.Conn, options ReadOnlyTransactionOptions) (*sql.Tx, error) {
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

// DmlBatch is used to execute a batch of DML statements on Spanner in a single round-trip.
type DmlBatch interface {
	// ExecContext buffers the given statement for execution on Spanner.
	// All buffered statements are sent to Spanner as a single request when the DmlBatch
	// function returns successfully.
	ExecContext(ctx context.Context, dml string, args ...any) error
}

var _ DmlBatch = &dmlBatch{}

type dmlBatch struct {
	conn *sql.Conn
}

// ExecuteBatchDml executes a batch of DML statements in a single round-trip to Spanner.
func ExecuteBatchDml(ctx context.Context, db *sql.DB, f func(ctx context.Context, batch DmlBatch) error) (SpannerResult, error) {
	conn, err := db.Conn(ctx)
	if err != nil {
		return nil, err
	}
	return ExecuteBatchDmlOnConn(ctx, conn, f)
}

// ExecuteBatchDmlOnConn executes a batch of DML statements on a specific connection in a single round-trip to Spanner.
func ExecuteBatchDmlOnConn(ctx context.Context, connection *sql.Conn, f func(ctx context.Context, batch DmlBatch) error) (SpannerResult, error) {
	// Start the DML batch.
	if err := connection.Raw(func(driverConn any) error {
		c, ok := driverConn.(*conn)
		if !ok {
			return spanner.ToSpannerError(status.Error(codes.InvalidArgument, "connection is not a Spanner connection"))
		}
		if _, err := c.startBatchDML(false); err != nil {
			return err
		}
		return nil
	}); err != nil {
		return nil, err
	}

	// Let the callback execute the statements on the batch.
	b := &dmlBatch{conn: connection}
	if err := f(ctx, b); err != nil {
		// The callback returned an error, abort the batch.
		_ = connection.Raw(func(driverConn any) error {
			c, _ := driverConn.(*conn)
			_ = c.AbortBatch()
			return nil
		})
		return nil, err
	}

	// Send the batch to Spanner.
	var res SpannerResult
	if err := connection.Raw(func(driverConn any) error {
		// We know that the connection is a Spanner connection, so we don't bother to check that again here.
		c, _ := driverConn.(*conn)
		var err error
		res, err = c.runDMLBatch(ctx)
		if err != nil {
			// Make sure the batch is removed from the connection/transaction.
			_ = c.AbortBatch()
			return err
		}
		return nil
	}); err != nil {
		return nil, err
	}

	return res, nil
}

func (b *dmlBatch) ExecContext(ctx context.Context, dml string, args ...any) error {
	if err := b.conn.Raw(func(driverConn any) error {
		c, ok := driverConn.(*conn)
		if !ok {
			return spanner.ToSpannerError(status.Error(codes.InvalidArgument, "connection is not a Spanner connection"))
		}
		if !c.InDMLBatch() {
			return spanner.ToSpannerError(status.Errorf(codes.FailedPrecondition, "this batch is no longer active"))
		}
		return nil
	}); err != nil {
		return err
	}
	_, err := b.conn.ExecContext(ctx, dml, args...)
	return err
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
	case uuid.UUID:
	case *uuid.UUID:
	case []uuid.UUID:
	case []*uuid.UUID:
	case spanner.NullUUID:
	case []spanner.NullUUID:
	}
	return true
}

func parseBeginTransactionOption(val string) (spanner.BeginTransactionOption, error) {
	switch strings.ToLower(val) {
	case strings.ToLower("DefaultBeginTransaction"):
		return spanner.DefaultBeginTransaction, nil
	case strings.ToLower("InlinedBeginTransaction"):
		return spanner.InlinedBeginTransaction, nil
	case strings.ToLower("ExplicitBeginTransaction"):
		return spanner.ExplicitBeginTransaction, nil
	}
	return spanner.DefaultBeginTransaction, spanner.ToSpannerError(status.Errorf(codes.InvalidArgument, "invalid or unsupported BeginTransactionOption: %v", val))
}

func parseConnectionStateType(val string) (connectionstate.Type, error) {
	switch strings.ToLower(val) {
	case strings.ToLower("Default"):
		return connectionstate.TypeDefault, nil
	case strings.ToLower("Transactional"):
		return connectionstate.TypeTransactional, nil
	case strings.ToLower("NonTransactional"):
		return connectionstate.TypeNonTransactional, nil
	}
	return connectionstate.TypeDefault, status.Errorf(codes.InvalidArgument, "invalid or unsupported connection state type: %v", val)
}

func parseAutocommitDmlMode(val string) (AutocommitDMLMode, error) {
	switch strings.ToLower(val) {
	case strings.ToLower("Transactional"):
		return Transactional, nil
	case strings.ToLower("PartitionedNonAtomic"):
		return PartitionedNonAtomic, nil
	}
	return Unspecified, status.Errorf(codes.InvalidArgument, "invalid or unsupported autocommit dml mode: %v", val)
}

func parseRpcPriority(val string) (spannerpb.RequestOptions_Priority, error) {
	val = strings.ToUpper(val)
	if priority, ok := spannerpb.RequestOptions_Priority_value[val]; ok {
		return spannerpb.RequestOptions_Priority(priority), nil
	}
	if !strings.HasPrefix(val, "PRIORITY_") {
		if priority, ok := spannerpb.RequestOptions_Priority_value["PRIORITY_"+val]; ok {
			return spannerpb.RequestOptions_Priority(priority), nil
		}
	}
	return spannerpb.RequestOptions_PRIORITY_UNSPECIFIED, status.Errorf(codes.InvalidArgument, "invalid or unsupported priority: %v", val)
}

func parseIsolationLevel(val string) (sql.IsolationLevel, error) {
	switch strings.Replace(strings.ToLower(strings.TrimSpace(val)), " ", "_", 1) {
	case "default":
		return sql.LevelDefault, nil
	case "read_uncommitted":
		return sql.LevelReadUncommitted, nil
	case "read_committed":
		return sql.LevelReadCommitted, nil
	case "write_committed":
		return sql.LevelWriteCommitted, nil
	case "repeatable_read":
		return sql.LevelRepeatableRead, nil
	case "snapshot":
		return sql.LevelSnapshot, nil
	case "serializable":
		return sql.LevelSerializable, nil
	case "linearizable":
		return sql.LevelLinearizable, nil
	}
	return sql.LevelDefault, spanner.ToSpannerError(status.Errorf(codes.InvalidArgument, "invalid or unsupported isolation level: %v", val))
}

func toProtoIsolationLevel(level sql.IsolationLevel) (spannerpb.TransactionOptions_IsolationLevel, error) {
	switch level {
	case sql.LevelSerializable:
		return spannerpb.TransactionOptions_SERIALIZABLE, nil
	case sql.LevelRepeatableRead:
		return spannerpb.TransactionOptions_REPEATABLE_READ, nil
	case sql.LevelSnapshot:
		return spannerpb.TransactionOptions_REPEATABLE_READ, nil
	case sql.LevelDefault:
		return spannerpb.TransactionOptions_ISOLATION_LEVEL_UNSPECIFIED, nil

	// Unsupported and unknown isolation levels.
	case sql.LevelReadUncommitted:
	case sql.LevelReadCommitted:
	case sql.LevelWriteCommitted:
	case sql.LevelLinearizable:
	default:
	}
	return spannerpb.TransactionOptions_ISOLATION_LEVEL_UNSPECIFIED, spanner.ToSpannerError(status.Errorf(codes.InvalidArgument, "invalid or unsupported isolation level: %v", level))
}

func toProtoIsolationLevelOrDefault(level sql.IsolationLevel) spannerpb.TransactionOptions_IsolationLevel {
	res, _ := toProtoIsolationLevel(level)
	return res
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
