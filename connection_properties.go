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
	"database/sql"
	"fmt"
	"strings"
	"time"

	"cloud.google.com/go/spanner"
	"cloud.google.com/go/spanner/apiv1/spannerpb"
	"github.com/googleapis/go-sql-spanner/connectionstate"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// connectionProperties contains all supported connection properties for Spanner.
// These properties are added to all connectionstate.ConnectionState instances that are created for Spanner connections.
var connectionProperties = map[string]connectionstate.ConnectionProperty{}

// The following variables define the various connectionstate.ConnectionProperty instances that are supported and used
// by the Spanner database/sql driver. They are defined as global variables, so they can be used directly in the driver
// to get/set the state of exactly that property.

var propertyConnectionStateType = createConnectionProperty(
	"connection_state_type",
	"The type of connection state to use for this connection. Can only be set at start up. "+
		"If no value is set, then the database dialect default will be used, "+
		"which is NON_TRANSACTIONAL for GoogleSQL and TRANSACTIONAL for PostgreSQL.",
	connectionstate.TypeDefault,
	false,
	[]connectionstate.Type{connectionstate.TypeDefault, connectionstate.TypeTransactional, connectionstate.TypeNonTransactional},
	connectionstate.ContextStartup,
	func(value string) (connectionstate.Type, error) {
		return parseConnectionStateType(value)
	},
)
var propertyAutoConfigEmulator = createConnectionProperty(
	"auto_config_emulator",
	"Automatically configure the connection to try to connect to the Cloud Spanner emulator (true/false). "+
		"The instance and database in the connection string will automatically be created if these do not yet exist "+
		"on the emulator. Add dialect=postgresql to the connection string to make sure that the database that is "+
		"created uses the PostgreSQL dialect.",
	false,
	false,
	nil,
	connectionstate.ContextStartup,
	connectionstate.ConvertBool,
)

var propertyIsolationLevel = createConnectionProperty(
	"isolation_level",
	"The transaction isolation level that is used by default for read/write transactions. The default is "+
		"LevelDefault, which means that the connection will use the default isolation level of the database that it is "+
		"connected to.",
	sql.LevelDefault,
	false,
	[]sql.IsolationLevel{sql.LevelSerializable, sql.LevelRepeatableRead, sql.LevelSnapshot, sql.LevelDefault},
	connectionstate.ContextUser,
	func(value string) (sql.IsolationLevel, error) {
		return parseIsolationLevel(value)
	},
)
var propertyReadLockMode = createConnectionProperty(
	"read_lock_mode",
	"This option controls the locking behavior for read operations and queries within a read/write transaction. "+
		"It works in conjunction with the transaction's isolation level.\n\n"+
		"PESSIMISTIC: Read locks are acquired immediately on read. This mode only applies to SERIALIZABLE isolation. "+
		"This mode prevents concurrent modifications by locking data throughout the transaction. This reduces commit-time "+
		"aborts due to conflicts, but can increase how long transactions wait for locks and the overall contention.\n\n"+
		"OPTIMISTIC: Locks for reads within the transaction are not acquired on read. Instead, the locks are acquired on "+
		"commit to validate that read/queried data has not changed since the transaction started. If a conflict is "+
		"detected, the transaction will fail. This mode only applies to SERIALIZABLE isolation. This mode defers locking "+
		"until commit, which can reduce contention and improve throughput. However, be aware that this increases the "+
		"risk of transaction aborts if there's significant write competition on the same data.\n\n"+
		"READ_LOCK_MODE_UNSPECIFIED: This is the default if no mode is set. The locking behavior depends on the isolation level:\n\n"+
		"REPEATABLE_READ: Locking semantics default to OPTIMISTIC. However, validation checks at commit are only "+
		"performed for queries using SELECT FOR UPDATE, statements with {@code LOCK_SCANNED_RANGES} hints, and DML statements.\n\n"+
		"For all other isolation levels: If the read lock mode is not set, it defaults to PESSIMISTIC locking.",
	spannerpb.TransactionOptions_ReadWrite_READ_LOCK_MODE_UNSPECIFIED,
	false,
	nil,
	connectionstate.ContextUser,
	func(value string) (spannerpb.TransactionOptions_ReadWrite_ReadLockMode, error) {
		name := strings.ToUpper(value)
		if _, ok := spannerpb.TransactionOptions_ReadWrite_ReadLockMode_value[name]; ok {
			return spannerpb.TransactionOptions_ReadWrite_ReadLockMode(spannerpb.TransactionOptions_ReadWrite_ReadLockMode_value[name]), nil
		}
		return spannerpb.TransactionOptions_ReadWrite_READ_LOCK_MODE_UNSPECIFIED, status.Errorf(codes.InvalidArgument, "unknown read lock mode: %v", value)
	},
)
var propertyBeginTransactionOption = createConnectionProperty(
	"begin_transaction_option",
	"BeginTransactionOption determines the default for how to begin transactions. "+
		"The driver uses spanner.InlinedBeginTransaction by default for both read-only and read/write transactions.",
	spanner.InlinedBeginTransaction,
	false,
	[]spanner.BeginTransactionOption{spanner.InlinedBeginTransaction, spanner.ExplicitBeginTransaction},
	connectionstate.ContextUser,
	func(value string) (spanner.BeginTransactionOption, error) {
		return parseBeginTransactionOption(value)
	},
)
var propertyAutocommitDmlMode = createConnectionProperty(
	"autocommit_dml_mode",
	"Determines the transaction type that is used to execute DML statements when the connection is in auto-commit mode.",
	Transactional,
	false,
	[]AutocommitDMLMode{Transactional, PartitionedNonAtomic},
	connectionstate.ContextUser,
	func(value string) (AutocommitDMLMode, error) {
		return parseAutocommitDmlMode(value)
	},
)
var propertyRetryAbortsInternally = createConnectionProperty(
	"retry_aborts_internally",
	"Should the connection automatically retry Aborted errors (true/false).",
	true,
	false,
	nil,
	connectionstate.ContextUser,
	connectionstate.ConvertBool,
)
var propertyReadOnlyStaleness = createConnectionProperty(
	"read_only_staleness",
	"The read-only staleness to use for read-only transactions and single-use queries.",
	spanner.TimestampBound{},
	false,
	nil,
	connectionstate.ContextUser,
	connectionstate.ConvertReadOnlyStaleness,
)

var propertyAutoBatchDml = createConnectionProperty(
	"auto_batch_dml",
	"Automatically buffer DML statements that are executed on this connection and execute them as one batch "+
		"when a non-DML statement is executed, or when the current transaction is committed. The update count that is "+
		"returned for DML statements that are buffered is by default 1. This default can be changed by setting the "+
		"connection variable auto_batch_dml_update_count to value other than 1. This setting is only in read/write "+
		"transactions. DML statements in auto-commit mode are executed directly.",
	false,
	false,
	nil,
	connectionstate.ContextUser,
	connectionstate.ConvertBool,
)
var propertyAutoBatchDmlUpdateCount = createConnectionProperty(
	"auto_batch_dml_update_count",
	"DML statements that are executed when auto_batch_dml is set to true, are not directly sent to Spanner, "+
		"but are buffered in the client until the batch is flushed. This property determines the update count that is "+
		"returned for these DML statements. The default is 1, as that is the update count that is expected by most ORMs "+
		"(e.g. gorm).",
	int64(1),
	false,
	nil,
	connectionstate.ContextUser,
	connectionstate.ConvertInt64,
)
var propertyAutoBatchDmlUpdateCountVerification = createConnectionProperty(
	"auto_batch_dml_update_count_verification",
	"The update count that is returned for DML statements that are buffered during an automatic DML batch is "+
		"by default 1. This value can be changed by setting the connection variable auto_batch_dml_update_count. The "+
		"update counts that are returned by Spanner when the DML statements are actually executed are verified against "+
		"the update counts that were returned when they were buffered. If these do not match, an error is returned. You "+
		"can disable this verification by setting auto_batch_dml_update_count_verification to false.",
	true,
	false,
	nil,
	connectionstate.ContextUser,
	connectionstate.ConvertBool,
)
var propertyRpcPriority = createConnectionProperty(
	"rpc_priority",
	"Sets the priority for all RPC invocations from this connection (HIGH/MEDIUM/LOW/UNSPECIFIED). "+
		"The default is UNSPECIFIED.",
	spannerpb.RequestOptions_PRIORITY_UNSPECIFIED,
	false,
	nil,
	connectionstate.ContextUser,
	func(value string) (spannerpb.RequestOptions_Priority, error) {
		return parseRpcPriority(value)
	},
)
var propertyOptimizerVersion = createConnectionProperty(
	"optimizer_version",
	"Sets the default query optimizer version to use for this connection.",
	"",
	false,
	nil,
	connectionstate.ContextUser,
	connectionstate.ConvertString,
)
var propertyOptimizerStatisticsPackage = createConnectionProperty(
	"optimizer_statistics_package",
	"Sets the query optimizer statistics package to use for this connection.",
	"",
	false,
	nil,
	connectionstate.ContextUser,
	connectionstate.ConvertString,
)
var propertyDecodeToNativeArrays = createConnectionProperty(
	"decode_to_native_arrays",
	"decode_to_native_arrays determines whether arrays that have a Go native type should be decoded to those "+
		"types rather than the corresponding spanner.NullTypeName type. Enabling this option will for example decode "+
		"ARRAY<BOOL> to []bool instead of []spanner.NullBool.",
	false,
	false,
	nil,
	connectionstate.ContextUser,
	connectionstate.ConvertBool,
)

// ------------------------------------------------------------------------------------------------
// Transaction connection properties.
// ------------------------------------------------------------------------------------------------

var propertyTransactionReadOnly = createConnectionProperty(
	"transaction_read_only",
	"transaction_read_only is the default read-only mode for transactions on this connection.",
	false,
	false,
	nil,
	connectionstate.ContextUser,
	connectionstate.ConvertBool,
)
var propertyTransactionDeferrable = createConnectionProperty(
	"transaction_deferrable",
	"transaction_deferrable is a no-op on Spanner. It is defined in this driver for compatibility with PostgreSQL.",
	false,
	false,
	nil,
	connectionstate.ContextUser,
	connectionstate.ConvertBool,
)
var propertyExcludeTxnFromChangeStreams = createConnectionProperty(
	"exclude_txn_from_change_streams",
	"exclude_txn_from_change_streams determines whether transactions on this connection should be excluded from "+
		"change streams. This property affects both normal read/write transactions, DML statements outside transactions, "+
		"and partitioned DML statements.",
	false,
	false,
	nil,
	connectionstate.ContextUser,
	connectionstate.ConvertBool,
)
var propertyTransactionTag = createConnectionProperty(
	"transaction_tag",
	"The transaction tag to add to the next read/write transaction on this connection.",
	"",
	false,
	nil,
	connectionstate.ContextUser,
	connectionstate.ConvertString,
)
var propertyMaxCommitDelay = createConnectionProperty(
	"max_commit_delay",
	"The max delay that Spanner may apply to commit requests to improve throughput.",
	time.Duration(0),
	false,
	nil,
	connectionstate.ContextUser,
	connectionstate.ConvertDuration,
)
var propertyCommitPriority = createConnectionProperty(
	"commit_priority",
	"Sets the priority for commit RPC invocations from this connection (HIGH/MEDIUM/LOW/UNSPECIFIED). "+
		"The default is UNSPECIFIED.",
	spannerpb.RequestOptions_PRIORITY_UNSPECIFIED,
	false,
	nil,
	connectionstate.ContextUser,
	func(value string) (spannerpb.RequestOptions_Priority, error) {
		return parseRpcPriority(value)
	},
)
var propertyReturnCommitStats = createConnectionProperty(
	"return_commit_stats",
	"return_commit_stats determines whether transactions should request Spanner to return commit statistics.",
	false,
	false,
	nil,
	connectionstate.ContextUser,
	connectionstate.ConvertBool,
)
var propertyTransactionBatchReadOnly = createConnectionProperty(
	"transaction_batch_read_only",
	"transaction_batch_read_only indicates whether read-only transactions on this connection should use a batch read-only transaction.",
	false,
	false,
	nil,
	connectionstate.ContextUser,
	connectionstate.ConvertBool,
)
var propertyTransactionTimeout = createConnectionProperty(
	"transaction_timeout",
	"The timeout to apply to all read/write transactions on this connection. "+
		"Setting the timeout to zero means no timeout.",
	time.Duration(0),
	false,
	nil,
	connectionstate.ContextUser,
	connectionstate.ConvertDuration,
)

// ------------------------------------------------------------------------------------------------
// Statement connection properties.
// ------------------------------------------------------------------------------------------------
var propertyStatementTag = createConnectionProperty(
	"statement_tag",
	"The statement (request) tag to add to the next statement on this connection.",
	"",
	false,
	nil,
	connectionstate.ContextUser,
	connectionstate.ConvertString,
)
var propertyStatementTimeout = createConnectionProperty(
	"statement_timeout",
	"The timeout to apply to all statements on this connection. "+
		"Setting the timeout to zero means no timeout.",
	time.Duration(0),
	false,
	nil,
	connectionstate.ContextUser,
	connectionstate.ConvertDuration,
)

// ------------------------------------------------------------------------------------------------
// Startup connection properties.
// ------------------------------------------------------------------------------------------------
var propertyEndpoint = createConnectionProperty(
	"endpoint",
	"The endpoint that the driver should connect to. The default is the default Spanner production endpoint "+
		"when auto_config_emulator=false, and the default Spanner emulator endpoint (localhost:9010) when "+
		"auto_config_emulator=true. This property takes precedence over any host name at the start of the "+
		"connection string.",
	"",
	false,
	nil,
	connectionstate.ContextStartup,
	connectionstate.ConvertString,
)
var propertyAuthority = createConnectionProperty(
	"authority",
	"The expected server name in the TLS handshake. By default, the endpoint hostname is used. This option "+
		"is useful when connecting to Spanner via Google Private Connect or other custom endpoints where the "+
		"endpoint hostname does not match the server’s TLS certificate.",
	"",
	false,
	nil,
	connectionstate.ContextStartup,
	connectionstate.ConvertString,
)
var propertyCredentials = createConnectionProperty(
	"credentials",
	"The location of the credentials file to use for this connection. If neither this property or encoded "+
		"credentials are set, the connection will use the default Google Cloud credentials for the runtime environment.",
	"",
	false,
	nil,
	connectionstate.ContextStartup,
	connectionstate.ConvertString,
)
var propertyCredentialsJson = createConnectionProperty(
	"credentials_json",
	"Service account or refresh token JSON credentials.",
	"",
	false,
	nil,
	connectionstate.ContextStartup,
	connectionstate.ConvertString,
)
var propertyUsePlainText = createConnectionProperty(
	"use_plain_text",
	"Use a plain text communication channel (i.e. non-TLS) for communicating with the server (true/false). "+
		"Set this value to true for communication with the Spanner emulator.",
	false,
	false,
	nil,
	connectionstate.ContextStartup,
	connectionstate.ConvertBool,
)
var propertyDisableNativeMetrics = createConnectionProperty(
	"disable_native_metrics",
	"Disables native metrics for this connection. If true, native metrics will not be emitted.",
	false,
	false,
	nil,
	connectionstate.ContextStartup,
	connectionstate.ConvertBool,
)
var propertyMinSessions = createConnectionProperty(
	"min_sessions",
	fmt.Sprintf("The minimum number of sessions in the backing session pool. The default is %d.", spanner.DefaultSessionPoolConfig.MinOpened),
	spanner.DefaultSessionPoolConfig.MinOpened,
	false,
	nil,
	connectionstate.ContextStartup,
	connectionstate.ConvertUint64,
)
var propertyMaxSessions = createConnectionProperty(
	"max_sessions",
	fmt.Sprintf("The maximum number of sessions in the backing session pool. The default is %d.", spanner.DefaultSessionPoolConfig.MaxOpened),
	spanner.DefaultSessionPoolConfig.MaxOpened,
	false,
	nil,
	connectionstate.ContextStartup,
	connectionstate.ConvertUint64,
)
var propertyNumChannels = createConnectionProperty(
	"num_channels",
	"The number of gRPC channels to use to communicate with Cloud Spanner.",
	0,
	false,
	nil,
	connectionstate.ContextStartup,
	connectionstate.ConvertInt,
)
var propertyDatabaseRole = createConnectionProperty(
	"database_role",
	"Sets the database role to use for this connection. The default is privileges assigned to IAM role",
	"",
	false,
	nil,
	connectionstate.ContextStartup,
	connectionstate.ConvertString,
)
var propertyDisableRouteToLeader = createConnectionProperty(
	"disable_route_to_leader",
	"Disable routing read/write transactions and partitioned DML to the leader region (true/false)",
	false,
	false,
	nil,
	connectionstate.ContextStartup,
	connectionstate.ConvertBool,
)
var propertyEnableEndToEndTracing = createConnectionProperty(
	"enable_end_to_end_tracing",
	"Enable end-to-end tracing (true/false) to generate traces for both the time that is spent in the "+
		"client, as well as time that is spent in the Spanner server. Server side traces can only go to Google "+
		"Cloud Trace, so to see end to end traces, the application should configure an exporter that exports the "+
		"traces to Google Cloud Trace.",
	false,
	false,
	nil,
	connectionstate.ContextStartup,
	connectionstate.ConvertBool,
)
var propertyStatementCacheSize = createConnectionProperty(
	"statement_cache_size",
	"The size of the internal cache that is used for connectors that are created from this connection string. "+
		"This cache stores the result of parsing SQL statements for query parameters and the type of statement "+
		"(Query / DML / DDL). The default size is 1000. This default can also be overridden by setting the environment "+
		"variable SPANNER_DEFAULT_STATEMENT_CACHE_SIZE.",
	0,
	false,
	nil,
	connectionstate.ContextStartup,
	connectionstate.ConvertInt,
)
var propertyDisableStatementCache = createConnectionProperty(
	"disable_statement_cache",
	"Disables the use of the statement cache.",
	false,
	false,
	nil,
	connectionstate.ContextStartup,
	connectionstate.ConvertBool,
)
var propertyConnectTimeout = createConnectionProperty(
	"connect_timeout",
	"The amount of time to wait before timing out when creating a new connection.",
	0,
	false,
	nil,
	connectionstate.ContextStartup,
	connectionstate.ConvertDuration,
)

// Generated read-only properties. These cannot be set by the user anywhere.
var propertyCommitTimestamp = createReadOnlyConnectionProperty(
	"commit_timestamp",
	"The commit timestamp of the last implicit or explicit read/write transaction that "+
		"was executed on the connection, or an error if the connection has not executed a read/write transaction "+
		"that committed successfully. The timestamp is in the local timezone.",
	(*time.Time)(nil),
	false,
	nil,
	connectionstate.ContextUser,
)
var propertyCommitResponse = createReadOnlyConnectionProperty(
	"commit_response",
	"The commit response of the last implicit or explicit read/write transaction that "+
		"was executed on the connection, or an error if the connection has not executed a read/write transaction "+
		"that committed successfully.",
	(*spanner.CommitResponse)(nil),
	false,
	nil,
	connectionstate.ContextUser,
)

func createReadOnlyConnectionProperty[T comparable](name, description string, defaultValue T, hasDefaultValue bool, validValues []T, context connectionstate.Context) *connectionstate.TypedConnectionProperty[T] {
	converter := connectionstate.CreateReadOnlyConverter[T](name)
	return createConnectionProperty(name, description, defaultValue, hasDefaultValue, validValues, context, converter)
}

func createConnectionProperty[T comparable](name, description string, defaultValue T, hasDefaultValue bool, validValues []T, context connectionstate.Context, converter func(value string) (T, error)) *connectionstate.TypedConnectionProperty[T] {
	prop := connectionstate.CreateConnectionProperty(name, description, defaultValue, hasDefaultValue, validValues, context, converter)
	connectionProperties[prop.Key()] = prop
	return prop
}

// createConfiguredConnectionState creates an (incomplete) connection state with only the given initial values.
// This is used during driver configuration, but should not be passed in to an actual connection, as it does not
// contain values for the properties that are not included in the initial values.
func createConfiguredConnectionState(initialValues map[string]connectionstate.ConnectionPropertyValue) *connectionstate.ConnectionState {
	state, _ := connectionstate.NewConnectionState(connectionstate.TypeNonTransactional, map[string]connectionstate.ConnectionProperty{}, initialValues)
	return state
}

func createInitialConnectionState(connectionStateType connectionstate.Type, initialValues map[string]connectionstate.ConnectionPropertyValue) *connectionstate.ConnectionState {
	state, _ := connectionstate.NewConnectionState(connectionStateType, connectionProperties, initialValues)
	return state
}
