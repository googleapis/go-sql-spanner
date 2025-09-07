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
	"encoding/json"
	"fmt"
	"io"
	"net"
	"reflect"
	"testing"
	"time"

	"cloud.google.com/go/spanner"
	"cloud.google.com/go/spanner/admin/database/apiv1/databasepb"
	"cloud.google.com/go/spanner/apiv1/spannerpb"
	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/googleapis/go-sql-spanner/connectionstate"
	"github.com/googleapis/go-sql-spanner/parser"
	"google.golang.org/api/option"
	"google.golang.org/grpc/codes"
)

func silentClose(c io.Closer) {
	_ = c.Close()
}

func TestExtractDnsParts(t *testing.T) {
	//goland:noinspection GoDeprecation
	tests := []struct {
		input               string
		wantConnectorConfig ConnectorConfig
		wantSpannerConfig   spanner.ClientConfig
		wantErr             bool
	}{
		{
			input: "projects/p/instances/i/databases/d",
			wantConnectorConfig: ConnectorConfig{
				Project:  "p",
				Instance: "i",
				Database: "d",
				Params:   map[string]string{},
			},
			wantSpannerConfig: spanner.ClientConfig{
				SessionPoolConfig: spanner.DefaultSessionPoolConfig,
				UserAgent:         userAgent,
			},
		},
		{
			input: "projects/DEFAULT_PROJECT_ID/instances/test-instance/databases/test-database",
			wantConnectorConfig: ConnectorConfig{
				Project:  "DEFAULT_PROJECT_ID",
				Instance: "test-instance",
				Database: "test-database",
				Params:   map[string]string{},
			},
			wantSpannerConfig: spanner.ClientConfig{
				SessionPoolConfig: spanner.DefaultSessionPoolConfig,
				UserAgent:         userAgent,
			},
		},
		{
			input: "localhost:9010/projects/p/instances/i/databases/d",
			wantConnectorConfig: ConnectorConfig{
				Host:     "localhost:9010",
				Project:  "p",
				Instance: "i",
				Database: "d",
				Params:   map[string]string{},
			},
			wantSpannerConfig: spanner.ClientConfig{
				SessionPoolConfig: spanner.DefaultSessionPoolConfig,
				UserAgent:         userAgent,
			},
		},
		{
			input: "spanner.googleapis.com/projects/p/instances/i/databases/d",
			wantConnectorConfig: ConnectorConfig{
				Host:     "spanner.googleapis.com",
				Project:  "p",
				Instance: "i",
				Database: "d",
				Params:   map[string]string{},
			},
			wantSpannerConfig: spanner.ClientConfig{
				SessionPoolConfig: spanner.DefaultSessionPoolConfig,
				UserAgent:         userAgent,
			},
		},
		{
			input: "spanner.googleapis.com/projects/p/instances/i/databases/d?usePlainText=true",
			wantConnectorConfig: ConnectorConfig{
				Host:     "spanner.googleapis.com",
				Project:  "p",
				Instance: "i",
				Database: "d",
				Params: map[string]string{
					"useplaintext": "true",
				},
			},
			wantSpannerConfig: spanner.ClientConfig{
				SessionPoolConfig:    spanner.DefaultSessionPoolConfig,
				UserAgent:            userAgent,
				DisableNativeMetrics: true,
			},
		},
		{
			input: "spanner.googleapis.com/projects/p/instances/i/databases/d;credentials=/path/to/credentials.json",
			wantConnectorConfig: ConnectorConfig{
				Host:     "spanner.googleapis.com",
				Project:  "p",
				Instance: "i",
				Database: "d",
				Params: map[string]string{
					"credentials": "/path/to/credentials.json",
				},
			},
			wantSpannerConfig: spanner.ClientConfig{
				SessionPoolConfig: spanner.DefaultSessionPoolConfig,
				UserAgent:         userAgent,
			},
		},
		{
			input: "spanner.googleapis.com/projects/p/instances/i/databases/d?credentials=/path/to/credentials.json;readonly=true",
			wantConnectorConfig: ConnectorConfig{
				Host:     "spanner.googleapis.com",
				Project:  "p",
				Instance: "i",
				Database: "d",
				Params: map[string]string{
					"credentials": "/path/to/credentials.json",
					"readonly":    "true",
				},
			},
			wantSpannerConfig: spanner.ClientConfig{
				SessionPoolConfig: spanner.DefaultSessionPoolConfig,
				UserAgent:         userAgent,
			},
		},
		{
			input: "spanner.googleapis.com/projects/p/instances/i/databases/d?usePlainText=true;",
			wantConnectorConfig: ConnectorConfig{
				Host:     "spanner.googleapis.com",
				Project:  "p",
				Instance: "i",
				Database: "d",
				Params: map[string]string{
					"useplaintext": "true",
				},
			},
			wantSpannerConfig: spanner.ClientConfig{
				SessionPoolConfig:    spanner.DefaultSessionPoolConfig,
				UserAgent:            userAgent,
				DisableNativeMetrics: true,
			},
		},
		{
			input: "projects/p/instances/i/databases/d?isolationLevel=repeatable_read;",
			wantConnectorConfig: ConnectorConfig{
				Project:  "p",
				Instance: "i",
				Database: "d",
				Params: map[string]string{
					"isolationlevel": "repeatable_read",
				},
				IsolationLevel: sql.LevelRepeatableRead,
			},
			wantSpannerConfig: spanner.ClientConfig{
				SessionPoolConfig: spanner.DefaultSessionPoolConfig,
				UserAgent:         userAgent,
			},
		},
		{
			input: "projects/p/instances/i/databases/d?beginTransactionOption=ExplicitBeginTransaction",
			wantConnectorConfig: ConnectorConfig{
				Project:  "p",
				Instance: "i",
				Database: "d",
				Params: map[string]string{
					"begintransactionoption": "ExplicitBeginTransaction",
				},
				BeginTransactionOption: spanner.ExplicitBeginTransaction,
			},
			wantSpannerConfig: spanner.ClientConfig{
				SessionPoolConfig: spanner.DefaultSessionPoolConfig,
				UserAgent:         userAgent,
			},
		},
		{
			input: "projects/p/instances/i/databases/d?StatementCacheSize=100;",
			wantConnectorConfig: ConnectorConfig{
				Project:  "p",
				Instance: "i",
				Database: "d",
				Params: map[string]string{
					"statementcachesize": "100",
				},
				StatementCacheSize: 100,
			},
			wantSpannerConfig: spanner.ClientConfig{
				SessionPoolConfig: spanner.DefaultSessionPoolConfig,
				UserAgent:         userAgent,
			},
		},
		{
			input: "spanner.googleapis.com/projects/p/instances/i/databases/d?minSessions=200;maxSessions=1000;numChannels=10;disableRouteToLeader=true;enableEndToEndTracing=true;disableNativeMetrics=true;rpcPriority=Medium;optimizerVersion=1;optimizerStatisticsPackage=latest;databaseRole=child",
			wantConnectorConfig: ConnectorConfig{
				Host:     "spanner.googleapis.com",
				Project:  "p",
				Instance: "i",
				Database: "d",
				Params: map[string]string{
					"minsessions":                "200",
					"maxsessions":                "1000",
					"numchannels":                "10",
					"disableroutetoleader":       "true",
					"enableendtoendtracing":      "true",
					"disablenativemetrics":       "true",
					"rpcpriority":                "Medium",
					"optimizerversion":           "1",
					"optimizerstatisticspackage": "latest",
					"databaserole":               "child",
				},
			},
			wantSpannerConfig: spanner.ClientConfig{
				SessionPoolConfig: spanner.SessionPoolConfig{
					MinOpened:                         200,
					MaxOpened:                         1000,
					WriteSessions:                     0.2,
					HealthCheckInterval:               spanner.DefaultSessionPoolConfig.HealthCheckInterval,
					HealthCheckWorkers:                spanner.DefaultSessionPoolConfig.HealthCheckWorkers,
					MaxBurst:                          spanner.DefaultSessionPoolConfig.MaxBurst, //lint:ignore SA1019 because it's a spanner default.
					MaxIdle:                           spanner.DefaultSessionPoolConfig.MaxIdle,
					TrackSessionHandles:               spanner.DefaultSessionPoolConfig.TrackSessionHandles,
					InactiveTransactionRemovalOptions: spanner.DefaultSessionPoolConfig.InactiveTransactionRemovalOptions,
				},
				UserAgent:             userAgent,
				DisableRouteToLeader:  true,
				EnableEndToEndTracing: true,
				DisableNativeMetrics:  true,
				QueryOptions:          spanner.QueryOptions{Priority: spannerpb.RequestOptions_PRIORITY_MEDIUM, Options: &spannerpb.ExecuteSqlRequest_QueryOptions{OptimizerVersion: "1", OptimizerStatisticsPackage: "latest"}},
				ReadOptions:           spanner.ReadOptions{Priority: spannerpb.RequestOptions_PRIORITY_MEDIUM},
				TransactionOptions:    spanner.TransactionOptions{CommitPriority: spannerpb.RequestOptions_PRIORITY_MEDIUM},
				DatabaseRole:          "child",
			},
		},
		{
			// intentional error case
			input:   "project/p/instances/i/databases/d",
			wantErr: true,
		},
	}
	for _, tc := range tests {
		t.Run(tc.input, func(t *testing.T) {
			config, err := ExtractConnectorConfig(tc.input)
			if err != nil {
				if tc.wantErr {
					return
				}
				t.Errorf("extract failed for %q: %v", tc.input, err)
			} else {
				if tc.wantErr {
					t.Error("did not encounter expected error")
				}
				if diff := cmp.Diff(config.Params, tc.wantConnectorConfig.Params); diff != "" {
					t.Errorf("connector config mismatch for %q\n%v", tc.input, diff)
				}
				conn, err := newOrCachedConnector(&Driver{connectors: make(map[string]*connector)}, tc.input)
				if err != nil {
					t.Fatalf("failed to get connector for %q: %v", tc.input, err)
				}
				if diff := cmp.Diff(conn.spannerClientConfig, tc.wantSpannerConfig, cmpopts.IgnoreUnexported(spanner.ClientConfig{}, spanner.SessionPoolConfig{}, spanner.InactiveTransactionRemovalOptions{}, spannerpb.ExecuteSqlRequest_QueryOptions{}, spanner.TransactionOptions{})); diff != "" {
					t.Errorf("connector Spanner client config mismatch for %q\n%v", tc.input, diff)
				}
				actualConfig := conn.connectorConfig
				actualConfig.name = ""
				if diff := cmp.Diff(actualConfig, tc.wantConnectorConfig, cmp.AllowUnexported(ConnectorConfig{})); diff != "" {
					t.Errorf("actual connector config mismatch for %q\n%v", tc.input, diff)
				}
			}
		})
	}
}

func TestParseBeginTransactionOption(t *testing.T) {
	tests := []struct {
		input   string
		want    spanner.BeginTransactionOption
		wantErr bool
	}{
		{
			input: "DefaultBeginTransaction",
			want:  spanner.DefaultBeginTransaction,
		},
		{
			input: "InlinedBeginTransaction",
			want:  spanner.InlinedBeginTransaction,
		},
		{
			input: "ExplicitBeginTransaction",
			want:  spanner.ExplicitBeginTransaction,
		},
		{
			input:   "invalid",
			wantErr: true,
		},
	}
	for i, test := range tests {
		t.Run(test.input, func(t *testing.T) {
			val, err := parseBeginTransactionOption(test.input)
			if (err != nil) != test.wantErr {
				t.Errorf("%d: parseBeginTransactionOption(%q) error = %v, wantErr %v", i, err, test.wantErr, err)
			}
			if g, w := val, test.want; g != w {
				t.Errorf("%d: parseBeginTransactionOption(%q) = %v, want %v", i, g, w, g)
			}
		})
	}
}

func TestToProtoIsolationLevel(t *testing.T) {
	tests := []struct {
		input   sql.IsolationLevel
		want    spannerpb.TransactionOptions_IsolationLevel
		wantErr bool
	}{
		{
			input: sql.LevelSerializable,
			want:  spannerpb.TransactionOptions_SERIALIZABLE,
		},
		{
			input: sql.LevelRepeatableRead,
			want:  spannerpb.TransactionOptions_REPEATABLE_READ,
		},
		{
			input: sql.LevelSnapshot,
			want:  spannerpb.TransactionOptions_REPEATABLE_READ,
		},
		{
			input: sql.LevelDefault,
			want:  spannerpb.TransactionOptions_ISOLATION_LEVEL_UNSPECIFIED,
		},
		{
			input:   sql.LevelReadUncommitted,
			wantErr: true,
		},
		{
			input:   sql.LevelReadCommitted,
			wantErr: true,
		},
		{
			input:   sql.LevelWriteCommitted,
			wantErr: true,
		},
		{
			input:   sql.LevelLinearizable,
			wantErr: true,
		},
		{
			input:   sql.IsolationLevel(1000),
			wantErr: true,
		},
	}
	for i, test := range tests {
		g, err := toProtoIsolationLevel(test.input)
		if test.wantErr && err == nil {
			t.Errorf("test %d: expected error for input %v, got none", i, test.input)
		} else if !test.wantErr && err != nil {
			t.Errorf("test %d: unexpected error for input %v: %v", i, test.input, err)
		} else if g != test.want {
			t.Errorf("test %d:\n Got: %v\nWant: %v", i, g, test.want)
		}
	}
}

func TestParseIsolationLevel(t *testing.T) {
	tests := []struct {
		input   string
		want    sql.IsolationLevel
		wantErr bool
	}{
		{
			input: "default",
			want:  sql.LevelDefault,
		},
		{
			input: " DEFAULT   ",
			want:  sql.LevelDefault,
		},
		{
			input: "read uncommitted",
			want:  sql.LevelReadUncommitted,
		},
		{
			input: "  read_uncommitted\n",
			want:  sql.LevelReadUncommitted,
		},
		{
			input: "read committed",
			want:  sql.LevelReadCommitted,
		},
		{
			input: "write committed",
			want:  sql.LevelWriteCommitted,
		},
		{
			input: "repeatable read",
			want:  sql.LevelRepeatableRead,
		},
		{
			input: "snapshot",
			want:  sql.LevelSnapshot,
		},
		{
			input: "serializable",
			want:  sql.LevelSerializable,
		},
		{
			input: "linearizable",
			want:  sql.LevelLinearizable,
		},
		{
			input:   "read serializable",
			wantErr: true,
		},
		{
			input:   "",
			wantErr: true,
		},
		{
			input:   "read-committed",
			wantErr: true,
		},
	}
	for _, tc := range tests {
		level, err := parseIsolationLevel(tc.input)
		if tc.wantErr && err == nil {
			t.Errorf("parseIsolationLevel(%q): expected error", tc.input)
		} else if !tc.wantErr && err != nil {
			t.Errorf("parseIsolationLevel(%q): unexpected error: %v", tc.input, err)
		} else if level != tc.want {
			t.Errorf("parseIsolationLevel(%q): got %v, want %v", tc.input, level, tc.want)
		}
	}
}

func ExampleCreateConnector() {
	connectorConfig := ConnectorConfig{
		Project:  "my-project",
		Instance: "my-instance",
		Database: "my-database",

		Configurator: func(config *spanner.ClientConfig, opts *[]option.ClientOption) {
			config.DisableRouteToLeader = true
		},
	}
	c, err := CreateConnector(connectorConfig)
	if err != nil {
		fmt.Println(err)
		return
	}
	db := sql.OpenDB(c)
	// Use the database ...

	defer silentClose(db)
}

func TestConnection_Reset(t *testing.T) {
	txClosed := false
	c := conn{
		logger: noopLogger,
		connector: &connector{
			connectorConfig: ConnectorConfig{},
		},
		state: createInitialConnectionState(connectionstate.TypeTransactional, map[string]connectionstate.ConnectionPropertyValue{
			propertyCommitResponse.Key(): propertyCommitResponse.CreateTypedInitialValue(nil),
		}),
		batch: &batch{tp: BatchTypeDml},
		tx: &readOnlyTransaction{
			logger: noopLogger,
			close: func(_ txResult) {
				txClosed = true
			},
		},
	}

	c.setCommitResponse(&spanner.CommitResponse{})
	if err := c.SetReadOnlyStaleness(spanner.ExactStaleness(time.Second)); err != nil {
		t.Fatal(err)
	}
	if err := c.ResetSession(context.Background()); err != nil {
		t.Fatalf("failed to reset session: %v", err)
	}
	if !cmp.Equal(c.ReadOnlyStaleness(), spanner.TimestampBound{}, cmp.AllowUnexported(spanner.TimestampBound{})) {
		t.Error("failed to reset read-only staleness")
	}
	if c.inBatch() {
		t.Error("failed to clear batch")
	}
	if _, err := c.CommitResponse(); err == nil {
		t.Errorf("failed to clear commit response")
	}
	if !txClosed {
		t.Error("failed to close transaction")
	}
}

func TestConnection_NoNestedTransactions(t *testing.T) {
	c := conn{
		logger: noopLogger,
		state:  createInitialConnectionState(connectionstate.TypeTransactional, map[string]connectionstate.ConnectionPropertyValue{}),
		tx:     &readOnlyTransaction{},
	}
	_, err := c.BeginTx(context.Background(), driver.TxOptions{})
	if err == nil {
		t.Errorf("unexpected nil error for BeginTx call")
	}
}

func TestConn_AbortBatch(t *testing.T) {
	c := &conn{logger: noopLogger, state: createInitialConnectionState(connectionstate.TypeTransactional, map[string]connectionstate.ConnectionPropertyValue{})}
	if err := c.StartBatchDDL(); err != nil {
		t.Fatalf("failed to start DDL batch: %v", err)
	}
	if !c.InDDLBatch() {
		t.Fatal("connection did not start DDL batch")
	}
	if err := c.AbortBatch(); err != nil {
		t.Fatalf("failed to abort DDL batch: %v", err)
	}
	if c.InDDLBatch() {
		t.Fatal("connection did not abort DDL batch")
	}

	if err := c.StartBatchDML(); err != nil {
		t.Fatalf("failed to start DML batch: %v", err)
	}
	if !c.InDMLBatch() {
		t.Fatal("connection did not start DML batch")
	}
	if err := c.AbortBatch(); err != nil {
		t.Fatalf("failed to abort DML batch: %v", err)
	}
	if c.InDMLBatch() {
		t.Fatal("connection did not abort DML batch")
	}
}

func TestConn_StartBatchDdl(t *testing.T) {
	for _, test := range []struct {
		name    string
		c       *conn
		wantErr bool
	}{
		{"Default", &conn{logger: noopLogger, state: createInitialConnectionState(connectionstate.TypeTransactional, map[string]connectionstate.ConnectionPropertyValue{})}, false},
		{"In DDL batch", &conn{logger: noopLogger, batch: &batch{tp: BatchTypeDdl}, state: createInitialConnectionState(connectionstate.TypeTransactional, map[string]connectionstate.ConnectionPropertyValue{})}, true},
		{"In DML batch", &conn{logger: noopLogger, batch: &batch{tp: BatchTypeDml}, state: createInitialConnectionState(connectionstate.TypeTransactional, map[string]connectionstate.ConnectionPropertyValue{})}, true},
		{"In read/write transaction", &conn{logger: noopLogger, tx: &readWriteTransaction{}, state: createInitialConnectionState(connectionstate.TypeTransactional, map[string]connectionstate.ConnectionPropertyValue{})}, true},
		{"In read-only transaction", &conn{logger: noopLogger, tx: &readOnlyTransaction{}, state: createInitialConnectionState(connectionstate.TypeTransactional, map[string]connectionstate.ConnectionPropertyValue{})}, true},
		{"In read/write transaction with a DML batch", &conn{logger: noopLogger, tx: &readWriteTransaction{batch: &batch{tp: BatchTypeDml}}, state: createInitialConnectionState(connectionstate.TypeTransactional, map[string]connectionstate.ConnectionPropertyValue{})}, true},
	} {
		err := test.c.StartBatchDDL()
		if test.wantErr {
			if err == nil {
				t.Fatalf("%s: no error returned for StartDdlBatch: %v", test.name, err)
			}
		} else {
			if err != nil {
				t.Fatalf("%s: failed to start DDL batch: %v", test.name, err)
			}
			if !test.c.InDDLBatch() {
				t.Fatalf("%s: connection did not start DDL batch", test.name)
			}
		}
	}
}

func TestConn_StartBatchDml(t *testing.T) {
	for _, test := range []struct {
		name    string
		c       *conn
		wantErr bool
	}{
		{"Default", &conn{logger: noopLogger, state: createInitialConnectionState(connectionstate.TypeTransactional, map[string]connectionstate.ConnectionPropertyValue{})}, false},
		{"In DDL batch", &conn{logger: noopLogger, state: createInitialConnectionState(connectionstate.TypeTransactional, map[string]connectionstate.ConnectionPropertyValue{}), batch: &batch{tp: BatchTypeDdl}}, true},
		{"In DML batch", &conn{logger: noopLogger, state: createInitialConnectionState(connectionstate.TypeTransactional, map[string]connectionstate.ConnectionPropertyValue{}), batch: &batch{tp: BatchTypeDml}}, true},
		{"In read/write transaction", &conn{logger: noopLogger, state: createInitialConnectionState(connectionstate.TypeTransactional, map[string]connectionstate.ConnectionPropertyValue{}), tx: &readWriteTransaction{logger: noopLogger}}, false},
		{"In read-only transaction", &conn{logger: noopLogger, state: createInitialConnectionState(connectionstate.TypeTransactional, map[string]connectionstate.ConnectionPropertyValue{}), tx: &readOnlyTransaction{logger: noopLogger}}, true},
		{"In read/write transaction with a DML batch", &conn{logger: noopLogger, state: createInitialConnectionState(connectionstate.TypeTransactional, map[string]connectionstate.ConnectionPropertyValue{}), tx: &readWriteTransaction{logger: noopLogger, batch: &batch{tp: BatchTypeDml}}}, true},
	} {
		err := test.c.StartBatchDML()
		if test.wantErr {
			if err == nil {
				t.Fatalf("%s: no error returned for StartDmlBatch: %v", test.name, err)
			}
		} else {
			if err != nil {
				t.Fatalf("%s: failed to start DML batch: %v", test.name, err)
			}
			if !test.c.InDMLBatch() {
				t.Fatalf("%s: connection did not start DML batch", test.name)
			}
		}
	}
}

func TestConn_NonDdlStatementsInDdlBatch(t *testing.T) {
	p, err := parser.NewStatementParser(databasepb.DatabaseDialect_GOOGLE_STANDARD_SQL, 1000)
	if err != nil {
		t.Fatal(err)
	}
	c := &conn{
		parser: p,
		logger: noopLogger,
		state:  createInitialConnectionState(connectionstate.TypeNonTransactional, map[string]connectionstate.ConnectionPropertyValue{}),
		batch:  &batch{tp: BatchTypeDdl},
		execSingleQuery: func(ctx context.Context, c *spanner.Client, statement spanner.Statement, tb spanner.TimestampBound, options *ExecOptions) *spanner.RowIterator {
			return &spanner.RowIterator{
				Metadata: &spannerpb.ResultSetMetadata{},
			}
		},
		execSingleDMLTransactional: func(ctx context.Context, c *spanner.Client, statement spanner.Statement, statementInfo *parser.StatementInfo, options *ExecOptions) (*result, *spanner.CommitResponse, error) {
			return &result{}, &spanner.CommitResponse{}, nil
		},
		execSingleDMLPartitioned: func(ctx context.Context, c *spanner.Client, statement spanner.Statement, options *ExecOptions) (int64, error) {
			return 0, nil
		},
	}
	ctx := context.Background()

	// Starting a transaction while in a DDL batch is not allowed.
	if _, err := c.BeginTx(ctx, driver.TxOptions{}); err == nil {
		t.Fatal("missing error for BeginTx")
	}
	// Starting a DML batch while in a DDL batch is not allowed.
	if err := c.StartBatchDML(); err == nil {
		t.Fatalf("missing error for StartBatchDML")
	}

	// Executing a single DML or query during a DDL batch is allowed.
	if _, err := c.ExecContext(ctx, "INSERT INTO Foo (Id, Value) VALUES (1, 'One')", []driver.NamedValue{}); err != nil {
		t.Fatalf("executing DML statement failed: %v", err)
	}
	if _, err := c.QueryContext(ctx, "SELECT * FROM Foo", []driver.NamedValue{}); err != nil {
		t.Fatalf("executing query failed: %v", err)
	}
}

func TestConn_NonDmlStatementsInDmlBatch(t *testing.T) {
	p, err := parser.NewStatementParser(databasepb.DatabaseDialect_GOOGLE_STANDARD_SQL, 1000)
	if err != nil {
		t.Fatal(err)
	}
	c := &conn{
		parser: p,
		logger: noopLogger,
		state:  createInitialConnectionState(connectionstate.TypeNonTransactional, map[string]connectionstate.ConnectionPropertyValue{}),
		batch:  &batch{tp: BatchTypeDml},
		execSingleQuery: func(ctx context.Context, c *spanner.Client, statement spanner.Statement, tb spanner.TimestampBound, options *ExecOptions) *spanner.RowIterator {
			return &spanner.RowIterator{}
		},
		execSingleDMLTransactional: func(ctx context.Context, c *spanner.Client, statement spanner.Statement, statementInfo *parser.StatementInfo, options *ExecOptions) (*result, *spanner.CommitResponse, error) {
			return &result{}, &spanner.CommitResponse{}, nil
		},
		execSingleDMLPartitioned: func(ctx context.Context, c *spanner.Client, statement spanner.Statement, options *ExecOptions) (int64, error) {
			return 0, nil
		},
	}
	ctx := context.Background()

	// Starting a transaction while in a DML batch is not allowed.
	if _, err := c.BeginTx(ctx, driver.TxOptions{}); err == nil {
		t.Fatal("missing error for BeginTx")
	}
	// Starting a DDL batch while in a DML batch is not allowed.
	if err := c.StartBatchDDL(); err == nil {
		t.Fatal("missing error for StartBatchDDL")
	}
	// Executing a single DDL statement during a DML batch is not allowed.
	if _, err := c.ExecContext(ctx, "CREATE TABLE Foo", []driver.NamedValue{}); err == nil {
		t.Fatal("missing error for DDL statement")
	}

	// Executing a single query during a DML batch is allowed.
	if _, err := c.QueryContext(ctx, "SELECT * FROM Foo", []driver.NamedValue{}); err != nil {
		t.Fatalf("executing query failed: %v", err)
	}
}

func TestConn_GetBatchedStatements(t *testing.T) {
	t.Parallel()

	p, err := parser.NewStatementParser(databasepb.DatabaseDialect_GOOGLE_STANDARD_SQL, 1000)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	c := &conn{logger: noopLogger, parser: p, state: createInitialConnectionState(connectionstate.TypeNonTransactional, map[string]connectionstate.ConnectionPropertyValue{})}
	if !reflect.DeepEqual(c.GetBatchedStatements(), []spanner.Statement{}) {
		t.Fatal("conn should return an empty slice when no batch is active")
	}
	if err := c.StartBatchDDL(); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(c.GetBatchedStatements(), []spanner.Statement{}) {
		t.Fatal("conn should return an empty slice when a batch contains no statements")
	}
	if _, err := c.ExecContext(ctx, "create table table1", []driver.NamedValue{}); err != nil {
		t.Fatal(err)
	}
	if _, err := c.ExecContext(ctx, "create table table2", []driver.NamedValue{}); err != nil {
		t.Fatal(err)
	}
	batchedStatements := c.GetBatchedStatements()
	if !reflect.DeepEqual([]spanner.Statement{
		{SQL: "create table table1", Params: map[string]interface{}{}},
		{SQL: "create table table2", Params: map[string]interface{}{}},
	}, batchedStatements) {
		t.Errorf("unexpected batched statements: %v", batchedStatements)
	}

	// Changing the returned slice does not change the batched statements.
	batchedStatements[0] = spanner.Statement{SQL: "drop table table1"}
	batchedStatements2 := c.GetBatchedStatements()
	if !reflect.DeepEqual([]spanner.Statement{
		{SQL: "create table table1", Params: map[string]interface{}{}},
		{SQL: "create table table2", Params: map[string]interface{}{}},
	}, batchedStatements2) {
		t.Errorf("unexpected batched statements: %v", batchedStatements2)
	}

	if err := c.AbortBatch(); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(c.GetBatchedStatements(), []spanner.Statement{}) {
		t.Fatal("conn should return an empty slice when no batch is active")
	}
}

func TestConn_GetCommitResponseAfterAutocommitDml(t *testing.T) {
	p, err := parser.NewStatementParser(databasepb.DatabaseDialect_GOOGLE_STANDARD_SQL, 1000)
	if err != nil {
		t.Fatal(err)
	}
	want := &spanner.CommitResponse{CommitTs: time.Now()}
	c := &conn{
		parser: p,
		logger: noopLogger,
		state:  createInitialConnectionState(connectionstate.TypeNonTransactional, map[string]connectionstate.ConnectionPropertyValue{}),
		execSingleQuery: func(ctx context.Context, c *spanner.Client, statement spanner.Statement, tb spanner.TimestampBound, options *ExecOptions) *spanner.RowIterator {
			return &spanner.RowIterator{}
		},
		execSingleDMLTransactional: func(ctx context.Context, c *spanner.Client, statement spanner.Statement, statementInfo *parser.StatementInfo, options *ExecOptions) (*result, *spanner.CommitResponse, error) {
			return &result{}, want, nil
		},
		execSingleDMLPartitioned: func(ctx context.Context, c *spanner.Client, statement spanner.Statement, options *ExecOptions) (int64, error) {
			return 0, nil
		},
	}
	ctx := context.Background()
	if _, err := c.ExecContext(ctx, "UPDATE FOO SET BAR=1 WHERE TRUE", []driver.NamedValue{}); err != nil {
		t.Fatalf("failed to execute DML statement: %v", err)
	}
	got, err := c.CommitTimestamp()
	if err != nil {
		t.Fatalf("failed to get commit timestamp: %v", err)
	}
	if !cmp.Equal(want.CommitTs, got) {
		t.Fatalf("commit timestamp mismatch\n Got: %v\nWant: %v", got, want)
	}
	gotResp, err := c.CommitResponse()
	if err != nil {
		t.Fatalf("failed to get commit response: %v", err)
	}
	if !cmp.Equal(want, gotResp) {
		t.Fatalf("commit response mismatch\n Got: %v\nWant: %v", gotResp, want)
	}
}

func TestConn_GetCommitResponseAfterAutocommitQuery(t *testing.T) {
	p, err := parser.NewStatementParser(databasepb.DatabaseDialect_GOOGLE_STANDARD_SQL, 1000)
	if err != nil {
		t.Fatal(err)
	}
	c := &conn{
		parser: p,
		logger: noopLogger,
		state:  createInitialConnectionState(connectionstate.TypeTransactional, map[string]connectionstate.ConnectionPropertyValue{}),
		execSingleQuery: func(ctx context.Context, c *spanner.Client, statement spanner.Statement, tb spanner.TimestampBound, options *ExecOptions) *spanner.RowIterator {
			return &spanner.RowIterator{}
		},
		execSingleDMLTransactional: func(ctx context.Context, c *spanner.Client, statement spanner.Statement, statementInfo *parser.StatementInfo, options *ExecOptions) (*result, *spanner.CommitResponse, error) {
			return &result{}, &spanner.CommitResponse{}, nil
		},
		execSingleDMLPartitioned: func(ctx context.Context, c *spanner.Client, statement spanner.Statement, options *ExecOptions) (int64, error) {
			return 0, nil
		},
	}
	ctx := context.Background()
	if _, err := c.QueryContext(ctx, "SELECT * FROM Foo", []driver.NamedValue{}); err != nil {
		t.Fatalf("failed to execute query: %v", err)
	}
	_, err = c.CommitTimestamp()
	if g, w := spanner.ErrCode(err), codes.FailedPrecondition; g != w {
		t.Fatalf("error code mismatch\n Got: %v\nWant: %v", g, w)
	}
	_, err = c.CommitResponse()
	if g, w := spanner.ErrCode(err), codes.FailedPrecondition; g != w {
		t.Fatalf("error code mismatch\n Got: %v\nWant: %v", g, w)
	}
}

func TestConn_CheckNamedValue(t *testing.T) {
	c := &conn{logger: noopLogger}

	type Person struct {
		Name string
		Age  int64
	}

	var testNil *ValuerNil

	tests := []struct {
		in   any
		want any
	}{
		// basic types should stay unchanged
		{in: int64(256), want: int64(256)},
		// driver.Valuer types should call .Value func
		{in: &ValuerPerson{Name: "hello", Age: 123}, want: "hello"},
		// structs should be sent to spanner
		{in: &Person{Name: "hello", Age: 123}, want: &Person{Name: "hello", Age: 123}},
		// nil pointer of type that implements driver.Valuer via value receiver should use nil
		{in: testNil, want: nil},
		// net.IP reflects to []byte. Allow model structs to have fields with
		// types that reflect to types supported by spanner.
		{in: net.IPv6loopback, want: []byte(net.IPv6loopback)},
		// Similarly, time.Duration is just an int64.
		{in: time.Duration(1), want: int64(time.Duration(1))},
	}

	for _, test := range tests {
		value := &driver.NamedValue{
			Ordinal: 1,
			Value:   test.in,
		}

		err := c.CheckNamedValue(value)
		if err != nil {
			t.Errorf("expected no error, got: %v", err)
			continue
		}

		if diff := cmp.Diff(test.want, value.Value); diff != "" {
			t.Errorf("wrong result, got %#v expected %#v:\n%v", value.Value, test.want, diff)
		}
	}
}

func TestConnectorConfigJson(t *testing.T) {
	_, err := json.Marshal(ConnectorConfig{})
	if err != nil {
		t.Fatalf("failed to marshal ConnectorConfig: %v", err)
	}
}

// ValuerPerson implements driver.Valuer
type ValuerPerson struct {
	Name string
	Age  int64
}

// Value implements conversion func for database.
func (p *ValuerPerson) Value() (driver.Value, error) {
	return p.Name, nil
}

// ValuerNil implements driver.Valuer
type ValuerNil struct {
	Name string
}

// Value implements conversion func for database. It explicitly implements driver.Valuer via a value receiver to
// test the driver for its ability to handle nil pointers to structs that implement driver.Valuer via value receivers
func (v ValuerNil) Value() (driver.Value, error) {
	return v.Name, nil
}
