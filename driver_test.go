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
	"database/sql/driver"
	"testing"
	"time"

	"cloud.google.com/go/spanner"
	"cloud.google.com/go/spanner/apiv1/spannerpb"
	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"google.golang.org/grpc/codes"
)

func TestExtractDnsParts(t *testing.T) {
	tests := []struct {
		input               string
		wantConnectorConfig connectorConfig
		wantSpannerConfig   spanner.ClientConfig
		wantErr             bool
	}{
		{
			input: "projects/p/instances/i/databases/d",
			wantConnectorConfig: connectorConfig{
				project:  "p",
				instance: "i",
				database: "d",
				params:   map[string]string{},
			},
			wantSpannerConfig: spanner.ClientConfig{
				SessionPoolConfig: spanner.DefaultSessionPoolConfig,
				UserAgent:         userAgent,
			},
		},
		{
			input: "projects/DEFAULT_PROJECT_ID/instances/test-instance/databases/test-database",
			wantConnectorConfig: connectorConfig{
				project:  "DEFAULT_PROJECT_ID",
				instance: "test-instance",
				database: "test-database",
				params:   map[string]string{},
			},
			wantSpannerConfig: spanner.ClientConfig{
				SessionPoolConfig: spanner.DefaultSessionPoolConfig,
				UserAgent:         userAgent,
			},
		},
		{
			input: "localhost:9010/projects/p/instances/i/databases/d",
			wantConnectorConfig: connectorConfig{
				host:     "localhost:9010",
				project:  "p",
				instance: "i",
				database: "d",
				params:   map[string]string{},
			},
			wantSpannerConfig: spanner.ClientConfig{
				SessionPoolConfig: spanner.DefaultSessionPoolConfig,
				UserAgent:         userAgent,
			},
		},
		{
			input: "spanner.googleapis.com/projects/p/instances/i/databases/d",
			wantConnectorConfig: connectorConfig{
				host:     "spanner.googleapis.com",
				project:  "p",
				instance: "i",
				database: "d",
				params:   map[string]string{},
			},
			wantSpannerConfig: spanner.ClientConfig{
				SessionPoolConfig: spanner.DefaultSessionPoolConfig,
				UserAgent:         userAgent,
			},
		},
		{
			input: "spanner.googleapis.com/projects/p/instances/i/databases/d?usePlainText=true",
			wantConnectorConfig: connectorConfig{
				host:     "spanner.googleapis.com",
				project:  "p",
				instance: "i",
				database: "d",
				params: map[string]string{
					"useplaintext": "true",
				},
			},
			wantSpannerConfig: spanner.ClientConfig{
				SessionPoolConfig: spanner.DefaultSessionPoolConfig,
				UserAgent:         userAgent,
			},
		},
		{
			input: "spanner.googleapis.com/projects/p/instances/i/databases/d;credentials=/path/to/credentials.json",
			wantConnectorConfig: connectorConfig{
				host:     "spanner.googleapis.com",
				project:  "p",
				instance: "i",
				database: "d",
				params: map[string]string{
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
			wantConnectorConfig: connectorConfig{
				host:     "spanner.googleapis.com",
				project:  "p",
				instance: "i",
				database: "d",
				params: map[string]string{
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
			wantConnectorConfig: connectorConfig{
				host:     "spanner.googleapis.com",
				project:  "p",
				instance: "i",
				database: "d",
				params: map[string]string{
					"useplaintext": "true",
				},
			},
			wantSpannerConfig: spanner.ClientConfig{
				SessionPoolConfig: spanner.DefaultSessionPoolConfig,
				UserAgent:         userAgent,
			},
		},
		{
			input: "spanner.googleapis.com/projects/p/instances/i/databases/d?minSessions=200;maxSessions=1000;numChannels=10;disableRouteToLeader=true;rpcPriority=Medium;optimizerVersion=1;optimizerStatisticsPackage=latest;databaseRole=child",
			wantConnectorConfig: connectorConfig{
				host:     "spanner.googleapis.com",
				project:  "p",
				instance: "i",
				database: "d",
				params: map[string]string{
					"minsessions":                "200",
					"maxsessions":                "1000",
					"numchannels":                "10",
					"disableroutetoleader":       "true",
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
					MaxBurst:                          spanner.DefaultSessionPoolConfig.MaxBurst,
					MaxIdle:                           spanner.DefaultSessionPoolConfig.MaxIdle,
					TrackSessionHandles:               spanner.DefaultSessionPoolConfig.TrackSessionHandles,
					InactiveTransactionRemovalOptions: spanner.DefaultSessionPoolConfig.InactiveTransactionRemovalOptions,
				},
				NumChannels:          10,
				UserAgent:            userAgent,
				DisableRouteToLeader: true,
				QueryOptions:         spanner.QueryOptions{Priority: spannerpb.RequestOptions_PRIORITY_MEDIUM, Options: &spannerpb.ExecuteSqlRequest_QueryOptions{OptimizerVersion: "1", OptimizerStatisticsPackage: "latest"}},
				ReadOptions:          spanner.ReadOptions{Priority: spannerpb.RequestOptions_PRIORITY_MEDIUM},
				TransactionOptions:   spanner.TransactionOptions{CommitPriority: spannerpb.RequestOptions_PRIORITY_MEDIUM},
				DatabaseRole:         "child",
			},
		},
		{
			// intential error case
			input:   "project/p/instances/i/databases/d",
			wantErr: true,
		},
	}
	for _, tc := range tests {
		t.Run(tc.input, func(t *testing.T) {
			config, err := extractConnectorConfig(tc.input)
			if err != nil {
				if tc.wantErr {
					return
				}
				t.Errorf("extract failed for %q: %v", tc.input, err)
			} else {
				if tc.wantErr {
					t.Error("did not encounter expected error")
				}
				if !cmp.Equal(config, tc.wantConnectorConfig, cmp.AllowUnexported(connectorConfig{})) {
					t.Errorf("connector config mismatch for %q\ngot: %v\nwant %v", tc.input, config, tc.wantConnectorConfig)
				}
				conn, err := newConnector(&Driver{connectors: make(map[string]*connector)}, tc.input)
				if err != nil {
					t.Errorf("failed to get connector for %q: %v", tc.input, err)
				}
				if !cmp.Equal(conn.spannerClientConfig, tc.wantSpannerConfig, cmpopts.IgnoreUnexported(spanner.ClientConfig{}, spanner.SessionPoolConfig{}, spanner.InactiveTransactionRemovalOptions{}, spannerpb.ExecuteSqlRequest_QueryOptions{})) {
					t.Errorf("connector Spanner client config mismatch for %q\n Got: %v\nWant: %v", tc.input, conn.spannerClientConfig, tc.wantSpannerConfig)
				}
			}
		})
	}

}

func TestConnection_Reset(t *testing.T) {
	txClosed := false
	c := conn{
		readOnlyStaleness: spanner.ExactStaleness(time.Second),
		batch:             &batch{tp: dml},
		commitTs:          &time.Time{},
		tx: &readOnlyTransaction{
			close: func() {
				txClosed = true
			},
		},
	}

	if err := c.ResetSession(context.Background()); err != nil {
		t.Fatalf("failed to reset session: %v", err)
	}
	if !cmp.Equal(c.readOnlyStaleness, spanner.TimestampBound{}, cmp.AllowUnexported(spanner.TimestampBound{})) {
		t.Error("failed to reset read-only staleness")
	}
	if c.inBatch() {
		t.Error("failed to clear batch")
	}
	if c.commitTs != nil {
		t.Errorf("failed to clear commit timestamp")
	}
	if !txClosed {
		t.Error("failed to close transaction")
	}
}

func TestConnection_NoNestedTransactions(t *testing.T) {
	c := conn{
		tx: &readOnlyTransaction{},
	}
	_, err := c.BeginTx(context.Background(), driver.TxOptions{})
	if err == nil {
		t.Errorf("unexpected nil error for BeginTx call")
	}
}

func TestConn_AbortBatch(t *testing.T) {
	c := &conn{}
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
		{"Default", &conn{}, false},
		{"In DDL batch", &conn{batch: &batch{tp: ddl}}, true},
		{"In DML batch", &conn{batch: &batch{tp: dml}}, true},
		{"In read/write transaction", &conn{tx: &readWriteTransaction{}}, true},
		{"In read-only transaction", &conn{tx: &readOnlyTransaction{}}, true},
		{"In read/write transaction with a DML batch", &conn{tx: &readWriteTransaction{batch: &batch{tp: dml}}}, true},
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
		{"Default", &conn{}, false},
		{"In DDL batch", &conn{batch: &batch{tp: ddl}}, true},
		{"In DML batch", &conn{batch: &batch{tp: dml}}, true},
		{"In read/write transaction", &conn{tx: &readWriteTransaction{}}, false},
		{"In read-only transaction", &conn{tx: &readOnlyTransaction{}}, true},
		{"In read/write transaction with a DML batch", &conn{tx: &readWriteTransaction{batch: &batch{tp: dml}}}, true},
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
	c := &conn{
		batch: &batch{tp: ddl},
		execSingleQuery: func(ctx context.Context, c *spanner.Client, statement spanner.Statement, tb spanner.TimestampBound) *spanner.RowIterator {
			return &spanner.RowIterator{}
		},
		execSingleDMLTransactional: func(ctx context.Context, c *spanner.Client, statement spanner.Statement) (int64, time.Time, error) {
			return 0, time.Time{}, nil
		},
		execSingleDMLPartitioned: func(ctx context.Context, c *spanner.Client, statement spanner.Statement) (int64, error) {
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
	c := &conn{
		batch: &batch{tp: dml},
		execSingleQuery: func(ctx context.Context, c *spanner.Client, statement spanner.Statement, tb spanner.TimestampBound) *spanner.RowIterator {
			return &spanner.RowIterator{}
		},
		execSingleDMLTransactional: func(ctx context.Context, c *spanner.Client, statement spanner.Statement) (int64, time.Time, error) {
			return 0, time.Time{}, nil
		},
		execSingleDMLPartitioned: func(ctx context.Context, c *spanner.Client, statement spanner.Statement) (int64, error) {
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

func TestConn_GetCommitTimestampAfterAutocommitDml(t *testing.T) {
	want := time.Now()
	c := &conn{
		execSingleQuery: func(ctx context.Context, c *spanner.Client, statement spanner.Statement, tb spanner.TimestampBound) *spanner.RowIterator {
			return &spanner.RowIterator{}
		},
		execSingleDMLTransactional: func(ctx context.Context, c *spanner.Client, statement spanner.Statement) (int64, time.Time, error) {
			return 0, want, nil
		},
		execSingleDMLPartitioned: func(ctx context.Context, c *spanner.Client, statement spanner.Statement) (int64, error) {
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
	if !cmp.Equal(want, got) {
		t.Fatalf("commit timestamp mismatch\n Got: %v\nWant: %v", got, want)
	}
}

func TestConn_GetCommitTimestampAfterAutocommitQuery(t *testing.T) {
	c := &conn{
		execSingleQuery: func(ctx context.Context, c *spanner.Client, statement spanner.Statement, tb spanner.TimestampBound) *spanner.RowIterator {
			return &spanner.RowIterator{}
		},
		execSingleDMLTransactional: func(ctx context.Context, c *spanner.Client, statement spanner.Statement) (int64, time.Time, error) {
			return 0, time.Time{}, nil
		},
		execSingleDMLPartitioned: func(ctx context.Context, c *spanner.Client, statement spanner.Statement) (int64, error) {
			return 0, nil
		},
	}
	ctx := context.Background()
	if _, err := c.QueryContext(ctx, "SELECT * FROM Foo", []driver.NamedValue{}); err != nil {
		t.Fatalf("failed to execute query: %v", err)
	}
	_, err := c.CommitTimestamp()
	if g, w := spanner.ErrCode(err), codes.FailedPrecondition; g != w {
		t.Fatalf("error code mismatch\n Got: %v\nWant: %v", g, w)
	}
}
