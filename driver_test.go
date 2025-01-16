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
	"net"
	"reflect"
	"testing"
	"time"

	"cloud.google.com/go/spanner"
	"cloud.google.com/go/spanner/apiv1/spannerpb"
	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"google.golang.org/api/option"
	"google.golang.org/grpc/codes"
)

func TestExtractDnsParts(t *testing.T) {
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
				if diff := cmp.Diff(config, tc.wantConnectorConfig, cmp.AllowUnexported(ConnectorConfig{})); diff != "" {
					t.Errorf("connector config mismatch for %q\n%v", tc.input, diff)
				}
				conn, err := newOrCachedConnector(&Driver{connectors: make(map[string]*connector)}, tc.input)
				if err != nil {
					t.Errorf("failed to get connector for %q: %v", tc.input, err)
				}
				if diff := cmp.Diff(conn.spannerClientConfig, tc.wantSpannerConfig, cmpopts.IgnoreUnexported(spanner.ClientConfig{}, spanner.SessionPoolConfig{}, spanner.InactiveTransactionRemovalOptions{}, spannerpb.ExecuteSqlRequest_QueryOptions{})); diff != "" {
					t.Errorf("connector Spanner client config mismatch for %q\n%v", tc.input, diff)
				}
			}
		})
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

	defer db.Close()
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
		execSingleQuery: func(ctx context.Context, c *spanner.Client, statement spanner.Statement, tb spanner.TimestampBound, options ExecOptions) *spanner.RowIterator {
			return &spanner.RowIterator{}
		},
		execSingleDMLTransactional: func(ctx context.Context, c *spanner.Client, statement spanner.Statement, options ExecOptions) (int64, time.Time, error) {
			return 0, time.Time{}, nil
		},
		execSingleDMLPartitioned: func(ctx context.Context, c *spanner.Client, statement spanner.Statement, options ExecOptions) (int64, error) {
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
		execSingleQuery: func(ctx context.Context, c *spanner.Client, statement spanner.Statement, tb spanner.TimestampBound, options ExecOptions) *spanner.RowIterator {
			return &spanner.RowIterator{}
		},
		execSingleDMLTransactional: func(ctx context.Context, c *spanner.Client, statement spanner.Statement, options ExecOptions) (int64, time.Time, error) {
			return 0, time.Time{}, nil
		},
		execSingleDMLPartitioned: func(ctx context.Context, c *spanner.Client, statement spanner.Statement, options ExecOptions) (int64, error) {
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

	ctx := context.Background()
	c := &conn{}
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

func TestConn_GetCommitTimestampAfterAutocommitDml(t *testing.T) {
	want := time.Now()
	c := &conn{
		execSingleQuery: func(ctx context.Context, c *spanner.Client, statement spanner.Statement, tb spanner.TimestampBound, options ExecOptions) *spanner.RowIterator {
			return &spanner.RowIterator{}
		},
		execSingleDMLTransactional: func(ctx context.Context, c *spanner.Client, statement spanner.Statement, options ExecOptions) (int64, time.Time, error) {
			return 0, want, nil
		},
		execSingleDMLPartitioned: func(ctx context.Context, c *spanner.Client, statement spanner.Statement, options ExecOptions) (int64, error) {
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
		execSingleQuery: func(ctx context.Context, c *spanner.Client, statement spanner.Statement, tb spanner.TimestampBound, options ExecOptions) *spanner.RowIterator {
			return &spanner.RowIterator{}
		},
		execSingleDMLTransactional: func(ctx context.Context, c *spanner.Client, statement spanner.Statement, options ExecOptions) (int64, time.Time, error) {
			return 0, time.Time{}, nil
		},
		execSingleDMLPartitioned: func(ctx context.Context, c *spanner.Client, statement spanner.Statement, options ExecOptions) (int64, error) {
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

func TestConn_CheckNamedValue(t *testing.T) {
	c := &conn{}

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
