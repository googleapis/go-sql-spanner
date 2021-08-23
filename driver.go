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
	"regexp"
	"strconv"
	"strings"

	"cloud.google.com/go/spanner"
	"github.com/cloudspannerecosystem/go-sql-spanner/internal"
	"google.golang.org/api/option"

	adminapi "cloud.google.com/go/spanner/admin/database/apiv1"
	adminpb "google.golang.org/genproto/googleapis/spanner/admin/database/v1"
	"google.golang.org/grpc"
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
			return nil, fmt.Errorf("invalid connection property: %s", keyValueString)
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
	return &conn{client: client, adminClient: adminClient, database: databaseName, retryAborts: c.retryAbortsInternally}, nil
}

func (c *connector) Driver() driver.Driver {
	return &Driver{}
}

type conn struct {
	closed      bool
	client      *spanner.Client
	adminClient *adminapi.DatabaseAdminClient
	tx          contextTransaction
	database    string
	retryAborts bool
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
	return nil
}

// IsValid implements the driver.Validator interface.
func (c *conn) IsValid() bool {
	return !c.closed
}

func (c *conn) Prepare(query string) (driver.Stmt, error) {
	return c.PrepareContext(context.Background(), query)
}

func (c *conn) PrepareContext(ctx context.Context, query string) (driver.Stmt, error) {
	args, err := internal.ParseNamedParameters(query)
	if err != nil {
		return nil, err
	}
	return &stmt{conn: c, query: query, numArgs: len(args)}, nil
}

func (c *conn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	stmt, err := prepareSpannerStmt(query, args)
	if err != nil {
		return nil, err
	}
	var iter rowIterator
	if c.tx == nil {
		iter = &readOnlyRowIterator{c.client.Single().Query(ctx, stmt)}
	} else {
		iter = c.tx.Query(ctx, stmt)
	}
	return &rows{it: iter}, nil
}

func (c *conn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	// Use admin API if DDL statement is provided.
	isDdl, err := internal.IsDdl(query)
	if err != nil {
		return nil, err
	}

	if isDdl {
		// TODO: Determine whether we want to return an error if a transaction
		// is active. Cloud Spanner does not support DDL in transactions, but
		// this makes it seem like the DDL statement is executed on the
		// transaction on this connection if it has a transaction.
		op, err := c.adminClient.UpdateDatabaseDdl(ctx, &adminpb.UpdateDatabaseDdlRequest{
			Database:   c.database,
			Statements: []string{query},
		})
		if err != nil {
			return nil, err
		}
		if err := op.Wait(ctx); err != nil {
			return nil, err
		}
		return &result{rowsAffected: 0}, nil
	}

	ss, err := prepareSpannerStmt(query, args)
	if err != nil {
		return nil, err
	}

	var rowsAffected int64
	if c.tx == nil {
		rowsAffected, err = c.execContextInNewRWTransaction(ctx, ss)
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
		return nil, errors.New("already in a transaction")
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

func (c *conn) execContextInNewRWTransaction(ctx context.Context, statement spanner.Statement) (int64, error) {
	var rowsAffected int64
	fn := func(ctx context.Context, tx *spanner.ReadWriteTransaction) error {
		count, err := tx.Update(ctx, statement)
		rowsAffected = count
		return err
	}
	_, err := c.client.ReadWriteTransaction(ctx, fn)
	if err != nil {
		return 0, err
	}
	return rowsAffected, nil
}
