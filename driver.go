// Copyright 2020 Google Inc. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
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
	"log"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"

	"cloud.google.com/go/spanner"
	"github.com/rakyll/go-sql-driver-spanner/internal"
	"google.golang.org/api/option"

	adminapi "cloud.google.com/go/spanner/admin/database/apiv1"
	adminpb "google.golang.org/genproto/googleapis/spanner/admin/database/v1"
	"google.golang.org/grpc"
)

const userAgent = "go-sql-driver-spanner/0.1"
const dsnRegExpString = "((?P<HOSTGROUP>[\\w.-]+(?:\\.[\\w\\.-]+)*[\\w\\-\\._~:/?#\\[\\]@!\\$&'\\(\\)\\*\\+,;=.]+)/)?projects/(?P<PROJECTGROUP>(([a-z]|[-.:]|[0-9])+|(DEFAULT_PROJECT_ID)))(/instances/(?P<INSTANCEGROUP>([a-z]|[-]|[0-9])+)(/databases/(?P<DATABASEGROUP>([a-z]|[-]|[_]|[0-9])+))?)?(([\\?|;])(?P<PARAMSGROUP>.*))?"

var dsnRegExp *regexp.Regexp

var _ driver.DriverContext = &Driver{}

func init() {
	var err error
	dsnRegExp, err = regexp.Compile(dsnRegExpString)
	if err != nil {
		log.Fatalf("could not compile Spanner dsn regexp: %v", err)
		return
	}
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
	host string
	project string
	instance string
	database string
	params map[string]string
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
		host: matches["HOSTGROUP"],
		project: matches["PROJECTGROUP"],
		instance: matches["INSTANCEGROUP"],
		database: matches["DATABASEGROUP"],
		params: params,
	}, nil
}

func extractConnectorParams(paramsString string) (map[string]string, error) {
	params := make(map[string]string)
	if paramsString == "" {
		return params, nil
	}
	keyValuePairs := strings.Split(paramsString, ";")
	for _, keyValueString := range keyValuePairs {
		keyValue := strings.SplitN(keyValueString, "=", 2)
		if keyValue == nil || len(keyValue) != 2 {
			return nil, fmt.Errorf("invalid connection property: %s", keyValueString)
		}
		params[keyValue[0]] = keyValue[1]
	}
	return params, nil
}

type connector struct {
	driver *Driver
	connectorConfig connectorConfig

	// config represents the optional advanced configuration to be used
	// by the Google Cloud Spanner client.
	config spanner.ClientConfig

	// options represent the optional Google Cloud client options
	// to be passed to the underlying client.
	options []option.ClientOption
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
			opts = append(opts,option.WithGRPCDialOption(grpc.WithInsecure()), option.WithoutAuthentication())
		}
	}
	config := spanner.ClientConfig{
		SessionPoolConfig: spanner.DefaultSessionPoolConfig,
	}
	return &connector{
		driver:          d,
		connectorConfig: connectorConfig,
		config:          config,
		options:         opts,
	}, nil
}

func (c *connector) Connect(ctx context.Context) (driver.Conn, error) {
	return openDriverConn(ctx, c)
}

func openDriverConn(ctx context.Context, c *connector) (driver.Conn, error) {
	if c.config.NumChannels == 0 {
//		c.config.NumChannels = 1 // TODO(jbd): Explain database/sql has a high-level management.
	}
	opts := append(c.options, option.WithUserAgent(userAgent))
	databaseName := fmt.Sprintf(
		"projects/%s/instances/%s/databases/%s",
		c.connectorConfig.project,
		c.connectorConfig.instance,
		c.connectorConfig.database)
	client, err := spanner.NewClientWithConfig(ctx, databaseName, c.config, opts...)
	if err != nil {
		return nil, err
	}

	adminClient, err := adminapi.NewDatabaseAdminClient(ctx, opts...)
	if err != nil {
		return nil, err
	}
	return &conn{client: client, adminClient: adminClient, database: databaseName}, nil
}

func createAdminClient(ctx context.Context) (adminClient *adminapi.DatabaseAdminClient, err error) {
	// Admin client will connect to emulator if SPANNER_EMULATOR_HOST
	// is set in the environment.
	if spannerHost, ok := os.LookupEnv("SPANNER_EMULATOR_HOST"); ok {
		adminClient, err = adminapi.NewDatabaseAdminClient(
			ctx,
			option.WithoutAuthentication(),
			option.WithEndpoint(spannerHost),
			option.WithGRPCDialOption(grpc.WithInsecure()))
		if err != nil {
			adminClient = nil
		}
	} else {
		adminClient, err = adminapi.NewDatabaseAdminClient(ctx)
		if err != nil {
			adminClient = nil
		}
	}
	return
}

func (c *connector) Driver() driver.Driver {
	return &Driver{}
}

type conn struct {
	client      *spanner.Client
	adminClient *adminapi.DatabaseAdminClient
	roTx        *spanner.ReadOnlyTransaction
	rwTx        *rwTx
	database    string
}

func (c *conn) Prepare(query string) (driver.Stmt, error) {
	panic("Using PrepareContext instead")
}

func (c *conn) PrepareContext(ctx context.Context, query string) (driver.Stmt, error) {
	// TODO(jbd): Mention emails need to be escaped.
	args, err := internal.NamedValueParamNames(query, -1)
	if err != nil {
		return nil, err
	}
	return &stmt{conn: c, query: query, numArgs: len(args)}, nil
}

func (c *conn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {

	// Use admin API if DDL statement is provided.
	isDdl, err := isDdl(query)
	if err != nil {
		return nil, err
	}

	if isDdl {
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

	if c.roTx != nil {
		return nil, errors.New("cannot write in read-only transaction")
	}
	ss, err := prepareSpannerStmt(query, args)
	if err != nil {
		return nil, err
	}

	var rowsAffected int64
	if c.rwTx == nil {
		rowsAffected, err = c.execContextInNewRWTransaction(ctx, ss)
	} else {
		rowsAffected, err = c.rwTx.ExecContext(ctx, ss)
	}
	if err != nil {
		return nil, err
	}
	return &result{rowsAffected: rowsAffected}, nil
}

func isDdl(query string) (bool, error) {

	matchddl, err := regexp.MatchString(`(?is)^\n*\s*(CREATE|DROP|ALTER)\s+.+$`, query)
	if err != nil {
		return false, err
	}

	return matchddl, nil
}

func (c *conn) Close() error {
	c.client.Close()
	return nil
}

func (c *conn) Begin() (driver.Tx, error) {
	panic("Using BeginTx instead")
}

func (c *conn) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	if c.inTransaction() {
		return nil, errors.New("already in a transaction")
	}

	if opts.ReadOnly {
		c.roTx = c.client.ReadOnlyTransaction().WithTimestampBound(spanner.StrongRead())
		return &roTx{close: func() {
			c.roTx.Close()
			c.roTx = nil
		}}, nil
	}

	connector := internal.NewRWConnector(ctx, c.client)
	c.rwTx = &rwTx{
		connector: connector,
		close: func() {
			c.rwTx = nil
		},
	}

	// TODO(jbd): Make sure we are not leaking
	// a goroutine in connector if timeout happens.
	select {
	case <-connector.Ready:
		return c.rwTx, nil
	case err := <-connector.Errors: // If received before Ready, transaction failed to start.
		return nil, err
	case <-time.Tick(10 * time.Second):
		return nil, errors.New("cannot begin transaction, timeout after 10 seconds")
	}
}

func (c *conn) inTransaction() bool {
	return c.roTx != nil || c.rwTx != nil
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
