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
	"time"

	"cloud.google.com/go/spanner"
	"github.com/rakyll/go-sql-driver-spanner/internal"
	"google.golang.org/api/option"
)

const userAgent = "go-sql-driver-spanner/0.1"

var _ driver.DriverContext = &Driver{}

func init() {
	sql.Register("spanner", &Driver{})
}

// Driver represents a Google Cloud Spanner database/sql driver.
type Driver struct {
	// Config represents the optional advanced configuration to be used
	// by the Google Cloud Spanner client.
	Config spanner.ClientConfig

	// Options represent the optional Google Cloud client options
	// to be passed to the underlying client.
	Options []option.ClientOption
}

// Open opens a connection to a Google Cloud Spanner database.
// Use fully qualified string:
//
// Example: projects/$PROJECT/instances/$INSTANCE/databases/$DATABASE
func (d *Driver) Open(name string) (driver.Conn, error) {
	return openDriverConn(context.Background(), d, name)
}

func (d *Driver) OpenConnector(name string) (driver.Connector, error) {
	return &connector{
		driver: d,
		name:   name,
	}, nil
}

type connector struct {
	driver *Driver
	name   string
}

func (c *connector) Connect(ctx context.Context) (driver.Conn, error) {
	return openDriverConn(ctx, c.driver, c.name)
}

func openDriverConn(ctx context.Context, d *Driver, name string) (driver.Conn, error) {
	if d.Config.NumChannels == 0 {
		d.Config.NumChannels = 1 // TODO(jbd): Explain database/sql has a high-level management.
	}
	opts := append(d.Options, option.WithUserAgent(userAgent))
	client, err := spanner.NewClientWithConfig(ctx, name, d.Config, opts...)
	if err != nil {
		return nil, err
	}
	return &conn{client: client}, nil
}

func (c *connector) Driver() driver.Driver {
	return &Driver{}
}

type conn struct {
	client *spanner.Client
	roTx   *spanner.ReadOnlyTransaction
	rwTx   *rwTx
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
