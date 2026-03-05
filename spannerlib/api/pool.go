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

package api

import (
	"context"
	"database/sql"
	"fmt"
	"sync"
	"sync/atomic"

	"cloud.google.com/go/spanner"
	spannerdriver "github.com/googleapis/go-sql-spanner"
	"google.golang.org/api/option"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// pools contains all open pools in this runtime.
var pools = sync.Map{}
var poolsIdx = atomic.Int64{}

// Pool is the equivalent of a sql.DB. It contains a pool of connections to the same database.
// All connections in a pool share the same Spanner client.
type Pool struct {
	db             *sql.DB
	connections    *sync.Map
	connectionsIdx atomic.Int64
}

// CreatePool creates a new Pool and stores it in the global map of pool.
// The connectionString must be in the form
// [host:port]/project/<project>/instances/<instance>/databases/<database>[?option1=value1[;option2=value2...]]
//
// Creating a pool is a relatively expensive operation, as each pool has its own Spanner client.
func CreatePool(ctx context.Context, userAgentSuffix, connectionString string) (int64, error) {
	config, err := spannerdriver.ExtractConnectorConfig(connectionString)
	if err != nil {
		return 0, err
	}
	config.Configurator = func(config *spanner.ClientConfig, opts *[]option.ClientOption) {
		config.UserAgent = fmt.Sprintf("spanner-lib-%s/%s", userAgentSuffix, spannerdriver.ModuleVersion)
	}
	connector, err := spannerdriver.CreateConnector(config)
	if err != nil {
		return 0, err
	}
	db := sql.OpenDB(connector)
	// Create a connection to force an error if the connection string is invalid.
	conn, err := db.Conn(ctx)
	if err != nil {
		return 0, err
	}
	_ = conn.Close()

	id := poolsIdx.Add(1)
	pool := &Pool{
		db:          db,
		connections: &sync.Map{},
	}
	pools.Store(id, pool)
	return id, nil
}

// ClosePool closes the pool with the given id. All open connections in the pool are also closed, and the pool cannot
// be used to create any new connections.
func ClosePool(ctx context.Context, id int64) error {
	p, ok := pools.LoadAndDelete(id)
	if !ok {
		// Closing an unknown pool or a pool that has previously been closed is a no-op.
		return nil
	}
	pool := p.(*Pool)
	pool.connections.Range(func(key, value interface{}) bool {
		conn := value.(*Connection)
		_ = conn.close(ctx)
		return true
	})
	if err := pool.db.Close(); err != nil {
		return err
	}
	return nil
}

// CreateConnection creates a new connection in the given pool. This is a relatively cheap operation, as a connection
// does not have its own physical connection to Spanner. Instead, all connections in the same pool share the same
// underlying Spanner client, which again contains a gRPC channel pool.
func CreateConnection(ctx context.Context, poolId int64) (int64, error) {
	pool, err := findPool(poolId)
	if err != nil {
		return 0, err
	}
	sqlConn, err := pool.db.Conn(ctx)
	if err != nil {
		return 0, err
	}
	id := poolsIdx.Add(1)
	conn := &Connection{
		backend: sqlConn,
		results: &sync.Map{},
	}
	pool.connections.Store(id, conn)

	return id, nil
}

func findPool(poolId int64) (*Pool, error) {
	p, ok := pools.Load(poolId)
	if !ok {
		return nil, status.Errorf(codes.NotFound, "pool %v not found", poolId)
	}
	pool := p.(*Pool)
	return pool, nil
}

func findConnection(poolId, connId int64) (*Connection, error) {
	pool, err := findPool(poolId)
	if err != nil {
		return nil, err
	}
	c, ok := pool.connections.Load(connId)
	if !ok {
		return nil, status.Errorf(codes.NotFound, "connection %v not found", connId)
	}
	conn := c.(*Connection)
	return conn, nil
}

func findRows(poolId, connId, rowsId int64) (*rows, error) {
	conn, err := findConnection(poolId, connId)
	if err != nil {
		return nil, err
	}
	r, ok := conn.results.Load(rowsId)
	if !ok {
		return nil, fmt.Errorf("rows %v not found", rowsId)
	}
	res := r.(*rows)
	return res, nil
}
