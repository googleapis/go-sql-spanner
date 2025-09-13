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
	"sync"
	"sync/atomic"
)

// CloseConnection looks up the connection with the given poolId and connId and closes it.
func CloseConnection(ctx context.Context, poolId, connId int64) error {
	pool, err := findPool(poolId)
	if err != nil {
		return err
	}
	c, ok := pool.connections.LoadAndDelete(connId)
	if !ok {
		// Closing an unknown connection or a connection that has previously been closed is a no-op.
		return nil
	}
	conn := c.(*Connection)
	return conn.close(ctx)
}

type Connection struct {
	// results contains the open query results for this connection.
	results    *sync.Map
	resultsIdx atomic.Int64

	// backend is the database/sql connection of this connection.
	backend *sql.Conn
}

func (conn *Connection) close(ctx context.Context) error {
	conn.closeResults(ctx)
	err := conn.backend.Close()
	if err != nil {
		return err
	}
	return nil
}

func (conn *Connection) closeResults(ctx context.Context) {
	conn.results.Range(func(key, value interface{}) bool {
		// TODO: Implement
		return true
	})
}
