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
	"strings"
	"sync"
	"sync/atomic"

	"cloud.google.com/go/spanner"
	"cloud.google.com/go/spanner/apiv1/spannerpb"
	spannerdriver "github.com/googleapis/go-sql-spanner"
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

func Execute(ctx context.Context, poolId, connId int64, executeSqlRequest *spannerpb.ExecuteSqlRequest) (int64, error) {
	conn, err := findConnection(poolId, connId)
	if err != nil {
		return 0, err
	}
	return conn.Execute(ctx, executeSqlRequest)
}

type Connection struct {
	// results contains the open query results for this connection.
	results    *sync.Map
	resultsIdx atomic.Int64

	// backend is the database/sql connection of this connection.
	backend *sql.Conn
}

type queryExecutor interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
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

func (conn *Connection) Execute(ctx context.Context, statement *spannerpb.ExecuteSqlRequest) (int64, error) {
	return execute(ctx, conn, conn.backend, statement)
}

func execute(ctx context.Context, conn *Connection, executor queryExecutor, statement *spannerpb.ExecuteSqlRequest) (int64, error) {
	params := extractParams(statement)
	it, err := executor.QueryContext(ctx, statement.Sql, params...)
	if err != nil {
		return 0, err
	}
	// The first result set should contain the metadata.
	if !it.Next() {
		return 0, fmt.Errorf("query returned no metadata")
	}
	metadata := &spannerpb.ResultSetMetadata{}
	if err := it.Scan(&metadata); err != nil {
		return 0, err
	}
	// Move to the next result set, which contains the normal data.
	if !it.NextResultSet() {
		return 0, fmt.Errorf("no results found after metadata")
	}
	id := conn.resultsIdx.Add(1)
	res := &rows{
		backend:  it,
		metadata: metadata,
	}
	if len(metadata.RowType.Fields) == 0 {
		// No rows returned. Read the stats now.
		res.readStats(ctx)
	}
	conn.results.Store(id, res)
	return id, nil
}

func extractParams(statement *spannerpb.ExecuteSqlRequest) []any {
	paramsLen := 1
	if statement.Params != nil {
		paramsLen = 1 + len(statement.Params.Fields)
	}
	params := make([]any, paramsLen)
	params = append(params, spannerdriver.ExecOptions{
		DecodeOption: spannerdriver.DecodeOptionProto,
		// TODO: Implement support for passing in stale query options
		// TimestampBound:          extractTimestampBound(statement),
		ReturnResultSetMetadata: true,
		ReturnResultSetStats:    true,
		DirectExecuteQuery:      true,
	})
	if statement.Params != nil {
		if statement.ParamTypes == nil {
			statement.ParamTypes = make(map[string]*spannerpb.Type)
		}
		for param, value := range statement.Params.Fields {
			genericValue := spanner.GenericColumnValue{
				Value: value,
				Type:  statement.ParamTypes[param],
			}
			if strings.HasPrefix(param, "_") {
				// Prefix the parameter name with a 'p' to work around the fact that database/sql does not allow
				// named arguments to start with anything else than a letter.
				params = append(params, sql.Named("p"+param, spannerdriver.SpannerNamedArg{NameInQuery: param, Value: genericValue}))
			} else {
				params = append(params, sql.Named(param, genericValue))
			}
		}
	}
	return params
}
