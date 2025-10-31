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
	"context"
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"time"

	"cloud.google.com/go/spanner"
	"cloud.google.com/go/spanner/admin/database/apiv1/databasepb"
	"github.com/googleapis/go-sql-spanner/parser"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type executableStatement interface {
	execContext(ctx context.Context, c *conn, opts *ExecOptions) (driver.Result, error)
	queryContext(ctx context.Context, c *conn, opts *ExecOptions) (driver.Rows, error)
}

func createExecutableStatement(stmt parser.ParsedStatement) (executableStatement, error) {
	switch stmt := stmt.(type) {
	case *parser.ParsedShowStatement:
		return &executableShowStatement{stmt: stmt}, nil
	case *parser.ParsedSetStatement:
		return &executableSetStatement{stmt: stmt}, nil
	case *parser.ParsedResetStatement:
		return &executableResetStatement{stmt: stmt}, nil
	case *parser.ParsedCreateDatabaseStatement:
		return &executableCreateDatabaseStatement{stmt: stmt}, nil
	case *parser.ParsedDropDatabaseStatement:
		return &executableDropDatabaseStatement{stmt: stmt}, nil
	case *parser.ParsedStartBatchStatement:
		return &executableStartBatchStatement{stmt: stmt}, nil
	case *parser.ParsedRunBatchStatement:
		return &executableRunBatchStatement{stmt: stmt}, nil
	case *parser.ParsedAbortBatchStatement:
		return &executableAbortBatchStatement{stmt: stmt}, nil
	case *parser.ParsedBeginStatement:
		return &executableBeginStatement{stmt: stmt}, nil
	case *parser.ParsedCommitStatement:
		return &executableCommitStatement{stmt: stmt}, nil
	case *parser.ParsedRollbackStatement:
		return &executableRollbackStatement{stmt: stmt}, nil
	}
	return nil, status.Errorf(codes.Internal, "unsupported statement type: %T", stmt)
}

type executableShowStatement struct {
	stmt *parser.ParsedShowStatement
}

func (s *executableShowStatement) execContext(ctx context.Context, c *conn, opts *ExecOptions) (driver.Result, error) {
	return nil, spanner.ToSpannerError(status.Errorf(codes.InvalidArgument, "%q cannot be used with execContext", s.stmt.Query()))
}

func (s *executableShowStatement) queryContext(ctx context.Context, c *conn, opts *ExecOptions) (driver.Rows, error) {
	col := s.stmt.Identifier.String()
	val, hasValue, err := c.showConnectionVariable(s.stmt.Identifier)
	if err != nil {
		return nil, err
	}
	var it rowIterator
	switch val := val.(type) {
	case bool:
		it, err = createBooleanIterator(col, val)
	case int64:
		it, err = createInt64Iterator(col, val)
	case string:
		it, err = createStringIterator(col, val)
	case *time.Time:
		it, err = createTimestampIterator(col, val)
	default:
		stringVal := ""
		if hasValue {
			if stringerVal, ok := val.(fmt.Stringer); ok {
				stringVal = stringerVal.String()
			} else {
				jsonVal, err := json.Marshal(val)
				if err != nil {
					return nil, status.Errorf(codes.InvalidArgument, "unsupported type: %T", val)
				}
				stringVal = string(jsonVal)
			}
		}
		it, err = createStringIterator(col, stringVal)
	}
	if err != nil {
		return nil, err
	}
	return createRows(it /*cancel=*/, nil, opts), nil
}

// SET [SESSION | LOCAL] [my_extension.]my_property {=|to} <value>
type executableSetStatement struct {
	stmt *parser.ParsedSetStatement
}

func (s *executableSetStatement) execContext(ctx context.Context, c *conn, opts *ExecOptions) (driver.Result, error) {
	if err := s.execute(c); err != nil {
		return nil, err
	}
	return driver.ResultNoRows, nil
}

func (s *executableSetStatement) queryContext(ctx context.Context, c *conn, opts *ExecOptions) (driver.Rows, error) {
	if err := s.execute(c); err != nil {
		return nil, err
	}
	return createEmptyRows(opts), nil
}

func (s *executableSetStatement) execute(c *conn) error {
	if len(s.stmt.Identifiers) != len(s.stmt.Literals) {
		return status.Errorf(codes.InvalidArgument, "statement contains %d identifiers, but %d values given", len(s.stmt.Identifiers), len(s.stmt.Literals))
	}
	for index := range s.stmt.Identifiers {
		if err := c.setConnectionVariable(s.stmt.Identifiers[index], s.stmt.Literals[index].Value, s.stmt.IsLocal, s.stmt.IsTransaction); err != nil {
			return err
		}
	}
	return nil
}

// RESET [my_extension.]my_property
type executableResetStatement struct {
	stmt *parser.ParsedResetStatement
}

func (s *executableResetStatement) execContext(ctx context.Context, c *conn, opts *ExecOptions) (driver.Result, error) {
	if err := c.setConnectionVariable(s.stmt.Identifier, "default", false, false); err != nil {
		return nil, err
	}
	return driver.ResultNoRows, nil
}

func (s *executableResetStatement) queryContext(ctx context.Context, c *conn, opts *ExecOptions) (driver.Rows, error) {
	if err := c.setConnectionVariable(s.stmt.Identifier, "default", false, false); err != nil {
		return nil, err
	}
	return createEmptyRows(opts), nil
}

func createEmptyRows(opts *ExecOptions) *rows {
	it := createEmptyIterator()
	return &rows{
		it:                      it,
		decodeOption:            opts.DecodeOption,
		decodeToNativeArrays:    opts.DecodeToNativeArrays,
		returnResultSetMetadata: opts.ReturnResultSetMetadata,
		returnResultSetStats:    opts.ReturnResultSetStats,
	}
}

type executableCreateDatabaseStatement struct {
	stmt *parser.ParsedCreateDatabaseStatement
}

func (s *executableCreateDatabaseStatement) execContext(ctx context.Context, c *conn, opts *ExecOptions) (driver.Result, error) {
	instance := fmt.Sprintf("projects/%s/instances/%s", c.connector.connectorConfig.Project, c.connector.connectorConfig.Instance)
	request := &databasepb.CreateDatabaseRequest{
		CreateStatement: s.stmt.Query(),
		Parent:          instance,
		DatabaseDialect: c.parser.Dialect,
	}
	op, err := c.adminClient.CreateDatabase(ctx, request)
	if err != nil {
		return nil, err
	}
	if _, err := op.Wait(ctx); err != nil {
		return nil, err
	}
	return driver.ResultNoRows, nil
}

func (s *executableCreateDatabaseStatement) queryContext(ctx context.Context, c *conn, opts *ExecOptions) (driver.Rows, error) {
	if _, err := s.execContext(ctx, c, opts); err != nil {
		return nil, err
	}
	return createEmptyRows(opts), nil
}

type executableDropDatabaseStatement struct {
	stmt *parser.ParsedDropDatabaseStatement
}

func (s *executableDropDatabaseStatement) execContext(ctx context.Context, c *conn, opts *ExecOptions) (driver.Result, error) {
	database := fmt.Sprintf(
		"projects/%s/instances/%s/databases/%s",
		c.connector.connectorConfig.Project,
		c.connector.connectorConfig.Instance,
		s.stmt.Identifier.String())
	request := &databasepb.DropDatabaseRequest{
		Database: database,
	}
	err := c.adminClient.DropDatabase(ctx, request)
	if err != nil {
		return nil, err
	}
	return driver.ResultNoRows, nil
}

func (s *executableDropDatabaseStatement) queryContext(ctx context.Context, c *conn, opts *ExecOptions) (driver.Rows, error) {
	if _, err := s.execContext(ctx, c, opts); err != nil {
		return nil, err
	}
	return createEmptyRows(opts), nil
}

type executableStartBatchStatement struct {
	stmt *parser.ParsedStartBatchStatement
}

func (s *executableStartBatchStatement) execContext(ctx context.Context, c *conn, opts *ExecOptions) (driver.Result, error) {
	switch s.stmt.Type {
	case parser.BatchTypeDml:
		return c.startBatchDML( /*automatic = */ false)
	case parser.BatchTypeDdl:
		return c.startBatchDDL()
	default:
		return nil, status.Errorf(codes.FailedPrecondition, "unknown batch type: %v", s.stmt.Type)
	}
}

func (s *executableStartBatchStatement) queryContext(ctx context.Context, c *conn, opts *ExecOptions) (driver.Rows, error) {
	if _, err := s.execContext(ctx, c, opts); err != nil {
		return nil, err
	}
	return createEmptyRows(opts), nil
}

type executableRunBatchStatement struct {
	stmt *parser.ParsedRunBatchStatement
}

func (s *executableRunBatchStatement) execContext(ctx context.Context, c *conn, opts *ExecOptions) (driver.Result, error) {
	return c.runBatch(ctx)
}

func (s *executableRunBatchStatement) queryContext(ctx context.Context, c *conn, opts *ExecOptions) (driver.Rows, error) {
	if _, err := s.execContext(ctx, c, opts); err != nil {
		return nil, err
	}
	return createEmptyRows(opts), nil
}

type executableAbortBatchStatement struct {
	stmt *parser.ParsedAbortBatchStatement
}

func (s *executableAbortBatchStatement) execContext(ctx context.Context, c *conn, opts *ExecOptions) (driver.Result, error) {
	return c.abortBatch()
}

func (s *executableAbortBatchStatement) queryContext(ctx context.Context, c *conn, opts *ExecOptions) (driver.Rows, error) {
	if _, err := s.execContext(ctx, c, opts); err != nil {
		return nil, err
	}
	return createEmptyRows(opts), nil
}

type executableBeginStatement struct {
	stmt *parser.ParsedBeginStatement
}

func (s *executableBeginStatement) execContext(ctx context.Context, c *conn, opts *ExecOptions) (driver.Result, error) {
	if len(s.stmt.Identifiers) != len(s.stmt.Literals) {
		return nil, status.Errorf(codes.InvalidArgument, "statement contains %d identifiers, but %d values given", len(s.stmt.Identifiers), len(s.stmt.Literals))
	}
	// The context that is passed in to c.BeginTx(..) becomes the transaction context. The transaction is automatically
	// rolled back when that context is cancelled. We therefore create a derived context here that is not cancelled when
	// the parent is cancelled.
	ctx = context.WithoutCancel(ctx)
	_, err := c.BeginTx(ctx, driver.TxOptions{})
	if err != nil {
		return nil, err
	}
	for index := range s.stmt.Identifiers {
		if err := c.setConnectionVariable(s.stmt.Identifiers[index], s.stmt.Literals[index].Value /*IsLocal=*/, true /*IsTransaction=*/, true); err != nil {
			return nil, err
		}
	}

	return driver.ResultNoRows, nil
}

func (s *executableBeginStatement) queryContext(ctx context.Context, c *conn, opts *ExecOptions) (driver.Rows, error) {
	if _, err := s.execContext(ctx, c, opts); err != nil {
		return nil, err
	}
	return createEmptyRows(opts), nil
}

type executableCommitStatement struct {
	stmt *parser.ParsedCommitStatement
}

func (s *executableCommitStatement) execContext(ctx context.Context, c *conn, opts *ExecOptions) (driver.Result, error) {
	_, err := c.Commit(ctx)
	if err != nil {
		return nil, err
	}
	return driver.ResultNoRows, nil
}

func (s *executableCommitStatement) queryContext(ctx context.Context, c *conn, opts *ExecOptions) (driver.Rows, error) {
	if _, err := s.execContext(ctx, c, opts); err != nil {
		return nil, err
	}
	return createEmptyRows(opts), nil
}

type executableRollbackStatement struct {
	stmt *parser.ParsedRollbackStatement
}

func (s *executableRollbackStatement) execContext(ctx context.Context, c *conn, opts *ExecOptions) (driver.Result, error) {
	if err := c.Rollback(ctx); err != nil {
		return nil, err
	}
	return driver.ResultNoRows, nil
}

func (s *executableRollbackStatement) queryContext(ctx context.Context, c *conn, opts *ExecOptions) (driver.Rows, error) {
	if _, err := s.execContext(ctx, c, opts); err != nil {
		return nil, err
	}
	return createEmptyRows(opts), nil
}
