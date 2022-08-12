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

	"cloud.google.com/go/spanner"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type stmt struct {
	conn    *conn
	numArgs int
	query   string
}

func (s *stmt) Close() error {
	return nil
}

func (s *stmt) NumInput() int {
	return s.numArgs
}

func (s *stmt) Exec(args []driver.Value) (driver.Result, error) {
	return nil, spanner.ToSpannerError(status.Errorf(codes.Unimplemented, "use ExecContext instead"))
}

func (s *stmt) ExecContext(ctx context.Context, args []driver.NamedValue) (driver.Result, error) {
	return s.conn.ExecContext(ctx, s.query, args)
}

func (s *stmt) Query(args []driver.Value) (driver.Rows, error) {
	return nil, spanner.ToSpannerError(status.Errorf(codes.Unimplemented, "use QueryContext instead"))
}

func (s *stmt) QueryContext(ctx context.Context, args []driver.NamedValue) (driver.Rows, error) {
	ss, err := prepareSpannerStmt(s.query, args)
	if err != nil {
		return nil, err
	}

	var it rowIterator
	if s.conn.tx != nil {
		it = s.conn.tx.Query(ctx, ss)
	} else {
		it = &readOnlyRowIterator{s.conn.client.Single().WithTimestampBound(s.conn.readOnlyStaleness).Query(ctx, ss)}
	}
	return &rows{it: it}, nil
}

func prepareSpannerStmt(q string, args []driver.NamedValue) (spanner.Statement, error) {
	q, err := convertPositionalParametersToNamedParameters('?', q)
	if err != nil {
		return spanner.Statement{}, err
	}
	names, err := parseNamedParameters(q)
	if err != nil {
		return spanner.Statement{}, err
	}
	if len(names) != len(args) {
		return spanner.Statement{}, spanner.ToSpannerError(status.Errorf(codes.InvalidArgument, "got %v argument values, but found %v parameters in the sql string", len(args), len(names)))
	}
	ss := spanner.NewStatement(q)
	for i, v := range args {
		name := args[i].Name
		if name == "" {
			name = names[i]
		}
		ss.Params[name] = v.Value
	}
	return ss, nil
}

type result struct {
	rowsAffected int64
}

func (r *result) LastInsertId() (int64, error) {
	return 0, spanner.ToSpannerError(status.Errorf(codes.Unimplemented, "Cloud Spanner does not support auto-generated ids"))
}

func (r *result) RowsAffected() (int64, error) {
	return r.rowsAffected, nil
}
