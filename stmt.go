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
	"database/sql/driver"
	"errors"

	"cloud.google.com/go/spanner"
	"github.com/cloudspannerecosystem/go-sql-spanner/internal"
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
	panic("Using ExecContext instead")
}

func (s *stmt) ExecContext(ctx context.Context, args []driver.NamedValue) (driver.Result, error) {
	return s.conn.ExecContext(ctx, s.query, args)
}

func (s *stmt) Query(args []driver.Value) (driver.Rows, error) {
	panic("Using QueryContext instead")
}

func (s *stmt) QueryContext(ctx context.Context, args []driver.NamedValue) (driver.Rows, error) {
	ss, err := prepareSpannerStmt(s.query, args)
	if err != nil {
		return nil, err
	}

	var it *spanner.RowIterator
	if s.conn.roTx != nil {
		it = s.conn.roTx.Query(ctx, ss)
	} else if s.conn.rwTx != nil {
		it = s.conn.rwTx.Query(ctx, ss)
	} else {
		it = s.conn.client.Single().Query(ctx, ss)
	}
	return &rows{it: it}, nil
}

func prepareSpannerStmt(q string, args []driver.NamedValue) (spanner.Statement, error) {
	names, err := internal.NamedValueParamNames(q, len(args))
	if err != nil {
		return spanner.Statement{}, err
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
	return 0, errors.New("spanner doesn't autogenerate IDs")
}

func (r *result) RowsAffected() (int64, error) {
	return r.rowsAffected, nil
}
