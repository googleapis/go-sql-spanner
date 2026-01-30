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

	"cloud.google.com/go/spanner"
	"github.com/googleapis/go-sql-spanner/connectionstate"
	"github.com/googleapis/go-sql-spanner/parser"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/structpb"
)

// SpannerNamedArg can be used for query parameters with a name that (might) start
// with an underscore. The generic database/sql package does not allow query parameters
// to start with an underscore, but Spanner allows this, and this struct can be used to
// work around the limitation in database/sql.
type SpannerNamedArg struct {
	NameInQuery string
	Value       any
}

var _ driver.Stmt = &stmt{}
var _ driver.StmtExecContext = &stmt{}
var _ driver.StmtQueryContext = &stmt{}
var _ driver.NamedValueChecker = &stmt{}

var nullValue = structpb.NewNullValue()

type stmt struct {
	conn            *conn
	numArgs         int
	query           string
	statementInfo   *parser.StatementInfo
	parsedStatement parser.ParsedStatement
	execOptions     *ExecOptions
}

func (s *stmt) Close() error {
	return nil
}

func (s *stmt) NumInput() int {
	return s.numArgs
}

func (s *stmt) Exec(_ []driver.Value) (driver.Result, error) {
	return nil, spanner.ToSpannerError(status.Errorf(codes.Unimplemented, "use ExecContext instead"))
}

func (s *stmt) ExecContext(ctx context.Context, args []driver.NamedValue) (driver.Result, error) {
	return s.conn.execParsed(ctx, s.query, s.parsedStatement, s.statementInfo, s.execOptions, args)
}

func (s *stmt) Query(_ []driver.Value) (driver.Rows, error) {
	return nil, spanner.ToSpannerError(status.Errorf(codes.Unimplemented, "use QueryContext instead"))
}

func (s *stmt) QueryContext(ctx context.Context, args []driver.NamedValue) (driver.Rows, error) {
	return s.conn.queryParsed(ctx, s.query, s.parsedStatement, s.statementInfo, s.execOptions, args)
}

func (s *stmt) CheckNamedValue(value *driver.NamedValue) error {
	if value == nil {
		return nil
	}

	if execOptions, ok := value.Value.(ExecOptions); ok {
		s.execOptions = &execOptions
		return driver.ErrRemoveArgument
	}
	if execOptions, ok := value.Value.(*ExecOptions); ok {
		s.execOptions = execOptions
		return driver.ErrRemoveArgument
	}
	return s.conn.CheckNamedValue(value)
}

func prepareSpannerStmt(state *connectionstate.ConnectionState, parser *parser.StatementParser, q string, args []driver.NamedValue) (spanner.Statement, error) {
	q, names, err := parser.ParseParameters(q)
	if err != nil {
		return spanner.Statement{}, err
	}
	ss := spanner.NewStatement(q)
	typedStrings := propertySendTypedStrings.GetValueOrDefault(state)
	for i, v := range args {
		value := v.Value
		name := args[i].Name
		if sa, ok := args[i].Value.(SpannerNamedArg); ok {
			name = sa.NameInQuery
			value = sa.Value
		}
		if name == "" && len(names) > i {
			name = names[i]
		}
		if name != "" {
			ss.Params[name] = convertParam(value, typedStrings)
		}
	}
	// Verify that all parameters have a value.
	for _, name := range names {
		if _, ok := ss.Params[name]; !ok {
			return spanner.Statement{}, spanner.ToSpannerError(status.Errorf(codes.InvalidArgument, "missing value for query parameter %v", name))
		}
	}
	return ss, nil
}

func convertParam(v driver.Value, typedStrings bool) driver.Value {
	switch v := v.(type) {
	default:
		return v
	case string:
		if typedStrings {
			return v
		}
		// Send strings as untyped parameter values to allow automatic conversion to any type that is encoded as
		// strings. This for example allows DATE, TIMESTAMP, INTERVAL, JSON, INT64, etc. to all be set as a string
		// by the application.
		return spanner.GenericColumnValue{Value: structpb.NewStringValue(v)}
	case *string:
		if typedStrings {
			return v
		}
		if v == nil {
			return spanner.GenericColumnValue{Value: nullValue}
		}
		return spanner.GenericColumnValue{Value: structpb.NewStringValue(*v)}
	case []string:
		if typedStrings {
			return v
		}
		if v == nil {
			return spanner.GenericColumnValue{Value: nullValue}
		}
		values := make([]*structpb.Value, len(v))
		for i, s := range v {
			values[i] = structpb.NewStringValue(s)
		}
		return spanner.GenericColumnValue{Value: structpb.NewListValue(&structpb.ListValue{Values: values})}
	case *[]string:
		if typedStrings {
			return v
		}
		if v == nil {
			return spanner.GenericColumnValue{Value: nullValue}
		}
		values := make([]*structpb.Value, len(*v))
		for i, s := range *v {
			values[i] = structpb.NewStringValue(s)
		}
		return spanner.GenericColumnValue{Value: structpb.NewListValue(&structpb.ListValue{Values: values})}
	case []*string:
		if typedStrings {
			return v
		}
		if v == nil {
			return spanner.GenericColumnValue{Value: nullValue}
		}
		values := make([]*structpb.Value, len(v))
		for i, s := range v {
			if s == nil {
				values[i] = nullValue
			} else {
				values[i] = structpb.NewStringValue(*s)
			}
		}
		return spanner.GenericColumnValue{Value: structpb.NewListValue(&structpb.ListValue{Values: values})}
	case int:
		return int64(v)
	case []int:
		if v == nil {
			return []int64(nil)
		}
		res := make([]int64, len(v))
		for i, val := range v {
			res[i] = int64(val)
		}
		return res
	case uint:
		return int64(v)
	case []uint:
		if v == nil {
			return []int64(nil)
		}
		res := make([]int64, len(v))
		for i, val := range v {
			res[i] = int64(val)
		}
		return res
	case uint64:
		return int64(v)
	case []uint64:
		if v == nil {
			return []int64(nil)
		}
		res := make([]int64, len(v))
		for i, val := range v {
			res[i] = int64(val)
		}
		return res
	case *uint64:
		if v == nil {
			return (*int64)(nil)
		}
		vi := int64(*v)
		return &vi
	case *int:
		if v == nil {
			return (*int64)(nil)
		}
		vi := int64(*v)
		return &vi
	case []*int:
		if v == nil {
			return []*int64(nil)
		}
		res := make([]*int64, len(v))
		for i, val := range v {
			if val == nil {
				res[i] = nil
			} else {
				z := int64(*val)
				res[i] = &z
			}
		}
		return res
	case *[]int:
		if v == nil {
			return []int64(nil)
		}
		res := make([]int64, len(*v))
		for i, val := range *v {
			res[i] = int64(val)
		}
		return res
	case *uint:
		if v == nil {
			return (*int64)(nil)
		}
		vi := int64(*v)
		return &vi
	case []*uint:
		if v == nil {
			return []*int64(nil)
		}
		res := make([]*int64, len(v))
		for i, val := range v {
			if val == nil {
				res[i] = nil
			} else {
				z := int64(*val)
				res[i] = &z
			}
		}
		return res
	case *[]uint:
		if v == nil {
			return []int64(nil)
		}
		res := make([]int64, len(*v))
		for i, val := range *v {
			res[i] = int64(val)
		}
		return res
	case []*uint64:
		if v == nil {
			return []*int64(nil)
		}
		res := make([]*int64, len(v))
		for i, val := range v {
			if val == nil {
				res[i] = nil
			} else {
				z := int64(*val)
				res[i] = &z
			}
		}
		return res
	case *[]uint64:
		if v == nil {
			return []int64(nil)
		}
		res := make([]int64, len(*v))
		for i, val := range *v {
			res[i] = int64(val)
		}
		return res
	}
}

var _ SpannerResult = &result{}

type result struct {
	rowsAffected      int64
	lastInsertId      int64
	hasLastInsertId   bool
	batchUpdateCounts []int64
	tx                *sql.Tx
}

var errNoLastInsertId = spanner.ToSpannerError(
	status.Errorf(codes.FailedPrecondition,
		"LastInsertId is only supported for INSERT statements that use a THEN RETURN clause "+
			"and that return exactly one row and one column "+
			"(e.g. `INSERT INTO MyTable (Val) values ('val1') THEN RETURN Id`)"))

func (r *result) LastInsertId() (int64, error) {
	if r.hasLastInsertId {
		return r.lastInsertId, nil
	}
	return 0, errNoLastInsertId
}

func (r *result) RowsAffected() (int64, error) {
	return r.rowsAffected, nil
}

var errNoBatchRowsAffected = spanner.ToSpannerError(status.Errorf(codes.FailedPrecondition, "BatchRowsAffected is only supported for batch DML results"))

func (r *result) BatchRowsAffected() ([]int64, error) {
	if r.batchUpdateCounts == nil {
		return nil, errNoBatchRowsAffected
	}
	return r.batchUpdateCounts, nil
}
