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
	"fmt"
	"strconv"
	"strings"

	"cloud.google.com/go/spanner"
	"cloud.google.com/go/spanner/admin/database/apiv1/databasepb"
	"cloud.google.com/go/spanner/apiv1/spannerpb"
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
	res, err := s.conn.execParsed(ctx, s.query, s.parsedStatement, s.statementInfo, s.execOptions, args)
	return res, checkAndEnrichError(s.conn.IsPostgreSQL(), err)
}

func (s *stmt) Query(_ []driver.Value) (driver.Rows, error) {
	return nil, spanner.ToSpannerError(status.Errorf(codes.Unimplemented, "use QueryContext instead"))
}

func (s *stmt) QueryContext(ctx context.Context, args []driver.NamedValue) (driver.Rows, error) {
	rows, err := s.conn.queryParsed(ctx, s.query, s.parsedStatement, s.statementInfo, s.execOptions, args)
	return rows, checkAndEnrichError(s.conn.IsPostgreSQL(), err)
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
	q, names, namesToIndex, err := parser.ParseParameters(q)
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
		if name != "" {
			found := false
			if _, ok := namesToIndex[name]; ok {
				found = true
			}
			if !found {
				for _, queryName := range names {
					if queryName == name {
						found = true
						break
					}
				}
			}
			if !found && len(namesToIndex) > 0 {
				for queryName := range namesToIndex {
					if strings.EqualFold(queryName, name) {
						name = queryName
						found = true
						break
					}
				}
			}
			if !found {
				for _, queryName := range names {
					if strings.EqualFold(queryName, name) {
						name = queryName
						break
					}
				}
			}
		}
		if name == "" && len(names) > i {
			name = names[i]
		} else if index, ok := namesToIndex[name]; ok {
			name = "p" + strconv.Itoa(index)
		}
		if name != "" {
			converted := convertParam(value, typedStrings)
			if parser.Dialect == databasepb.DatabaseDialect_POSTGRESQL {
				converted = convertPGParam(converted)
			}
			ss.Params[name] = converted
		}
	}
	// Verify that all parameters have a value.
	for _, name := range names {
		if _, ok := ss.Params[name]; !ok {
			originalName := name
			for k, v := range namesToIndex {
				if fmt.Sprintf("p%d", v) == name {
					originalName = k
					break
				}
			}
			return spanner.Statement{}, spanner.ToSpannerError(status.Errorf(codes.InvalidArgument, "missing value for query parameter @%s", originalName))
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

func convertPGParam(v driver.Value) driver.Value {
	if gcv, ok := v.(spanner.GenericColumnValue); ok {
		if gcv.Type != nil && gcv.Value != nil {
			if gcv.Type.Code == spannerpb.TypeCode_BOOL {
				if s, ok := gcv.Value.Kind.(*structpb.Value_StringValue); ok && s != nil {
					if b, err := parsePGBoolLiteral(s.StringValue); err == nil {
						gcv.Value = structpb.NewBoolValue(b)
						return gcv
					}
				}
			} else if gcv.Type.Code == spannerpb.TypeCode_ARRAY {
				if s, ok := gcv.Value.Kind.(*structpb.Value_StringValue); ok && s != nil {
					if parsedList, err := parsePGArrayLiteral(s.StringValue, gcv.Type.ArrayElementType); err == nil && parsedList != nil {
						gcv.Value = parsedList
						return gcv
					}
				}
			}
		}
	}
	return v
}

func parsePGBoolLiteral(s string) (bool, error) {
	lower := strings.ToLower(strings.TrimSpace(s))
	switch lower {
	case "t", "tr", "tru", "true", "y", "ye", "yes", "on", "1":
		return true, nil
	case "f", "fa", "fal", "fals", "false", "n", "no", "of", "off", "0":
		return false, nil
	}
	return false, fmt.Errorf("invalid boolean value: %q", s)
}

func isPGWhitespace(ch byte) bool {
	return ch == ' ' || ch == '\t' || ch == '\n' || ch == '\r'
}

func parsePGArrayLiteral(str string, elemType *spannerpb.Type) (*structpb.Value, error) {
	trimmed := strings.TrimSpace(str)
	if !strings.HasPrefix(trimmed, "{") || !strings.HasSuffix(trimmed, "}") {
		return nil, fmt.Errorf("invalid PG array literal format")
	}
	inner := strings.TrimSpace(trimmed[1 : len(trimmed)-1])
	if inner == "" {
		return structpb.NewListValue(&structpb.ListValue{Values: []*structpb.Value{}}), nil
	}

	var values []*structpb.Value
	var current strings.Builder
	inQuotes := false
	escapeNext := false
	wasQuoted := false

	for i := 0; i < len(inner); i++ {
		ch := inner[i]
		if !inQuotes && isPGWhitespace(ch) && current.Len() == 0 {
			continue
		}
		if escapeNext {
			current.WriteByte(ch)
			escapeNext = false
			continue
		}
		if ch == '\\' {
			escapeNext = true
			continue
		}
		if ch == '"' {
			inQuotes = !inQuotes
			wasQuoted = true
			continue
		}
		if !inQuotes && (ch == '{' || ch == '}') {
			return nil, fmt.Errorf("nested or unquoted brace in array literal not supported: %c", ch)
		}
		if ch == ',' && !inQuotes {
			if current.Len() == 0 && !wasQuoted {
				return nil, fmt.Errorf("consecutive or leading comma in array literal")
			}
			val, err := convertPGArrayToken(current.String(), wasQuoted, elemType)
			if err != nil {
				return nil, err
			}
			values = append(values, val)
			current.Reset()
			wasQuoted = false
			continue
		}
		if !inQuotes && wasQuoted {
			if !isPGWhitespace(ch) {
				return nil, fmt.Errorf("unexpected character %q after closing quote", ch)
			}
			continue
		}
		current.WriteByte(ch)
	}
	if inQuotes {
		return nil, fmt.Errorf("unclosed double quote in array literal")
	}
	if escapeNext {
		return nil, fmt.Errorf("trailing backslash in array literal")
	}
	if current.Len() == 0 && !wasQuoted && len(values) > 0 {
		return nil, fmt.Errorf("trailing comma in array literal")
	}
	val, err := convertPGArrayToken(current.String(), wasQuoted, elemType)
	if err != nil {
		return nil, err
	}
	values = append(values, val)
	return structpb.NewListValue(&structpb.ListValue{Values: values}), nil
}

func convertPGArrayToken(token string, wasQuoted bool, elemType *spannerpb.Type) (*structpb.Value, error) {
	if !wasQuoted && strings.EqualFold(strings.TrimSpace(token), "NULL") {
		return structpb.NewNullValue(), nil
	}
	t := token
	if !wasQuoted {
		t = strings.TrimSpace(token)
	}

	elemCode := spannerpb.TypeCode_TYPE_CODE_UNSPECIFIED
	if elemType != nil {
		elemCode = elemType.Code
	}

	switch elemCode {
	case spannerpb.TypeCode_BOOL:
		b, err := parsePGBoolLiteral(t)
		if err != nil {
			return nil, err
		}
		return structpb.NewBoolValue(b), nil
	case spannerpb.TypeCode_FLOAT64, spannerpb.TypeCode_FLOAT32:
		trimmedT := strings.TrimSpace(t)
		lower := strings.ToLower(trimmedT)
		if lower == "inf" || lower == "infinity" || lower == "+inf" || lower == "+infinity" {
			return structpb.NewStringValue("Infinity"), nil
		}
		if lower == "-inf" || lower == "-infinity" {
			return structpb.NewStringValue("-Infinity"), nil
		}
		if lower == "nan" {
			return structpb.NewStringValue("NaN"), nil
		}
		f, err := strconv.ParseFloat(trimmedT, 64)
		if err != nil {
			return nil, fmt.Errorf("invalid float value: %q", t)
		}
		return structpb.NewNumberValue(f), nil
	default:
		return structpb.NewStringValue(t), nil
	}
}

var _ SpannerResult = &result{}

type result struct {
	rowsAffected      int64
	lastInsertId      int64
	hasLastInsertId   bool
	batchUpdateCounts []int64
	operationID       string
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

var errNoOperationID = spanner.ToSpannerError(status.Errorf(codes.FailedPrecondition, "OperationID is only supported for DDL statements executed in ASYNC mode"))

func (r *result) OperationID() (string, error) {
	if r.operationID == "" {
		return "", errNoOperationID
	}
	return r.operationID, nil
}
