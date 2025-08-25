package spannerdriver

import (
	"context"
	"database/sql/driver"
	"fmt"
	"time"

	"cloud.google.com/go/spanner"
	"cloud.google.com/go/spanner/admin/database/apiv1/databasepb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type parsedStatement interface {
	parse(parser *statementParser, query string) error
	executableStatement(c *conn) *executableClientSideStatement
}

func parseStatement(parser *statementParser, keyword, query string) (parsedStatement, error) {
	var stmt parsedStatement
	if isShowStatementKeyword(keyword) {
		stmt = &parsedShowStatement{}
	} else if isSetStatementKeyword(keyword) {
		stmt = &parsedSetStatement{}
	} else {
		return nil, nil
	}
	if err := stmt.parse(parser, query); err != nil {
		return nil, err
	}
	return stmt, nil
}

// SHOW [VARIABLE] [my_extension.]my_property
type parsedShowStatement struct {
	query      string
	identifier identifier
}

func (s *parsedShowStatement) parse(parser *statementParser, query string) error {
	// Parse a statement of the form
	// SHOW [VARIABLE] [my_extension.]my_property
	sp := &simpleParser{sql: []byte(query), statementParser: parser}
	if _, ok := sp.eatKeyword("SHOW"); !ok {
		return status.Error(codes.InvalidArgument, "statement does not start with SHOW")
	}
	// Just eat and ignore the keyword VARIABLE.
	_, _ = sp.eatKeyword("VARIABLE")
	identifier, err := sp.eatIdentifier()
	if err != nil {
		return err
	}
	if sp.hasMoreTokens() {
		return status.Errorf(codes.InvalidArgument, "unexpected tokens at position %d in %q", sp.pos, sp.sql)
	}
	s.query = query
	s.identifier = identifier
	return nil
}

func (s *parsedShowStatement) execContext(ctx context.Context, c *conn, params string, opts ExecOptions, args []driver.NamedValue) (driver.Result, error) {
	return nil, spanner.ToSpannerError(status.Errorf(codes.InvalidArgument, "%q cannot be used with execContext", s.query))
}

func (s *parsedShowStatement) queryContext(ctx context.Context, c *conn, params string, opts ExecOptions, args []driver.NamedValue) (driver.Rows, error) {
	col := s.identifier.String()
	val, err := c.showConnectionVariable(s.identifier)
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
		if stringerVal, ok := val.(fmt.Stringer); ok {
			it, err = createStringIterator(col, stringerVal.String())
		} else {
			err = status.Errorf(codes.InvalidArgument, "unsupported type: %T", val)
		}
	}
	if err != nil {
		return nil, err
	}
	return createRows(it, opts), nil
}

func (s *parsedShowStatement) executableStatement(c *conn) *executableClientSideStatement {
	return &executableClientSideStatement{
		conn:  c,
		query: s.query,
		clientSideStatement: &clientSideStatement{
			Name:         "SHOW",
			execContext:  s.execContext,
			queryContext: s.queryContext,
		},
	}
}

// SET [LOCAL] [my_extension.]my_property {=|to} <value>
type parsedSetStatement struct {
	query      string
	identifier identifier
	literal    literal
	isLocal    bool
}

func (s *parsedSetStatement) parse(parser *statementParser, query string) error {
	// Parse a statement of the form
	// SET [LOCAL] [my_extension.]my_property {=|to} <value>
	sp := &simpleParser{sql: []byte(query), statementParser: parser}
	if _, ok := sp.eatKeyword("SET"); !ok {
		return status.Errorf(codes.InvalidArgument, "syntax error: expected SET")
	}
	_, isLocal := sp.eatKeyword("LOCAL")
	identifier, err := sp.eatIdentifier()
	if err != nil {
		return err
	}
	if !sp.eatToken('=') {
		// PostgreSQL supports both SET my_property TO <value> and SET my_property = <value>.
		if parser.dialect == databasepb.DatabaseDialect_POSTGRESQL {
			if _, ok := sp.eatKeyword("TO"); !ok {
				return status.Errorf(codes.InvalidArgument, "missing {=|to} in SET statement")
			}
		} else {
			return status.Errorf(codes.InvalidArgument, "missing = in SET statement")
		}
	}
	literalValue, err := sp.eatLiteral()
	if err != nil {
		return err
	}
	if sp.hasMoreTokens() {
		return status.Errorf(codes.InvalidArgument, "unexpected tokens at position %d in %q", sp.pos, sp.sql)
	}
	s.query = query
	s.identifier = identifier
	s.literal = literalValue
	s.isLocal = isLocal
	return nil
}

func (s *parsedSetStatement) executableStatement(c *conn) *executableClientSideStatement {
	return &executableClientSideStatement{
		conn:   c,
		query:  s.query,
		params: s.literal.value,
		clientSideStatement: &clientSideStatement{
			Name:         "SET",
			execContext:  s.execContext,
			queryContext: s.queryContext,
		},
	}
}

func (s *parsedSetStatement) execContext(ctx context.Context, c *conn, params string, opts ExecOptions, args []driver.NamedValue) (driver.Result, error) {
	if err := c.setConnectionVariable(s.identifier, s.literal.value, s.isLocal); err != nil {
		return nil, err
	}
	return driver.ResultNoRows, nil
}

func (s *parsedSetStatement) queryContext(ctx context.Context, c *conn, params string, opts ExecOptions, args []driver.NamedValue) (driver.Rows, error) {
	if err := c.setConnectionVariable(s.identifier, s.literal.value, s.isLocal); err != nil {
		return nil, err
	}
	it := createEmptyIterator()
	return &rows{
		it:                      it,
		decodeOption:            opts.DecodeOption,
		decodeToNativeArrays:    opts.DecodeToNativeArrays,
		returnResultSetMetadata: opts.ReturnResultSetMetadata,
		returnResultSetStats:    opts.ReturnResultSetStats,
	}, nil
}
