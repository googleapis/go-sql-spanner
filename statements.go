package spannerdriver

import (
	"context"
	"database/sql/driver"
	"encoding/json"
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
	} else if isResetStatementKeyword(keyword) {
		stmt = &parsedResetStatement{}
	} else if isCreateKeyword(keyword) && isCreateDatabase(parser, query) {
		stmt = &parsedCreateDatabaseStatement{}
	} else if isDropKeyword(keyword) && isDropDatabase(parser, query) {
		stmt = &parsedDropDatabaseStatement{}
	} else if isStartStatementKeyword(keyword) {
		stmt = &parsedStartBatchStatement{}
	} else if isRunStatementKeyword(keyword) {
		stmt = &parsedRunBatchStatement{}
	} else if isAbortStatementKeyword(keyword) {
		stmt = &parsedAbortBatchStatement{}
	} else {
		return nil, nil
	}
	if err := stmt.parse(parser, query); err != nil {
		return nil, err
	}
	return stmt, nil
}

func isCreateDatabase(parser *statementParser, query string) bool {
	sp := &simpleParser{sql: []byte(query), statementParser: parser}
	if _, ok := sp.eatKeyword("create"); !ok {
		return false
	}
	if _, ok := sp.eatKeyword("database"); !ok {
		return false
	}
	return true
}

func isDropDatabase(parser *statementParser, query string) bool {
	sp := &simpleParser{sql: []byte(query), statementParser: parser}
	if _, ok := sp.eatKeyword("drop"); !ok {
		return false
	}
	if _, ok := sp.eatKeyword("database"); !ok {
		return false
	}
	return true
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
	if parser.dialect == databasepb.DatabaseDialect_GOOGLE_STANDARD_SQL {
		// Just eat and ignore the keyword VARIABLE.
		if _, ok := sp.eatKeyword("VARIABLE"); !ok {
			return status.Error(codes.InvalidArgument, "missing keyword VARIABLE")
		}
	}
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

func (s *parsedShowStatement) execContext(ctx context.Context, c *conn, params string, opts *ExecOptions, args []driver.NamedValue) (driver.Result, error) {
	return nil, spanner.ToSpannerError(status.Errorf(codes.InvalidArgument, "%q cannot be used with execContext", s.query))
}

func (s *parsedShowStatement) queryContext(ctx context.Context, c *conn, params string, opts *ExecOptions, args []driver.NamedValue) (driver.Rows, error) {
	col := s.identifier.String()
	val, hasValue, err := c.showConnectionVariable(s.identifier)
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

// SET [SESSION | LOCAL] [my_extension.]my_property {=|to} <value>
type parsedSetStatement struct {
	query      string
	identifier identifier
	literal    literal
	isLocal    bool
}

func (s *parsedSetStatement) parse(parser *statementParser, query string) error {
	// Parse a statement of the form
	// SET [SESSION | LOCAL] [my_extension.]my_property {=|to} <value>
	sp := &simpleParser{sql: []byte(query), statementParser: parser}
	if _, ok := sp.eatKeyword("SET"); !ok {
		return status.Errorf(codes.InvalidArgument, "syntax error: expected SET")
	}
	_, isLocal := sp.eatKeyword("LOCAL")
	if !isLocal && parser.dialect == databasepb.DatabaseDialect_POSTGRESQL {
		// Just eat and ignore the SESSION keyword if it exists, as SESSION is the default.
		_, _ = sp.eatKeyword("SESSION")
	}
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

func (s *parsedSetStatement) execContext(ctx context.Context, c *conn, params string, opts *ExecOptions, args []driver.NamedValue) (driver.Result, error) {
	if err := c.setConnectionVariable(s.identifier, s.literal.value, s.isLocal); err != nil {
		return nil, err
	}
	return driver.ResultNoRows, nil
}

func (s *parsedSetStatement) queryContext(ctx context.Context, c *conn, params string, opts *ExecOptions, args []driver.NamedValue) (driver.Rows, error) {
	if err := c.setConnectionVariable(s.identifier, s.literal.value, s.isLocal); err != nil {
		return nil, err
	}
	return createEmptyRows(opts), nil
}

// RESET [my_extension.]my_property
type parsedResetStatement struct {
	query      string
	identifier identifier
}

func (s *parsedResetStatement) parse(parser *statementParser, query string) error {
	// Parse a statement of the form
	// REST [my_extension.]my_property
	sp := &simpleParser{sql: []byte(query), statementParser: parser}
	if _, ok := sp.eatKeyword("RESET"); !ok {
		return status.Error(codes.InvalidArgument, "statement does not start with RESET")
	}
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

func (s *parsedResetStatement) execContext(ctx context.Context, c *conn, params string, opts *ExecOptions, args []driver.NamedValue) (driver.Result, error) {
	if err := c.setConnectionVariable(s.identifier, "default", false); err != nil {
		return nil, err
	}
	return driver.ResultNoRows, nil
}

func (s *parsedResetStatement) queryContext(ctx context.Context, c *conn, params string, opts *ExecOptions, args []driver.NamedValue) (driver.Rows, error) {
	if err := c.setConnectionVariable(s.identifier, "default", false); err != nil {
		return nil, err
	}
	return createEmptyRows(opts), nil
}

func (s *parsedResetStatement) executableStatement(c *conn) *executableClientSideStatement {
	return &executableClientSideStatement{
		conn:  c,
		query: s.query,
		clientSideStatement: &clientSideStatement{
			Name:         "RESET",
			execContext:  s.execContext,
			queryContext: s.queryContext,
		},
	}
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

type parsedCreateDatabaseStatement struct {
	query      string
	identifier identifier
}

func (s *parsedCreateDatabaseStatement) parse(parser *statementParser, query string) error {
	// Parse a statement of the form
	// CREATE DATABASE <database-name>
	sp := &simpleParser{sql: []byte(query), statementParser: parser}
	if _, ok := sp.eatKeyword("CREATE"); !ok {
		return status.Error(codes.InvalidArgument, "statement does not start with CREATE DATABASE")
	}
	if _, ok := sp.eatKeyword("DATABASE"); !ok {
		return status.Error(codes.InvalidArgument, "statement does not start with CREATE DATABASE")
	}
	identifier, err := sp.eatIdentifier()
	if err != nil {
		return err
	}
	s.query = query
	s.identifier = identifier
	return nil
}

func (s *parsedCreateDatabaseStatement) execContext(ctx context.Context, c *conn, params string, opts *ExecOptions, args []driver.NamedValue) (driver.Result, error) {
	instance := fmt.Sprintf("projects/%s/instances/%s", c.connector.connectorConfig.Project, c.connector.connectorConfig.Instance)
	request := &databasepb.CreateDatabaseRequest{
		CreateStatement: s.query,
		Parent:          instance,
		DatabaseDialect: c.parser.dialect,
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

func (s *parsedCreateDatabaseStatement) queryContext(ctx context.Context, c *conn, params string, opts *ExecOptions, args []driver.NamedValue) (driver.Rows, error) {
	if _, err := s.execContext(ctx, c, params, opts, args); err != nil {
		return nil, err
	}
	return createEmptyRows(opts), nil
}

func (s *parsedCreateDatabaseStatement) executableStatement(c *conn) *executableClientSideStatement {
	return &executableClientSideStatement{
		conn:  c,
		query: s.query,
		clientSideStatement: &clientSideStatement{
			Name:         "CREATE DATABASE",
			execContext:  s.execContext,
			queryContext: s.queryContext,
		},
	}
}

type parsedDropDatabaseStatement struct {
	query      string
	identifier identifier
}

func (s *parsedDropDatabaseStatement) parse(parser *statementParser, query string) error {
	// Parse a statement of the form
	// DROP DATABASE <database-name>
	sp := &simpleParser{sql: []byte(query), statementParser: parser}
	if _, ok := sp.eatKeyword("DROP"); !ok {
		return status.Error(codes.InvalidArgument, "statement does not start with DROP DATABASE")
	}
	if _, ok := sp.eatKeyword("DATABASE"); !ok {
		return status.Error(codes.InvalidArgument, "statement does not start with DROP DATABASE")
	}
	identifier, err := sp.eatIdentifier()
	if err != nil {
		return err
	}
	s.query = query
	s.identifier = identifier
	return nil
}

func (s *parsedDropDatabaseStatement) execContext(ctx context.Context, c *conn, params string, opts *ExecOptions, args []driver.NamedValue) (driver.Result, error) {
	database := fmt.Sprintf(
		"projects/%s/instances/%s/databases/%s",
		c.connector.connectorConfig.Project,
		c.connector.connectorConfig.Instance,
		s.identifier.String())
	request := &databasepb.DropDatabaseRequest{
		Database: database,
	}
	err := c.adminClient.DropDatabase(ctx, request)
	if err != nil {
		return nil, err
	}
	return driver.ResultNoRows, nil
}

func (s *parsedDropDatabaseStatement) queryContext(ctx context.Context, c *conn, params string, opts *ExecOptions, args []driver.NamedValue) (driver.Rows, error) {
	if _, err := s.execContext(ctx, c, params, opts, args); err != nil {
		return nil, err
	}
	return createEmptyRows(opts), nil
}

func (s *parsedDropDatabaseStatement) executableStatement(c *conn) *executableClientSideStatement {
	return &executableClientSideStatement{
		conn:  c,
		query: s.query,
		clientSideStatement: &clientSideStatement{
			Name:         "DROP DATABASE",
			execContext:  s.execContext,
			queryContext: s.queryContext,
		},
	}
}

type parsedStartBatchStatement struct {
	query string
	tp    batchType
}

func (s *parsedStartBatchStatement) parse(parser *statementParser, query string) error {
	// Parse a statement of the form
	// START BATCH {DDL | DML}
	sp := &simpleParser{sql: []byte(query), statementParser: parser}
	if !sp.eatKeywords([]string{"START", "BATCH"}) {
		return status.Error(codes.InvalidArgument, "statement does not start with START BATCH")
	}
	if _, ok := sp.eatKeyword("DML"); ok {
		s.tp = dml
	} else if _, ok := sp.eatKeyword("DDL"); ok {
		s.tp = ddl
	} else {
		return status.Errorf(codes.InvalidArgument, "unexpected token at pos %d in %q, expected DML or DDL", sp.pos, sp.sql)
	}
	if sp.hasMoreTokens() {
		return status.Errorf(codes.InvalidArgument, "unexpected tokens at position %d in %q", sp.pos, sp.sql)
	}
	s.query = query
	return nil
}

func (s *parsedStartBatchStatement) execContext(ctx context.Context, c *conn, params string, opts *ExecOptions, args []driver.NamedValue) (driver.Result, error) {
	switch s.tp {
	case dml:
		return c.startBatchDML( /*automatic = */ false)
	case ddl:
		return c.startBatchDDL()
	default:
		return nil, status.Errorf(codes.FailedPrecondition, "unknown batch type: %v", s.tp)
	}
}

func (s *parsedStartBatchStatement) queryContext(ctx context.Context, c *conn, params string, opts *ExecOptions, args []driver.NamedValue) (driver.Rows, error) {
	if _, err := s.execContext(ctx, c, params, opts, args); err != nil {
		return nil, err
	}
	return createEmptyRows(opts), nil
}

func (s *parsedStartBatchStatement) executableStatement(c *conn) *executableClientSideStatement {
	return &executableClientSideStatement{
		conn:  c,
		query: s.query,
		clientSideStatement: &clientSideStatement{
			Name:         "START BATCH",
			execContext:  s.execContext,
			queryContext: s.queryContext,
		},
	}
}

type parsedRunBatchStatement struct {
	query string
}

func (s *parsedRunBatchStatement) parse(parser *statementParser, query string) error {
	// Parse a statement of the form
	// RUN BATCH
	sp := &simpleParser{sql: []byte(query), statementParser: parser}
	if !sp.eatKeywords([]string{"RUN", "BATCH"}) {
		return status.Error(codes.InvalidArgument, "statement does not start with RUN BATCH")
	}
	if sp.hasMoreTokens() {
		return status.Errorf(codes.InvalidArgument, "unexpected tokens at position %d in %q", sp.pos, sp.sql)
	}
	s.query = query
	return nil
}

func (s *parsedRunBatchStatement) execContext(ctx context.Context, c *conn, params string, opts *ExecOptions, args []driver.NamedValue) (driver.Result, error) {
	return c.runBatch(ctx)
}

func (s *parsedRunBatchStatement) queryContext(ctx context.Context, c *conn, params string, opts *ExecOptions, args []driver.NamedValue) (driver.Rows, error) {
	if _, err := s.execContext(ctx, c, params, opts, args); err != nil {
		return nil, err
	}
	return createEmptyRows(opts), nil
}

func (s *parsedRunBatchStatement) executableStatement(c *conn) *executableClientSideStatement {
	return &executableClientSideStatement{
		conn:  c,
		query: s.query,
		clientSideStatement: &clientSideStatement{
			Name:         "RUN BATCH",
			execContext:  s.execContext,
			queryContext: s.queryContext,
		},
	}
}

type parsedAbortBatchStatement struct {
	query string
}

func (s *parsedAbortBatchStatement) parse(parser *statementParser, query string) error {
	// Parse a statement of the form
	// ABORT BATCH
	sp := &simpleParser{sql: []byte(query), statementParser: parser}
	if !sp.eatKeywords([]string{"ABORT", "BATCH"}) {
		return status.Error(codes.InvalidArgument, "statement does not start with ABORT BATCH")
	}
	if sp.hasMoreTokens() {
		return status.Errorf(codes.InvalidArgument, "unexpected tokens at position %d in %q", sp.pos, sp.sql)
	}
	s.query = query
	return nil
}

func (s *parsedAbortBatchStatement) execContext(ctx context.Context, c *conn, params string, opts *ExecOptions, args []driver.NamedValue) (driver.Result, error) {
	return c.abortBatch()
}

func (s *parsedAbortBatchStatement) queryContext(ctx context.Context, c *conn, params string, opts *ExecOptions, args []driver.NamedValue) (driver.Rows, error) {
	if _, err := s.execContext(ctx, c, params, opts, args); err != nil {
		return nil, err
	}
	return createEmptyRows(opts), nil
}

func (s *parsedAbortBatchStatement) executableStatement(c *conn) *executableClientSideStatement {
	return &executableClientSideStatement{
		conn:  c,
		query: s.query,
		clientSideStatement: &clientSideStatement{
			Name:         "ABORT BATCH",
			execContext:  s.execContext,
			queryContext: s.queryContext,
		},
	}
}
