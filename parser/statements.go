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

package parser

import (
	"fmt"

	"cloud.google.com/go/spanner/admin/database/apiv1/databasepb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type ParsedStatement interface {
	parse(parser *StatementParser, query string) error
	Name() string
	Query() string
}

func parseStatement(parser *StatementParser, keyword, query string) (ParsedStatement, error) {
	var stmt ParsedStatement
	if isShowStatementKeyword(keyword) {
		stmt = &ParsedShowStatement{}
	} else if isSetStatementKeyword(keyword) {
		stmt = &ParsedSetStatement{}
	} else if isResetStatementKeyword(keyword) {
		stmt = &ParsedResetStatement{}
	} else if isStartStatementKeyword(keyword) {
		if parser.Dialect == databasepb.DatabaseDialect_POSTGRESQL && isStartTransaction(parser, query) {
			stmt = &ParsedBeginStatement{}
		} else {
			stmt = &ParsedStartBatchStatement{}
		}
	} else if isRunStatementKeyword(keyword) {
		stmt = &ParsedRunBatchStatement{}
	} else if isAbortStatementKeyword(keyword) {
		stmt = &ParsedAbortBatchStatement{}
	} else if isBeginStatementKeyword(keyword) {
		stmt = &ParsedBeginStatement{}
	} else if isCommitStatementKeyword(keyword) {
		stmt = &ParsedCommitStatement{}
	} else if isRollbackStatementKeyword(keyword) {
		stmt = &ParsedRollbackStatement{}
	} else {
		return nil, nil
	}
	if err := stmt.parse(parser, query); err != nil {
		return nil, err
	}
	return stmt, nil
}

func isCreateDatabase(parser *StatementParser, query string) bool {
	sp := &simpleParser{sql: []byte(query), statementParser: parser}
	if !sp.eatKeyword("create") {
		return false
	}
	if !sp.eatKeyword("database") {
		return false
	}
	return true
}

func isDropDatabase(parser *StatementParser, query string) bool {
	sp := &simpleParser{sql: []byte(query), statementParser: parser}
	if !sp.eatKeyword("drop") {
		return false
	}
	if !sp.eatKeyword("database") {
		return false
	}
	return true
}

func isStartTransaction(parser *StatementParser, query string) bool {
	sp := &simpleParser{sql: []byte(query), statementParser: parser}
	if !sp.eatKeyword("start") {
		return false
	}
	if !sp.hasMoreTokens() {
		// START is a synonym for START TRANSACTION
		return true
	}
	if sp.eatKeyword("transaction") || sp.eatKeyword("work") {
		return true
	}
	return false
}

// ParsedShowStatement is a statement of the form
// SHOW [VARIABLE] [my_extension.]my_property
type ParsedShowStatement struct {
	query      string
	Identifier Identifier
}

func (s *ParsedShowStatement) Name() string {
	return "SHOW"
}

func (s *ParsedShowStatement) Query() string {
	return s.query
}

func (s *ParsedShowStatement) parse(parser *StatementParser, query string) error {
	// Parse a statement of the form
	// SHOW [VARIABLE] [my_extension.]my_property
	sp := &simpleParser{sql: []byte(query), statementParser: parser}
	if !sp.eatKeyword("SHOW") {
		return status.Error(codes.InvalidArgument, "statement does not start with SHOW")
	}
	if parser.Dialect == databasepb.DatabaseDialect_GOOGLE_STANDARD_SQL {
		// Just eat and ignore the keyword VARIABLE.
		if !sp.eatKeyword("VARIABLE") {
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
	s.Identifier = identifier
	return nil
}

// ParsedSetStatement is a statement of the form
// SET [SESSION | LOCAL] [my_extension.]my_property {=|to} <value>
//
// It also covers statements of the form SET TRANSACTION. This is a
// synonym for SET LOCAL, but is only supported for a specific set of
// properties, and may only be executed before a transaction has been
// activated. Examples include:
// SET TRANSACTION READ ONLY
// SET TRANSACTION ISOLATION LEVEL [SERIALIZABLE | REPEATABLE READ]
//
// One SET statement can set more than one property.
type ParsedSetStatement struct {
	query string
	// Identifiers contains the properties that are being set. The number of elements in this slice
	// must be equal to the number of Literals.
	Identifiers []Identifier
	// Literals contains the values that should be set for the properties.
	Literals []Literal
	// IsLocal indicates whether this is a SET LOCAL statement or not.
	IsLocal bool
	// IsTransaction indicates whether this is a SET TRANSACTION statement or not.
	// IsTransaction automatically also implies IsLocal.
	IsTransaction bool
}

func (s *ParsedSetStatement) Name() string {
	return "SET"
}

func (s *ParsedSetStatement) Query() string {
	return s.query
}

func (s *ParsedSetStatement) parse(parser *StatementParser, query string) error {
	// Parse a statement of the form
	// SET [SESSION | LOCAL] [my_extension.]my_property {=|to} <value>
	sp := &simpleParser{sql: []byte(query), statementParser: parser}
	if !sp.eatKeyword("SET") {
		return status.Errorf(codes.InvalidArgument, "syntax error: expected SET")
	}
	isLocal := sp.eatKeyword("LOCAL")
	isTransaction := false
	if !isLocal {
		isTransaction = sp.eatKeyword("TRANSACTION")
	}
	if !isLocal && !isTransaction && parser.Dialect == databasepb.DatabaseDialect_POSTGRESQL {
		// Just eat and ignore the SESSION keyword if it exists, as SESSION is the default.
		_ = sp.eatKeyword("SESSION")
	}
	if isTransaction {
		return s.parseSetTransaction(sp, query)
	}
	identifier, err := sp.eatIdentifier()
	if err != nil {
		return err
	}
	if !sp.eatToken('=') {
		// PostgreSQL supports both SET my_property TO <value> and SET my_property = <value>.
		if parser.Dialect == databasepb.DatabaseDialect_POSTGRESQL {
			if !sp.eatKeyword("TO") {
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
	s.Identifiers = []Identifier{identifier}
	s.Literals = []Literal{literalValue}
	s.IsLocal = isLocal
	return nil
}

func (s *ParsedSetStatement) parseSetTransaction(sp *simpleParser, query string) error {
	if !sp.hasMoreTokens() {
		return status.Errorf(codes.InvalidArgument, "syntax error: missing TRANSACTION OPTION, expected one of ISOLATION LEVEL, READ WRITE, or READ ONLY")
	}
	s.query = query
	s.IsLocal = true
	s.IsTransaction = true

	var err error
	s.Identifiers, s.Literals, err = parseTransactionOptions(sp)
	if err != nil {
		return err
	}
	return nil
}

func parseTransactionOptions(sp *simpleParser) ([]Identifier, []Literal, error) {
	identifiers := make([]Identifier, 0, 2)
	literals := make([]Literal, 0, 2)
	var err error
	for {
		if sp.peekKeyword("ISOLATION") {
			identifiers, literals, err = parseTransactionIsolationLevel(sp, identifiers, literals)
			if err != nil {
				return nil, nil, err
			}
		} else if sp.peekKeyword("READ") {
			identifiers, literals, err = parseTransactionMode(sp, identifiers, literals)
			if err != nil {
				return nil, nil, err
			}
		} else if sp.statementParser.Dialect == databasepb.DatabaseDialect_POSTGRESQL && (sp.peekKeyword("DEFERRABLE") || sp.peekKeyword("NOT")) {
			// https://www.postgresql.org/docs/current/sql-set-transaction.html
			identifiers, literals, err = parseTransactionDeferrable(sp, identifiers, literals)
			if err != nil {
				return nil, nil, err
			}
		} else {
			return nil, nil, status.Error(codes.InvalidArgument, "invalid TRANSACTION option, expected one of ISOLATION LEVEL, READ WRITE, or READ ONLY")
		}
		if !sp.hasMoreTokens() {
			return identifiers, literals, nil
		}
		// Eat and ignore any commas separating the various options.
		sp.eatToken(',')
	}
}

func parseTransactionIsolationLevel(sp *simpleParser, identifiers []Identifier, literals []Literal) ([]Identifier, []Literal, error) {
	if !sp.eatKeywords([]string{"ISOLATION", "LEVEL"}) {
		return nil, nil, status.Errorf(codes.InvalidArgument, "syntax error: expected ISOLATION LEVEL")
	}
	var value Literal
	if sp.eatKeyword("SERIALIZABLE") {
		value = Literal{Value: "serializable"}
	} else if sp.eatKeywords([]string{"REPEATABLE", "READ"}) {
		value = Literal{Value: "repeatable_read"}
	} else {
		return nil, nil, status.Errorf(codes.InvalidArgument, "syntax error: expected SERIALIZABLE OR REPETABLE READ")
	}

	identifiers = append(identifiers, Identifier{Parts: []string{"isolation_level"}})
	literals = append(literals, value)
	return identifiers, literals, nil
}

func parseTransactionMode(sp *simpleParser, identifiers []Identifier, literals []Literal) ([]Identifier, []Literal, error) {
	readOnly := false
	if sp.eatKeywords([]string{"READ", "ONLY"}) {
		readOnly = true
	} else if sp.eatKeywords([]string{"READ", "WRITE"}) {
		readOnly = false
	} else {
		return nil, nil, status.Errorf(codes.InvalidArgument, "syntax error: expected READ ONLY or READ WRITE")
	}

	identifiers = append(identifiers, Identifier{Parts: []string{"transaction_read_only"}})
	literals = append(literals, Literal{Value: fmt.Sprintf("%v", readOnly)})
	return identifiers, literals, nil
}

func parseTransactionDeferrable(sp *simpleParser, identifiers []Identifier, literals []Literal) ([]Identifier, []Literal, error) {
	deferrable := false
	if sp.eatKeywords([]string{"NOT", "DEFERRABLE"}) {
		deferrable = false
	} else if sp.eatKeyword("DEFERRABLE") {
		deferrable = true
	} else {
		return nil, nil, status.Errorf(codes.InvalidArgument, "syntax error: expected [NOT] DEFERRABLE")
	}

	identifiers = append(identifiers, Identifier{Parts: []string{"transaction_deferrable"}})
	literals = append(literals, Literal{Value: fmt.Sprintf("%v", deferrable)})
	return identifiers, literals, nil
}

// ParsedResetStatement is a statement of the form
// RESET [my_extension.]my_property
type ParsedResetStatement struct {
	query      string
	Identifier Identifier
}

func (s *ParsedResetStatement) Name() string {
	return "RESET"
}

func (s *ParsedResetStatement) Query() string {
	return s.query
}

func (s *ParsedResetStatement) parse(parser *StatementParser, query string) error {
	// Parse a statement of the form
	// REST [my_extension.]my_property
	sp := &simpleParser{sql: []byte(query), statementParser: parser}
	if !sp.eatKeyword("RESET") {
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
	s.Identifier = identifier
	return nil
}

type ParsedCreateDatabaseStatement struct {
	query      string
	Identifier Identifier
}

func (s *ParsedCreateDatabaseStatement) Name() string {
	return "CREATE DATABASE"
}

func (s *ParsedCreateDatabaseStatement) Query() string {
	return s.query
}

func (s *ParsedCreateDatabaseStatement) parse(parser *StatementParser, query string) error {
	// Parse a statement of the form
	// CREATE DATABASE <database-name>
	sp := &simpleParser{sql: []byte(query), statementParser: parser}
	if !sp.eatKeyword("CREATE") {
		return status.Error(codes.InvalidArgument, "statement does not start with CREATE DATABASE")
	}
	if !sp.eatKeyword("DATABASE") {
		return status.Error(codes.InvalidArgument, "statement does not start with CREATE DATABASE")
	}
	identifier, err := sp.eatIdentifier()
	if err != nil {
		return err
	}
	s.query = query
	s.Identifier = identifier
	return nil
}

type ParsedDropDatabaseStatement struct {
	query      string
	Identifier Identifier
}

func (s *ParsedDropDatabaseStatement) Name() string {
	return "DROP DATABASE"
}

func (s *ParsedDropDatabaseStatement) Query() string {
	return s.query
}

func (s *ParsedDropDatabaseStatement) Parse(parser *StatementParser, query string) error {
	return s.parse(parser, query)
}

func (s *ParsedDropDatabaseStatement) parse(parser *StatementParser, query string) error {
	// Parse a statement of the form
	// DROP DATABASE <database-name>
	sp := &simpleParser{sql: []byte(query), statementParser: parser}
	if !sp.eatKeyword("DROP") {
		return status.Error(codes.InvalidArgument, "statement does not start with DROP DATABASE")
	}
	if !sp.eatKeyword("DATABASE") {
		return status.Error(codes.InvalidArgument, "statement does not start with DROP DATABASE")
	}
	identifier, err := sp.eatIdentifier()
	if err != nil {
		return err
	}
	s.query = query
	s.Identifier = identifier
	return nil
}

type BatchType int

const (
	BatchTypeDdl BatchType = iota
	BatchTypeDml
)

type ParsedStartBatchStatement struct {
	query string
	Type  BatchType
}

func (s *ParsedStartBatchStatement) Name() string {
	return "START BATCH"
}

func (s *ParsedStartBatchStatement) Query() string {
	return s.query
}

func (s *ParsedStartBatchStatement) parse(parser *StatementParser, query string) error {
	// Parse a statement of the form
	// START BATCH {DDL | DML}
	sp := &simpleParser{sql: []byte(query), statementParser: parser}
	if !sp.eatKeywords([]string{"START", "BATCH"}) {
		return status.Error(codes.InvalidArgument, "statement does not start with START BATCH")
	}
	if sp.eatKeyword("DML") {
		s.Type = BatchTypeDml
	} else if sp.eatKeyword("DDL") {
		s.Type = BatchTypeDdl
	} else {
		return status.Errorf(codes.InvalidArgument, "unexpected token at pos %d in %q, expected DML or DDL", sp.pos, sp.sql)
	}
	if sp.hasMoreTokens() {
		return status.Errorf(codes.InvalidArgument, "unexpected tokens at position %d in %q", sp.pos, sp.sql)
	}
	s.query = query
	return nil
}

type ParsedRunBatchStatement struct {
	query string
}

func (s *ParsedRunBatchStatement) Name() string {
	return "RUN BATCH"
}

func (s *ParsedRunBatchStatement) Query() string {
	return s.query
}

func (s *ParsedRunBatchStatement) parse(parser *StatementParser, query string) error {
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

type ParsedAbortBatchStatement struct {
	query string
}

func (s *ParsedAbortBatchStatement) Name() string {
	return "ABORT BATCH"
}

func (s *ParsedAbortBatchStatement) Query() string {
	return s.query
}

func (s *ParsedAbortBatchStatement) parse(parser *StatementParser, query string) error {
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

type ParsedBeginStatement struct {
	query string
	// Identifiers contains the transaction properties that were included in the BEGIN statement. E.g. the statement
	// BEGIN TRANSACTION READ ONLY contains the transaction property 'transaction_read_only'.
	Identifiers []Identifier
	// Literals contains the transaction property values that were included in the BEGIN statement. E.g. the statement
	// BEGIN TRANSACTION READ ONLY contains the value 'true' for the property 'transaction_read_only'.
	Literals []Literal
}

func (s *ParsedBeginStatement) Name() string {
	return "BEGIN"
}

func (s *ParsedBeginStatement) Query() string {
	return s.query
}

func (s *ParsedBeginStatement) parse(parser *StatementParser, query string) error {
	// Parse a statement of the form
	// GoogleSQL: BEGIN [TRANSACTION] [READ WRITE | READ ONLY | ISOLATION LEVEL {SERIALIZABLE | READ COMMITTED}]
	// PostgreSQL: {START | BEGIN} [{TRANSACTION | WORK}] (https://www.postgresql.org/docs/current/sql-begin.html)
	// TODO: Support transaction modes in the BEGIN / START statement.
	sp := &simpleParser{sql: []byte(query), statementParser: parser}
	if sp.statementParser.Dialect == databasepb.DatabaseDialect_POSTGRESQL {
		if !sp.eatKeyword("START") && !sp.eatKeyword("BEGIN") {
			return status.Error(codes.InvalidArgument, "statement does not start with BEGIN or START")
		}
		// Just ignore the optional keywords TRANSACTION and WORK, but eat at most one of them.
		if sp.eatKeyword("TRANSACTION") {
			// ignore
		} else if sp.eatKeyword("WORK") {
			// ignore
		}
	} else {
		if !sp.eatKeyword("BEGIN") {
			return status.Error(codes.InvalidArgument, "statement does not start with BEGIN")
		}
		// Just ignore the optional TRANSACTION keyword.
		_ = sp.eatKeyword("TRANSACTION")
	}

	if sp.hasMoreTokens() {
		var err error
		s.Identifiers, s.Literals, err = parseTransactionOptions(sp)
		if err != nil {
			return err
		}
	}

	s.query = query
	return nil
}

type ParsedCommitStatement struct {
	query string
}

func (s *ParsedCommitStatement) Name() string {
	return "COMMIT"
}

func (s *ParsedCommitStatement) Query() string {
	return s.query
}

func (s *ParsedCommitStatement) parse(parser *StatementParser, query string) error {
	// Parse a statement of the form
	// GoogleSQL: COMMIT [TRANSACTION]
	// PostgreSQL: COMMIT [{TRANSACTION | WORK}] (https://www.postgresql.org/docs/current/sql-commit.html)
	sp := &simpleParser{sql: []byte(query), statementParser: parser}
	if !sp.eatKeyword("COMMIT") {
		return status.Error(codes.InvalidArgument, "statement does not start with COMMIT")
	}
	if sp.statementParser.Dialect == databasepb.DatabaseDialect_POSTGRESQL {
		// Just ignore the optional keywords TRANSACTION and WORK, but eat at most one of them.
		if sp.eatKeyword("TRANSACTION") {
			// ignore
		} else if sp.eatKeyword("WORK") {
			// ignore
		}
	} else {
		// Just ignore the optional TRANSACTION keyword.
		_ = sp.eatKeyword("TRANSACTION")
	}

	if sp.hasMoreTokens() {
		return status.Errorf(codes.InvalidArgument, "unexpected tokens at position %d in %q", sp.pos, sp.sql)
	}
	s.query = query
	return nil
}

type ParsedRollbackStatement struct {
	query string
}

func (s *ParsedRollbackStatement) Name() string {
	return "ROLLBACK"
}

func (s *ParsedRollbackStatement) Query() string {
	return s.query
}

func (s *ParsedRollbackStatement) parse(parser *StatementParser, query string) error {
	// Parse a statement of the form
	// GoogleSQL: ROLLBACK [TRANSACTION]
	// PostgreSQL: ROLLBACK [{TRANSACTION | WORK}] (https://www.postgresql.org/docs/current/sql-rollback.html)
	sp := &simpleParser{sql: []byte(query), statementParser: parser}
	if !sp.eatKeyword("ROLLBACK") {
		return status.Error(codes.InvalidArgument, "statement does not start with ROLLBACK")
	}
	if sp.statementParser.Dialect == databasepb.DatabaseDialect_POSTGRESQL {
		// Just ignore the optional keywords TRANSACTION and WORK, but eat at most one of them.
		if sp.eatKeyword("TRANSACTION") {
			// ignore
		} else if sp.eatKeyword("WORK") {
			// ignore
		}
	} else {
		// Just ignore the optional TRANSACTION keyword.
		_ = sp.eatKeyword("TRANSACTION")
	}

	if sp.hasMoreTokens() {
		return status.Errorf(codes.InvalidArgument, "unexpected tokens at position %d in %q", sp.pos, sp.sql)
	}
	s.query = query
	return nil
}
