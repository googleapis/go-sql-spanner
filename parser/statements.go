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
	} else if isCreateKeyword(keyword) && isCreateDatabase(parser, query) {
		stmt = &ParsedCreateDatabaseStatement{}
	} else if isDropKeyword(keyword) && isDropDatabase(parser, query) {
		stmt = &ParsedDropDatabaseStatement{}
	} else if isStartStatementKeyword(keyword) {
		stmt = &ParsedStartBatchStatement{}
	} else if isRunStatementKeyword(keyword) {
		stmt = &ParsedRunBatchStatement{}
	} else if isAbortStatementKeyword(keyword) {
		stmt = &ParsedAbortBatchStatement{}
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
	if _, ok := sp.eatKeyword("create"); !ok {
		return false
	}
	if _, ok := sp.eatKeyword("database"); !ok {
		return false
	}
	return true
}

func isDropDatabase(parser *StatementParser, query string) bool {
	sp := &simpleParser{sql: []byte(query), statementParser: parser}
	if _, ok := sp.eatKeyword("drop"); !ok {
		return false
	}
	if _, ok := sp.eatKeyword("database"); !ok {
		return false
	}
	return true
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
	if _, ok := sp.eatKeyword("SHOW"); !ok {
		return status.Error(codes.InvalidArgument, "statement does not start with SHOW")
	}
	if parser.Dialect == databasepb.DatabaseDialect_GOOGLE_STANDARD_SQL {
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
	s.Identifier = identifier
	return nil
}

// ParsedSetStatement is a statement of the form
// SET [SESSION | LOCAL] [my_extension.]my_property {=|to} <value>
type ParsedSetStatement struct {
	query      string
	Identifier Identifier
	Literal    Literal
	IsLocal    bool
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
	if _, ok := sp.eatKeyword("SET"); !ok {
		return status.Errorf(codes.InvalidArgument, "syntax error: expected SET")
	}
	_, isLocal := sp.eatKeyword("LOCAL")
	if !isLocal && parser.Dialect == databasepb.DatabaseDialect_POSTGRESQL {
		// Just eat and ignore the SESSION keyword if it exists, as SESSION is the default.
		_, _ = sp.eatKeyword("SESSION")
	}
	identifier, err := sp.eatIdentifier()
	if err != nil {
		return err
	}
	if !sp.eatToken('=') {
		// PostgreSQL supports both SET my_property TO <value> and SET my_property = <value>.
		if parser.Dialect == databasepb.DatabaseDialect_POSTGRESQL {
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
	s.Identifier = identifier
	s.Literal = literalValue
	s.IsLocal = isLocal
	return nil
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

func (s *ParsedDropDatabaseStatement) parse(parser *StatementParser, query string) error {
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
	if _, ok := sp.eatKeyword("DML"); ok {
		s.Type = BatchTypeDml
	} else if _, ok := sp.eatKeyword("DDL"); ok {
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
