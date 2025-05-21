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
	"encoding/json"
	"fmt"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"unicode/utf8"

	"cloud.google.com/go/spanner"
	"cloud.google.com/go/spanner/admin/database/apiv1/databasepb"
	lru "github.com/hashicorp/golang-lru"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var ddlStatements = map[string]bool{"CREATE": true, "DROP": true, "ALTER": true, "ANALYZE": true, "GRANT": true, "REVOKE": true, "RENAME": true}
var selectStatements = map[string]bool{"SELECT": true, "WITH": true, "GRAPH": true, "FROM": true}
var insertStatements = map[string]bool{"INSERT": true}
var updateStatements = map[string]bool{"UPDATE": true}
var deleteStatements = map[string]bool{"DELETE": true}
var dmlStatements = union(insertStatements, union(updateStatements, deleteStatements))

func union(m1 map[string]bool, m2 map[string]bool) map[string]bool {
	res := make(map[string]bool, len(m1)+len(m2))
	for k, v := range m1 {
		res[k] = v
	}
	for k, v := range m2 {
		res[k] = v
	}
	return res
}

type simpleParser struct {
	statementParser *statementParser
	sql             []byte
	pos             int
}

// eatToken advances the parser by one position and returns true
// if the next byte is equal to the given byte. This function only
// works for characters that can be encoded in one byte.
func (p *simpleParser) eatToken(t byte) bool {
	p.skipWhitespaces()
	if p.pos >= len(p.sql) {
		return false
	}
	if p.sql[p.pos] != t {
		return false
	}
	p.pos++
	return true
}

// readKeyword reads the keyword at the current position.
// A keyword can only contain upper and lower case ASCII letters (A-Z, a-z).
func (p *simpleParser) readKeyword() string {
	p.skipWhitespaces()
	start := p.pos
	for ; p.pos < len(p.sql) && !isSpace(p.sql[p.pos]); p.pos++ {
		if isMultibyte(p.sql[p.pos]) {
			break
		}
		if isSpace(p.sql[p.pos]) {
			break
		}
		// Only upper/lower-case letters are allowed in keywords.
		if !((p.sql[p.pos] >= 'A' && p.sql[p.pos] <= 'Z') || (p.sql[p.pos] >= 'a' && p.sql[p.pos] <= 'z')) {
			break
		}
	}
	return string(p.sql[start:p.pos])
}

// skipWhitespaces skips comments and other whitespaces from the current
// position until it encounters a non-whitespace / non-comment.
// The position of the parser is updated.
func (p *simpleParser) skipWhitespaces() {
	p.pos = p.statementParser.skipWhitespaces(p.sql, p.pos)
}

// skipStatementHint skips any statement hint at the start of the statement.
func (p *simpleParser) skipStatementHint() bool {
	if p.eatToken('@') && p.eatToken('{') {
		for ; p.pos < len(p.sql); p.pos++ {
			// We don't have to worry about an '}' being inside a statement hint
			// key or value, as it is not a valid part of an identifier, and
			// statement hints have a fixed set of possible values.
			if p.sql[p.pos] == '}' {
				p.pos++
				return true
			}
		}
	}
	return false
}

// isMultibyte returns true if the character at the current position
// is a multibyte utf8 character.
func (p *simpleParser) isMultibyte() bool {
	return isMultibyte(p.sql[p.pos])
}

// nextChar moves the parser to the next character. This takes into
// account that some characters could be multibyte characters.
func (p *simpleParser) nextChar() {
	if !p.isMultibyte() {
		p.pos++
		return
	}
	_, size := utf8.DecodeRune(p.sql[p.pos:])
	p.pos += size
}

// isMultibyte returns true if the byte value indicates that the character
// at this position is a multibyte utf8 character.
func isMultibyte(b uint8) bool {
	// If the first byte of a utf8 character is larger than 0x7F, then
	// that indicates that the character is a multibyte character.
	return b > 0x7F
}

func isLatinLetter(b byte) bool {
	return (b >= 'A' && b <= 'Z') || (b >= 'a' && b <= 'z')
}

func isLatinDigit(b byte) bool {
	return b >= '0' && b <= '9'
}

// isSpace returns true if the given character is a valid space character.
func isSpace(c byte) bool {
	switch c {
	case '\t', '\n', '\v', '\f', '\r', ' ', 0x85, 0xA0:
		return true
	}
	return false
}

var createParserLock sync.Mutex
var statementParsers = sync.Map{}

func getStatementParser(dialect databasepb.DatabaseDialect, cacheSize int) (*statementParser, error) {
	key := fmt.Sprintf("%v-%v", dialect, cacheSize)
	if val, ok := statementParsers.Load(key); ok {
		return val.(*statementParser), nil
	} else {
		// The statement parser map is thread-safe, so we don't need this lock for that,
		// but we take this lock to be sure that we only *create* one instance of
		// statementParser per combination of dialect and cache size.
		createParserLock.Lock()
		defer createParserLock.Unlock()
		parser, err := newStatementParser(dialect, cacheSize)
		if err != nil {
			return nil, err
		}
		statementParsers.Store(key, parser)
		return parser, nil
	}
}

type statementsCacheEntry struct {
	sql    string
	params []string
	info   *statementInfo
}

type statementParser struct {
	dialect         databasepb.DatabaseDialect
	statementsCache *lru.Cache
}

func newStatementParser(dialect databasepb.DatabaseDialect, cacheSize int) (*statementParser, error) {
	cache, err := lru.New(cacheSize)
	if err != nil {
		return nil, err
	}
	return &statementParser{dialect: dialect, statementsCache: cache}, nil
}

func (p *statementParser) supportsHashSingleLineComments() bool {
	return p.dialect != databasepb.DatabaseDialect_POSTGRESQL
}

func (p *statementParser) supportsNestedComments() bool {
	return p.dialect == databasepb.DatabaseDialect_POSTGRESQL
}

// parseParameters returns the parameters in the given sql string, if the input
// sql contains positional parameters it returns the converted sql string with
// all positional parameters replaced with named parameters.
// The sql string must be a valid Cloud Spanner sql statement. It may contain
// comments and (string) literals without any restrictions. That is, string
// literals containing for example an email address ('test@test.com') will be
// recognized as a string literal and not returned as a named parameter.
func (p *statementParser) parseParameters(sql string) (string, []string, error) {
	return p.findParams(sql)
}

// Skips all whitespaces from the given position and returns the
// position of the next non-whitespace character or len(sql) if
// the string does not contain any whitespaces after pos.
func (p *statementParser) skipWhitespaces(sql []byte, pos int) int {
	for pos < len(sql) {
		c := sql[pos]
		if isMultibyte(c) {
			_, size := utf8.DecodeRune(sql[pos:])
			pos += size
			continue
		}
		if c == '-' && len(sql) > pos+1 && sql[pos+1] == '-' {
			// This is a single line comment starting with '--'.
			pos = p.skipSingleLineComment(sql, pos+2)
		} else if p.supportsHashSingleLineComments() && c == '#' {
			// This is a single line comment starting with '#'.
			pos = p.skipSingleLineComment(sql, pos+1)
		} else if c == '/' && len(sql) > pos+1 && sql[pos+1] == '*' {
			// This is a multi line comment starting with '/*'.
			pos = p.skipMultiLineComment(sql, pos)
		} else if !isSpace(c) {
			break
		} else {
			pos++
		}
	}
	return pos
}

// Skips the next character, quoted literal, quoted identifier or comment in
// the given sql string from the given position and returns the position of the
// next character.
func (p *statementParser) skip(sql []byte, pos int) (int, error) {
	if pos >= len(sql) {
		return pos, nil
	}
	c := sql[pos]
	if isMultibyte(c) {
		_, size := utf8.DecodeRune(sql[pos:])
		pos += size
		return pos, nil
	}

	if c == '\'' || c == '"' || c == '`' {
		// This is a quoted string or quoted identifier.
		return p.skipQuoted(sql, pos, c)
	} else if c == '-' && len(sql) > pos+1 && sql[pos+1] == '-' {
		// This is a single line comment starting with '--'.
		return p.skipSingleLineComment(sql, pos+2), nil
	} else if p.supportsHashSingleLineComments() && c == '#' {
		// This is a single line comment starting with '#'.
		return p.skipSingleLineComment(sql, pos+1), nil
	} else if c == '/' && len(sql) > pos+1 && sql[pos+1] == '*' {
		// This is a multi line comment starting with '/*'.
		return p.skipMultiLineComment(sql, pos), nil
	}
	return pos + 1, nil
}

func (p *statementParser) skipSingleLineComment(sql []byte, pos int) int {
	for pos < len(sql) {
		c := sql[pos]
		if isMultibyte(c) {
			_, size := utf8.DecodeRune(sql[pos:])
			pos += size
			continue
		}
		if c == '\n' {
			break
		}
		pos++
	}
	return min(pos+1, len(sql))
}

func (p *statementParser) skipMultiLineComment(sql []byte, pos int) int {
	// Skip '/*'.
	pos = pos + 2
	level := 1
	// Search for '*/' sequences. Depending on whether the dialect supports nested comments or not,
	// the comment is considered terminated at either the first occurrence of '*/', or when we have
	// seen as many '*/' as there have been occurrences of '/*'.
	for pos < len(sql) {
		if isMultibyte(sql[pos]) {
			_, size := utf8.DecodeRune(sql[pos:])
			pos += size
			continue
		}
		if sql[pos] == '*' && len(sql) > pos+1 && sql[pos+1] == '/' {
			if p.supportsNestedComments() {
				level--
				if level == 0 {
					return pos + 2
				}
			} else {
				return pos + 2
			}
		} else if p.supportsNestedComments() {
			if sql[pos] == '/' && len(sql) > pos+1 && sql[pos+1] == '*' {
				level++
			}
		}
		pos++
	}
	return pos
}

func (p *statementParser) skipQuoted(sql []byte, pos int, quote byte) (int, error) {
	isTripleQuoted := len(sql) > pos+2 && sql[pos+1] == quote && sql[pos+2] == quote
	if isTripleQuoted && (isMultibyte(sql[pos+1]) || isMultibyte(sql[pos+2])) {
		isTripleQuoted = false
	}
	if isTripleQuoted {
		pos += 3
	} else {
		pos += 1
	}
	for pos < len(sql) {
		c := sql[pos]
		if isMultibyte(c) {
			_, size := utf8.DecodeRune(sql[pos:])
			pos += size
			continue
		}
		if c == quote {
			if isTripleQuoted {
				// Check if this is the end of the triple-quoted string.
				if len(sql) > pos+2 && sql[pos+1] == quote && sql[pos+2] == quote {
					return pos + 3, nil
				}
			} else {
				// This was the end quote.
				return pos + 1, nil
			}
		} else if len(sql) > pos+1 && c == '\\' && sql[pos+1] == quote {
			// This is an escaped quote (e.g. 'foo\'bar').
			// Note that in raw strings, the \ officially does not start an
			// escape sequence, but the result is still the same, as in a raw
			// string 'both characters are preserved'.
			pos += 1
		} else if !isTripleQuoted && c == '\n' {
			break
		}
		pos++
	}
	return 0, spanner.ToSpannerError(status.Errorf(codes.InvalidArgument, "SQL statement contains an unclosed literal: %s", string(sql)))
}

// RemoveCommentsAndTrim removes any comments in the query string and trims any
// spaces at the beginning and end of the query. This makes checking what type
// of query a string is a lot easier, as only the first word(s) need to be
// checked after this has been removed.
func (p *statementParser) removeCommentsAndTrim(sql string) (string, error) {
	const singleQuote = '\''
	const doubleQuote = '"'
	const backtick = '`'
	const hyphen = '-'
	const dash = '#'
	const slash = '/'
	const asterisk = '*'
	isInQuoted := false
	isInSingleLineComment := false
	isInMultiLineComment := false
	var startQuote rune
	lastCharWasEscapeChar := false
	isTripleQuoted := false
	res := strings.Builder{}
	res.Grow(len(sql))
	index := 0
	runes := []rune(sql)
	for index < len(runes) {
		c := runes[index]
		if isInQuoted {
			if (c == '\n' || c == '\r') && !isTripleQuoted {
				return "", spanner.ToSpannerError(status.Errorf(codes.InvalidArgument, "statement contains an unclosed literal: %s", sql))
			} else if c == startQuote {
				if lastCharWasEscapeChar {
					lastCharWasEscapeChar = false
				} else if isTripleQuoted {
					if len(runes) > index+2 && runes[index+1] == startQuote && runes[index+2] == startQuote {
						isInQuoted = false
						startQuote = 0
						isTripleQuoted = false
						res.WriteRune(c)
						res.WriteRune(c)
						index += 2
					}
				} else {
					isInQuoted = false
					startQuote = 0
				}
			} else if c == '\\' {
				lastCharWasEscapeChar = true
			} else {
				lastCharWasEscapeChar = false
			}
			res.WriteRune(c)
		} else {
			// We are not in a quoted string.
			if isInSingleLineComment {
				if c == '\n' {
					isInSingleLineComment = false
					// Include the line feed in the result.
					res.WriteRune(c)
				}
			} else if isInMultiLineComment {
				if len(runes) > index+1 && c == asterisk && runes[index+1] == slash {
					isInMultiLineComment = false
					index++
				}
			} else {
				if c == dash || (len(runes) > index+1 && c == hyphen && runes[index+1] == hyphen) {
					// This is a single line comment.
					isInSingleLineComment = true
				} else if len(runes) > index+1 && c == slash && runes[index+1] == asterisk {
					isInMultiLineComment = true
					index++
				} else {
					if c == singleQuote || c == doubleQuote || c == backtick {
						isInQuoted = true
						startQuote = c
						// Check whether it is a triple-quote.
						if len(runes) > index+2 && runes[index+1] == startQuote && runes[index+2] == startQuote {
							isTripleQuoted = true
							res.WriteRune(c)
							res.WriteRune(c)
							index += 2
						}
					}
					res.WriteRune(c)
				}
			}
		}
		index++
	}
	if isInQuoted {
		return "", spanner.ToSpannerError(status.Errorf(codes.InvalidArgument, "statement contains an unclosed literal: %s", sql))
	}
	trimmed := strings.TrimSpace(res.String())
	if len(trimmed) > 0 && trimmed[len(trimmed)-1] == ';' {
		return trimmed[:len(trimmed)-1], nil
	}
	return trimmed, nil
}

// Removes any statement hints at the beginning of the statement.
func (p *statementParser) removeStatementHint(sql string) string {
	parser := &simpleParser{sql: []byte(sql), statementParser: p}
	if parser.skipStatementHint() {
		return sql[parser.pos:]
	}
	return sql
}

// findParams finds all query parameters in the given SQL string.
// The SQL string may contain comments and statement hints.
func (p *statementParser) findParams(sql string) (string, []string, error) {
	if val, ok := p.statementsCache.Get(sql); ok {
		res := val.(*statementsCacheEntry)
		return res.sql, res.params, nil
	} else {
		namedParamsSql, params, err := p.calculateFindParamsResult(sql)
		if err != nil {
			return "", nil, err
		}
		info := p.detectStatementType(sql)
		p.statementsCache.Add(sql, &statementsCacheEntry{sql: namedParamsSql, params: params, info: info})
		return namedParamsSql, params, nil
	}
}

func (p *statementParser) calculateFindParamsResult(sql string) (string, []string, error) {
	const positionalParamChar = '?'
	const paramPrefix = '@'
	hasNamedParameter := false
	hasPositionalParameter := false
	numPotentialParams := strings.Count(sql, string(positionalParamChar)) + strings.Count(sql, string(paramPrefix))
	// Spanner does not support more than 950 query parameters in a query,
	// so we limit the number of parameters that we pre-allocate room for
	// here to that number. This prevents allocation of a large slice if
	// the SQL string happens to contain a string literal that contains
	// a lot of question marks.
	namedParams := make([]string, 0, min(numPotentialParams, 950))
	parsedSQL := strings.Builder{}
	parsedSQL.Grow(len(sql))
	positionalParameterIndex := 1
	parser := &simpleParser{sql: []byte(sql), statementParser: p}
	for parser.pos < len(parser.sql) {
		startPos := parser.pos
		parser.skipWhitespaces()
		parsedSQL.WriteString(string(parser.sql[startPos:parser.pos]))
		if parser.pos >= len(parser.sql) {
			break
		}
		if parser.isMultibyte() {
			startPos = parser.pos
			parser.nextChar()
			parsedSQL.WriteString(string(parser.sql[startPos:parser.pos]))
			continue
		}
		c := parser.sql[parser.pos]
		// We are not in a quoted string. It's a parameter if it is an '@' followed by a letter or an underscore.
		// See https://cloud.google.com/spanner/docs/lexical#identifiers for identifier rules.
		if c == paramPrefix && len(parser.sql) > parser.pos+1 && (isLatinLetter(parser.sql[parser.pos+1]) || parser.sql[parser.pos+1] == '_') {
			if hasPositionalParameter {
				return sql, nil, spanner.ToSpannerError(status.Errorf(codes.InvalidArgument, "statement must not contain both named and positional parameter: %s", sql))
			}
			parsedSQL.WriteByte(c)
			parser.pos++
			startIndex := parser.pos
			for parser.pos < len(parser.sql) {
				if parser.isMultibyte() || !(isLatinLetter(parser.sql[parser.pos]) || isLatinDigit(parser.sql[parser.pos]) || parser.sql[parser.pos] == '_') {
					hasNamedParameter = true
					namedParams = append(namedParams, string(parser.sql[startIndex:parser.pos]))
					parsedSQL.WriteByte(parser.sql[parser.pos])
					break
				}
				if parser.pos == len(parser.sql)-1 {
					hasNamedParameter = true
					namedParams = append(namedParams, string(parser.sql[startIndex:]))
					parsedSQL.WriteByte(parser.sql[parser.pos])
					break
				}
				parsedSQL.WriteByte(parser.sql[parser.pos])
				parser.pos++
			}
		} else if c == positionalParamChar {
			if hasNamedParameter {
				return sql, nil, spanner.ToSpannerError(status.Errorf(codes.InvalidArgument, "statement must not contain both named and positional parameter: %s", sql))
			}
			hasPositionalParameter = true
			parsedSQL.WriteString("@p" + strconv.Itoa(positionalParameterIndex))
			namedParams = append(namedParams, "p"+strconv.Itoa(positionalParameterIndex))
			positionalParameterIndex++
			parser.pos++
		} else {
			startPos = parser.pos
			newPos, err := p.skip(parser.sql, parser.pos)
			if err != nil {
				return sql, nil, err
			}
			parsedSQL.WriteString(string(parser.sql[startPos:newPos]))
			parser.pos = newPos
		}
	}
	if hasNamedParameter {
		return sql, namedParams, nil
	}
	return parsedSQL.String(), namedParams, nil
}

// isDDL returns true if the given sql string is a DDL statement.
// This function assumes that any comments and hints at the start
// of the sql string have been removed.
func (p *statementParser) isDDL(query string) bool {
	info := p.detectStatementType(query)
	return info.statementType == statementTypeDdl
}

func isDDLKeyword(keyword string) bool {
	return isStatementKeyword(keyword, ddlStatements)
}

// isDml returns true if the given sql string is a Dml statement.
// This function assumes that any comments and hints at the start
// of the sql string have been removed.
func (p *statementParser) isDml(query string) bool {
	info := p.detectStatementType(query)
	return info.statementType == statementTypeDml
}

func isDmlKeyword(keyword string) bool {
	return isStatementKeyword(keyword, dmlStatements)
}

// isQuery returns true if the given sql string is a SELECT statement.
// This function assumes that any comments and hints at the start
// of the sql string have been removed.
func (p *statementParser) isQuery(query string) bool {
	info := p.detectStatementType(query)
	return info.statementType == statementTypeQuery
}

func isQueryKeyword(keyword string) bool {
	return isStatementKeyword(keyword, selectStatements)
}

func isStatementKeyword(keyword string, keywords map[string]bool) bool {
	_, ok := keywords[keyword]
	return ok
}

// clientSideStatements are loaded from the client_side_statements.json file.
type clientSideStatements struct {
	Statements []*clientSideStatement `json:"statements"`
	executor   *statementExecutor
}

// clientSideStatement is the definition of a statement that can be executed on
// a connection and that will be handled by the connection itself, instead of
// sending it to Spanner.
type clientSideStatement struct {
	Name                          string `json:"name"`
	ExecutorName                  string `json:"executorName"`
	execContext                   func(ctx context.Context, c *conn, params string, args []driver.NamedValue) (driver.Result, error)
	queryContext                  func(ctx context.Context, c *conn, params string, args []driver.NamedValue) (driver.Rows, error)
	ResultType                    string `json:"resultType"`
	Regex                         string `json:"regex"`
	regexp                        *regexp.Regexp
	MethodName                    string `json:"method"`
	method                        func(query string) error
	ExampleStatements             []string `json:"exampleStatements"`
	ExamplePrerequisiteStatements []string `json:"examplePrerequisiteStatements"`

	setStatement `json:"setStatement"`
}

type setStatement struct {
	PropertyName  string `json:"propertyName"`
	Separator     string `json:"separator"`
	AllowedValues string `json:"allowedValues"`
	ConverterName string `json:"converterName"`
}

var statementsInit sync.Once
var statements *clientSideStatements
var statementsCompileErr error

// compileStatements loads all client side statements from the json file and
// assigns the Go methods to the different statements that should be executed
// when on of the statements is executed on a connection.
func compileStatements() error {
	statements = new(clientSideStatements)
	err := json.Unmarshal([]byte(jsonFile), statements)
	if err != nil {
		return err
	}
	statements.executor = &statementExecutor{}
	for _, stmt := range statements.Statements {
		stmt.regexp, err = regexp.Compile(stmt.Regex)
		if err != nil {
			return err
		}
		i := reflect.ValueOf(statements.executor).MethodByName(strings.TrimPrefix(stmt.MethodName, "statement")).Interface()
		if execContext, ok := i.(func(ctx context.Context, c *conn, query string, args []driver.NamedValue) (driver.Result, error)); ok {
			stmt.execContext = execContext
		}
		if queryContext, ok := i.(func(ctx context.Context, c *conn, query string, args []driver.NamedValue) (driver.Rows, error)); ok {
			stmt.queryContext = queryContext
		}
	}
	return nil
}

// executableClientSideStatement is the combination of a pre-defined client-side
// statement, the connection it should be executed on and any additional
// parameters that were included in the statement.
type executableClientSideStatement struct {
	*clientSideStatement
	conn   *conn
	query  string
	params string
}

func (c *executableClientSideStatement) ExecContext(ctx context.Context, args []driver.NamedValue) (driver.Result, error) {
	if c.clientSideStatement.execContext == nil {
		return nil, spanner.ToSpannerError(status.Errorf(codes.InvalidArgument, "%q cannot be used with execContext", c.query))
	}
	return c.clientSideStatement.execContext(ctx, c.conn, c.params, args)
}

func (c *executableClientSideStatement) QueryContext(ctx context.Context, args []driver.NamedValue) (driver.Rows, error) {
	if c.clientSideStatement.queryContext == nil {
		return nil, spanner.ToSpannerError(status.Errorf(codes.InvalidArgument, "%q cannot be used with queryContext", c.query))
	}
	return c.clientSideStatement.queryContext(ctx, c.conn, c.params, args)
}

// parseClientSideStatement returns the executableClientSideStatement that
// corresponds with the given query string, or nil if it is not a valid client
// side statement.
func parseClientSideStatement(c *conn, query string) (*executableClientSideStatement, error) {
	statementsInit.Do(func() {
		if err := compileStatements(); err != nil {
			statementsCompileErr = err
		}
	})
	if statementsCompileErr != nil {
		return nil, statementsCompileErr
	}
	for _, stmt := range statements.Statements {
		if stmt.regexp.MatchString(query) {
			var params string
			if stmt.setStatement.Separator != "" {
				p := strings.SplitN(query, stmt.setStatement.Separator, 2)
				if len(p) == 2 {
					params = strings.TrimSpace(p[1])
				}
			}
			return &executableClientSideStatement{stmt, c, query, params}, nil
		}
	}
	return nil, nil
}

type statementType int

const (
	statementTypeUnknown statementType = iota
	statementTypeQuery
	statementTypeDml
	statementTypeDdl
)

type dmlType int

const (
	dmlTypeUnknown dmlType = iota
	dmlTypeInsert
	dmlTypeUpdate
	dmlTypeDelete
)

type statementInfo struct {
	statementType statementType
	dmlType       dmlType
}

// detectStatementType returns the type of SQL statement based on the first
// keyword that is found in the SQL statement.
func (p *statementParser) detectStatementType(sql string) *statementInfo {
	if val, ok := p.statementsCache.Get(sql); ok {
		res := val.(*statementsCacheEntry)
		return res.info
	} else {
		info := p.calculateDetectStatementType(sql)
		namedParamsSql, params, err := p.calculateFindParamsResult(sql)
		if err == nil {
			p.statementsCache.Add(sql, &statementsCacheEntry{sql: namedParamsSql, params: params, info: info})
		}
		return info
	}
}

func (p *statementParser) calculateDetectStatementType(sql string) *statementInfo {
	parser := &simpleParser{sql: []byte(sql), statementParser: p}
	_ = parser.skipStatementHint()
	keyword := strings.ToUpper(parser.readKeyword())
	if isQueryKeyword(keyword) {
		return &statementInfo{statementType: statementTypeQuery}
	} else if isDmlKeyword(keyword) {
		return &statementInfo{
			statementType: statementTypeDml,
			dmlType:       detectDmlKeyword(keyword),
		}
	} else if isDDLKeyword(keyword) {
		return &statementInfo{statementType: statementTypeDdl}
	}
	return &statementInfo{statementType: statementTypeUnknown}
}

func detectDmlKeyword(keyword string) dmlType {
	if isStatementKeyword(keyword, insertStatements) {
		return dmlTypeInsert
	} else if isStatementKeyword(keyword, updateStatements) {
		return dmlTypeUpdate
	} else if isStatementKeyword(keyword, deleteStatements) {
		return dmlTypeDelete
	}
	return dmlTypeUnknown
}
