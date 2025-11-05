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
	"strconv"
	"strings"
	"sync"
	"unicode/utf8"

	"cloud.google.com/go/spanner"
	"cloud.google.com/go/spanner/admin/database/apiv1/databasepb"
	lru "github.com/hashicorp/golang-lru/v2"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var ddlStatements = map[string]bool{"CREATE": true, "DROP": true, "ALTER": true, "ANALYZE": true, "GRANT": true, "REVOKE": true, "RENAME": true}
var selectStatements = map[string]bool{"SELECT": true, "WITH": true, "GRAPH": true, "FROM": true}
var insertStatements = map[string]bool{"INSERT": true}
var updateStatements = map[string]bool{"UPDATE": true}
var deleteStatements = map[string]bool{"DELETE": true}
var dmlStatements = union(insertStatements, union(updateStatements, deleteStatements))
var clientSideKeywords = map[string]bool{
	"SHOW":     true,
	"SET":      true,
	"RESET":    true,
	"START":    true,
	"RUN":      true,
	"ABORT":    true,
	"BEGIN":    true,
	"COMMIT":   true,
	"ROLLBACK": true,
	"CREATE":   true, // CREATE DATABASE is handled as a client-side statement
	"DROP":     true, // DROP DATABASE is handled as a client-side statement
}
var createStatements = map[string]bool{"CREATE": true}
var dropStatements = map[string]bool{"DROP": true}
var showStatements = map[string]bool{"SHOW": true}
var setStatements = map[string]bool{"SET": true}
var resetStatements = map[string]bool{"RESET": true}
var startStatements = map[string]bool{"START": true}
var runStatements = map[string]bool{"RUN": true}
var abortStatements = map[string]bool{"ABORT": true}
var beginStatements = map[string]bool{"BEGIN": true}
var commitStatements = map[string]bool{"COMMIT": true}
var rollbackStatements = map[string]bool{"ROLLBACK": true}

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

type statementsCacheEntry struct {
	sql             string
	params          []string
	info            *StatementInfo
	parsedStatement ParsedStatement
}

var createParserLock sync.Mutex
var statementParsers = sync.Map{}

func getStatementParser(dialect databasepb.DatabaseDialect, cacheSize int) (*StatementParser, error) {
	key := fmt.Sprintf("%v-%v", dialect, cacheSize)
	if val, ok := statementParsers.Load(key); ok {
		return val.(*StatementParser), nil
	} else {
		// The statement parser map is thread-safe, so we don't need this lock for that,
		// but we take this lock to be sure that we only *create* one instance of
		// StatementParser per combination of dialect and cache size.
		createParserLock.Lock()
		defer createParserLock.Unlock()
		parser, err := NewStatementParser(dialect, cacheSize)
		if err != nil {
			return nil, err
		}
		statementParsers.Store(key, parser)
		return parser, nil
	}
}

// StatementParser is a simple, dialect-aware SQL statement parser for Spanner.
// It can be used to determine the type of SQL statement (e.g. DQL/DML/DDL), and
// extract further information from the statement, such as the query parameters.
//
// This is an internal type that can receive breaking changes without prior notice.
type StatementParser struct {
	Dialect         databasepb.DatabaseDialect
	useCache        bool
	statementsCache *lru.Cache[string, *statementsCacheEntry]
}

// NewStatementParser creates a new parser for the given SQL dialect and with the given
// cache size. Parsers can be shared among multiple database connections. The Spanner
// database/sql driver will only create one parser per database dialect and cache size combination.
//
// This is an internal function that can receive breaking changes without prior notice.
func NewStatementParser(dialect databasepb.DatabaseDialect, cacheSize int) (*StatementParser, error) {
	if cacheSize > 0 {
		cache, err := lru.New[string, *statementsCacheEntry](cacheSize)
		if err != nil {
			return nil, err
		}
		return &StatementParser{Dialect: dialect, statementsCache: cache, useCache: true}, nil
	}
	return &StatementParser{Dialect: dialect}, nil
}

// CacheSize returns the current size of the statement cache of this StatementParser.
func (p *StatementParser) CacheSize() int {
	if p.useCache {
		return p.statementsCache.Len()
	}
	return 0
}

// UseCache returns true if this StatementParser uses a cache.
func (p *StatementParser) UseCache() bool {
	return p.useCache
}

// supportsHashSingleLineComments returns true if the database dialect of this parser supports
// comments of the following form:
//
// # This is a single-line comment.
//
// GoogleSQL supports this type of comment.
// PostgreSQL does not support this type of comment.
func (p *StatementParser) supportsHashSingleLineComments() bool {
	return p.Dialect != databasepb.DatabaseDialect_POSTGRESQL
}

// supportsNestedComments returns true if the database dialect of this parser supports
// nested comments. Nested comments means that comments of this style are supported:
//
// /* This is a comment. /* This is a nested comment. */ This is still a comment. */
//
// GoogleSQL does not support nested comments.
// PostgreSQL supports nested comments.
func (p *StatementParser) supportsNestedComments() bool {
	return p.Dialect == databasepb.DatabaseDialect_POSTGRESQL
}

// identifierQuoteToken returns the token that is used for quoted identifiers.
// GoogleSQL uses ` (backtick) for quoted identifiers.
// PostgreSQL uses " (double quotes) for quoted identifiers.
func (p *StatementParser) identifierQuoteToken() byte {
	if p.Dialect == databasepb.DatabaseDialect_POSTGRESQL {
		return '"'
	}
	return '`'
}

// supportsBacktickQuotes returns true if the dialect supports backticks as a valid quote token.
// GoogleSQL supports backtick quotes for identifiers.
// PostgreSQL does not support backticks as a valid quote token.
func (p *StatementParser) supportsBacktickQuotes() bool {
	return p.Dialect != databasepb.DatabaseDialect_POSTGRESQL
}

// supportsDoubleQuotedStringLiterals returns true if the SQL dialect supports
// STRING literals starting with ".
func (p *StatementParser) supportsDoubleQuotedStringLiterals() bool {
	return p.Dialect != databasepb.DatabaseDialect_POSTGRESQL
}

// supportsTripleQuotedLiterals returns true if the dialect supports quoted strings and identifiers
// that start with three occurrences of the same quote token. Triple-quoted strings and identifiers
// are allowed to contain linefeeds and single occurrences of the quote token. Example:
//
// ”'This is a string, and it's allowed to use a single quote inside the string”'
//
// GoogleSQL supports triple-quoted literals.
// PostgreSQL does not support triple-quoted literals.
func (p *StatementParser) supportsTripleQuotedLiterals() bool {
	return p.Dialect != databasepb.DatabaseDialect_POSTGRESQL
}

// supportsDollarQuotedStrings returns true if the dialect supports strings that use double dollar signs
// to mark the start and end of a string. The two dollar signs can optionally contain a tag. Examples:
//
// $$ This is a dollar-quoted string without a tag $$
// $my_tag$ This is a dollar-quoted string with a tag $my_tag$
//
// GoogleSQL does not support dollar-quoted strings.
// PostgreSQL supports dollar-quoted strings.
func (p *StatementParser) supportsDollarQuotedStrings() bool {
	return p.Dialect == databasepb.DatabaseDialect_POSTGRESQL
}

// supportsBackslashEscape returns true if the dialect supports escaping a quote within a quoted
// literal by prefixing it with a backslash. Example:
// PostgreSQL: 'It\'s true' => This is invalid. The first part is the string 'It\', and the rest is invalid syntax.
// GoogleSQL: 'It\'s true' => This is the string "It's true".
func (p *StatementParser) supportsBackslashEscape() bool {
	return p.Dialect != databasepb.DatabaseDialect_POSTGRESQL
}

// supportsEscapeQuoteWithQuote returns true if the dialect supports escaping a quote within a quoted
// literal by repeating the quote twice. Example (note that the way that two single quotes are written in the following
// examples is something that is enforced by gofmt):
// PostgreSQL: 'It”s true' => This is the string "It's true"
// GoogleSQL: 'It”s true' => These are two strings: "It" and "s true".
func (p *StatementParser) supportsEscapeQuoteWithQuote() bool {
	return p.Dialect == databasepb.DatabaseDialect_POSTGRESQL
}

// supportsLinefeedInString returns true if the dialect allows linefeeds in standard string literals.
func (p *StatementParser) supportsLinefeedInString() bool {
	return p.Dialect == databasepb.DatabaseDialect_POSTGRESQL
}

var googleSqlParamPrefixes = map[byte]bool{'@': true}
var postgresqlParamPrefixes = map[byte]bool{'$': true, '@': true}

func (p *StatementParser) paramPrefixes() map[byte]bool {
	if p.Dialect == databasepb.DatabaseDialect_POSTGRESQL {
		return postgresqlParamPrefixes
	}
	return googleSqlParamPrefixes
}

func (p *StatementParser) isValidParamsFirstChar(prefix, c byte) bool {
	if p.Dialect == databasepb.DatabaseDialect_POSTGRESQL && prefix == '$' {
		// PostgreSQL query parameters have the form $1, $2, ..., $10, ...
		return isLatinDigit(c)
	}
	// GoogleSQL query parameters have the form @name or @_name123
	return isLatinLetter(c) || c == '_'
}

func (p *StatementParser) isValidParamsChar(prefix, c byte) bool {
	if p.Dialect == databasepb.DatabaseDialect_POSTGRESQL && prefix == '$' {
		// PostgreSQL query parameters have the form $1, $2, ..., $10, ...
		return isLatinDigit(c)
	}
	// GoogleSQL query parameters have the form @name or @_name123
	return isLatinLetter(c) || isLatinDigit(c) || c == '_'
}

func (p *StatementParser) positionalParameterToNamed(positionalParameterIndex int) string {
	if p.Dialect == databasepb.DatabaseDialect_POSTGRESQL {
		return "$" + strconv.Itoa(positionalParameterIndex)
	}
	return "@p" + strconv.Itoa(positionalParameterIndex)
}

// ParseParameters returns the parameters in the given sql string, if the input
// sql contains positional parameters it returns the converted sql string with
// all positional parameters replaced with named parameters.
// The sql string must be a valid Cloud Spanner sql statement. It may contain
// comments and (string) literals without any restrictions. That is, string
// literals containing for example an email address ('test@test.com') will be
// recognized as a string literal and not returned as a named parameter.
func (p *StatementParser) ParseParameters(sql string) (string, []string, error) {
	return p.findParams(sql)
}

// Skips all whitespaces from the given position and returns the
// position of the next non-whitespace character or len(sql) if
// the string does not contain any whitespaces after pos.
//
// PostgreSQL hints are encoded as comments in the following form:
// /*@ hint_key=hint_value[, hint_key2=hint_value2[,...]] */
// The skipPgHints argument indicates whether those comments should also be skipped or not.
func (p *StatementParser) skipWhitespacesAndComments(sql []byte, pos int, skipPgHints bool) int {
	for pos < len(sql) {
		c := sql[pos]
		if isMultibyte(c) {
			break
		}
		if c == '-' && len(sql) > pos+1 && sql[pos+1] == '-' {
			// This is a single line comment starting with '--'.
			pos = p.skipSingleLineComment(sql, pos+2)
		} else if p.supportsHashSingleLineComments() && c == '#' {
			// This is a single line comment starting with '#'.
			pos = p.skipSingleLineComment(sql, pos+1)
		} else if c == '/' && len(sql) > pos+1 && sql[pos+1] == '*' {
			// This is a multi line comment starting with '/*'.
			if !skipPgHints && len(sql) > pos+2 && sql[pos+2] == '@' {
				// This is a PostgreSQL hint, and we should not skip it.
				break
			}
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
func (p *StatementParser) skip(sql []byte, pos int) (int, error) {
	if pos >= len(sql) {
		return pos, nil
	}
	c := sql[pos]
	if isMultibyte(c) {
		_, size := utf8.DecodeRune(sql[pos:])
		pos += size
		return pos, nil
	}

	if c == '\'' || c == '"' || (c == '`' && p.supportsBacktickQuotes()) {
		// This is a quoted string or quoted identifier.
		pos, _, err := p.skipQuoted(sql, pos, c)
		return pos, err
	} else if c == '-' && len(sql) > pos+1 && sql[pos+1] == '-' {
		// This is a single line comment starting with '--'.
		return p.skipSingleLineComment(sql, pos+2), nil
	} else if p.supportsHashSingleLineComments() && c == '#' {
		// This is a single line comment starting with '#'.
		return p.skipSingleLineComment(sql, pos+1), nil
	} else if c == '/' && len(sql) > pos+1 && sql[pos+1] == '*' {
		// This is a multi line comment starting with '/*'.
		return p.skipMultiLineComment(sql, pos), nil
	} else if c == '$' && p.supportsDollarQuotedStrings() {
		return p.skipDollarQuotedString(sql, pos)
	}
	return pos + 1, nil
}

func (p *StatementParser) skipDollarQuotedString(sql []byte, pos int) (int, error) {
	sp := &simpleParser{sql: sql, pos: pos, statementParser: p}
	tag, ok := sp.eatDollarTag()
	if !ok {
		// Not a valid dollar tag, so only skip the current character.
		return pos + 1, nil
	}
	if _, ok = sp.eatDollarQuotedString(tag); ok {
		return sp.pos, nil
	}
	return 0, spanner.ToSpannerError(status.Errorf(codes.InvalidArgument, "SQL statement contains an unclosed literal: %s", string(sql)))
}

func (p *StatementParser) skipSingleLineComment(sql []byte, pos int) int {
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

func (p *StatementParser) skipMultiLineComment(sql []byte, pos int) int {
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

// skipQuoted skips a quoted string at the given position in the sql string and
// returns the new position, the quote length, or an error if the quoted string
// could not be read.
// The quote length is either 1 for normal quoted strings, and 3 for triple-quoted string.
func (p *StatementParser) skipQuoted(sql []byte, pos int, quote byte) (int, int, error) {
	isTripleQuoted := p.supportsTripleQuotedLiterals() && len(sql) > pos+2 && sql[pos+1] == quote && sql[pos+2] == quote
	if isTripleQuoted && (isMultibyte(sql[pos+1]) || isMultibyte(sql[pos+2])) {
		isTripleQuoted = false
	}
	var quoteLength int
	if isTripleQuoted {
		quoteLength = 3
	} else {
		quoteLength = 1
	}
	pos += quoteLength
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
					return pos + 3, quoteLength, nil
				}
			} else {
				if p.supportsEscapeQuoteWithQuote() && len(sql) > pos+1 && sql[pos+1] == quote {
					pos += 2
					continue
				}
				// This was the end quote.
				return pos + 1, quoteLength, nil
			}
		} else if p.supportsBackslashEscape() && len(sql) > pos+1 && c == '\\' && sql[pos+1] == quote {
			// This is an escaped quote (e.g. 'foo\'bar').
			// Note that in raw strings, the \ officially does not start an
			// escape sequence, but the result is still the same, as in a raw
			// string 'both characters are preserved'.
			pos += 1
		} else if !p.supportsLinefeedInString() && !isTripleQuoted && c == '\n' {
			break
		}
		pos++
	}
	return 0, 0, spanner.ToSpannerError(status.Errorf(codes.InvalidArgument, "SQL statement contains an unclosed literal: %s", string(sql)))
}

// findParams finds all query parameters in the given SQL string.
// The SQL string may contain comments and statement hints.
func (p *StatementParser) findParams(sql string) (string, []string, error) {
	if !p.useCache {
		return p.calculateFindParamsResult(sql)
	}
	if val, ok := p.statementsCache.Get(sql); ok {
		return val.sql, val.params, nil
	} else {
		namedParamsSql, params, err := p.calculateFindParamsResult(sql)
		if err != nil {
			return "", nil, err
		}
		info := p.DetectStatementType(sql)
		cachedParams := make([]string, len(params))
		copy(cachedParams, params)
		p.statementsCache.Add(sql, &statementsCacheEntry{sql: namedParamsSql, params: cachedParams, info: info})
		return namedParamsSql, params, nil
	}
}

func (p *StatementParser) calculateFindParamsResult(sql string) (string, []string, error) {
	const positionalParamChar = '?'
	paramPrefixes := p.paramPrefixes()
	hasNamedParameter := false
	hasPositionalParameter := false
	numPotentialParams := strings.Count(sql, string(positionalParamChar))
	for prefix := range paramPrefixes {
		numPotentialParams += strings.Count(sql, string(prefix))
	}
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
		parser.skipWhitespacesAndComments()
		parsedSQL.Write(parser.sql[startPos:parser.pos])
		if parser.pos >= len(parser.sql) {
			break
		}
		if parser.isMultibyte() {
			startPos = parser.pos
			parser.nextChar()
			parsedSQL.Write(parser.sql[startPos:parser.pos])
			continue
		}
		c := parser.sql[parser.pos]
		// We are not in a quoted string.
		// GoogleSQL: It's a parameter if it is an '@' followed by a letter or an underscore.
		// PostgreSQL: It's a parameter if it is a '$' followed by a digit.
		// See https://cloud.google.com/spanner/docs/lexical#identifiers for identifier rules.
		// Note that for PostgreSQL we support both styles of parameters (both $1 and @name).
		// The reason for this is that other PostgreSQL drivers for Go also do this.
		if _, ok := paramPrefixes[c]; ok && len(parser.sql) > parser.pos+1 && p.isValidParamsFirstChar(c, parser.sql[parser.pos+1]) {
			if hasPositionalParameter {
				return sql, nil, spanner.ToSpannerError(status.Errorf(codes.InvalidArgument, "statement must not contain both named and positional parameter: %s", sql))
			}
			paramPrefix := c
			paramNamePrefix := ""
			if paramPrefix == '$' {
				paramNamePrefix = "p"
			}
			parsedSQL.WriteByte(paramPrefix)
			parser.pos++
			startIndex := parser.pos
			for parser.pos < len(parser.sql) {
				if parser.isMultibyte() || !p.isValidParamsChar(paramPrefix, parser.sql[parser.pos]) {
					hasNamedParameter = true
					namedParams = append(namedParams, paramNamePrefix+string(parser.sql[startIndex:parser.pos]))
					parsedSQL.WriteByte(parser.sql[parser.pos])
					break
				}
				if parser.pos == len(parser.sql)-1 {
					hasNamedParameter = true
					namedParams = append(namedParams, paramNamePrefix+string(parser.sql[startIndex:]))
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
			parsedSQL.WriteString(p.positionalParameterToNamed(positionalParameterIndex))
			namedParams = append(namedParams, "p"+strconv.Itoa(positionalParameterIndex))
			positionalParameterIndex++
			parser.pos++
		} else {
			startPos = parser.pos
			newPos, err := p.skip(parser.sql, parser.pos)
			if err != nil {
				return sql, nil, err
			}
			parsedSQL.Write(parser.sql[startPos:newPos])
			parser.pos = newPos
		}
	}
	if hasNamedParameter {
		return sql, namedParams, nil
	}
	return parsedSQL.String(), namedParams, nil
}

// ParseClientSideStatement returns the executableClientSideStatement that
// corresponds with the given query string, or nil if it is not a valid client
// side statement.
func (p *StatementParser) ParseClientSideStatement(query string) (ParsedStatement, error) {
	if p.useCache {
		if val, ok := p.statementsCache.Get(query); ok {
			if val.info.StatementType == StatementTypeClientSide {
				return val.parsedStatement, nil
			}
			return nil, nil
		}
	}
	// Determine whether it could be a valid client-side statement by looking at the first keyword.
	sp := &simpleParser{sql: []byte(query), statementParser: p}
	keyword := strings.ToUpper(sp.readKeyword())
	if _, ok := clientSideKeywords[keyword]; !ok {
		return nil, nil
	}

	if stmt, err := parseStatement(p, keyword, query); err != nil {
		return nil, err
	} else if stmt != nil {
		if p.useCache {
			cacheEntry := &statementsCacheEntry{
				sql:             query,
				parsedStatement: stmt,
				info: &StatementInfo{
					StatementType: StatementTypeClientSide,
				},
			}
			p.statementsCache.Add(query, cacheEntry)
		}
		return stmt, nil
	}
	return nil, nil
}

// isDDL returns true if the given sql string is a DDL statement.
// This function assumes that any comments and hints at the start
// of the sql string have been removed.
func (p *StatementParser) isDDL(query string) bool {
	info := p.DetectStatementType(query)
	return info.StatementType == StatementTypeDdl
}

func isDDLKeyword(keyword string) bool {
	return isStatementKeyword(keyword, ddlStatements)
}

// isDml returns true if the given sql string is a Dml statement.
// This function assumes that any comments and hints at the start
// of the sql string have been removed.
func (p *StatementParser) isDml(query string) bool {
	info := p.DetectStatementType(query)
	return info.StatementType == StatementTypeDml
}

func isDmlKeyword(keyword string) bool {
	return isStatementKeyword(keyword, dmlStatements)
}

// isQuery returns true if the given sql string is a SELECT statement.
// This function assumes that any comments and hints at the start
// of the sql string have been removed.
func (p *StatementParser) isQuery(query string) bool {
	info := p.DetectStatementType(query)
	return info.StatementType == StatementTypeQuery
}

func isCreateKeyword(keyword string) bool {
	return isStatementKeyword(keyword, createStatements)
}

func isDropKeyword(keyword string) bool {
	return isStatementKeyword(keyword, dropStatements)
}

func isQueryKeyword(keyword string) bool {
	return isStatementKeyword(keyword, selectStatements)
}

func isShowStatementKeyword(keyword string) bool {
	return isStatementKeyword(keyword, showStatements)
}

func isSetStatementKeyword(keyword string) bool {
	return isStatementKeyword(keyword, setStatements)
}

func isResetStatementKeyword(keyword string) bool {
	return isStatementKeyword(keyword, resetStatements)
}

func isStartStatementKeyword(keyword string) bool {
	return isStatementKeyword(keyword, startStatements)
}

func isRunStatementKeyword(keyword string) bool {
	return isStatementKeyword(keyword, runStatements)
}

func isAbortStatementKeyword(keyword string) bool {
	return isStatementKeyword(keyword, abortStatements)
}

func isBeginStatementKeyword(keyword string) bool {
	return isStatementKeyword(keyword, beginStatements)
}

func isCommitStatementKeyword(keyword string) bool {
	return isStatementKeyword(keyword, commitStatements)
}

func isRollbackStatementKeyword(keyword string) bool {
	return isStatementKeyword(keyword, rollbackStatements)
}

func isStatementKeyword(keyword string, keywords map[string]bool) bool {
	_, ok := keywords[keyword]
	return ok
}

// StatementType indicates the type of SQL statement.
type StatementType int

const (
	// StatementTypeUnknown indicates that the parser was not able to determine the
	// type of SQL statement. This could be an indication that the SQL string is invalid,
	// or that it uses a syntax that is not (yet) supported by the parser.
	StatementTypeUnknown StatementType = iota
	// StatementTypeQuery indicates that the statement is a query that will return rows from
	// Spanner, and that will not make any modifications to the database.
	StatementTypeQuery
	// StatementTypeDml indicates that the statement is a data modification language (DML)
	// statement that will make modifications to the data in the database. It may or may not
	// return rows, depending on whether it contains a THEN RETURN (GoogleSQL) or RETURNING
	// (PostgreSQL) clause.
	StatementTypeDml
	// StatementTypeDdl indicates that the statement is a data definition language (DDL)
	// statement that will modify the schema of the database. It will never return rows.
	StatementTypeDdl
	// StatementTypeClientSide indicates that the statement will be handled client-side in
	// the database/sql driver, and not be sent to Spanner. Examples of this includes SHOW
	// and SET statements.
	StatementTypeClientSide
)

// DmlType designates the type of modification that a DML statement will execute.
type DmlType int

const (
	DmlTypeUnknown DmlType = iota
	DmlTypeInsert
	DmlTypeUpdate
	DmlTypeDelete
)

// StatementInfo contains the type of SQL statement, and in case of a DML statement,
// the type of DML command.
type StatementInfo struct {
	StatementType StatementType
	DmlType       DmlType
}

// DetectStatementType returns the type of SQL statement based on the first
// keyword that is found in the SQL statement.
func (p *StatementParser) DetectStatementType(sql string) *StatementInfo {
	if !p.useCache {
		return p.calculateDetectStatementType(sql)
	}
	if val, ok := p.statementsCache.Get(sql); ok {
		return val.info
	} else {
		info := p.calculateDetectStatementType(sql)
		namedParamsSql, params, err := p.calculateFindParamsResult(sql)
		if err == nil {
			p.statementsCache.Add(sql, &statementsCacheEntry{sql: namedParamsSql, params: params, info: info})
		}
		return info
	}
}

func (p *StatementParser) calculateDetectStatementType(sql string) *StatementInfo {
	parser := &simpleParser{sql: []byte(sql), statementParser: p}
	_, _ = parser.skipStatementHint()
	keyword := strings.ToUpper(parser.readKeyword())
	if isQueryKeyword(keyword) {
		return &StatementInfo{StatementType: StatementTypeQuery}
	} else if isDmlKeyword(keyword) {
		return &StatementInfo{
			StatementType: StatementTypeDml,
			DmlType:       detectDmlKeyword(keyword),
		}
	} else if isDDLKeyword(keyword) {
		return &StatementInfo{StatementType: StatementTypeDdl}
	}
	return &StatementInfo{StatementType: StatementTypeUnknown}
}

func detectDmlKeyword(keyword string) DmlType {
	if isStatementKeyword(keyword, insertStatements) {
		return DmlTypeInsert
	} else if isStatementKeyword(keyword, updateStatements) {
		return DmlTypeUpdate
	} else if isStatementKeyword(keyword, deleteStatements) {
		return DmlTypeDelete
	}
	return DmlTypeUnknown
}

func (p *StatementParser) extractSetStatementsFromHints(sql string) (*ParsedSetStatement, error) {
	sp := &simpleParser{sql: []byte(sql), statementParser: p}
	if ok, startPos := sp.skipStatementHint(); ok {
		// Mark the start and end of the statement hint and extract the values in the hint.
		endPos := sp.pos
		sp.pos = startPos
		if p.Dialect == databasepb.DatabaseDialect_POSTGRESQL {
			// eatTokensOnly will only look for the following character sequence: '/*@'
			// It will not interpret it as a comment.
			sp.eatTokensOnly(postgreSqlStatementHintPrefix)
		} else {
			sp.eatTokens(googleSqlStatementHintPrefix)
		}
		// The default is that the hint ends with a single '}'.
		endIndex := endPos - 1
		if p.Dialect == databasepb.DatabaseDialect_POSTGRESQL {
			// The hint ends with '*/'
			endIndex = endPos - 2
		}
		if endIndex > sp.pos && endIndex < len(sql) {
			return p.extractConnectionVariables(sql[sp.pos:endIndex])
		}
	}
	return nil, nil
}

func (p *StatementParser) extractConnectionVariables(sql string) (*ParsedSetStatement, error) {
	sp := &simpleParser{sql: []byte(sql), statementParser: p}
	statement := &ParsedSetStatement{
		Identifiers: make([]Identifier, 0, 2),
		Literals:    make([]Literal, 0, 2),
	}
	for {
		if !sp.hasMoreTokens() {
			break
		}
		identifier, err := sp.eatIdentifier()
		if err != nil {
			return nil, err
		}
		if !sp.eatToken('=') {
			return nil, status.Errorf(codes.InvalidArgument, "missing '=' token after %s in hint", identifier)
		}
		literal, err := sp.eatLiteral()
		if err != nil {
			return nil, err
		}
		statement.Identifiers = append(statement.Identifiers, identifier)
		statement.Literals = append(statement.Literals, literal)
		if !sp.eatToken(',') {
			break
		}
	}
	if sp.hasMoreTokens() {
		return nil, status.Errorf(codes.InvalidArgument, "unexpected tokens: %s", string(sp.sql[sp.pos:]))
	}
	return statement, nil
}
