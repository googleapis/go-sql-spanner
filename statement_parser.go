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
	"unicode"
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
	"SHOW":  true,
	"SET":   true,
	"RESET": true,
	"START": true,
	"RUN":   true,
	"ABORT": true,
}
var showStatements = map[string]bool{"SHOW": true}
var setStatements = map[string]bool{"SET": true}
var resetStatements = map[string]bool{"RESET": true}

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

// hasMoreTokens returns true if there are more (non-whitespace) tokens in the SQL string.
func (p *simpleParser) hasMoreTokens() bool {
	startPos := p.pos
	defer func() { p.pos = startPos }()
	p.skipWhitespacesAndComments()
	return p.pos < len(p.sql)
}

// eatToken advances the parser by one position and returns true
// if the next byte is equal to the given byte. This function only
// works for characters that can be encoded in one byte.
//
// Any whitespaces and/or comments before the token will be skipped.
func (p *simpleParser) eatToken(t byte) bool {
	return p.eatTokenWithWhitespaceOption(t, true)
}

// eatTokenOnly advances the parser by one position and returns true
// if the next byte is equal to the given byte. This function only
// works for characters that can be encoded in one byte.
//
// This function will only look at the first byte it encounters.
// Any whitespaces or comments will not be skipped first.
func (p *simpleParser) eatTokenOnly(t byte) bool {
	return p.eatTokenWithWhitespaceOption(t, false)
}

func (p *simpleParser) eatTokenWithWhitespaceOption(t byte, eatWhiteSpaces bool) bool {
	if eatWhiteSpaces {
		p.skipWhitespacesAndComments()
	}
	if p.pos >= len(p.sql) {
		return false
	}
	if p.sql[p.pos] != t {
		return false
	}
	p.pos++
	return true
}

func (p *simpleParser) eatDollarQuotedString(tag string) (string, bool) {
	startPos := p.pos
	for ; p.pos < len(p.sql); p.nextChar() {
		if p.isMultibyte() {
			continue
		}
		if p.isDollar() {
			posBeforeTag := p.pos
			potentialTag, ok := p.eatDollarTag()
			if !ok {
				p.pos = posBeforeTag
				continue
			}
			if potentialTag == tag {
				return string(p.sql[startPos:posBeforeTag]), true
			}
			// This is a nested dollar-tag. Nested dollar-quoted strings are allowed.
			if _, ok = p.eatDollarQuotedString(potentialTag); !ok {
				return "", false
			}
		}
	}
	return "", false
}

func (p *simpleParser) eatDollarTag() (string, bool) {
	if !p.eatTokenOnly('$') {
		return "", false
	}
	if p.pos >= len(p.sql) {
		return "", false
	}
	// $$ is a valid start of a dollar-quoted string without a tag.
	if p.eatTokenOnly('$') {
		return "", true
	}
	startPos := p.pos
	first := true
	for ; p.pos < len(p.sql); p.nextChar() {
		if first {
			first = false
			if !p.isValidFirstIdentifierChar() {
				return "", false
			}
		} else {
			if p.eatToken('$') {
				return string(p.sql[startPos : p.pos-1]), true
			}
			if !p.isValidIdentifierChar() {
				return "", false
			}
		}
	}
	return "", false
}

func (p *simpleParser) isValidFirstIdentifierChar() bool {
	if p.isMultibyte() {
		r, _ := utf8.DecodeRune(p.sql[p.pos:])
		return unicode.IsLetter(r)
	}
	return p.sql[p.pos] == '_' || isLatinLetter(p.sql[p.pos])
}

func (p *simpleParser) isValidIdentifierChar() bool {
	if p.isMultibyte() {
		r, _ := utf8.DecodeRune(p.sql[p.pos:])
		return unicode.IsLetter(r) || unicode.IsDigit(r)
	}
	return p.sql[p.pos] == '_' || p.sql[p.pos] == '$' || isLatinLetter(p.sql[p.pos]) || isLatinDigit(p.sql[p.pos])
}

func (p *simpleParser) isValidDollarTagIdentifierChar() bool {
	return p.isValidIdentifierChar() && !p.isDollar()
}

func (p *simpleParser) isDollar() bool {
	return !p.isMultibyte() && p.sql[p.pos] == '$'
}

type identifier struct {
	parts []string
}

func (i *identifier) String() string {
	return strings.Join(i.parts, ".")
}

// eatIdentifier reads the identifier at the current parser position, updates the parser position,
// and returns the identifier.
func (p *simpleParser) eatIdentifier() (identifier, error) {
	// TODO: Add support for quoted identifiers.
	p.skipWhitespacesAndComments()
	if p.pos >= len(p.sql) {
		return identifier{}, status.Errorf(codes.InvalidArgument, "no identifier found at position %d", p.pos)
	}
	startPos := p.pos
	first := true
	result := identifier{parts: make([]string, 0, 1)}
	appendLastPart := true
	for p.pos < len(p.sql) {
		if first {
			first = false
			if !p.isValidFirstIdentifierChar() {
				return identifier{}, status.Errorf(codes.InvalidArgument, "invalid first identifier character found at position %d: %s", p.pos, p.sql[p.pos:p.pos+1])
			}
		} else {
			if !p.isValidIdentifierChar() {
				result.parts = append(result.parts, string(p.sql[startPos:p.pos]))
				if p.eatToken('.') {
					p.skipWhitespacesAndComments()
					startPos = p.pos
					first = true
					continue
				} else {
					appendLastPart = false
					break
				}
			}
		}
		p.nextChar()
	}
	if first {
		return identifier{}, status.Errorf(codes.InvalidArgument, "no identifier found at position %d", startPos)
	}
	if appendLastPart && p.pos == len(p.sql) {
		result.parts = append(result.parts, string(p.sql[startPos:p.pos]))
	}
	return result, nil
}

type literal struct {
	value string
}

func (p *simpleParser) eatLiteral() (literal, error) {
	p.skipWhitespacesAndComments()
	if p.pos >= len(p.sql) {
		return literal{}, status.Errorf(codes.InvalidArgument, "missing literal at position %d", p.pos)
	}
	startPos := p.pos
	c := p.sql[p.pos]
	if c == '\'' || (c == '"' && p.statementParser.supportsDoubleQuotedStringLiterals()) {
		pos, quoteLen, err := p.statementParser.skipQuoted(p.sql, p.pos, c)
		if err != nil {
			return literal{}, err
		}
		p.pos = pos
		value := string(p.sql[startPos+quoteLen : p.pos-quoteLen])
		return literal{value: value}, nil
	}

	value := p.readUnquotedLiteral()
	if value == "" {
		return literal{}, status.Errorf(codes.InvalidArgument, "missing literal at position %d", p.pos)
	}
	return literal{value: value}, nil
}

// eatKeyword eats the given keyword at the current position of the parser if it exists.
//
// Returns the actual keyword that was read and true if the keyword is found, and updates the position of the parser.
// Returns an empty string and false without updating the position of the parser if the keyword was not found.
func (p *simpleParser) eatKeyword(keyword string) (string, bool) {
	startPos := p.pos
	found := p.readKeyword()
	if !strings.EqualFold(found, keyword) {
		p.pos = startPos
		return "", false
	}
	return found, true
}

// readKeyword reads the keyword at the current position.
// A keyword can only contain upper and lower case ASCII letters (A-Z, a-z).
func (p *simpleParser) readKeyword() string {
	p.skipWhitespacesAndComments()
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

// readUnquotedLiteral reads the unquoted literal at the current position.
// An unquoted literal can contain upper and lower case ASCII letters (A-Z, a-z),
// numbers, and decimal points.
func (p *simpleParser) readUnquotedLiteral() string {
	p.skipWhitespacesAndComments()
	start := p.pos
	for ; p.pos < len(p.sql) && !isSpace(p.sql[p.pos]); p.pos++ {
		if isMultibyte(p.sql[p.pos]) {
			break
		}
		if isSpace(p.sql[p.pos]) {
			break
		}
		// Only upper/lower-case letters, numbers, and decimal points are allowed in unquoted literals.
		if !((p.sql[p.pos] >= 'A' && p.sql[p.pos] <= 'Z') ||
			(p.sql[p.pos] >= 'a' && p.sql[p.pos] <= 'z') ||
			(p.sql[p.pos] >= '0' && p.sql[p.pos] <= '9') ||
			p.sql[p.pos] == '.') {
			break
		}
	}
	return string(p.sql[start:p.pos])
}

// skipWhitespacesAndComments skips comments and other whitespaces from the current
// position until it encounters a non-whitespace / non-comment.
// The position of the parser is updated.
func (p *simpleParser) skipWhitespacesAndComments() {
	p.pos = p.statementParser.skipWhitespacesAndComments(p.sql, p.pos)
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
	sql                 string
	params              []string
	info                *statementInfo
	clientSideStatement *clientSideStatement
}

type statementParser struct {
	dialect         databasepb.DatabaseDialect
	useCache        bool
	statementsCache *lru.Cache[string, *statementsCacheEntry]
}

func newStatementParser(dialect databasepb.DatabaseDialect, cacheSize int) (*statementParser, error) {
	if cacheSize > 0 {
		cache, err := lru.New[string, *statementsCacheEntry](cacheSize)
		if err != nil {
			return nil, err
		}
		return &statementParser{dialect: dialect, statementsCache: cache, useCache: true}, nil
	}
	return &statementParser{dialect: dialect}, nil
}

func (p *statementParser) supportsHashSingleLineComments() bool {
	return p.dialect != databasepb.DatabaseDialect_POSTGRESQL
}

func (p *statementParser) supportsNestedComments() bool {
	return p.dialect == databasepb.DatabaseDialect_POSTGRESQL
}

func (p *statementParser) supportsBacktickQuotes() bool {
	return p.dialect != databasepb.DatabaseDialect_POSTGRESQL
}

// supportsDoubleQuotedStringLiterals returns true if the SQL dialect supports
// STRING literals starting with ".
func (p *statementParser) supportsDoubleQuotedStringLiterals() bool {
	return p.dialect != databasepb.DatabaseDialect_POSTGRESQL
}

func (p *statementParser) supportsTripleQuotedLiterals() bool {
	return p.dialect != databasepb.DatabaseDialect_POSTGRESQL
}

func (p *statementParser) supportsDollarQuotedStrings() bool {
	return p.dialect == databasepb.DatabaseDialect_POSTGRESQL
}

// supportsBackslashEscape returns true if the dialect supports escaping a quote within a quoted
// literal by prefixing it with a backslash. Example:
// PostgreSQL: 'It\'s true' => This is invalid. The first part is the string 'It\', and the rest is invalid syntax.
// GoogleSQL: 'It\'s true' => This is the string "It's true".
func (p *statementParser) supportsBackslashEscape() bool {
	return p.dialect != databasepb.DatabaseDialect_POSTGRESQL
}

// supportsEscapeQuoteWithQuote returns true if the dialect supports escaping a quote within a quoted
// literal by repeating the quote twice. Example (note that the way that two single quotes are written in the following
// examples is something that is enforced by gofmt):
// PostgreSQL: 'It”s true' => This is the string "It's true"
// GoogleSQL: 'It”s true' => These are two strings: "It" and "s true".
func (p *statementParser) supportsEscapeQuoteWithQuote() bool {
	return p.dialect == databasepb.DatabaseDialect_POSTGRESQL
}

// supportsLinefeedInString returns true if the dialect allows linefeeds in standard string literals.
func (p *statementParser) supportsLinefeedInString() bool {
	return p.dialect == databasepb.DatabaseDialect_POSTGRESQL
}

var googleSqlParamPrefixes = map[byte]bool{'@': true}
var postgresqlParamPrefixes = map[byte]bool{'$': true, '@': true}

func (p *statementParser) paramPrefixes() map[byte]bool {
	if p.dialect == databasepb.DatabaseDialect_POSTGRESQL {
		return postgresqlParamPrefixes
	}
	return googleSqlParamPrefixes
}

func (p *statementParser) isValidParamsFirstChar(prefix, c byte) bool {
	if p.dialect == databasepb.DatabaseDialect_POSTGRESQL && prefix == '$' {
		// PostgreSQL query parameters have the form $1, $2, ..., $10, ...
		return isLatinDigit(c)
	}
	// GoogleSQL query parameters have the form @name or @_name123
	return isLatinLetter(c) || c == '_'
}

func (p *statementParser) isValidParamsChar(prefix, c byte) bool {
	if p.dialect == databasepb.DatabaseDialect_POSTGRESQL && prefix == '$' {
		// PostgreSQL query parameters have the form $1, $2, ..., $10, ...
		return isLatinDigit(c)
	}
	// GoogleSQL query parameters have the form @name or @_name123
	return isLatinLetter(c) || isLatinDigit(c) || c == '_'
}

func (p *statementParser) positionalParameterToNamed(positionalParameterIndex int) string {
	if p.dialect == databasepb.DatabaseDialect_POSTGRESQL {
		return "$" + strconv.Itoa(positionalParameterIndex)
	}
	return "@p" + strconv.Itoa(positionalParameterIndex)
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
func (p *statementParser) skipWhitespacesAndComments(sql []byte, pos int) int {
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

func (p *statementParser) skipDollarQuotedString(sql []byte, pos int) (int, error) {
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

func (p *statementParser) skipQuoted(sql []byte, pos int, quote byte) (int, int, error) {
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
		info := p.detectStatementType(sql)
		cachedParams := make([]string, len(params))
		copy(cachedParams, params)
		p.statementsCache.Add(sql, &statementsCacheEntry{sql: namedParamsSql, params: cachedParams, info: info})
		return namedParamsSql, params, nil
	}
}

func (p *statementParser) calculateFindParamsResult(sql string) (string, []string, error) {
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

// isDDL returns true if the given sql string is a DDL statement.
// This function assumes that any comments and hints at the start
// of the sql string have been removed.
func (p *statementParser) isDDL(query string) bool {
	info := p.detectStatementType(query)
	return info.statementType == StatementTypeDdl
}

func isDDLKeyword(keyword string) bool {
	return isStatementKeyword(keyword, ddlStatements)
}

// isDml returns true if the given sql string is a Dml statement.
// This function assumes that any comments and hints at the start
// of the sql string have been removed.
func (p *statementParser) isDml(query string) bool {
	info := p.detectStatementType(query)
	return info.statementType == StatementTypeDml
}

func isDmlKeyword(keyword string) bool {
	return isStatementKeyword(keyword, dmlStatements)
}

// isQuery returns true if the given sql string is a SELECT statement.
// This function assumes that any comments and hints at the start
// of the sql string have been removed.
func (p *statementParser) isQuery(query string) bool {
	info := p.detectStatementType(query)
	return info.statementType == StatementTypeQuery
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
	execContext                   func(ctx context.Context, c *conn, params string, opts *ExecOptions, args []driver.NamedValue) (driver.Result, error)
	queryContext                  func(ctx context.Context, c *conn, params string, opts *ExecOptions, args []driver.NamedValue) (driver.Rows, error)
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
		if execContext, ok := i.(func(ctx context.Context, c *conn, query string, opts *ExecOptions, args []driver.NamedValue) (driver.Result, error)); ok {
			stmt.execContext = execContext
		}
		if queryContext, ok := i.(func(ctx context.Context, c *conn, query string, opts *ExecOptions, args []driver.NamedValue) (driver.Rows, error)); ok {
			stmt.queryContext = queryContext
		}
		if stmt.queryContext == nil && stmt.execContext != nil {
			stmt.queryContext = func(ctx context.Context, c *conn, params string, opts *ExecOptions, args []driver.NamedValue) (driver.Rows, error) {
				_, err := stmt.execContext(ctx, c, params, opts, args)
				if err != nil {
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

func (c *executableClientSideStatement) ExecContext(ctx context.Context, opts *ExecOptions, args []driver.NamedValue) (driver.Result, error) {
	if c.clientSideStatement.execContext == nil {
		return nil, spanner.ToSpannerError(status.Errorf(codes.InvalidArgument, "%q cannot be used with execContext", c.query))
	}
	return c.clientSideStatement.execContext(ctx, c.conn, c.params, opts, args)
}

func (c *executableClientSideStatement) QueryContext(ctx context.Context, opts *ExecOptions, args []driver.NamedValue) (driver.Rows, error) {
	if c.clientSideStatement.queryContext == nil {
		return nil, spanner.ToSpannerError(status.Errorf(codes.InvalidArgument, "%q cannot be used with queryContext", c.query))
	}
	return c.clientSideStatement.queryContext(ctx, c.conn, c.params, opts, args)
}

// parseClientSideStatement returns the executableClientSideStatement that
// corresponds with the given query string, or nil if it is not a valid client
// side statement.
func (p *statementParser) parseClientSideStatement(c *conn, query string) (*executableClientSideStatement, error) {
	if p.useCache {
		if val, ok := p.statementsCache.Get(query); ok {
			if val.info.statementType == StatementTypeClientSide {
				var params string
				if len(val.params) > 0 {
					params = val.params[0]
				}
				return &executableClientSideStatement{val.clientSideStatement, c, query, params}, nil
			}
			return nil, nil
		}
	}
	// Determine whether it could be a valid client-side statement by looking at the first keyword.
	// This is a lot more efficient than looping through all regular expressions for client-side
	// statements to check whether the statement matches that expression.
	sp := &simpleParser{sql: []byte(query), statementParser: c.parser}
	keyword := strings.ToUpper(sp.readKeyword())
	if _, ok := clientSideKeywords[keyword]; !ok {
		return nil, nil
	}

	statementsInit.Do(func() {
		if err := compileStatements(); err != nil {
			statementsCompileErr = err
		}
	})
	if statementsCompileErr != nil {
		return nil, statementsCompileErr
	}
	// TODO: Replace all regex-based statements with parsed statements.
	for _, stmt := range statements.Statements {
		if stmt.regexp.MatchString(query) {
			var params string
			if stmt.setStatement.Separator != "" {
				p := strings.SplitN(query, stmt.setStatement.Separator, 2)
				if len(p) == 2 {
					params = strings.TrimSpace(p[1])
				}
			}
			if p.useCache {
				cacheEntry := &statementsCacheEntry{
					sql:                 query,
					params:              []string{params},
					clientSideStatement: stmt,
					info: &statementInfo{
						statementType: StatementTypeClientSide,
					},
				}
				p.statementsCache.Add(query, cacheEntry)
			}
			return &executableClientSideStatement{stmt, c, query, params}, nil
		}
	}
	// TODO: Cache parsed statements.
	if stmt, err := parseStatement(p, keyword, query); err != nil {
		return nil, err
	} else if stmt != nil {
		return stmt.executableStatement(c), nil
	}
	return nil, nil
}

func (p *statementParser) parseShowStatement(c *conn, query string) (*executableClientSideStatement, error) {
	stmt := &parsedShowStatement{}
	if err := stmt.parse(p, query); err != nil {
		return nil, err
	}
	return stmt.executableStatement(c), nil
}

func (p *statementParser) parseSetStatement(c *conn, query string) (*executableClientSideStatement, error) {
	stmt := &parsedSetStatement{}
	if err := stmt.parse(p, query); err != nil {
		return nil, err
	}
	return stmt.executableStatement(c), nil
}

type StatementType int

const (
	StatementTypeUnknown StatementType = iota
	StatementTypeQuery
	StatementTypeDml
	StatementTypeDdl
	StatementTypeClientSide
	StatementTypeShow
	StatementTypeSet
)

func (st StatementType) String() string {
	switch st {
	case StatementTypeQuery:
		return "Query"
	case StatementTypeDml:
		return "Dml"
	case StatementTypeDdl:
		return "Ddl"
	case StatementTypeClientSide:
		return "ClientSide"
	case StatementTypeShow:
		return "Show"
	case StatementTypeSet:
		return "Set"
	default:
		return "Unknown"
	}
}

type dmlType int

const (
	dmlTypeUnknown dmlType = iota
	dmlTypeInsert
	dmlTypeUpdate
	dmlTypeDelete
)

type statementInfo struct {
	statementType StatementType
	dmlType       dmlType
}

// detectStatementType returns the type of SQL statement based on the first
// keyword that is found in the SQL statement.
func (p *statementParser) detectStatementType(sql string) *statementInfo {
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

func (p *statementParser) calculateDetectStatementType(sql string) *statementInfo {
	parser := &simpleParser{sql: []byte(sql), statementParser: p}
	_ = parser.skipStatementHint()
	keyword := strings.ToUpper(parser.readKeyword())
	if isQueryKeyword(keyword) {
		return &statementInfo{statementType: StatementTypeQuery}
	} else if isDmlKeyword(keyword) {
		return &statementInfo{
			statementType: StatementTypeDml,
			dmlType:       detectDmlKeyword(keyword),
		}
	} else if isDDLKeyword(keyword) {
		return &statementInfo{statementType: StatementTypeDdl}
	}
	return &statementInfo{statementType: StatementTypeUnknown}
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
