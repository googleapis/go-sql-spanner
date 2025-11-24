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
	"strings"
	"unicode"
	"unicode/utf8"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type simpleParser struct {
	statementParser *StatementParser
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

// eatTokens advances the parser by len(tokens) positions and returns true
// if the next len(tokens) bytes are equal to the given bytes. This function
// only works for characters that can be encoded in one byte.
//
// Any whitespace and/or comments before or between the tokens will be skipped.
func (p *simpleParser) eatTokens(tokens []byte) bool {
	if len(tokens) == 0 {
		return true
	}
	if len(tokens) == 1 {
		return p.eatToken(tokens[0])
	}

	startPos := p.pos
	for _, t := range tokens {
		if !p.eatToken(t) {
			p.pos = startPos
			return false
		}
	}
	return true
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
			// This looks like a nested dollar-tag. Nested dollar-quoted strings are allowed.
			// These should just be ignored, as we don't know if it is actually a nested
			// dollar-quoted string, or just a random dollar sign inside the current string.
			p.pos = posBeforeTag
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
			if p.eatTokenWithWhitespaceOption('$' /*eatWhiteSpaces=*/, false) {
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

// Identifier represents an identifier (e.g. table name, variable name) in a SQL string.
// An identifier can consist of multiple parts separated by a dot in a SQL string.
// E.g. table name my_schema.my_table is an Identifier with two parts:
//   - my_schema
//   - my_table
type Identifier struct {
	Parts []string
}

func (i *Identifier) String() string {
	return strings.Join(i.Parts, ".")
}

// eatIdentifier reads the identifier at the current parser position, updates the parser position,
// and returns the identifier.
func (p *simpleParser) eatIdentifier() (Identifier, error) {
	p.skipWhitespacesAndComments()
	if p.pos >= len(p.sql) {
		return Identifier{}, status.Errorf(codes.InvalidArgument, "no identifier found at position %d", p.pos)
	}

	startPos := p.pos
	first := true
	result := Identifier{Parts: make([]string, 0, 1)}
	appendLastPart := true
	for p.pos < len(p.sql) {
		if first {
			first = false
			// Check if this is a quoted identifier.
			if p.sql[p.pos] == p.statementParser.identifierQuoteToken() {
				pos, quoteLen, err := p.statementParser.skipQuoted(p.sql, p.pos, p.sql[p.pos])
				if err != nil {
					return Identifier{}, err
				}
				p.pos = pos
				result.Parts = append(result.Parts, string(p.sql[startPos+quoteLen:pos-quoteLen]))
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
			if !p.isValidFirstIdentifierChar() {
				return Identifier{}, status.Errorf(codes.InvalidArgument, "invalid first identifier character found at position %d: %s", p.pos, p.sql[p.pos:p.pos+1])
			}
		} else {
			if !p.isValidIdentifierChar() {
				result.Parts = append(result.Parts, string(p.sql[startPos:p.pos]))
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
		return Identifier{}, status.Errorf(codes.InvalidArgument, "no identifier found at position %d", startPos)
	}
	if appendLastPart && p.pos == len(p.sql) {
		result.Parts = append(result.Parts, string(p.sql[startPos:p.pos]))
	}
	return result, nil
}

// Literal is a string, number or other literal in a SQL string.
type Literal struct {
	Value string
}

func (p *simpleParser) eatLiteral() (Literal, error) {
	p.skipWhitespacesAndComments()
	if p.pos >= len(p.sql) {
		return Literal{}, status.Errorf(codes.InvalidArgument, "missing literal at position %d", p.pos)
	}
	startPos := p.pos
	c := p.sql[p.pos]
	if c == '\'' || (c == '"' && p.statementParser.supportsDoubleQuotedStringLiterals()) {
		pos, quoteLen, err := p.statementParser.skipQuoted(p.sql, p.pos, c)
		if err != nil {
			return Literal{}, err
		}
		p.pos = pos
		value := string(p.sql[startPos+quoteLen : p.pos-quoteLen])
		return Literal{Value: value}, nil
	}

	value := p.readUnquotedLiteral()
	if value == "" {
		return Literal{}, status.Errorf(codes.InvalidArgument, "missing literal at position %d", p.pos)
	}
	return Literal{Value: value}, nil
}

// eatKeywords eats the given keywords in the order of the slice.
// Returns true and updates the position of the parser if all the keywords were successfully eaten.
// Returns false and does not update the position of the parser if not all keywords could be eaten.
func (p *simpleParser) eatKeywords(keywords []string) bool {
	startPos := p.pos
	for _, keyword := range keywords {
		if !p.eatKeyword(keyword) {
			p.pos = startPos
			return false
		}
	}
	return true
}

// peekKeyword checks if the next keyword is the given keyword.
// The position of the parser is not updated.
func (p *simpleParser) peekKeyword(keyword string) bool {
	pos := p.pos
	defer func() {
		p.pos = pos
	}()
	return p.eatKeyword(keyword)
}

// eatKeyword eats the given keyword at the current position of the parser if it exists
// and returns true if the keyword was found. Otherwise, it returns false.
func (p *simpleParser) eatKeyword(keyword string) bool {
	_, ok := p.eatAndReturnKeyword(keyword)
	return ok
}

// eatAndReturnKeyword eats the given keyword at the current position of the parser if it exists.
//
// Returns the actual keyword that was read and true if the keyword is found, and updates the position of the parser.
// Returns an empty string and false without updating the position of the parser if the keyword was not found.
func (p *simpleParser) eatAndReturnKeyword(keyword string) (string, bool) {
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
		// Only upper/lower-case letters and underscores are allowed in keywords.
		if !((p.sql[p.pos] >= 'A' && p.sql[p.pos] <= 'Z') || (p.sql[p.pos] >= 'a' && p.sql[p.pos] <= 'z')) && p.sql[p.pos] != '_' {
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

func (p *simpleParser) skipExpressionInBrackets() error {
	if p.pos >= len(p.sql) || p.sql[p.pos] != '(' {
		return nil
	}
	p.pos++
	level := 1
	for p.pos < len(p.sql) && level > 0 {
		if !p.isMultibyte() && p.sql[p.pos] == ')' {
			level--
			p.pos++
		} else if !p.isMultibyte() && p.sql[p.pos] == '(' {
			level++
			p.pos++
		} else {
			newPos, err := p.statementParser.skip(p.sql, p.pos)
			if err != nil {
				return err
			}
			p.pos = newPos
		}
	}
	return nil
}

var statementHintPrefix = []byte{'@', '{'}

// skipStatementHint skips any statement hint at the start of the statement.
func (p *simpleParser) skipStatementHint() bool {
	if p.eatTokens(statementHintPrefix) {
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
	if p.pos >= len(p.sql) {
		return false
	}
	return isMultibyte(p.sql[p.pos])
}

// nextChar moves the parser to the next character. This takes into
// account that some characters could be multibyte characters.
func (p *simpleParser) nextChar() {
	if p.pos >= len(p.sql) {
		return
	}
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
