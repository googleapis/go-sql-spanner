// Copyright 2021 Google Inc. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package internal

import (
	"fmt"
	"strings"
	"unicode"
)

var ddlStatements = map[string]bool{"CREATE": true, "DROP": true, "ALTER": true}
var selectStatements = map[string]bool{"SELECT": true, "WITH": true}
var dmlStatements = map[string]bool{"INSERT": true, "UPDATE": true, "DELETE": true}
var selectAndDmlStatements = union(selectStatements, dmlStatements)

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

// ParseNamedParameters returns the named parameters in the given sql string.
// The sql string must be a valid Cloud Spanner sql statement. It may contain
// comments and (string) literals without any restrictions. That is, string
// literals containing for example an email address ('test@test.com') will be
// recognized as a string literal and not returned as a named parameter.
func ParseNamedParameters(sql string) ([]string, error) {
	sql, err := removeCommentsAndTrim(sql)
	if err != nil {
		return nil, err
	}
	sql = removeStatementHint(sql)
	return findParams(sql)
}

// RemoveCommentsAndTrim removes any comments in the query string and trims any
// spaces at the beginning and end of the query. This makes checking what type
// of query a string is a lot easier, as only the first word(s) need to be
// checked after this has been removed.
func removeCommentsAndTrim(sql string) (string, error) {
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
				return "", fmt.Errorf("statement contains an unclosed literal: %s", sql)
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
		return "", fmt.Errorf("statement contains an unclosed literal: %s", sql)
	}
	trimmed := strings.TrimSpace(res.String())
	if len(trimmed) > 0 && trimmed[len(trimmed)-1] == ';' {
		return trimmed[:len(trimmed)-1], nil
	}
	return trimmed, nil
}

// Removes any statement hints at the beginning of the statement.
// It assumes that any comments have already been removed.
func removeStatementHint(sql string) string {
	// Valid statement hints at the beginning of a query statement can only contain a fixed set of
	// possible values. Although it is possible to add a @{FORCE_INDEX=...} as a statement hint, the
	// only allowed value is _BASE_TABLE. This means that we can safely assume that the statement
	// hint will not contain any special characters, for example a closing curly brace or one of the
	// keywords SELECT, UPDATE, DELETE, WITH, and that we can keep the check simple by just
	// searching for the first occurrence of a keyword that should be preceded by a closing curly
	// brace at the end of the statement hint.
	startStatementHintIndex := strings.Index(sql, "{")
	// Statement hints are allowed for both queries and DML statements.
	startQueryIndex := -1
	upperCaseSql := strings.ToUpper(sql)
	for keyword := range selectAndDmlStatements {
		if startQueryIndex = strings.Index(upperCaseSql, keyword); startQueryIndex > -1 {
			break
		}
	}
	if startQueryIndex > -1 {
		endStatementHintIndex := strings.LastIndex(sql[:startQueryIndex], "}")
		if startStatementHintIndex == -1 || startStatementHintIndex > endStatementHintIndex {
			// Looks like an invalid statement hint. Just ignore at this point
			// and let the caller handle the invalid query.
			return sql
		}
		return strings.TrimSpace(sql[endStatementHintIndex+1:])
	}
	// Seems invalid, just return the original statement.
	return sql
}

// This function assumes that all comments and statement hints have already
// been removed from the statement.
func findParams(sql string) ([]string, error) {
	const paramPrefix = '@'
	const singleQuote = '\''
	const doubleQuote = '"'
	const backtick = '`'
	isInQuoted := false
	var startQuote rune
	lastCharWasEscapeChar := false
	isTripleQuoted := false
	res := make([]string, 0)
	index := 0
	runes := []rune(sql)
	for index < len(runes) {
		c := runes[index]
		if isInQuoted {
			if (c == '\n' || c == '\r') && !isTripleQuoted {
				return nil, fmt.Errorf("statement contains an unclosed literal: %s", sql)
			} else if c == startQuote {
				if lastCharWasEscapeChar {
					lastCharWasEscapeChar = false
				} else if isTripleQuoted {
					if len(runes) > index+2 && runes[index+1] == startQuote && runes[index+2] == startQuote {
						isInQuoted = false
						startQuote = 0
						isTripleQuoted = false
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
		} else {
			// We are not in a quoted string.
			if c == paramPrefix && len(runes) > index+1 && !unicode.IsSpace(runes[index+1]) {
				index++
				startIndex := index
				for index < len(runes) {
					if !(unicode.IsLetter(runes[index]) || unicode.IsDigit(runes[index]) || runes[index] == '_') {
						res = append(res, string(runes[startIndex:index]))
						break
					}
					if index == len(runes)-1 {
						res = append(res, string(runes[startIndex:]))
						break
					}
					index++
				}
			} else {
				if c == singleQuote || c == doubleQuote || c == backtick {
					isInQuoted = true
					startQuote = c
					// Check whether it is a triple-quote.
					if len(runes) > index+2 && runes[index+1] == startQuote && runes[index+2] == startQuote {
						isTripleQuoted = true
						index += 2
					}
				}
			}
		}
		index++
	}
	if isInQuoted {
		return nil, fmt.Errorf("statement contains an unclosed literal: %s", sql)
	}
	return res, nil
}

func IsDdl(query string) (bool, error) {
	query, err := removeCommentsAndTrim(query)
	if err != nil {
		return false, err
	}
	// We can safely check if the string starts with a specific string, as we
	// have already removed all leading spaces, and there are no keywords that
	// start with the same substring as one of the DDL keywords.
	for ddl := range ddlStatements {
		if strings.EqualFold(query[:len(ddl)], ddl) {
			return true, nil
		}
	}
	return false, nil
}
