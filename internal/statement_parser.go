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
)

// RemoveCommentsAndTrim removes any comments in the query string and trims any
// spaces at the beginning and end of the query. This makes checking what type
// of query a string is a lot easier, as only the first word(s) need to be
// checked after this has been removed.
func RemoveCommentsAndTrim(query string) (string, error) {
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
	var startQuote uint8
	lastCharWasEscapeChar := false
	isTripleQuoted := false
	res := strings.Builder{}
	res.Grow(len(query))
	index := 0
	for index < len(query) {
		c := query[index]
		if isInQuoted {
			if (c == '\n' || c == '\r') && !isTripleQuoted {
				return "", fmt.Errorf("query contains an unclosed literal: %s", query)
			} else if c == startQuote {
				if lastCharWasEscapeChar {
					lastCharWasEscapeChar = false
				} else if isTripleQuoted {
					if len(query) > index + 2 && query[index+1] == startQuote && query[index+2] == startQuote {
						isInQuoted = false
						startQuote = 0
						isTripleQuoted = false
						res.WriteByte(c)
						res.WriteByte(c)
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
			res.WriteByte(c)
		} else {
			// We are not in a quoted string.
			if isInSingleLineComment {
				if c == '\n' {
					isInSingleLineComment = false
					// Include the line feed in the result.
					res.WriteByte(c)
				}
			} else if isInMultiLineComment {
				if len(query) > index + 1 && c == asterisk && query[index+1] == slash {
					isInMultiLineComment = false
					index++
				}
			} else {
				if c == dash || (len(query) > index + 1 && c == hyphen && query[index+1] == hyphen) {
					// This is a single line comment.
					isInSingleLineComment = true
				} else if len(query) > index + 1 && c == slash && query[index+1] == asterisk {
					isInMultiLineComment = true
					index++
				} else {
					if c == singleQuote || c == doubleQuote || c == backtick {
						isInQuoted = true
						startQuote = c
						// Check whether it is a triple-quote.
						if len(query) > index+2 && query[index+1] == startQuote && query[index+2] == startQuote {
							isTripleQuoted = true
							res.WriteByte(c)
							res.WriteByte(c)
							index += 2
						}
					}
					res.WriteByte(c)
				}
			}
		}
		index++
	}
	if isInQuoted {
		return "", fmt.Errorf("query contains an unclosed literal: %s", query)
	}
	trimmed := strings.TrimSpace(res.String())
	if len(trimmed) > 0 && trimmed[len(trimmed) - 1] == ';' {
		return trimmed[:len(trimmed) - 1], nil
	}
	return trimmed, nil
}
