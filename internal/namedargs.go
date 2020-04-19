// Copyright 2020 Google Inc. All Rights Reserved.
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
	"regexp"
)

var namedValueParamNameRegex = regexp.MustCompile("@(\\w+)")

func NamedValueParamNames(q string, n int) ([]string, error) {
	var names []string
	matches := namedValueParamNameRegex.FindAllStringSubmatch(q, n)
	if m := len(matches); n != -1 && m < n {
		return nil, fmt.Errorf("query has %d placeholders but %d arguments are provided", m, n)
	}
	for _, m := range matches {
		names = append(names, m[1])
	}
	return names, nil
}
