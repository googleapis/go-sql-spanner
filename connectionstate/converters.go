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

package connectionstate

import (
	"fmt"
	"regexp"
	"strconv"
	"time"

	"cloud.google.com/go/spanner"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func ConvertBool(value string) (bool, error) {
	return strconv.ParseBool(value)
}

func ConvertInt(value string) (int, error) {
	return strconv.Atoi(value)
}

func ConvertInt64(value string) (int64, error) {
	return strconv.ParseInt(value, 10, 64)
}

func ConvertUint64(value string) (uint64, error) {
	return strconv.ParseUint(value, 10, 64)
}

func ConvertString(value string) (string, error) {
	return value, nil
}

var durationRegEx = regexp.MustCompile(`(?i)^\s*((?P<duration>(\d{1,19})(s|ms|us|ns))|(?P<number>\d{1,19})|(?P<null>NULL))\s*$`)

func ConvertDuration(value string) (time.Duration, error) {
	return parseDuration(durationRegEx, value)
}

func parseDuration(re *regexp.Regexp, value string) (time.Duration, error) {
	matches := matchesToMap(re, value)
	if matches["duration"] == "" && matches["number"] == "" && matches["null"] == "" {
		return 0, spanner.ToSpannerError(status.Error(codes.InvalidArgument, fmt.Sprintf("No duration found: %v", value)))
	}
	if matches["duration"] != "" {
		d, err := time.ParseDuration(matches["duration"])
		if err != nil {
			return 0, spanner.ToSpannerError(status.Errorf(codes.InvalidArgument, "Invalid duration: %s", matches["duration"]))
		}
		return d, nil
	} else if matches["number"] != "" {
		d, err := strconv.Atoi(matches["number"])
		if err != nil {
			return 0, spanner.ToSpannerError(status.Errorf(codes.InvalidArgument, "Invalid duration: %s", matches["number"]))
		}
		return time.Millisecond * time.Duration(d), nil
	} else if matches["null"] != "" {
		return time.Duration(0), nil
	}
	return time.Duration(0), spanner.ToSpannerError(status.Errorf(codes.InvalidArgument, "Unrecognized duration: %s", value))
}

func matchesToMap(re *regexp.Regexp, s string) map[string]string {
	matches := make(map[string]string)
	match := re.FindStringSubmatch(s)
	if match == nil {
		return matches
	}
	for i, name := range re.SubexpNames() {
		if i != 0 && name != "" {
			matches[name] = match[i]
		}
	}
	return matches
}
