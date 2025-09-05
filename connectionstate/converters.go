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

func CreateReadOnlyConverter[T any](property string) func(value string) (T, error) {
	return func(value string) (T, error) {
		var t T
		return t, status.Errorf(codes.FailedPrecondition, "property %s is read-only", property)
	}
}

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

var strongRegexp = regexp.MustCompile(`(?i)^\s*STRONG\s*$`)
var exactStalenessRegexp = regexp.MustCompile(`(?i)^\s*(?P<type>EXACT_STALENESS)\s+(?P<duration>(\d{1,19})(s|ms|us|ns))\s*$`)
var maxStalenessRegexp = regexp.MustCompile(`(?i)^\s*(?P<type>MAX_STALENESS)\s+(?P<duration>(\d{1,19})(s|ms|us|ns))\s*$`)
var readTimestampRegexp = regexp.MustCompile(`(?i)^\s*(?P<type>READ_TIMESTAMP)\s+(?P<timestamp>(\d{4})-(\d{2})-(\d{2})([Tt](\d{2}):(\d{2}):(\d{2})(\.\d{1,9})?)([Zz]|([+-])(\d{2}):(\d{2})))\s*$`)
var minReadTimestampRegexp = regexp.MustCompile(`(?i)^\s*(?P<type>MIN_READ_TIMESTAMP)\s+(?P<timestamp>(\d{4})-(\d{2})-(\d{2})([Tt](\d{2}):(\d{2}):(\d{2})(\.\d{1,9})?)([Zz]|([+-])(\d{2}):(\d{2})))\s*$`)

func ConvertReadOnlyStaleness(value string) (spanner.TimestampBound, error) {
	var staleness spanner.TimestampBound
	if strongRegexp.MatchString(value) {
		staleness = spanner.StrongRead()
	} else if exactStalenessRegexp.MatchString(value) {
		d, err := parseDuration(exactStalenessRegexp, value)
		if err != nil {
			return staleness, err
		}
		staleness = spanner.ExactStaleness(d)
	} else if maxStalenessRegexp.MatchString(value) {
		d, err := parseDuration(maxStalenessRegexp, value)
		if err != nil {
			return staleness, err
		}
		staleness = spanner.MaxStaleness(d)
	} else if readTimestampRegexp.MatchString(value) {
		t, err := parseTimestamp(readTimestampRegexp, value)
		if err != nil {
			return staleness, err
		}
		staleness = spanner.ReadTimestamp(t)
	} else if minReadTimestampRegexp.MatchString(value) {
		t, err := parseTimestamp(minReadTimestampRegexp, value)
		if err != nil {
			return staleness, err
		}
		staleness = spanner.MinReadTimestamp(t)
	} else {
		return staleness, spanner.ToSpannerError(status.Errorf(codes.InvalidArgument, "invalid ReadOnlyStaleness value: %s", value))
	}
	return staleness, nil
}

func parseTimestamp(re *regexp.Regexp, params string) (time.Time, error) {
	matches := matchesToMap(re, params)
	if matches["timestamp"] == "" {
		return time.Time{}, spanner.ToSpannerError(status.Error(codes.InvalidArgument, "No timestamp found in staleness string"))
	}
	t, err := time.Parse(time.RFC3339Nano, matches["timestamp"])
	if err != nil {
		return time.Time{}, spanner.ToSpannerError(status.Errorf(codes.InvalidArgument, "Invalid timestamp: %s", matches["timestamp"]))
	}
	return t, nil
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
