// Copyright 2026 Google LLC
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
	"io"
	"regexp"
	"strings"

	"google.golang.org/api/iterator"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var sqlstateRegex = regexp.MustCompile(`(?i)SQLSTATE[\s:]*([0-9A-Z]{5})`)

// checkAndEnrichError inspects an error and enriches it with a standard
// PostgreSQL SQLSTATE code if isPG is true. It safely passes through nil,
// io.EOF, and iterator.Done signals untouched.
func checkAndEnrichError(isPG bool, err error) error {
	if err == nil || !isPG {
		return err
	}
	if err == io.EOF || err == iterator.Done {
		return err
	}
	return WithPGSQLState(err)
}

// ToPGSQLState inspects a Cloud Spanner error and returns the corresponding
// standard 5-character PostgreSQL SQLSTATE code.
//
// Official PostgreSQL Error Codes Reference:
// https://www.postgresql.org/docs/current/errcodes-appendix.html
//
// Standard mappings:
//   - "23505" (Unique Violation): Duplicate key / AlreadyExists
//   - "23502" (Not Null Violation): Null value in column
//   - "23503" (Foreign Key Violation): Foreign key constraint failure
//   - "23514" (Check Violation): Check constraint failure
//   - "22001" (String Data Right Truncation): String exceeds maximum allowed length
//   - "22003" (Numeric Value Out of Range): Numeric / integer overflow or range error (codes.OutOfRange)
//   - "22012" (Division By Zero): Division by zero
//   - "40001" (Serialization Failure): Transaction aborted / retryable conflict (codes.Aborted)
//   - "42P01" (Undefined Table): Table or relation not found
//   - "42703" (Undefined Column): Column not found / Unrecognized name
//   - "42601" (Syntax Error): Syntax or parse error
//   - "42501" (Insufficient Privilege): Permission denied (codes.PermissionDenied)
//   - "28000" (Invalid Authorization Specification): Unauthenticated (codes.Unauthenticated)
//   - "53000" (Insufficient Resources): Insufficient resources / quota exceeded (codes.ResourceExhausted)
//   - "57014" (Query Canceled): Canceled or deadline exceeded (codes.Canceled, codes.DeadlineExceeded)
//   - "08006" (Connection Failure): Connection unavailable (codes.Unavailable)
//   - "0A000" (Feature Not Supported): Unimplemented (codes.Unimplemented)
//   - "XX000" (Internal / Fallback): Default fallback for unclassified database errors
//
// Returns "" if err is nil.
func ToPGSQLState(err error) string {
	if err == nil {
		return ""
	}
	errStr := err.Error()
	if match := sqlstateRegex.FindStringSubmatch(errStr); len(match) > 1 {
		return strings.ToUpper(match[1])
	}

	// 1. Standard gRPC status codes
	s, ok := status.FromError(err)
	if ok {
		switch s.Code() {
		case codes.AlreadyExists:
			return "23505"
		case codes.Aborted:
			return "40001"
		case codes.PermissionDenied:
			return "42501"
		case codes.Unauthenticated:
			return "28000"
		case codes.Canceled, codes.DeadlineExceeded:
			return "57014"
		case codes.Unavailable:
			return "08006"
		case codes.Unimplemented:
			return "0A000"
		case codes.OutOfRange:
			return "22003"
		case codes.ResourceExhausted:
			return "53000"
		}
	}

	// 2. Error message text patterns fallback (for structural/schema/constraint errors and non-status errors).
	// NOTE: String matching on err.Error() is fragile if Spanner backend changes message formatting.
	// This fallback is used when specific gRPC status codes (e.g. InvalidArgument) or ErrorInfo metadata
	// are not detailed enough to distinguish syntax errors, undefined relations/columns, or constraints.
	lower := strings.ToLower(errStr)
	if strings.Contains(lower, "alreadyexists") || strings.Contains(lower, "duplicate key") || strings.Contains(lower, "unique index") {
		return "23505"
	}
	if strings.Contains(lower, "must not be null") || strings.Contains(lower, "null value in column") || strings.Contains(lower, "cannot be null") {
		return "23502"
	}
	if strings.Contains(lower, "foreign key") {
		return "23503"
	}
	if strings.Contains(lower, "check constraint") {
		return "23514"
	}
	if strings.Contains(lower, "exceeds maximum allowed length") || strings.Contains(lower, "value too long") {
		return "22001"
	}
	if strings.Contains(lower, "out of range") || strings.Contains(lower, "numeric overflow") {
		return "22003"
	}
	if strings.Contains(lower, "division by zero") {
		return "22012"
	}
	if strings.Contains(lower, "syntax error") || strings.Contains(lower, "parse error") {
		return "42601"
	}
	if strings.Contains(lower, "table not found") || ((strings.Contains(lower, "table") || strings.Contains(lower, "relation")) && (strings.Contains(lower, "not found") || strings.Contains(lower, "does not exist"))) {
		return "42P01"
	}
	if strings.Contains(lower, "column not found") || strings.Contains(lower, "unrecognized name") || (strings.Contains(lower, "column") && (strings.Contains(lower, "not found") || strings.Contains(lower, "does not exist"))) {
		return "42703"
	}
	if strings.Contains(lower, "permission denied") || strings.Contains(lower, "insufficient privilege") {
		return "42501"
	}

	return "XX000"
}

// WithPGSQLState inspects a Cloud Spanner error and, if it does not already
// contain an explicit SQLSTATE code, wraps the error so its message starts
// with "[SQLSTATE xxxxx]" while preserving error unwrapping (Unwrap() error)
// and gRPC status extraction (GRPCStatus() *status.Status).
// Returns nil if err is nil.
func WithPGSQLState(err error) error {
	if err == nil {
		return nil
	}
	if _, ok := err.(*pgError); ok {
		return err
	}
	code := ToPGSQLState(err)
	return &pgError{code: code, err: err}
}

type pgError struct {
	code string
	err  error
}

func (e *pgError) Error() string {
	if sqlstateRegex.MatchString(e.err.Error()) {
		return e.err.Error()
	}
	return "[SQLSTATE " + e.code + "] " + e.err.Error()
}

func (e *pgError) Unwrap() error {
	return e.err
}

func (e *pgError) GRPCStatus() *status.Status {
	s, ok := status.FromError(e.err)
	if ok {
		p := s.Proto()
		if !sqlstateRegex.MatchString(p.Message) {
			p.Message = "[SQLSTATE " + e.code + "] " + p.Message
		}
		return status.FromProto(p)
	}
	return status.New(codes.Unknown, e.Error())
}
