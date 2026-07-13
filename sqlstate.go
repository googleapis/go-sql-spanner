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
	"database/sql/driver"
	"errors"
	"io"
	"regexp"
	"strings"

	"google.golang.org/api/iterator"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var (
	sqlstateRegex                     = regexp.MustCompile(`(?i)SQLSTATE[\s:]*([0-9A-Z]{5})`)
	relationNotFoundRegex             = regexp.MustCompile(`(?i)relation .+ does not exist`)
	columnNotFoundRegex               = regexp.MustCompile(`(?i)column .+ of relation .+ does not exist`)
	pkViolationRegex                  = regexp.MustCompile(`(?i)Row .+ in table .+ already exists`)
	pkViolationEmulatorRegex          = regexp.MustCompile(`(?i)Failed to insert row with primary key .+ due to previously existing row`)
	uniqueIndexViolationRegex         = regexp.MustCompile(`(?i)Unique index violation on index .+ at index key .+`)
	uniqueIndexViolationEmulatorRegex = regexp.MustCompile(`(?i)UNIQUE violation on index .+ duplicate key: .+`)
	foreignKeyViolationRegex          = regexp.MustCompile(`(?i)Foreign key constraint .+ is violated on table .+\. Cannot find referenced values in .+`)
	foreignKeyViolationEmulatorRegex  = regexp.MustCompile(`(?i)Foreign key .+ constraint violation on table .+\. Cannot find referenced key .+ in table .+`)
	cannotDropTableIndicesRegex       = regexp.MustCompile(`(?i)Cannot drop table .+ with indices`)
	onlyRestrictBehaviorRegex         = regexp.MustCompile(`(?i)Only <RESTRICT> behavior is supported by <DROP> statement\.`)
)

const pgErrCodeKey = "pg_sqlerrcode"

func extractSpannerPGErrorCode(s *status.Status) string {
	if s == nil {
		return ""
	}
	for _, detail := range s.Details() {
		if ei, ok := detail.(*errdetails.ErrorInfo); ok && ei != nil && ei.Metadata != nil {
			if code, exists := ei.Metadata[pgErrCodeKey]; exists && len(code) == 5 {
				return strings.ToUpper(code)
			}
			if code, exists := ei.Metadata["sqlstate"]; exists && len(code) == 5 {
				return strings.ToUpper(code)
			}
		}
	}
	return ""
}

// checkAndEnrichError inspects an error and enriches it with a standard
// PostgreSQL SQLSTATE code if isPG is true. It safely passes through nil,
// io.EOF, and iterator.Done signals untouched.
func checkAndEnrichError(isPG bool, err error) error {
	if err == nil || !isPG {
		return err
	}
	if err == io.EOF || err == iterator.Done || err == driver.ErrBadConn || err == driver.ErrSkip {
		return err
	}
	return WithPGSQLState(err)
}

type grpcStatus interface {
	GRPCStatus() *status.Status
}

func extractGRPCStatus(err error) (*status.Status, bool) {
	if s, ok := status.FromError(err); ok && s != nil && s.Code() != codes.Unknown {
		return s, true
	}
	var gs grpcStatus
	if errors.As(err, &gs) {
		if s := gs.GRPCStatus(); s != nil && s.Code() != codes.Unknown {
			return s, true
		}
	}
	if s, ok := status.FromError(err); ok && s != nil {
		return s, true
	}
	return nil, false
}

// ToPGSQLState inspects a Cloud Spanner error and returns the corresponding
// standard 5-character PostgreSQL SQLSTATE code.
//
// Official PostgreSQL Error Codes Reference:
// https://www.postgresql.org/docs/current/errcodes-appendix.html
//
// Standard mappings:
//   - "23505" (Unique Violation): Duplicate key / AlreadyExists
//   - "23503" (Foreign Key Violation): Foreign key constraint failure
//   - "40001" (Serialization Failure): Transaction aborted / retryable conflict (codes.Aborted)
//   - "42P01" (Undefined Table): Table or relation not found
//   - "42703" (Undefined Column): Column not found / Unrecognized name
//   - "42601" (Syntax Error): Syntax or parse error
//   - "42501" (Insufficient Privilege): Permission denied (codes.PermissionDenied)
//   - "28000" (Invalid Authorization Specification): Unauthenticated (codes.Unauthenticated)
//   - "57014" (Query Canceled): Canceled or deadline exceeded (codes.Canceled, codes.DeadlineExceeded)
//   - "08006" (Connection Failure): Connection unavailable (codes.Unavailable)
//   - "0A000" (Feature Not Supported): Unimplemented (codes.Unimplemented)
//   - "25000" (Invalid Transaction State): Invalid transaction or batch lifecycle state
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

	// 1. Check if Spanner backend explicitly returned a pg_sqlerrcode inside ErrorInfo metadata.
	s, ok := extractGRPCStatus(err)
	if ok {
		if code := extractSpannerPGErrorCode(s); code != "" {
			return code
		}
		switch s.Code() {
		case codes.Aborted:
			return "40001" // SerializationFailure
		case codes.Canceled, codes.DeadlineExceeded:
			return "57014" // QueryCanceled
		case codes.PermissionDenied:
			return "42501" // InsufficientPrivilege
		case codes.Unauthenticated:
			return "28000" // InvalidAuthorizationSpecification
		case codes.Unavailable:
			return "08006" // ConnectionFailure
		case codes.Unimplemented:
			return "0A000" // FeatureNotSupported
		}
	}

	// 2. Strict combination check: gRPC ErrorCode + ErrorMessage text patterns.
	// Combines exact pattern from PGAdapter (PGExceptionFactory) with specific
	// strings required by existing PostgreSQL driver test suites and client-side statement parsers.
	code := codes.Unknown
	if ok && s != nil {
		code = s.Code()
	}

	// UndefinedColumn (42703): NOT_FOUND / INVALID_ARGUMENT + exact column not found regex
	if (code == codes.NotFound || code == codes.InvalidArgument) && columnNotFoundRegex.MatchString(errStr) {
		return "42703"
	}

	// UndefinedTable (42P01): NOT_FOUND / INVALID_ARGUMENT + exact relation not found regex
	if (code == codes.NotFound || code == codes.InvalidArgument) && relationNotFoundRegex.MatchString(errStr) {
		return "42P01"
	}

	// UniqueViolation (23505): ALREADY_EXISTS + exact primary key / unique index violation regexes
	if code == codes.AlreadyExists &&
		(pkViolationRegex.MatchString(errStr) || pkViolationEmulatorRegex.MatchString(errStr) ||
			uniqueIndexViolationRegex.MatchString(errStr) || uniqueIndexViolationEmulatorRegex.MatchString(errStr)) {
		return "23505"
	}

	// ForeignKeyViolation (23503): FAILED_PRECONDITION / INVALID_ARGUMENT + exact foreign key violation regexes
	if (code == codes.FailedPrecondition || code == codes.InvalidArgument) &&
		(foreignKeyViolationRegex.MatchString(errStr) || foreignKeyViolationEmulatorRegex.MatchString(errStr)) {
		return "23503"
	}

	// FeatureNotSupported (0A000): FAILED_PRECONDITION / INVALID_ARGUMENT + exact drop table/restrict behavior regexes
	if (code == codes.FailedPrecondition || code == codes.InvalidArgument) &&
		(cannotDropTableIndicesRegex.MatchString(errStr) || onlyRestrictBehaviorRegex.MatchString(errStr)) {
		return "0A000"
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
	s, ok := extractGRPCStatus(e.err)
	if ok && s != nil {
		p := s.Proto()
		if p != nil {
			if !sqlstateRegex.MatchString(p.Message) {
				p.Message = "[SQLSTATE " + e.code + "] " + p.Message
			}
			return status.FromProto(p)
		}
	}
	return status.New(codes.Unknown, e.Error())
}
