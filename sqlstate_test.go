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
	"fmt"
	"io"
	"testing"
	"time"

	"google.golang.org/api/iterator"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"
)

func errWithErrorInfo(code codes.Code, msg string, meta map[string]string) error {
	st, _ := status.New(code, msg).WithDetails(&errdetails.ErrorInfo{
		Metadata: meta,
	})
	return st.Err()
}

func TestToPGSQLState(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want string
	}{
		{
			name: "NilError",
			err:  nil,
			want: "",
		},
		{
			name: "ErrorInfoMetadataPGErrorCode",
			err:  errWithErrorInfo(codes.InvalidArgument, "division by zero", map[string]string{"pg_sqlerrcode": "22012"}),
			want: "22012",
		},
		{
			name: "ErrorInfoMetadataSQLStateKey",
			err:  errWithErrorInfo(codes.InvalidArgument, "some error", map[string]string{"sqlstate": "42P07"}),
			want: "42P07",
		},
		{
			name: "ExistingSQLStateEmbedded",
			err:  errors.New("spanner error: [SQLSTATE 42P01] table users not found"),
			want: "42P01",
		},
		{
			name: "ExistingSQLStateColonFormat",
			err:  errors.New("spanner error: SQLSTATE: 42703 column foo does not exist"),
			want: "42703",
		},
		{
			name: "GRPCAlreadyExistsCode",
			err:  status.Error(codes.AlreadyExists, "Row [1] in table users already exists"),
			want: "23505",
		},
		{
			name: "RelationDoesNotExist",
			err:  status.Error(codes.NotFound, `relation "users" does not exist`),
			want: "42P01",
		},
		{
			name: "UndefinedColumnUnrecognizedName",
			err:  status.Error(codes.InvalidArgument, `column "foo" of relation "users" does not exist`),
			want: "42703",
		},
		{
			name: "ForeignKeyViolation",
			err:  status.Error(codes.FailedPrecondition, `Foreign key constraint "fk_order" is violated on table "orders". Cannot find referenced values in "users"`),
			want: "23503",
		},
		{
			name: "SerializationFailure",
			err:  status.Error(codes.Aborted, "Transaction was aborted due to concurrent modification"),
			want: "40001",
		},
		{
			name: "PermissionDenied",
			err:  status.Error(codes.PermissionDenied, "permission denied for table users"),
			want: "42501",
		},
		{
			name: "QueryCanceled",
			err:  status.Error(codes.Canceled, "context canceled"),
			want: "57014",
		},
		{
			name: "ConnectionFailure",
			err:  status.Error(codes.Unavailable, "server is unavailable"),
			want: "08006",
		},
		{
			name: "AlreadyExistsWithSpaceStringMatch",
			err:  status.Error(codes.AlreadyExists, "Failed to insert row with primary key [1] due to previously existing row"),
			want: "23505",
		},
		{
			name: "WrappedStatusErrorExtracted",
			err:  fmt.Errorf("outer wrapper: %w", status.Error(codes.AlreadyExists, "UNIQUE violation on index idx_name duplicate key: foo")),
			want: "23505",
		},
		{
			name: "FallbackXX000",
			err:  status.Error(codes.Internal, "some unclassified internal spanner error"),
			want: "XX000",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if g, w := ToPGSQLState(tc.err), tc.want; g != w {
				t.Errorf("SQLSTATE mismatch\nGot:  %v\nWant: %v", g, w)
			}
		})
	}
}

func TestWithPGSQLState(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want string
	}{
		{
			name: "NilError",
			err:  nil,
			want: "",
		},
		{
			name: "ExistingSQLStateNotDoubleWrapped",
			err:  errors.New("spanner error: [SQLSTATE 42P01] table users not found"),
			want: "spanner error: [SQLSTATE 42P01] table users not found",
		},
		{
			name: "WrapStatusError",
			err:  status.Error(codes.NotFound, `relation "users" does not exist`),
			want: `[SQLSTATE 42P01] rpc error: code = NotFound desc = relation "users" does not exist`,
		},
		{
			name: "WrapStandardError",
			err:  status.Error(codes.AlreadyExists, "Row [1] in table users already exists"),
			want: `[SQLSTATE 23505] rpc error: code = AlreadyExists desc = Row [1] in table users already exists`,
		},
		{
			name: "WrappedInStandardErrorNotDoubleWrapped",
			err:  fmt.Errorf("outer wrapper: %w", WithPGSQLState(status.Error(codes.NotFound, `relation "users" does not exist`))),
			want: `outer wrapper: [SQLSTATE 42P01] rpc error: code = NotFound desc = relation "users" does not exist`,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			gotErr := WithPGSQLState(tc.err)
			gotStr := ""
			if gotErr != nil {
				gotStr = gotErr.Error()
			}
			if g, w := gotStr, tc.want; g != w {
				t.Errorf("WithPGSQLState mismatch\nGot:  %v\nWant: %v", g, w)
			}
		})
	}
}

func TestWithPGSQLState_ExtractsGRPCStatusThroughChain(t *testing.T) {
	inner := status.Error(codes.AlreadyExists, "duplicate key")
	wrapped := fmt.Errorf("outer helper: %w", inner)
	enriched := WithPGSQLState(wrapped)
	if g, w := status.Code(enriched), codes.AlreadyExists; g != w {
		t.Errorf("expected status.Code to extract AlreadyExists through chain, got: %v", g)
	}
}

func TestCheckAndEnrichError_PassThroughSentinels(t *testing.T) {
	sentinels := []error{io.EOF, iterator.Done, driver.ErrBadConn, driver.ErrSkip}
	for _, sentinel := range sentinels {
		if got := checkAndEnrichError(true, sentinel); got != sentinel {
			t.Errorf("expected checkAndEnrichError(true, %v) to pass through untouched, got: %v", sentinel, got)
		}
	}
}

func TestWithPGSQLState_PreservesStatusDetails(t *testing.T) {
	st, _ := status.New(codes.Aborted, "Transaction was aborted due to concurrent modification").WithDetails(
		&errdetails.RetryInfo{
			RetryDelay: durationpb.New(1 * time.Second),
		},
	)
	origErr := st.Err()
	wrapped := WithPGSQLState(origErr)

	s, ok := status.FromError(wrapped)
	if !ok {
		t.Fatal("expected status.FromError to succeed on wrapped error")
	}
	if g, w := s.Code(), codes.Aborted; g != w {
		t.Errorf("status code mismatch\nGot:  %v\nWant: %v", g, w)
	}
	if g, w := s.Message(), "[SQLSTATE 40001] Transaction was aborted due to concurrent modification"; g != w {
		t.Errorf("status message mismatch\nGot:  %v\nWant: %v", g, w)
	}
	details := s.Details()
	if len(details) != 1 {
		t.Fatalf("expected 1 detail, got %d", len(details))
	}
	retryInfo, ok := details[0].(*errdetails.RetryInfo)
	if !ok {
		t.Fatalf("expected detail to be *errdetails.RetryInfo, got %T", details[0])
	}
	if retryInfo.GetRetryDelay().GetSeconds() != 1 {
		t.Errorf("expected RetryDelay seconds 1, got %v", retryInfo.GetRetryDelay().GetSeconds())
	}
}
