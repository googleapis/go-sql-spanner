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
	"errors"
	"testing"
	"time"

	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"
)

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
			err:  status.Error(codes.AlreadyExists, "row already exists"),
			want: "23505",
		},
		{
			name: "DuplicateKeyStringMatch",
			err:  errors.New("duplicate key value violates unique constraint"),
			want: "23505",
		},
		{
			name: "SyntaxError",
			err:  status.Error(codes.InvalidArgument, "Syntax error: unexpected token at line 1"),
			want: "42601",
		},
		{
			name: "UndefinedTableNotFound",
			err:  status.Error(codes.NotFound, "Table not found: users"),
			want: "42P01",
		},
		{
			name: "UndefinedTableDoesNotExist",
			err:  status.Error(codes.NotFound, "Table 'public.users' does not exist"),
			want: "42P01",
		},
		{
			name: "RelationDoesNotExist",
			err:  status.Error(codes.NotFound, `relation "users" does not exist`),
			want: "42P01",
		},
		{
			name: "UndefinedColumnUnrecognizedName",
			err:  status.Error(codes.InvalidArgument, "Unrecognized name: foo"),
			want: "42703",
		},
		{
			name: "UndefinedColumnNotFound",
			err:  status.Error(codes.InvalidArgument, "Column not found: bar"),
			want: "42703",
		},
		{
			name: "UndefinedColumnDoesNotExist",
			err:  status.Error(codes.InvalidArgument, "column 'bar' does not exist"),
			want: "42703",
		},
		{
			name: "NotNullViolation",
			err:  status.Error(codes.InvalidArgument, "null value in column 'id' violates not-null constraint"),
			want: "23502",
		},
		{
			name: "ForeignKeyViolation",
			err:  status.Error(codes.FailedPrecondition, "foreign key constraint violation on table 'orders'"),
			want: "23503",
		},
		{
			name: "CheckViolation",
			err:  status.Error(codes.InvalidArgument, "check constraint 'ck_age' failed"),
			want: "23514",
		},
		{
			name: "StringTruncation",
			err:  status.Error(codes.InvalidArgument, "value too long for type character varying(50)"),
			want: "22001",
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
			name: "OutOfRange",
			err:  status.Error(codes.OutOfRange, "numeric value out of range"),
			want: "22003",
		},
		{
			name: "ResourceExhausted",
			err:  status.Error(codes.ResourceExhausted, "quota exceeded"),
			want: "53000",
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
			err:  status.Error(codes.NotFound, "Table not found: users"),
			want: "[SQLSTATE 42P01] rpc error: code = NotFound desc = Table not found: users",
		},
		{
			name: "WrapStandardError",
			err:  errors.New("duplicate key value violates unique constraint"),
			want: "[SQLSTATE 23505] duplicate key value violates unique constraint",
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
