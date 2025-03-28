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

package spannerdriver

import (
	"context"
	"database/sql"
	"reflect"
	"testing"

	"cloud.google.com/go/spanner/apiv1/spannerpb"
	"github.com/googleapis/go-sql-spanner/testutil"
)

func TestBeginTx(t *testing.T) {
	t.Parallel()

	db, server, teardown := setupTestDBConnection(t)
	defer teardown()
	ctx := context.Background()

	tx, _ := db.BeginTx(ctx, &sql.TxOptions{})
	_, _ = tx.ExecContext(ctx, testutil.UpdateBarSetFoo)
	_ = tx.Rollback()

	requests := drainRequestsFromServer(server.TestSpanner)
	beginRequests := requestsOfType(requests, reflect.TypeOf(&spannerpb.BeginTransactionRequest{}))
	if g, w := len(beginRequests), 1; g != w {
		t.Fatalf("begin requests count mismatch\n Got: %v\nWant: %v", g, w)
	}
	request := beginRequests[0].(*spannerpb.BeginTransactionRequest)
	if g, w := request.Options.GetIsolationLevel(), spannerpb.TransactionOptions_ISOLATION_LEVEL_UNSPECIFIED; g != w {
		t.Fatalf("begin isolation level mismatch\n Got: %v\nWant: %v", g, w)
	}
}

func TestBeginTxWithIsolationLevel(t *testing.T) {
	t.Parallel()

	db, server, teardown := setupTestDBConnection(t)
	defer teardown()
	ctx := context.Background()

	for _, level := range []sql.IsolationLevel{
		sql.LevelDefault,
		sql.LevelSnapshot,
		sql.LevelRepeatableRead,
		sql.LevelSerializable,
	} {
		originalLevel := level
		for _, disableRetryAborts := range []bool{true, false} {
			if disableRetryAborts {
				level = WithDisableRetryAborts(originalLevel)
			} else {
				level = originalLevel
			}
			tx, _ := db.BeginTx(ctx, &sql.TxOptions{
				Isolation: level,
			})
			_, _ = tx.ExecContext(ctx, testutil.UpdateBarSetFoo)
			_ = tx.Rollback()

			requests := drainRequestsFromServer(server.TestSpanner)
			beginRequests := requestsOfType(requests, reflect.TypeOf(&spannerpb.BeginTransactionRequest{}))
			if g, w := len(beginRequests), 1; g != w {
				t.Fatalf("begin requests count mismatch\n Got: %v\nWant: %v", g, w)
			}
			request := beginRequests[0].(*spannerpb.BeginTransactionRequest)
			wantIsolationLevel, _ := toProtoIsolationLevel(originalLevel)
			if g, w := request.Options.GetIsolationLevel(), wantIsolationLevel; g != w {
				t.Fatalf("begin isolation level mismatch\n Got: %v\nWant: %v", g, w)
			}
		}
	}
}

func TestBeginTxWithInvalidIsolationLevel(t *testing.T) {
	t.Parallel()

	db, _, teardown := setupTestDBConnection(t)
	defer teardown()
	ctx := context.Background()

	for _, level := range []sql.IsolationLevel{
		sql.LevelReadUncommitted,
		sql.LevelReadCommitted,
		sql.LevelWriteCommitted,
		sql.LevelLinearizable,
	} {
		originalLevel := level
		for _, disableRetryAborts := range []bool{true, false} {
			if disableRetryAborts {
				level = WithDisableRetryAborts(originalLevel)
			} else {
				level = originalLevel
			}
			_, err := db.BeginTx(ctx, &sql.TxOptions{
				Isolation: level,
			})
			if err == nil {
				t.Fatalf("BeginTx should have failed with invalid isolation level: %v", level)
			}
		}
	}
}
