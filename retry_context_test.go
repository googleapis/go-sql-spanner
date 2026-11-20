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
	"context"
	"crypto/sha256"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"cloud.google.com/go/spanner"
	"cloud.google.com/go/spanner/admin/database/apiv1/databasepb"
	spannerpb "cloud.google.com/go/spanner/apiv1/spannerpb"
	"github.com/googleapis/gax-go/v2"
	"github.com/googleapis/go-sql-spanner/connectionstate"
	"github.com/googleapis/go-sql-spanner/testutil"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"
)

type unfiredDeadlineContext struct {
	context.Context
	deadline time.Time
}

func (c unfiredDeadlineContext) Deadline() (time.Time, bool) {
	return c.deadline, true
}

type deadlineErrWithoutDeadlineContext struct {
	context.Context
	done <-chan struct{}
}

func (c deadlineErrWithoutDeadlineContext) Done() <-chan struct{} {
	return c.done
}

func (c deadlineErrWithoutDeadlineContext) Err() error {
	return context.DeadlineExceeded
}

type controllableDeadlineContext struct {
	context.Context
	mu          sync.Mutex
	doneOnce    sync.Once
	done        chan struct{}
	deadline    time.Time
	hasDeadline bool
	err         error
}

func newControllableDeadlineContext(hasDeadline bool) *controllableDeadlineContext {
	var deadline time.Time
	if hasDeadline {
		deadline = time.Now().Add(time.Hour)
	}
	return &controllableDeadlineContext{
		Context:     context.Background(),
		done:        make(chan struct{}),
		deadline:    deadline,
		hasDeadline: hasDeadline,
	}
}

func (c *controllableDeadlineContext) Deadline() (time.Time, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.deadline, c.hasDeadline
}

func (c *controllableDeadlineContext) Done() <-chan struct{} {
	return c.done
}

func (c *controllableDeadlineContext) Err() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.err
}

func (c *controllableDeadlineContext) cancel() {
	c.mu.Lock()
	if c.err == nil {
		c.err = context.Canceled
	}
	c.mu.Unlock()
	c.doneOnce.Do(func() { close(c.done) })
}

func (c *controllableDeadlineContext) expire() {
	c.mu.Lock()
	if c.err == nil {
		c.err = context.DeadlineExceeded
		c.deadline = time.Now().Add(-time.Nanosecond)
		c.hasDeadline = true
	}
	c.mu.Unlock()
	c.doneOnce.Do(func() { close(c.done) })
}

func abortedWithRetryDelay(delay time.Duration) error {
	st := status.New(codes.Aborted, "Aborted")
	st, _ = st.WithDetails(&errdetails.RetryInfo{
		RetryDelay: durationpb.New(delay),
	})
	return st.Err()
}

func assertContextError(t *testing.T, err, wantErr error, wantCode codes.Code) {
	t.Helper()
	if g, w := err, wantErr; g == nil || errors.Is(g, ErrAbortedDueToConcurrentModification) || !errors.Is(g, w) {
		t.Fatalf("error mismatch\nGot:  %v\nWant: %v", g, w)
	}
	if g, w := spanner.ErrCode(err), wantCode; g != w {
		t.Fatalf("error code mismatch\nGot:  %v\nWant: %v", g, w)
	}
}

func assertSingleRollbackRequest(t *testing.T, server *testutil.MockedSpannerInMemTestServer) {
	t.Helper()
	reqs := server.TestSpanner.DrainRequestsFromServer()
	rollbackReqs := testutil.RequestsOfType(reqs, reflect.TypeOf(&spannerpb.RollbackRequest{}))
	if g, w := len(rollbackReqs), 1; g != w {
		t.Errorf("rollback requests count mismatch\nGot:  %v\nWant: %v", g, w)
	}
}

func newFailureContext(parent context.Context, deadline bool) (context.Context, func(), context.CancelFunc) {
	if deadline {
		controllableCtx := newControllableDeadlineContext(true)
		return controllableCtx, controllableCtx.expire, controllableCtx.cancel
	}
	ctx, cancel := context.WithCancel(parent)
	return ctx, cancel, cancel
}

func failRPCOnHook(server *testutil.MockedSpannerInMemTestServer, method string, fail func()) {
	server.TestSpanner.SetMethodHook(method, func(srvCtx context.Context, _ interface{}) error {
		fail()
		<-srvCtx.Done()
		return status.FromContextError(srvCtx.Err()).Err()
	})
}

func createAndRollbackSavepoint(t *testing.T, ctx context.Context, tx *sql.Tx, dml string) {
	t.Helper()
	if _, err := tx.ExecContext(ctx, "SAVEPOINT s1"); err != nil {
		t.Fatalf("SAVEPOINT s1 failed: %v", err)
	}
	if _, err := tx.ExecContext(ctx, dml); err != nil {
		t.Fatalf("DML after savepoint failed: %v", err)
	}
	if _, err := tx.ExecContext(ctx, "ROLLBACK TO SAVEPOINT s1"); err != nil {
		t.Fatalf("ROLLBACK TO SAVEPOINT s1 failed: %v", err)
	}
}

func createAndRollbackConnSavepoint(ctx context.Context, c *conn, dml string) error {
	if _, err := c.ExecContext(ctx, "SAVEPOINT s1", nil); err != nil {
		return fmt.Errorf("SAVEPOINT s1 failed: %w", err)
	}
	if _, err := c.ExecContext(ctx, dml, nil); err != nil {
		return fmt.Errorf("DML after savepoint failed: %w", err)
	}
	if _, err := c.ExecContext(ctx, "ROLLBACK TO SAVEPOINT s1", nil); err != nil {
		return fmt.Errorf("ROLLBACK TO SAVEPOINT s1 failed: %w", err)
	}
	return nil
}

func TestCombineContexts_TrivialCases(t *testing.T) {
	t.Parallel()

	type ctxKey struct{}
	activeCtx, cancelActive := context.WithCancel(context.Background())
	defer cancelActive()
	valueCtx := context.WithValue(context.Background(), ctxKey{}, "val")

	tests := []struct {
		name      string
		primary   context.Context
		secondary context.Context
		wantSame  context.Context
	}{
		{
			name:      "both_nil",
			primary:   nil,
			secondary: nil,
		},
		{
			name:      "nil_primary_active_secondary",
			primary:   nil,
			secondary: activeCtx,
			wantSame:  activeCtx,
		},
		{
			name:      "nil_secondary",
			primary:   activeCtx,
			secondary: nil,
			wantSame:  activeCtx,
		},
		{
			name:      "identical_contexts",
			primary:   activeCtx,
			secondary: activeCtx,
			wantSame:  activeCtx,
		},
		{
			name:      "non_cancellable_secondary",
			primary:   activeCtx,
			secondary: valueCtx,
			wantSame:  activeCtx,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			combined, cleanup := combineContexts(tc.primary, tc.secondary)
			if combined == nil {
				t.Fatal("expected non-nil combined context")
			}
			if tc.wantSame != nil {
				if g, w := combined, tc.wantSame; g != w {
					t.Errorf("combined context mismatch\nGot:  %v\nWant: %v", g, w)
				}
			}
			cleanup()
			if g, w := activeCtx.Err(), error(nil); g != w {
				t.Errorf("parent context error after cleanup mismatch\nGot:  %v\nWant: %v", g, w)
			}
		})
	}
}

func TestCombineContexts_AlreadyDoneSecondary(t *testing.T) {
	t.Parallel()

	type ctxKey struct{}
	cancelledSec, cancelSec := context.WithCancel(context.Background())
	cancelSec()

	expiredSec, cancelExpired := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer cancelExpired()

	closedCh := make(chan struct{})
	close(closedCh)

	tests := []struct {
		name      string
		secondary context.Context
		wantErr   error
	}{
		{
			name:      "already_cancelled_secondary",
			secondary: cancelledSec,
			wantErr:   context.Canceled,
		},
		{
			name:      "already_expired_secondary",
			secondary: expiredSec,
			wantErr:   context.DeadlineExceeded,
		},
		{
			name:      "custom_deadline_err_without_deadline",
			secondary: deadlineErrWithoutDeadlineContext{Context: context.Background(), done: closedCh},
			wantErr:   context.Canceled,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			primary := context.WithValue(context.Background(), ctxKey{}, "keep")
			combined, cleanup := combineContexts(primary, tc.secondary)
			defer cleanup()

			if g, w := combined.Err(), tc.wantErr; !errors.Is(g, w) {
				t.Errorf("error mismatch\nGot:  %v\nWant: %v", g, w)
			}
			if g, w := combined.Value(ctxKey{}), "keep"; g != w {
				t.Errorf("context value mismatch\nGot:  %v\nWant: %v", g, w)
			}
		})
	}
}

func TestCombineContexts_CancelPrimary(t *testing.T) {
	t.Parallel()

	type ctxKey struct{}
	primaryBase, cancelPrimary := context.WithCancel(context.Background())
	primary := context.WithValue(primaryBase, ctxKey{}, "primary-val")
	secondary, cancelSecondary := context.WithCancel(context.Background())
	defer cancelSecondary()

	combined, cleanup := combineContexts(primary, secondary)
	defer cleanup()

	if g, w := combined.Value(ctxKey{}), "primary-val"; g != w {
		t.Errorf("value mismatch\nGot:  %v\nWant: %v", g, w)
	}

	cancelPrimary()
	<-combined.Done()
	if g, w := combined.Err(), context.Canceled; !errors.Is(g, w) {
		t.Errorf("error mismatch\nGot:  %v\nWant: %v", g, w)
	}
	if g, w := secondary.Err(), error(nil); g != w {
		t.Errorf("secondary context error mismatch\nGot:  %v\nWant: %v", g, w)
	}
}

func TestCombineContexts_CancelSecondary(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name               string
		expireWithoutTimer bool
	}{
		{name: "cancelled_secondary"},
		{name: "custom_deadline_err_without_deadline_later", expireWithoutTimer: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			primary, cancelPrimary := context.WithCancel(context.Background())
			defer cancelPrimary()

			var secondary context.Context
			var triggerSecondary func()
			if tc.expireWithoutTimer {
				controllable := newControllableDeadlineContext(false)
				defer controllable.cancel()
				secondary = controllable
				triggerSecondary = controllable.expire
			} else {
				var cancelSecondary context.CancelFunc
				secondary, cancelSecondary = context.WithCancel(context.Background())
				defer cancelSecondary()
				triggerSecondary = cancelSecondary
			}

			combined, cleanup := combineContexts(primary, secondary)
			defer cleanup()

			triggerSecondary()
			<-combined.Done()
			if g, w := combined.Err(), context.Canceled; !errors.Is(g, w) {
				t.Errorf("error mismatch\nGot:  %v\nWant: %v", g, w)
			}
			if g, w := primary.Err(), error(nil); g != w {
				t.Errorf("primary context error mismatch\nGot:  %v\nWant: %v", g, w)
			}
		})
	}
}

func TestCombineContexts_CleanupDoesNotCancelParents(t *testing.T) {
	t.Parallel()

	primary, cancelPrimary := context.WithCancel(context.Background())
	defer cancelPrimary()
	secondary, cancelSecondary := context.WithCancel(context.Background())
	defer cancelSecondary()

	combined, cleanup := combineContexts(primary, secondary)
	cleanup()

	if g, w := combined.Err(), context.Canceled; !errors.Is(g, w) {
		t.Errorf("combined error after cleanup mismatch\nGot:  %v\nWant: %v", g, w)
	}
	if g, w := primary.Err(), error(nil); g != w {
		t.Errorf("primary context error after cleanup mismatch\nGot:  %v\nWant: %v", g, w)
	}
	if g, w := secondary.Err(), error(nil); g != w {
		t.Errorf("secondary context error after cleanup mismatch\nGot:  %v\nWant: %v", g, w)
	}
}

func TestCombineContexts_Deadlines(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		primaryDelay   time.Duration
		secondaryDelay time.Duration
		customNilDone  bool
		wantDelay      time.Duration
	}{
		{
			name:           "earlier_deadline_on_primary",
			primaryDelay:   20 * time.Millisecond,
			secondaryDelay: time.Second,
			wantDelay:      20 * time.Millisecond,
		},
		{
			name:           "earlier_deadline_on_secondary",
			primaryDelay:   time.Second,
			secondaryDelay: 20 * time.Millisecond,
			wantDelay:      20 * time.Millisecond,
		},
		{
			name:           "equal_deadlines_preserve_deadline_exceeded",
			primaryDelay:   20 * time.Millisecond,
			secondaryDelay: 20 * time.Millisecond,
			wantDelay:      20 * time.Millisecond,
		},
		{
			name:           "custom_deadline_context_with_nil_done",
			primaryDelay:   time.Second,
			secondaryDelay: 20 * time.Millisecond,
			customNilDone:  true,
			wantDelay:      20 * time.Millisecond,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				now := time.Now()
				primaryDeadline := now.Add(tc.primaryDelay)
				secondaryDeadline := now.Add(tc.secondaryDelay)
				wantDeadline := now.Add(tc.wantDelay)

				primary, cancelPrimary := context.WithDeadline(context.Background(), primaryDeadline)
				defer cancelPrimary()

				var secondary context.Context
				var cancelSecondary context.CancelFunc
				if tc.customNilDone {
					secondary = unfiredDeadlineContext{Context: context.Background(), deadline: secondaryDeadline}
					cancelSecondary = func() {}
				} else {
					secondary, cancelSecondary = context.WithDeadline(context.Background(), secondaryDeadline)
				}
				defer cancelSecondary()

				combined, cleanup := combineContexts(primary, secondary)
				defer cleanup()

				gotDeadline, ok := combined.Deadline()
				if !ok {
					t.Fatal("expected deadline on combined context")
				}
				if g, w := gotDeadline, wantDeadline; !g.Equal(w) {
					t.Errorf("deadline mismatch\nGot:  %v\nWant: %v", g, w)
				}

				time.Sleep(tc.wantDelay)
				synctest.Wait()
				<-combined.Done()
				if g, w := combined.Err(), context.DeadlineExceeded; !errors.Is(g, w) {
					t.Errorf("error mismatch\nGot:  %v\nWant: %v", g, w)
				}
			})
		})
	}
}

func TestIsContextExpiredOrCancelled(t *testing.T) {
	t.Parallel()

	futureCtx, cancelFuture := context.WithDeadline(context.Background(), time.Now().Add(time.Minute))
	defer cancelFuture()

	cancelledCtx, cancelCancelled := context.WithCancel(context.Background())
	cancelCancelled()

	expiredCtx, cancelExpired := context.WithDeadline(context.Background(), time.Now().Add(-time.Millisecond))
	defer cancelExpired()

	tests := []struct {
		name string
		ctx  context.Context
		want bool
	}{
		{
			name: "nil_context",
			ctx:  nil,
			want: false,
		},
		{
			name: "background_context",
			ctx:  context.Background(),
			want: false,
		},
		{
			name: "future_deadline",
			ctx:  futureCtx,
			want: false,
		},
		{
			name: "cancelled_context",
			ctx:  cancelledCtx,
			want: true,
		},
		{
			name: "expired_deadline",
			ctx:  expiredCtx,
			want: true,
		},
		{
			name: "past_deadline_unfired_timer",
			ctx:  unfiredDeadlineContext{Context: context.Background(), deadline: time.Now().Add(-time.Millisecond)},
			want: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if g, w := isContextExpiredOrCancelled(tc.ctx), tc.want; g != w {
				t.Errorf("isContextExpiredOrCancelled mismatch\nGot:  %v\nWant: %v", g, w)
			}
		})
	}
}

func TestIsContextCancelledOrTimedOut(t *testing.T) {
	t.Parallel()

	cancelledCtx, cancel := context.WithCancel(context.Background())
	cancel()

	expiredCtx, cancelExpired := context.WithDeadline(context.Background(), time.Now().Add(-time.Millisecond))
	defer cancelExpired()

	unfiredExpiredCtx := unfiredDeadlineContext{
		Context:  context.Background(),
		deadline: time.Now().Add(-time.Millisecond),
	}

	tests := []struct {
		name string
		ctx  context.Context
		err  error
		want bool
	}{
		{
			name: "nil_ctx",
			ctx:  nil,
			err:  context.Canceled,
			want: false,
		},
		{
			name: "nil_err",
			ctx:  cancelledCtx,
			err:  nil,
			want: false,
		},
		{
			name: "active_ctx_with_canceled_err",
			ctx:  context.Background(),
			err:  context.Canceled,
			want: false,
		},
		{
			name: "active_ctx_with_deadline_exceeded_err",
			ctx:  context.Background(),
			err:  spanner.ToSpannerError(status.Error(codes.DeadlineExceeded, "deadline")),
			want: false,
		},
		{
			name: "cancelled_ctx_with_context_canceled",
			ctx:  cancelledCtx,
			err:  context.Canceled,
			want: true,
		},
		{
			name: "cancelled_ctx_with_spanner_canceled",
			ctx:  cancelledCtx,
			err:  spanner.ToSpannerError(status.Error(codes.Canceled, "cancelled")),
			want: true,
		},
		{
			name: "expired_ctx_with_context_deadline_exceeded",
			ctx:  expiredCtx,
			err:  context.DeadlineExceeded,
			want: true,
		},
		{
			name: "expired_ctx_with_spanner_deadline_exceeded",
			ctx:  expiredCtx,
			err:  spanner.ToSpannerError(status.Error(codes.DeadlineExceeded, "deadline")),
			want: true,
		},
		{
			name: "unfired_timer_past_deadline_with_spanner_deadline_exceeded",
			ctx:  unfiredExpiredCtx,
			err:  spanner.ToSpannerError(status.Error(codes.DeadlineExceeded, "deadline")),
			want: true,
		},
		{
			name: "cancelled_ctx_with_concurrent_modification",
			ctx:  cancelledCtx,
			err:  ErrAbortedDueToConcurrentModification,
			want: false,
		},
		{
			name: "cancelled_ctx_with_invalid_argument",
			ctx:  cancelledCtx,
			err:  spanner.ToSpannerError(status.Error(codes.InvalidArgument, "invalid")),
			want: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if g, w := isContextCancelledOrTimedOut(tc.ctx, tc.err), tc.want; g != w {
				t.Errorf("isContextCancelledOrTimedOut mismatch\nGot:  %v\nWant: %v", g, w)
			}
		})
	}
}

func TestChecksumRowIterator_RetryQueryContext(t *testing.T) {
	t.Parallel()

	var nilCtx context.Context
	stoppedIt := &checksumRowIterator{stopped: true}
	retryCtx := context.Background()
	queryCtx, stopWatch, iterCancel := stoppedIt.retryQueryContext(retryCtx)
	if g, w := queryCtx, retryCtx; g != w {
		t.Errorf("queryCtx mismatch\nGot:  %v\nWant: %v", g, w)
	}
	if g, w := stopWatch(), true; g != w {
		t.Errorf("stopWatch mismatch\nGot:  %v\nWant: %v", g, w)
	}
	iterCancel()

	nilStoppedCtx, nilStoppedStopWatch, nilStoppedCancel := stoppedIt.retryQueryContext(nilCtx)
	if g, w := nilStoppedCtx, context.Background(); g != w {
		t.Errorf("stopped nil retryCtx mismatch\nGot:  %v\nWant: %v", g, w)
	}
	if g, w := nilStoppedStopWatch(), true; g != w {
		t.Errorf("stopped nil stopWatch mismatch\nGot:  %v\nWant: %v", g, w)
	}
	nilStoppedCancel()

	openIt := &checksumRowIterator{}
	openQueryCtx, openStopWatch, openCancel := openIt.retryQueryContext(nilCtx)
	defer openCancel()
	if g, w := openStopWatch(), true; g != w {
		t.Errorf("open stopWatch mismatch\nGot:  %v\nWant: %v", g, w)
	}
	if g, w := openQueryCtx.Err(), error(nil); g != w {
		t.Errorf("openQueryCtx.Err mismatch\nGot:  %v\nWant: %v", g, w)
	}
	openCancel()
	if g, w := openQueryCtx.Err(), context.Canceled; !errors.Is(g, w) {
		t.Errorf("openQueryCtx.Err after cancel mismatch\nGot:  %v\nWant: %v", g, w)
	}
}

func TestRetryContextError(t *testing.T) {
	t.Parallel()

	cancelledCtx, cancel := context.WithCancel(context.Background())
	cancel()
	expiredCtx, cancelExpired := context.WithDeadline(context.Background(), time.Now().Add(-time.Millisecond))
	defer cancelExpired()
	unfiredExpired := unfiredDeadlineContext{Context: context.Background(), deadline: time.Now().Add(-time.Millisecond)}

	tests := []struct {
		name        string
		ctx         context.Context
		fallbackCtx context.Context
		err         error
		wantErr     error
		wantCode    codes.Code
	}{
		{
			name:     "cancelled_ctx_nil_err",
			ctx:      cancelledCtx,
			err:      nil,
			wantErr:  context.Canceled,
			wantCode: codes.Canceled,
		},
		{
			name:     "expired_ctx_with_canceled_err",
			ctx:      expiredCtx,
			err:      context.Canceled,
			wantErr:  context.DeadlineExceeded,
			wantCode: codes.DeadlineExceeded,
		},
		{
			name:        "healthy_ctx_expired_fallback_with_spanner_err",
			ctx:         context.Background(),
			fallbackCtx: expiredCtx,
			err:         spanner.ToSpannerError(status.Error(codes.DeadlineExceeded, "deadline")),
			wantErr:     context.DeadlineExceeded,
			wantCode:    codes.DeadlineExceeded,
		},
		{
			name:     "expired_ctx_with_already_joined_err",
			ctx:      expiredCtx,
			err:      errors.Join(spanner.ToSpannerError(status.Error(codes.DeadlineExceeded, "deadline")), context.DeadlineExceeded),
			wantErr:  context.DeadlineExceeded,
			wantCode: codes.DeadlineExceeded,
		},
		{
			name:     "healthy_ctx_and_fallback_with_err",
			ctx:      context.Background(),
			err:      context.DeadlineExceeded,
			wantErr:  context.DeadlineExceeded,
			wantCode: codes.Unknown,
		},
		{
			name:     "unfired_expired_ctx_nil_err",
			ctx:      unfiredExpired,
			err:      nil,
			wantErr:  context.DeadlineExceeded,
			wantCode: codes.DeadlineExceeded,
		},
		{
			name:     "nil_ctx_nil_err",
			ctx:      nil,
			err:      nil,
			wantErr:  context.Canceled,
			wantCode: codes.Unknown,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if g, w := retryContextError(tc.ctx, tc.fallbackCtx, tc.err), tc.wantErr; !errors.Is(g, w) {
				t.Errorf("error mismatch\nGot:  %v\nWant: %v", g, w)
			}
			if g, w := spanner.ErrCode(retryContextError(tc.ctx, tc.fallbackCtx, tc.err)), tc.wantCode; g != w {
				t.Errorf("spanner.ErrCode mismatch\nGot:  %v\nWant: %v", g, w)
			}
		})
	}
}

func TestChecksumRowIterator_Retry_PreFlightContextErrors(t *testing.T) {
	t.Parallel()

	db, _, teardown := setupTestDBConnection(t)
	defer teardown()
	ctx := context.Background()

	dbConn, err := db.Conn(ctx)
	if err != nil {
		t.Fatalf("db.Conn failed: %v", err)
	}
	defer silentClose(dbConn)

	tx, err := dbConn.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		t.Fatalf("BeginTx failed: %v", err)
	}
	defer func() { _ = tx.Rollback() }()

	if _, err := tx.ExecContext(ctx, testutil.UpdateBarSetFoo); err != nil {
		t.Fatalf("DML failed: %v", err)
	}

	cancelledCtx, cancel := context.WithCancel(ctx)
	cancel()

	var openRetryErr, stoppedRetryErr, openItCtxErr, stoppedItCtxErr error
	if err := dbConn.Raw(func(driverConn any) error {
		c := driverConn.(*conn)
		rwTx := c.tx.contextTransaction.(*readWriteTransaction).rwTx
		openIt := &checksumRowIterator{
			ctx:  ctx,
			stmt: spanner.NewStatement(testutil.SelectFooFromBar),
			hash: sha256.New(),
			nc:   0,
		}
		openRetryErr = openIt.retry(cancelledCtx, rwTx)

		stoppedIt := &checksumRowIterator{
			ctx:     ctx,
			stmt:    spanner.NewStatement(testutil.SelectFooFromBar),
			hash:    sha256.New(),
			nc:      0,
			stopped: true,
		}
		stoppedRetryErr = stoppedIt.retry(cancelledCtx, rwTx)

		openExpiredIt := &checksumRowIterator{
			ctx:  cancelledCtx,
			stmt: spanner.NewStatement(testutil.SelectFooFromBar),
			hash: sha256.New(),
			nc:   0,
		}
		openItCtxErr = openExpiredIt.retry(ctx, rwTx)

		stoppedExpiredIt := &checksumRowIterator{
			ctx:     cancelledCtx,
			stmt:    spanner.NewStatement(testutil.SelectFooFromBar),
			hash:    sha256.New(),
			nc:      0,
			stopped: true,
		}
		stoppedItCtxErr = stoppedExpiredIt.retry(ctx, rwTx)
		return nil
	}); err != nil {
		t.Fatalf("dbConn.Raw failed: %v", err)
	}
	assertContextError(t, openRetryErr, context.Canceled, codes.Canceled)
	assertContextError(t, stoppedRetryErr, context.Canceled, codes.Canceled)
	assertContextError(t, openItCtxErr, context.Canceled, codes.Canceled)
	if stoppedItCtxErr != nil {
		t.Errorf("stopped iterator retry with expired it.ctx mismatch\nGot:  %v\nWant: nil", stoppedItCtxErr)
	}
}

func TestChecksumRowIterator_ReplaceIterator_ContextErrors(t *testing.T) {
	t.Parallel()

	db, _, teardown := setupTestDBConnection(t)
	defer teardown()
	bg := context.Background()

	dbConn, err := db.Conn(bg)
	if err != nil {
		t.Fatalf("db.Conn failed: %v", err)
	}
	defer silentClose(dbConn)

	tx, err := dbConn.BeginTx(bg, &sql.TxOptions{})
	if err != nil {
		t.Fatalf("BeginTx failed: %v", err)
	}
	defer func() { _ = tx.Rollback() }()

	if _, err := tx.ExecContext(bg, testutil.UpdateBarSetFoo); err != nil {
		t.Fatalf("DML failed: %v", err)
	}

	type replaceMode int
	const (
		modeStopWatchFired replaceMode = iota
		modeRetryCtxExpired
		modeItCtxExpired
		modeStoppedWithItCtxExpired
	)

	tests := []struct {
		name     string
		mode     replaceMode
		wantErr  error
		wantCode codes.Code
	}{
		{
			name:     "open_stopWatch_already_fired",
			mode:     modeStopWatchFired,
			wantErr:  context.Canceled,
			wantCode: codes.Canceled,
		},
		{
			name:     "open_retry_ctx_deadline_exceeded",
			mode:     modeRetryCtxExpired,
			wantErr:  context.DeadlineExceeded,
			wantCode: codes.DeadlineExceeded,
		},
		{
			name:     "open_it_ctx_deadline_exceeded",
			mode:     modeItCtxExpired,
			wantErr:  context.DeadlineExceeded,
			wantCode: codes.DeadlineExceeded,
		},
		{
			name: "stopped_with_expired_it_ctx_succeeds",
			mode: modeStoppedWithItCtxExpired,
		},
	}

	if err := dbConn.Raw(func(driverConn any) error {
		c := driverConn.(*conn)
		rwTx := c.tx.contextTransaction.(*readWriteTransaction).rwTx
		for _, tc := range tests {
			t.Run(tc.name, func(t *testing.T) {
				unfiredExpired := unfiredDeadlineContext{
					Context:  bg,
					deadline: time.Now().Add(-time.Millisecond),
				}
				retryCtx := bg
				itCtx := bg
				stopped := false
				var cancelRetry context.CancelFunc
				switch tc.mode {
				case modeStopWatchFired:
					retryCtx, cancelRetry = context.WithCancel(bg)
					defer cancelRetry()
				case modeRetryCtxExpired:
					retryCtx = unfiredExpired
				case modeItCtxExpired:
					itCtx = unfiredExpired
				case modeStoppedWithItCtxExpired:
					itCtx = unfiredExpired
					stopped = true
				}

				it := &checksumRowIterator{
					ctx:         itCtx,
					RowIterator: rwTx.QueryWithOptions(bg, spanner.NewStatement(testutil.SelectFooFromBar), spanner.QueryOptions{}),
					stmt:        spanner.NewStatement(testutil.SelectFooFromBar),
					stopped:     stopped,
				}
				defer it.Stop()

				queryCtx, stopWatch, iterCancel := it.retryQueryContext(retryCtx)
				retryIt := rwTx.QueryWithOptions(queryCtx, it.stmt, spanner.QueryOptions{})

				if tc.mode == modeStopWatchFired {
					// Cancel retryCtx and wait for the AfterFunc callback to cancel queryCtx,
					// guaranteeing stopWatch() returns false before replaceIterator runs.
					cancelRetry()
					<-queryCtx.Done()
				}
				gotErr := it.replaceIterator(retryCtx, queryCtx, retryIt, stopWatch, iterCancel)
				if gotErr != nil {
					stopWatch()
					retryIt.Stop()
					iterCancel()
				}
				if tc.wantErr != nil {
					assertContextError(t, gotErr, tc.wantErr, tc.wantCode)
				} else if gotErr != nil {
					t.Fatalf("unexpected error: %v", gotErr)
				}
			})
		}
		return nil
	}); err != nil {
		t.Fatalf("dbConn.Raw failed: %v", err)
	}
}

func TestSavepoint_EnsureReplayed_UnfiredDeadlinePreFlight(t *testing.T) {
	t.Parallel()

	expiredUnfired := unfiredDeadlineContext{
		Context:  context.Background(),
		deadline: time.Now().Add(-time.Millisecond),
	}

	txWithExpiredTxCtx := &readWriteTransaction{
		ctx:    expiredUnfired,
		state:  readWriteTxStateNeedsReplay,
		conn:   &conn{},
		logger: noopLogger,
	}
	if g, w := txWithExpiredTxCtx.ensureReplayed(context.Background()), context.DeadlineExceeded; !errors.Is(g, w) {
		t.Errorf("tx.ctx pre-flight error mismatch\nGot:  %v\nWant: %v", g, w)
	}

	txWithExpiredOpCtx := &readWriteTransaction{
		ctx:    context.Background(),
		state:  readWriteTxStateNeedsReplay,
		conn:   &conn{},
		logger: noopLogger,
	}
	if g, w := txWithExpiredOpCtx.ensureReplayed(expiredUnfired), context.DeadlineExceeded; !errors.Is(g, w) {
		t.Errorf("opCtx pre-flight error mismatch\nGot:  %v\nWant: %v", g, w)
	}
}

func TestSavepoint_EnsureReplayed_OpContextTimeoutRecovers(t *testing.T) {
	t.Parallel()

	type replayKind string
	const (
		replayDML      replayKind = "DML"
		replayBatchDML replayKind = "BatchDML"
		replayQuery    replayKind = "Query"
	)

	tests := []struct {
		name         string
		kind         replayKind
		cancel       bool
		directCommit bool
		queryTrigger bool
		multiStmt    bool
	}{
		{name: "dml_op_timeout", kind: replayDML},
		{name: "multi_stmt_dml_op_timeout", kind: replayDML, multiStmt: true},
		{name: "batch_dml_op_timeout", kind: replayBatchDML},
		{name: "query_op_timeout", kind: replayQuery},
		{name: "dml_op_cancelled", kind: replayDML, cancel: true},
		{name: "dml_op_timeout_then_commit", kind: replayDML, directCommit: true},
		{name: "dml_op_timeout_on_query_trigger", kind: replayDML, queryTrigger: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			db, server, teardown := setupTestDBConnection(t)
			defer teardown()
			ctx := context.Background()

			dbConn, err := db.Conn(ctx)
			if err != nil {
				t.Fatalf("db.Conn failed: %v", err)
			}
			defer silentClose(dbConn)

			tx, err := dbConn.BeginTx(ctx, &sql.TxOptions{})
			if err != nil {
				t.Fatalf("BeginTx failed: %v", err)
			}
			defer func() { _ = tx.Rollback() }()

			switch tc.kind {
			case replayDML:
				if _, err := tx.ExecContext(ctx, testutil.UpdateBarSetFoo); err != nil {
					t.Fatalf("initial DML failed: %v", err)
				}
				if tc.multiStmt {
					if _, err := tx.ExecContext(ctx, testutil.UpdateBarSetFoo); err != nil {
						t.Fatalf("second initial DML failed: %v", err)
					}
				}
			case replayBatchDML:
				if _, err := tx.ExecContext(ctx, "START BATCH DML"); err != nil {
					t.Fatalf("START BATCH DML failed: %v", err)
				}
				if _, err := tx.ExecContext(ctx, testutil.UpdateBarSetFoo); err != nil {
					t.Fatalf("batch DML statement failed: %v", err)
				}
				if _, err := tx.ExecContext(ctx, "RUN BATCH"); err != nil {
					t.Fatalf("RUN BATCH failed: %v", err)
				}
			case replayQuery:
				rows, err := tx.QueryContext(ctx, testutil.SelectFooFromBar)
				if err != nil {
					t.Fatalf("QueryContext failed: %v", err)
				}
				for rows.Next() {
				}
				if err := rows.Err(); err != nil {
					t.Fatalf("rows.Err failed: %v", err)
				}
				if err := rows.Close(); err != nil {
					t.Fatalf("rows.Close failed: %v", err)
				}
			}

			createAndRollbackSavepoint(t, ctx, tx, testutil.UpdateSingersSetLastName)
			_ = server.TestSpanner.DrainRequestsFromServer()

			method := testutil.MethodExecuteStreamingSql
			if tc.kind == replayBatchDML {
				method = testutil.MethodExecuteBatchDml
			}
			opCtx, failOp, cleanupOp := newFailureContext(ctx, !tc.cancel)
			defer cleanupOp()
			if tc.multiStmt {
				// Let statement 1 succeed (creating a backend tx ID via inline begin),
				// then expire opCtx on statement 2 so recreateAndReplay rolls back the partial tx.
				var calls atomic.Int32
				server.TestSpanner.SetMethodHook(method, func(srvCtx context.Context, _ interface{}) error {
					if calls.Add(1) == 1 {
						return nil
					}
					failOp()
					<-srvCtx.Done()
					return status.FromContextError(srvCtx.Err()).Err()
				})
			} else {
				failRPCOnHook(server, method, failOp)
			}

			if tc.queryTrigger {
				var rows *sql.Rows
				rows, err = tx.QueryContext(opCtx, testutil.SelectFooFromBar)
				if rows != nil {
					_ = rows.Close()
				}
			} else {
				_, err = tx.ExecContext(opCtx, testutil.UpdateSingersSetLastName)
			}
			server.TestSpanner.SetMethodHook(method, nil)
			if tc.cancel {
				assertContextError(t, err, context.Canceled, codes.Canceled)
			} else {
				assertContextError(t, err, context.DeadlineExceeded, codes.DeadlineExceeded)
			}

			// Verify transaction remains in NeedsReplay state and did not get poisoned to ReplayFailed.
			var gotState readWriteTxState
			var gotRwTx *spanner.ReadWriteStmtBasedTransaction
			if err := dbConn.Raw(func(driverConn any) error {
				c := driverConn.(*conn)
				rwTx := c.tx.contextTransaction.(*readWriteTransaction)
				gotState = rwTx.state
				gotRwTx = rwTx.rwTx
				return nil
			}); err != nil {
				t.Fatalf("dbConn.Raw failed: %v", err)
			}
			if g, w := gotState, readWriteTxStateNeedsReplay; g != w {
				t.Errorf("tx.state mismatch\nGot:  %v\nWant: %v", g, w)
			}
			if g, w := gotRwTx, (*spanner.ReadWriteStmtBasedTransaction)(nil); g != w {
				t.Errorf("rwTx after failed replay mismatch\nGot:  %v\nWant: %v", g, w)
			}

			failedReqs := server.TestSpanner.DrainRequestsFromServer()
			if tc.multiStmt {
				rollbackReqs := testutil.RequestsOfType(failedReqs, reflect.TypeOf(&spannerpb.RollbackRequest{}))
				if g, w := len(rollbackReqs), 1; g != w {
					t.Errorf("partial replay RollbackRequest count mismatch\nGot:  %v\nWant: %v", g, w)
				}
			}

			if !tc.directCommit {
				if _, err := tx.ExecContext(ctx, testutil.UpdateSingersSetLastName); err != nil {
					t.Fatalf("recovery statement failed: %v", err)
				}
			}
			if err := tx.Commit(); err != nil {
				t.Fatalf("Commit failed after recovery: %v", err)
			}

			reqs := server.TestSpanner.DrainRequestsFromServer()
			execReqs := testutil.RequestsOfType(reqs, reflect.TypeOf(&spannerpb.ExecuteSqlRequest{}))
			batchReqs := testutil.RequestsOfType(reqs, reflect.TypeOf(&spannerpb.ExecuteBatchDmlRequest{}))
			switch tc.kind {
			case replayDML:
				wantSQL := []string{testutil.UpdateBarSetFoo}
				if tc.multiStmt {
					wantSQL = append(wantSQL, testutil.UpdateBarSetFoo)
				}
				if !tc.directCommit {
					wantSQL = append(wantSQL, testutil.UpdateSingersSetLastName)
				}
				if g, w := len(execReqs), len(wantSQL); g != w {
					t.Fatalf("recovery ExecuteSqlRequest count mismatch\nGot:  %v\nWant: %v", g, w)
				}
				for i, sqlText := range wantSQL {
					if g, w := execReqs[i].(*spannerpb.ExecuteSqlRequest).Sql, sqlText; g != w {
						t.Errorf("recovery SQL[%d] mismatch\nGot:  %v\nWant: %v", i, g, w)
					}
				}
			case replayBatchDML:
				if g, w := len(batchReqs), 1; g != w {
					t.Fatalf("recovery ExecuteBatchDmlRequest count mismatch\nGot:  %v\nWant: %v", g, w)
				}
				if g, w := len(execReqs), 1; g != w {
					t.Fatalf("recovery ExecuteSqlRequest count mismatch\nGot:  %v\nWant: %v", g, w)
				}
				if g, w := execReqs[0].(*spannerpb.ExecuteSqlRequest).Sql, testutil.UpdateSingersSetLastName; g != w {
					t.Errorf("recovery SQL mismatch\nGot:  %v\nWant: %v", g, w)
				}
			case replayQuery:
				if g, w := len(execReqs), 2; g != w {
					t.Fatalf("recovery ExecuteSqlRequest count mismatch\nGot:  %v\nWant: %v", g, w)
				}
				if g, w := execReqs[0].(*spannerpb.ExecuteSqlRequest).Sql, testutil.SelectFooFromBar; g != w {
					t.Errorf("recovery SQL[0] mismatch\nGot:  %v\nWant: %v", g, w)
				}
				if g, w := execReqs[1].(*spannerpb.ExecuteSqlRequest).Sql, testutil.UpdateSingersSetLastName; g != w {
					t.Errorf("recovery SQL[1] mismatch\nGot:  %v\nWant: %v", g, w)
				}
			}
		})
	}
}

func TestSavepoint_EnsureReplayed_BeginTransactionTimeout(t *testing.T) {
	t.Parallel()

	db, server, teardown := setupTestDBConnectionWithParams(t, "beginTransactionOption=ExplicitBeginTransaction")
	defer teardown()
	ctx := context.Background()

	dbConn, err := db.Conn(ctx)
	if err != nil {
		t.Fatalf("db.Conn failed: %v", err)
	}
	defer silentClose(dbConn)

	tx, err := dbConn.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		t.Fatalf("BeginTx failed: %v", err)
	}
	defer func() { _ = tx.Rollback() }()

	if _, err := tx.ExecContext(ctx, testutil.UpdateBarSetFoo); err != nil {
		t.Fatalf("initial DML failed: %v", err)
	}
	createAndRollbackSavepoint(t, ctx, tx, testutil.UpdateSingersSetLastName)

	opCtx := newControllableDeadlineContext(true)
	defer opCtx.cancel()
	failRPCOnHook(server, testutil.MethodBeginTransaction, opCtx.expire)

	_, err = tx.ExecContext(opCtx, testutil.UpdateSingersSetLastName)
	server.TestSpanner.SetMethodHook(testutil.MethodBeginTransaction, nil)
	assertContextError(t, err, context.DeadlineExceeded, codes.DeadlineExceeded)

	if _, err := tx.ExecContext(ctx, testutil.UpdateSingersSetLastName); err != nil {
		t.Fatalf("recovery statement failed: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("Commit failed after recovery: %v", err)
	}
}

func TestSavepoint_EnsureReplayed_TxContextCancelledMidReplay(t *testing.T) {
	t.Parallel()

	db, server, teardown := setupTestDBConnection(t)
	defer teardown()

	dbConn, err := db.Conn(context.Background())
	if err != nil {
		t.Fatalf("db.Conn failed: %v", err)
	}
	defer silentClose(dbConn)

	txCtx, cancelTx := context.WithCancel(context.Background())
	defer cancelTx()

	var firstErr, secondErr error
	var gotState readWriteTxState
	var gotRwTx *spanner.ReadWriteStmtBasedTransaction
	var execReqsAfter int
	if err := dbConn.Raw(func(driverConn any) error {
		c := driverConn.(*conn)
		dtx, err := c.BeginTx(txCtx, driver.TxOptions{})
		if err != nil {
			return err
		}
		defer func() { _ = dtx.Rollback() }()

		if _, err := c.ExecContext(context.Background(), testutil.UpdateBarSetFoo, nil); err != nil {
			return fmt.Errorf("initial DML failed: %w", err)
		}
		if err := createAndRollbackConnSavepoint(context.Background(), c, testutil.UpdateSingersSetLastName); err != nil {
			return err
		}
		_ = server.TestSpanner.DrainRequestsFromServer()

		// Cancel txCtx deterministically mid-replay while opCtx is context.Background().
		failRPCOnHook(server, testutil.MethodExecuteStreamingSql, cancelTx)

		_, firstErr = c.ExecContext(context.Background(), testutil.UpdateSingersSetLastName, nil)
		server.TestSpanner.SetMethodHook(testutil.MethodExecuteStreamingSql, nil)

		rwTx := c.tx.contextTransaction.(*readWriteTransaction)
		gotState = rwTx.state
		gotRwTx = rwTx.rwTx

		_ = server.TestSpanner.DrainRequestsFromServer()

		// Subsequent statement with an active opCtx must fail immediately via pre-flight check without RPCs
		// and return context.Canceled (not 25P02).
		_, secondErr = c.ExecContext(context.Background(), testutil.UpdateSingersSetLastName, nil)
		requestsAfter := server.TestSpanner.DrainRequestsFromServer()
		execReqsAfter = len(testutil.RequestsOfType(requestsAfter, reflect.TypeOf(&spannerpb.ExecuteSqlRequest{})))
		return nil
	}); err != nil {
		t.Fatalf("dbConn.Raw failed: %v", err)
	}

	assertContextError(t, firstErr, context.Canceled, codes.Canceled)
	if g, w := gotState, readWriteTxStateNeedsReplay; g != w {
		t.Errorf("tx.state mismatch\nGot:  %v\nWant: %v", g, w)
	}
	if g, w := gotRwTx, (*spanner.ReadWriteStmtBasedTransaction)(nil); g != w {
		t.Errorf("rwTx after failed replay mismatch\nGot:  %v\nWant: %v", g, w)
	}
	assertContextError(t, secondErr, context.Canceled, codes.Canceled)
	if g, w := execReqsAfter, 0; g != w {
		t.Errorf("execute requests count mismatch\nGot:  %v\nWant: %v", g, w)
	}
}

func TestSavepoint_OpenRowIteratorSurvivesReplayAndExecContext(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name               string
		useTxTimeout       bool
		useTxCancelContext bool
	}{
		{name: "tx_with_cancel", useTxCancelContext: true},
		{name: "tx_with_transaction_timeout", useTxTimeout: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			db, server, teardown := setupTestDBConnection(t)
			defer teardown()

			const querySQL = "SELECT FOO FROM BAR_MULTI"
			_ = server.TestSpanner.PutStatementResult(querySQL, &testutil.StatementResult{
				Type:      testutil.StatementResultResultSet,
				ResultSet: testutil.CreateSingleColumnInt64ResultSet([]int64{10, 20, 30, 40}, "FOO"),
			})

			ctx := context.Background()
			dbConn, err := db.Conn(ctx)
			if err != nil {
				t.Fatalf("db.Conn failed: %v", err)
			}
			defer silentClose(dbConn)

			if tc.useTxTimeout {
				if _, err := dbConn.ExecContext(ctx, "SET transaction_timeout = '3600s'"); err != nil {
					t.Fatalf("SET transaction_timeout failed: %v", err)
				}
			}

			txCtx := ctx
			var cancelTx context.CancelFunc
			if tc.useTxCancelContext {
				txCtx, cancelTx = context.WithCancel(ctx)
				defer cancelTx()
			}

			tx, err := dbConn.BeginTx(txCtx, &sql.TxOptions{})
			if err != nil {
				t.Fatalf("BeginTx failed: %v", err)
			}
			defer func() { _ = tx.Rollback() }()

			rows, err := tx.QueryContext(ctx, querySQL)
			if err != nil {
				t.Fatalf("QueryContext failed: %v", err)
			}
			defer func() { _ = rows.Close() }()

			if !rows.Next() {
				t.Fatalf("expected first row, err=%v", rows.Err())
			}
			var val int64
			if err := rows.Scan(&val); err != nil {
				t.Fatalf("Scan failed: %v", err)
			}
			if g, w := val, int64(10); g != w {
				t.Fatalf("first row mismatch\nGot:  %v\nWant: %v", g, w)
			}

			createAndRollbackSavepoint(t, ctx, tx, testutil.UpdateBarSetFoo)

			// Trigger ensureReplayed via ExecContext with a short-lived opCtx that gets cancelled when ExecContext returns.
			opCtx := newControllableDeadlineContext(true)
			defer opCtx.cancel()
			if _, err := tx.ExecContext(opCtx, testutil.UpdateSingersSetLastName); err != nil {
				t.Fatalf("ExecContext after rollback to savepoint failed: %v", err)
			}
			opCtx.cancel()

			// Continue streaming the remaining rows from the open iterator.
			var remaining []int64
			for rows.Next() {
				var v int64
				if err := rows.Scan(&v); err != nil {
					t.Fatalf("Scan remaining row failed: %v", err)
				}
				remaining = append(remaining, v)
			}
			if err := rows.Err(); err != nil {
				t.Fatalf("rows.Err after replay failed: %v", err)
			}
			if g, w := remaining, []int64{20, 30, 40}; !reflect.DeepEqual(g, w) {
				t.Fatalf("remaining rows mismatch\nGot:  %v\nWant: %v", g, w)
			}
			if err := rows.Close(); err != nil {
				t.Fatalf("rows.Close failed: %v", err)
			}
			if err := tx.Commit(); err != nil {
				t.Fatalf("Commit failed: %v", err)
			}
		})
	}
}

func TestSavepoint_OpenRowIterator_OpContextDeadlineExceededPreservesStream(t *testing.T) {
	t.Parallel()

	db, server, teardown := setupTestDBConnection(t)
	defer teardown()

	const querySQL = "SELECT FOO FROM BAR_MULTI_TIMEOUT"
	_ = server.TestSpanner.PutStatementResult(querySQL, &testutil.StatementResult{
		Type:      testutil.StatementResultResultSet,
		ResultSet: testutil.CreateSingleColumnInt64ResultSet([]int64{10, 20, 30}, "FOO"),
	})

	ctx := context.Background()
	tx, err := db.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		t.Fatalf("BeginTx failed: %v", err)
	}
	defer func() { _ = tx.Rollback() }()

	// Keep the iterator open on a healthy context.Background() query context.
	rows, err := tx.QueryContext(ctx, querySQL)
	if err != nil {
		t.Fatalf("QueryContext failed: %v", err)
	}
	defer func() { _ = rows.Close() }()

	if !rows.Next() {
		t.Fatalf("expected first row, err=%v", rows.Err())
	}

	createAndRollbackSavepoint(t, ctx, tx, testutil.UpdateBarSetFoo)

	// Expire opCtx deterministically during replay of the open query.
	// checksumRowIterator.retry's stopWatch cancels iterCtx with Canceled, which retryContextError
	// must translate back to DeadlineExceeded.
	opCtx := newControllableDeadlineContext(true)
	defer opCtx.cancel()
	failRPCOnHook(server, testutil.MethodExecuteStreamingSql, opCtx.expire)

	_, err = tx.ExecContext(opCtx, testutil.UpdateSingersSetLastName)
	server.TestSpanner.SetMethodHook(testutil.MethodExecuteStreamingSql, nil)
	assertContextError(t, err, context.DeadlineExceeded, codes.DeadlineExceeded)

	// Subsequent statement with healthy context recovers and keeps the open stream alive.
	if _, err := tx.ExecContext(ctx, testutil.UpdateSingersSetLastName); err != nil {
		t.Fatalf("recovery ExecContext failed: %v", err)
	}
	var remaining []int64
	for rows.Next() {
		var v int64
		if err := rows.Scan(&v); err != nil {
			t.Fatalf("Scan failed: %v", err)
		}
		remaining = append(remaining, v)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows.Err failed: %v", err)
	}
	if g, w := remaining, []int64{20, 30}; !reflect.DeepEqual(g, w) {
		t.Fatalf("remaining rows mismatch\nGot:  %v\nWant: %v", g, w)
	}
	if err := rows.Close(); err != nil {
		t.Fatalf("rows.Close failed: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("Commit failed: %v", err)
	}
}

func TestSavepoint_OpenRowIterator_QueryContextFailureFailsReplay(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		timeout   bool
		midReplay bool
		wantErr   error
		wantCode  codes.Code
	}{
		{
			name:     "query_ctx_cancelled_preflight",
			wantErr:  context.Canceled,
			wantCode: codes.Canceled,
		},
		{
			name:     "query_ctx_deadline_exceeded_preflight",
			timeout:  true,
			wantErr:  context.DeadlineExceeded,
			wantCode: codes.DeadlineExceeded,
		},
		{
			name:      "query_ctx_cancelled_mid_replay",
			midReplay: true,
			wantErr:   context.Canceled,
			wantCode:  codes.Canceled,
		},
		{
			name:      "query_ctx_deadline_exceeded_mid_replay",
			timeout:   true,
			midReplay: true,
			wantErr:   context.DeadlineExceeded,
			wantCode:  codes.DeadlineExceeded,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			db, server, teardown := setupTestDBConnection(t)
			defer teardown()

			const querySQL = "SELECT FOO FROM BAR_QUERY_FAIL"
			_ = server.TestSpanner.PutStatementResult(querySQL, &testutil.StatementResult{
				Type:      testutil.StatementResultResultSet,
				ResultSet: testutil.CreateSingleColumnInt64ResultSet([]int64{10, 20}, "FOO"),
			})

			ctx := context.Background()
			dbConn, err := db.Conn(ctx)
			if err != nil {
				t.Fatalf("db.Conn failed: %v", err)
			}
			defer silentClose(dbConn)

			queryCtx, failQueryCtx, cleanupQueryCtx := newFailureContext(ctx, tc.timeout)
			defer cleanupQueryCtx()

			var execErr, recoveryExecErr, recoveryCommitErr error
			var gotState readWriteTxState
			var gotRwTx *spanner.ReadWriteStmtBasedTransaction
			var execReqsOnPreflight int
			if err := dbConn.Raw(func(driverConn any) error {
				c := driverConn.(*conn)
				dtx, err := c.BeginTx(ctx, driver.TxOptions{})
				if err != nil {
					return err
				}
				defer func() { _ = dtx.Rollback() }()

				rows, err := c.QueryContext(queryCtx, querySQL, nil)
				if err != nil {
					return fmt.Errorf("QueryContext failed: %w", err)
				}
				rowsClosed := false
				defer func() {
					if !rowsClosed {
						_ = rows.Close()
					}
				}()

				dest := make([]driver.Value, 1)
				if err := rows.Next(dest); err != nil {
					return fmt.Errorf("rows.Next failed: %w", err)
				}
				if err := createAndRollbackConnSavepoint(ctx, c, testutil.UpdateBarSetFoo); err != nil {
					return err
				}
				_ = server.TestSpanner.DrainRequestsFromServer()

				if tc.midReplay {
					failRPCOnHook(server, testutil.MethodExecuteStreamingSql, failQueryCtx)
				} else {
					failQueryCtx()
				}

				_, execErr = c.ExecContext(ctx, testutil.UpdateSingersSetLastName, nil)
				server.TestSpanner.SetMethodHook(testutil.MethodExecuteStreamingSql, nil)

				if !tc.midReplay {
					reqs := server.TestSpanner.DrainRequestsFromServer()
					execReqsOnPreflight = len(testutil.RequestsOfType(reqs, reflect.TypeOf(&spannerpb.ExecuteSqlRequest{})))
				}

				rwTx := c.tx.contextTransaction.(*readWriteTransaction)
				gotState = rwTx.state
				gotRwTx = rwTx.rwTx

				// Closing the failed iterator unblocks replay so healthy statements can recover and commit.
				if err := rows.Close(); err != nil {
					return fmt.Errorf("rows.Close failed: %w", err)
				}
				rowsClosed = true
				_, recoveryExecErr = c.ExecContext(ctx, testutil.UpdateSingersSetLastName, nil)
				if recoveryExecErr == nil {
					recoveryCommitErr = dtx.Commit()
				}
				return nil
			}); err != nil {
				t.Fatalf("dbConn.Raw failed: %v", err)
			}

			assertContextError(t, execErr, tc.wantErr, tc.wantCode)
			if !tc.midReplay {
				if g, w := execReqsOnPreflight, 0; g != w {
					t.Errorf("preflight ExecuteSqlRequest count mismatch\nGot:  %v\nWant: %v", g, w)
				}
			}
			if g, w := gotState, readWriteTxStateNeedsReplay; g != w {
				t.Errorf("tx.state mismatch\nGot:  %v\nWant: %v", g, w)
			}
			if g, w := gotRwTx, (*spanner.ReadWriteStmtBasedTransaction)(nil); g != w {
				t.Errorf("rwTx after failed replay mismatch\nGot:  %v\nWant: %v", g, w)
			}
			if recoveryExecErr != nil {
				t.Fatalf("recovery ExecContext after closing expired iterator failed: %v", recoveryExecErr)
			}
			if recoveryCommitErr != nil {
				t.Fatalf("recovery Commit after closing expired iterator failed: %v", recoveryCommitErr)
			}
		})
	}
}

func TestRetry_PropagatesContextErrorAndCleansUpBackendTx(t *testing.T) {
	t.Parallel()

	type stmtKind string
	const (
		stmtDML      stmtKind = "DML"
		stmtBatchDML stmtKind = "BatchDML"
		stmtQuery    stmtKind = "Query"
	)

	tests := []struct {
		name      string
		kind      stmtKind
		cancel    bool
		openQuery bool
	}{
		{name: "dml_deadline_exceeded", kind: stmtDML},
		{name: "dml_cancelled", kind: stmtDML, cancel: true},
		{name: "batch_dml_deadline_exceeded", kind: stmtBatchDML},
		{name: "batch_dml_cancelled", kind: stmtBatchDML, cancel: true},
		{name: "query_deadline_exceeded", kind: stmtQuery},
		{name: "query_cancelled", kind: stmtQuery, cancel: true},
		{name: "open_query_deadline_exceeded", kind: stmtQuery, openQuery: true},
		{name: "open_query_cancelled", kind: stmtQuery, cancel: true, openQuery: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			db, server, teardown := setupTestDBConnectionWithParams(t, "minSessions=1;maxSessions=1")
			defer teardown()
			db.SetMaxOpenConns(1)

			// Warm up the session pool before starting the transaction deadline.
			if _, err := db.ExecContext(context.Background(), testutil.UpdateBarSetFoo); err != nil {
				t.Fatalf("warmup query failed: %v", err)
			}

			txCtx, failTx, cleanupTx := newFailureContext(context.Background(), !tc.cancel)
			defer cleanupTx()

			tx, err := db.BeginTx(txCtx, &sql.TxOptions{})
			if err != nil {
				t.Fatalf("BeginTx failed: %v", err)
			}
			defer func() { _ = tx.Rollback() }()

			method := testutil.MethodExecuteStreamingSql
			switch tc.kind {
			case stmtDML:
				if _, err := tx.ExecContext(txCtx, testutil.UpdateBarSetFoo); err != nil {
					t.Fatalf("DML failed: %v", err)
				}
			case stmtBatchDML:
				method = testutil.MethodExecuteBatchDml
				if _, err := tx.ExecContext(txCtx, "START BATCH DML"); err != nil {
					t.Fatalf("START BATCH DML failed: %v", err)
				}
				if _, err := tx.ExecContext(txCtx, testutil.UpdateBarSetFoo); err != nil {
					t.Fatalf("batch DML failed: %v", err)
				}
				if _, err := tx.ExecContext(txCtx, "RUN BATCH"); err != nil {
					t.Fatalf("RUN BATCH failed: %v", err)
				}
			case stmtQuery:
				method = testutil.MethodExecuteStreamingSql
				rows, err := tx.QueryContext(txCtx, testutil.SelectFooFromBar)
				if err != nil {
					t.Fatalf("QueryContext failed: %v", err)
				}
				if tc.openQuery {
					if !rows.Next() {
						t.Fatalf("expected first row, err=%v", rows.Err())
					}
					defer func() { _ = rows.Close() }()
				} else {
					for rows.Next() {
					}
					if err := rows.Err(); err != nil {
						t.Fatalf("rows.Err failed: %v", err)
					}
					if err := rows.Close(); err != nil {
						t.Fatalf("rows.Close failed: %v", err)
					}
				}
			}

			_ = server.TestSpanner.DrainRequestsFromServer()

			// Use a 1ns RetryDelay so gax.Sleep returns immediately and then cancel/expire
			// txCtx deterministically when the replay RPC executes on the mock server.
			server.TestSpanner.PutExecutionTime(testutil.MethodCommitTransaction, testutil.SimulatedExecutionTime{
				Errors: []error{abortedWithRetryDelay(time.Nanosecond)},
			})
			failRPCOnHook(server, method, failTx)

			err = tx.Commit()
			server.TestSpanner.SetMethodHook(method, nil)
			if tc.cancel {
				assertContextError(t, err, context.Canceled, codes.Canceled)
			} else {
				assertContextError(t, err, context.DeadlineExceeded, codes.DeadlineExceeded)
			}

			// Verify that the reset backend transaction created by ResetForRetry was rolled back.
			assertSingleRollbackRequest(t, server)

			// Verify the connection/session pool remains usable.
			if _, err := db.ExecContext(context.Background(), testutil.UpdateSingersSetLastName); err != nil {
				t.Fatalf("db unusable after failed retry commit: %v", err)
			}
		})
	}
}

func TestSavepoint_OpenRowIterator_RetryAbortedThenRecovers(t *testing.T) {
	t.Parallel()

	db, server, teardown := setupTestDBConnection(t)
	defer teardown()

	const querySQL = "SELECT FOO FROM BAR_RETRY_ABORT"
	_ = server.TestSpanner.PutStatementResult(querySQL, &testutil.StatementResult{
		Type:      testutil.StatementResultResultSet,
		ResultSet: testutil.CreateSingleColumnInt64ResultSet([]int64{10, 20, 30}, "FOO"),
	})

	ctx := context.Background()
	tx, err := db.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		t.Fatalf("BeginTx failed: %v", err)
	}
	defer func() { _ = tx.Rollback() }()

	rows, err := tx.QueryContext(ctx, querySQL)
	if err != nil {
		t.Fatalf("QueryContext failed: %v", err)
	}
	defer func() { _ = rows.Close() }()

	if !rows.Next() {
		t.Fatalf("expected first row, err=%v", rows.Err())
	}
	createAndRollbackSavepoint(t, ctx, tx, testutil.UpdateBarSetFoo)

	// First ExecuteStreamingSql during savepoint replay (replaying querySQL in checksumRowIterator.retry)
	// returns Aborted (1ns RetryDelay); retryUntilSuccess retries cleanly and keeps the stream alive.
	server.TestSpanner.PutExecutionTime(testutil.MethodExecuteStreamingSql, testutil.SimulatedExecutionTime{
		Errors: []error{abortedWithRetryDelay(time.Nanosecond)},
	})

	if _, err := tx.ExecContext(ctx, testutil.UpdateSingersSetLastName); err != nil {
		t.Fatalf("ExecContext after aborted replay failed: %v", err)
	}

	var remaining []int64
	for rows.Next() {
		var v int64
		if err := rows.Scan(&v); err != nil {
			t.Fatalf("Scan failed: %v", err)
		}
		remaining = append(remaining, v)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows.Err failed: %v", err)
	}
	if g, w := remaining, []int64{20, 30}; !reflect.DeepEqual(g, w) {
		t.Fatalf("remaining rows mismatch\nGot:  %v\nWant: %v", g, w)
	}
	if err := rows.Close(); err != nil {
		t.Fatalf("rows.Close failed: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("Commit failed: %v", err)
	}
}

func TestSavepoint_EnsureReplayed_RowNextAndRunBatchContextFailure(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		trigger  string
		wantErr  error
		wantCode codes.Code
	}{
		{
			name:     "row_next_after_rollback_with_cancelled_query_ctx",
			trigger:  "row_next",
			wantErr:  context.Canceled,
			wantCode: codes.Canceled,
		},
		{
			name:     "run_batch_after_rollback_with_expired_op_ctx",
			trigger:  "run_batch",
			wantErr:  context.DeadlineExceeded,
			wantCode: codes.DeadlineExceeded,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			db, server, teardown := setupTestDBConnection(t)
			defer teardown()

			const querySQL = "SELECT FOO FROM BAR_NEXT_BATCH"
			_ = server.TestSpanner.PutStatementResult(querySQL, &testutil.StatementResult{
				Type:      testutil.StatementResultResultSet,
				ResultSet: testutil.CreateSingleColumnInt64ResultSet([]int64{10, 20}, "FOO"),
			})

			ctx := context.Background()
			dbConn, err := db.Conn(ctx)
			if err != nil {
				t.Fatalf("db.Conn failed: %v", err)
			}
			defer silentClose(dbConn)

			opCtx := newControllableDeadlineContext(tc.wantErr == context.DeadlineExceeded)
			defer opCtx.cancel()

			var triggerErr, recoveryErr, commitErr error
			var gotState readWriteTxState
			if err := dbConn.Raw(func(driverConn any) error {
				c := driverConn.(*conn)
				dtx, err := c.BeginTx(ctx, driver.TxOptions{})
				if err != nil {
					return err
				}
				defer func() { _ = dtx.Rollback() }()

				switch tc.trigger {
				case "row_next":
					rows, err := c.QueryContext(opCtx, querySQL, nil)
					if err != nil {
						return fmt.Errorf("QueryContext failed: %w", err)
					}
					rowsClosed := false
					defer func() {
						if !rowsClosed {
							_ = rows.Close()
						}
					}()
					dest := make([]driver.Value, 1)
					if err := rows.Next(dest); err != nil {
						return fmt.Errorf("initial rows.Next failed: %w", err)
					}
					if err := createAndRollbackConnSavepoint(ctx, c, testutil.UpdateBarSetFoo); err != nil {
						return err
					}
					opCtx.cancel()
					triggerErr = rows.Next(dest)
					gotState = c.tx.contextTransaction.(*readWriteTransaction).state
					if err := rows.Close(); err != nil {
						return fmt.Errorf("rows.Close failed: %w", err)
					}
					rowsClosed = true
					commitErr = dtx.Commit()
				case "run_batch":
					if _, err := c.ExecContext(ctx, testutil.UpdateBarSetFoo, nil); err != nil {
						return fmt.Errorf("initial DML failed: %w", err)
					}
					if err := createAndRollbackConnSavepoint(ctx, c, testutil.UpdateSingersSetLastName); err != nil {
						return err
					}
					if _, err := c.ExecContext(ctx, "START BATCH DML", nil); err != nil {
						return fmt.Errorf("START BATCH DML failed: %w", err)
					}
					if _, err := c.ExecContext(ctx, testutil.UpdateSingersSetLastName, nil); err != nil {
						return fmt.Errorf("batch DML failed: %w", err)
					}
					opCtx.expire()
					_, triggerErr = c.ExecContext(opCtx, "RUN BATCH", nil)
					gotState = c.tx.contextTransaction.(*readWriteTransaction).state
					_ = server.TestSpanner.DrainRequestsFromServer()

					_, recoveryErr = c.ExecContext(ctx, "RUN BATCH", nil)
					if recoveryErr == nil {
						commitErr = dtx.Commit()
					}
				}
				return nil
			}); err != nil {
				t.Fatalf("dbConn.Raw failed: %v", err)
			}

			assertContextError(t, triggerErr, tc.wantErr, tc.wantCode)
			if g, w := gotState, readWriteTxStateNeedsReplay; g != w {
				t.Errorf("tx.state mismatch\nGot:  %v\nWant: %v", g, w)
			}
			if recoveryErr != nil {
				t.Fatalf("recovery RUN BATCH failed: %v", recoveryErr)
			}
			if commitErr != nil {
				t.Fatalf("Commit failed after recovery: %v", commitErr)
			}
			if tc.trigger == "run_batch" {
				reqs := server.TestSpanner.DrainRequestsFromServer()
				execReqs := testutil.RequestsOfType(reqs, reflect.TypeOf(&spannerpb.ExecuteSqlRequest{}))
				batchReqs := testutil.RequestsOfType(reqs, reflect.TypeOf(&spannerpb.ExecuteBatchDmlRequest{}))
				if g, w := len(execReqs), 1; g != w {
					t.Errorf("recovery ExecuteSqlRequest count mismatch\nGot:  %v\nWant: %v", g, w)
				}
				if g, w := len(batchReqs), 1; g != w {
					t.Errorf("recovery ExecuteBatchDmlRequest count mismatch\nGot:  %v\nWant: %v", g, w)
				}
			}
		})
	}
}

func TestCommit_AbortedReplayThenSleepContextTimeoutCleansUpBackendTx(t *testing.T) {
	t.Parallel()

	db, server, teardown := setupTestDBConnectionWithParams(t, "minSessions=1;maxSessions=1")
	defer teardown()
	db.SetMaxOpenConns(1)

	if _, err := db.ExecContext(context.Background(), testutil.UpdateBarSetFoo); err != nil {
		t.Fatalf("warmup query failed: %v", err)
	}

	dbConn, err := db.Conn(context.Background())
	if err != nil {
		t.Fatalf("db.Conn failed: %v", err)
	}
	defer silentClose(dbConn)

	txCtx := newControllableDeadlineContext(true)
	defer txCtx.cancel()

	var commitErr error
	if err := dbConn.Raw(func(driverConn any) error {
		c := driverConn.(*conn)
		dtx, err := c.BeginTx(txCtx, driver.TxOptions{})
		if err != nil {
			return err
		}
		defer func() { _ = dtx.Rollback() }()

		if _, err := c.ExecContext(txCtx, testutil.UpdateBarSetFoo, nil); err != nil {
			return fmt.Errorf("DML failed: %w", err)
		}
		rwTx := c.tx.contextTransaction.(*readWriteTransaction)
		var sleepCalls int
		rwTx.sleep = func(sleepCtx context.Context, delay time.Duration) error {
			sleepCalls++
			if sleepCalls == 2 {
				txCtx.expire()
			}
			return gax.Sleep(sleepCtx, delay)
		}
		defer func() { rwTx.sleep = nil }()

		_ = server.TestSpanner.DrainRequestsFromServer()

		// Commit aborts (1ns retry delay), ResetForRetry creates a new backend tx,
		// replay returns Aborted with a 10s RetryDelay and the second sleep expires txCtx.
		server.TestSpanner.PutExecutionTime(testutil.MethodCommitTransaction, testutil.SimulatedExecutionTime{
			Errors: []error{abortedWithRetryDelay(time.Nanosecond)},
		})
		server.TestSpanner.PutExecutionTime(testutil.MethodExecuteStreamingSql, testutil.SimulatedExecutionTime{
			Errors: []error{abortedWithRetryDelay(10 * time.Second)},
		})

		commitErr = dtx.Commit()
		return nil
	}); err != nil {
		t.Fatalf("dbConn.Raw failed: %v", err)
	}
	_ = dbConn.Close()

	assertContextError(t, commitErr, context.DeadlineExceeded, codes.DeadlineExceeded)
	assertSingleRollbackRequest(t, server)

	if _, err := db.ExecContext(context.Background(), testutil.UpdateSingersSetLastName); err != nil {
		t.Fatalf("db unusable after failed retry sleep commit: %v", err)
	}
}

func TestRetry_ResetForRetryFailure(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		onCommit      bool
		cancel        bool
		nonContextErr bool
		wantErr       error
		wantCode      codes.Code
	}{
		{
			name:     "commit_tx_ctx_timeout",
			onCommit: true,
			wantErr:  context.DeadlineExceeded,
			wantCode: codes.DeadlineExceeded,
		},
		{
			name:     "commit_tx_ctx_cancelled",
			onCommit: true,
			cancel:   true,
			wantErr:  context.Canceled,
			wantCode: codes.Canceled,
		},
		{
			name:     "exec_context_op_ctx_timeout",
			wantErr:  context.DeadlineExceeded,
			wantCode: codes.DeadlineExceeded,
		},
		{
			name:          "reset_non_context_permission_denied",
			onCommit:      true,
			nonContextErr: true,
			wantCode:      codes.PermissionDenied,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			db, server, teardown := setupTestDBConnectionWithParams(t, "minSessions=1;maxSessions=1")
			defer teardown()
			db.SetMaxOpenConns(1)

			if _, err := db.ExecContext(context.Background(), testutil.UpdateBarSetFoo); err != nil {
				t.Fatalf("warmup query failed: %v", err)
			}

			failCtx, failFn, cleanupFailCtx := newFailureContext(context.Background(), !tc.cancel)
			defer cleanupFailCtx()

			beginCtx := context.Background()
			if tc.onCommit && !tc.nonContextErr {
				beginCtx = failCtx
			}
			tx, err := db.BeginTx(beginCtx, &sql.TxOptions{})
			if err != nil {
				t.Fatalf("BeginTx failed: %v", err)
			}
			defer func() { _ = tx.Rollback() }()

			if _, err := tx.ExecContext(context.Background(), testutil.UpdateBarSetFoo); err != nil {
				t.Fatalf("DML failed: %v", err)
			}

			abortMethod := testutil.MethodExecuteStreamingSql
			if tc.onCommit {
				abortMethod = testutil.MethodCommitTransaction
			}
			server.TestSpanner.PutExecutionTime(abortMethod, testutil.SimulatedExecutionTime{
				Errors: []error{abortedWithRetryDelay(time.Nanosecond)},
			})
			if tc.nonContextErr {
				server.TestSpanner.PutExecutionTime(testutil.MethodBeginTransaction, testutil.SimulatedExecutionTime{
					Errors: []error{status.Error(codes.PermissionDenied, "permission denied")},
				})
			} else {
				failRPCOnHook(server, testutil.MethodBeginTransaction, failFn)
			}

			if tc.onCommit {
				err = tx.Commit()
			} else {
				_, err = tx.ExecContext(failCtx, testutil.UpdateSingersSetLastName)
			}
			server.TestSpanner.SetMethodHook(testutil.MethodBeginTransaction, nil)
			server.TestSpanner.PutExecutionTime(abortMethod, testutil.SimulatedExecutionTime{})
			server.TestSpanner.PutExecutionTime(testutil.MethodBeginTransaction, testutil.SimulatedExecutionTime{})
			if tc.wantErr != nil {
				assertContextError(t, err, tc.wantErr, tc.wantCode)
			} else if g, w := spanner.ErrCode(err), tc.wantCode; g != w {
				t.Fatalf("error code mismatch\nGot:  %v\nWant: %v", g, w)
			}
			if !tc.onCommit {
				if err := tx.Rollback(); err != nil {
					t.Fatalf("Rollback failed: %v", err)
				}
			}

			if _, err := db.ExecContext(context.Background(), testutil.UpdateSingersSetLastName); err != nil {
				t.Fatalf("db unusable after ResetForRetry failure: %v", err)
			}
		})
	}
}

func TestNonSavepoint_ExecContextAbortedThenOpTimeout(t *testing.T) {
	t.Parallel()

	db, server, teardown := setupTestDBConnectionWithParams(t, "minSessions=1;maxSessions=1")
	defer teardown()
	db.SetMaxOpenConns(1)
	ctx := context.Background()

	tx, err := db.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		t.Fatalf("BeginTx failed: %v", err)
	}
	defer func() { _ = tx.Rollback() }()

	if _, err := tx.ExecContext(ctx, testutil.UpdateBarSetFoo); err != nil {
		t.Fatalf("initial DML failed: %v", err)
	}
	_ = server.TestSpanner.DrainRequestsFromServer()

	// First ExecuteStreamingSql returns Aborted (1ns RetryDelay), triggering ResetForRetry +
	// replay of UpdateBarSetFoo, during which opCtx expires deterministically.
	opCtx := newControllableDeadlineContext(true)
	defer opCtx.cancel()
	var execCalls atomic.Int32
	server.TestSpanner.SetMethodHook(testutil.MethodExecuteStreamingSql, func(srvCtx context.Context, _ interface{}) error {
		if execCalls.Add(1) == 1 {
			return nil
		}
		opCtx.expire()
		<-srvCtx.Done()
		return status.FromContextError(srvCtx.Err()).Err()
	})
	server.TestSpanner.PutExecutionTime(testutil.MethodExecuteStreamingSql, testutil.SimulatedExecutionTime{
		Errors: []error{abortedWithRetryDelay(time.Nanosecond)},
	})

	_, err = tx.ExecContext(opCtx, testutil.UpdateSingersSetLastName)
	server.TestSpanner.SetMethodHook(testutil.MethodExecuteStreamingSql, nil)
	assertContextError(t, err, context.DeadlineExceeded, codes.DeadlineExceeded)

	server.TestSpanner.PutExecutionTime(testutil.MethodExecuteStreamingSql, testutil.SimulatedExecutionTime{})
	assertSingleRollbackRequest(t, server)
}

func TestExecContext_AbortedSleepTimeoutCleansUpBackendTx(t *testing.T) {
	t.Parallel()

	db, server, teardown := setupTestDBConnectionWithParams(t, "minSessions=1;maxSessions=1")
	defer teardown()
	db.SetMaxOpenConns(1)
	ctx := context.Background()

	dbConn, err := db.Conn(ctx)
	if err != nil {
		t.Fatalf("db.Conn failed: %v", err)
	}
	defer silentClose(dbConn)

	tx, err := dbConn.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		t.Fatalf("BeginTx failed: %v", err)
	}
	defer func() { _ = tx.Rollback() }()

	if _, err := tx.ExecContext(ctx, testutil.UpdateBarSetFoo); err != nil {
		t.Fatalf("initial DML failed: %v", err)
	}
	_ = server.TestSpanner.DrainRequestsFromServer()

	opCtx := newControllableDeadlineContext(true)
	defer opCtx.cancel()

	if err := dbConn.Raw(func(driverConn any) error {
		c := driverConn.(*conn)
		rwTx := c.tx.contextTransaction.(*readWriteTransaction)
		rwTx.sleep = func(sleepCtx context.Context, delay time.Duration) error {
			opCtx.expire()
			return gax.Sleep(sleepCtx, delay)
		}
		return nil
	}); err != nil {
		t.Fatalf("dbConn.Raw failed: %v", err)
	}

	// Next DML returns Aborted with a 10s RetryDelay and the sleep hook expires opCtx immediately.
	server.TestSpanner.PutExecutionTime(testutil.MethodExecuteStreamingSql, testutil.SimulatedExecutionTime{
		Errors: []error{abortedWithRetryDelay(10 * time.Second)},
	})

	_, err = tx.ExecContext(opCtx, testutil.UpdateSingersSetLastName)
	assertContextError(t, err, context.DeadlineExceeded, codes.DeadlineExceeded)

	server.TestSpanner.PutExecutionTime(testutil.MethodExecuteStreamingSql, testutil.SimulatedExecutionTime{})

	var gotRwTx *spanner.ReadWriteStmtBasedTransaction
	if err := dbConn.Raw(func(driverConn any) error {
		c := driverConn.(*conn)
		gotRwTx = c.tx.contextTransaction.(*readWriteTransaction).rwTx
		return nil
	}); err != nil {
		t.Fatalf("dbConn.Raw failed: %v", err)
	}
	if g, w := gotRwTx, (*spanner.ReadWriteStmtBasedTransaction)(nil); g != w {
		t.Errorf("rwTx after gax.Sleep timeout mismatch\nGot:  %v\nWant: %v", g, w)
	}

	assertSingleRollbackRequest(t, server)

	// Subsequent statement and Commit on the transaction must fail cleanly with FailedPrecondition.
	_, err = tx.ExecContext(ctx, testutil.UpdateSingersSetLastName)
	if g, w := spanner.ErrCode(err), codes.FailedPrecondition; g != w {
		t.Errorf("subsequent statement error code mismatch\nGot:  %v\nWant: %v", g, w)
	}
	err = tx.Commit()
	if g, w := spanner.ErrCode(err), codes.FailedPrecondition; g != w {
		t.Errorf("Commit error code mismatch\nGot:  %v\nWant: %v", g, w)
	}
}

func TestExecContext_ConcurrentModificationCleansUpBackendTx(t *testing.T) {
	t.Parallel()

	db, server, teardown := setupTestDBConnectionWithParams(t, "minSessions=1;maxSessions=1")
	defer teardown()
	db.SetMaxOpenConns(1)
	ctx := context.Background()

	dbConn, err := db.Conn(ctx)
	if err != nil {
		t.Fatalf("db.Conn failed: %v", err)
	}
	defer silentClose(dbConn)

	tx, err := dbConn.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		t.Fatalf("BeginTx failed: %v", err)
	}
	defer func() { _ = tx.Rollback() }()

	if _, err := tx.ExecContext(ctx, testutil.UpdateBarSetFoo); err != nil {
		t.Fatalf("initial DML failed: %v", err)
	}
	_ = server.TestSpanner.DrainRequestsFromServer()

	// Next DML aborts and replay of UpdateBarSetFoo returns a different row count,
	// triggering ErrAbortedDueToConcurrentModification in readWriteTransaction.retry.
	_ = server.TestSpanner.PutStatementResult(testutil.UpdateBarSetFoo, &testutil.StatementResult{
		Type:        testutil.StatementResultUpdateCount,
		UpdateCount: testutil.UpdateBarSetFooRowCount + 1,
	})
	server.TestSpanner.PutExecutionTime(testutil.MethodExecuteStreamingSql, testutil.SimulatedExecutionTime{
		Errors: []error{abortedWithRetryDelay(time.Nanosecond)},
	})

	_, err = tx.ExecContext(ctx, testutil.UpdateSingersSetLastName)
	if g, w := err, ErrAbortedDueToConcurrentModification; !errors.Is(g, w) {
		t.Fatalf("ExecContext error mismatch\nGot:  %v\nWant: %v", g, w)
	}

	var gotRwTx *spanner.ReadWriteStmtBasedTransaction
	if err := dbConn.Raw(func(driverConn any) error {
		c := driverConn.(*conn)
		gotRwTx = c.tx.contextTransaction.(*readWriteTransaction).rwTx
		return nil
	}); err != nil {
		t.Fatalf("dbConn.Raw failed: %v", err)
	}
	if g, w := gotRwTx, (*spanner.ReadWriteStmtBasedTransaction)(nil); g != w {
		t.Errorf("rwTx after ErrAbortedDueToConcurrentModification mismatch\nGot:  %v\nWant: %v", g, w)
	}

	assertSingleRollbackRequest(t, server)
}

func TestRetryUntilSuccess_ObservesTxContextDuringNonSavepointRetry(t *testing.T) {
	t.Parallel()

	type failureMode int
	const (
		modeCancelDuringSleep failureMode = iota
		modeCancelMidReplayDML
		modeCancelMidReplayQueryNext
		modeExpiredPreflightOnAbort
	)

	tests := []struct {
		name     string
		mode     failureMode
		wantErr  error
		wantCode codes.Code
	}{
		{
			name:     "cancel_tx_ctx_during_sleep",
			mode:     modeCancelDuringSleep,
			wantErr:  context.Canceled,
			wantCode: codes.Canceled,
		},
		{
			name:     "cancel_tx_ctx_mid_replay_dml",
			mode:     modeCancelMidReplayDML,
			wantErr:  context.Canceled,
			wantCode: codes.Canceled,
		},
		{
			name:     "cancel_tx_ctx_mid_replay_query_next",
			mode:     modeCancelMidReplayQueryNext,
			wantErr:  context.Canceled,
			wantCode: codes.Canceled,
		},
		{
			name:     "expired_tx_ctx_preflight_on_abort",
			mode:     modeExpiredPreflightOnAbort,
			wantErr:  context.DeadlineExceeded,
			wantCode: codes.DeadlineExceeded,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			db, server, teardown := setupTestDBConnectionWithParams(t, "minSessions=1;maxSessions=1")
			defer teardown()
			db.SetMaxOpenConns(1)

			const querySQL = "SELECT FOO FROM BAR_RETRY_TXCTX"
			if tc.mode == modeCancelMidReplayQueryNext {
				_ = server.TestSpanner.PutStatementResult(querySQL, &testutil.StatementResult{
					Type:      testutil.StatementResultResultSet,
					ResultSet: testutil.CreateSingleColumnInt64ResultSet([]int64{10, 20}, "FOO"),
				})
			}

			dbConn, err := db.Conn(context.Background())
			if err != nil {
				t.Fatalf("db.Conn failed: %v", err)
			}
			defer silentClose(dbConn)

			txCtx := newControllableDeadlineContext(tc.mode == modeExpiredPreflightOnAbort)
			defer txCtx.cancel()

			var opErr error
			var gotRwTx *spanner.ReadWriteStmtBasedTransaction
			if err := dbConn.Raw(func(driverConn any) error {
				c := driverConn.(*conn)
				dtx, err := c.BeginTx(txCtx, driver.TxOptions{})
				if err != nil {
					return err
				}
				defer func() { _ = dtx.Rollback() }()

				// Activate and record initial statement on healthy context.Background().
				if tc.mode == modeCancelMidReplayQueryNext {
					rows, err := c.QueryContext(context.Background(), querySQL, nil)
					if err != nil {
						return fmt.Errorf("QueryContext failed: %w", err)
					}
					defer func() { _ = rows.Close() }()
					dest := make([]driver.Value, 1)
					if err := rows.Next(dest); err != nil {
						return fmt.Errorf("initial rows.Next failed: %w", err)
					}
					_ = server.TestSpanner.DrainRequestsFromServer()
				} else {
					if _, err := c.ExecContext(context.Background(), testutil.UpdateBarSetFoo, nil); err != nil {
						return fmt.Errorf("initial DML failed: %w", err)
					}
					_ = server.TestSpanner.DrainRequestsFromServer()
				}

				rwTx := c.tx.contextTransaction.(*readWriteTransaction)
				switch tc.mode {
				case modeCancelDuringSleep:
					rwTx.sleep = func(sleepCtx context.Context, delay time.Duration) error {
						txCtx.cancel()
						return gax.Sleep(sleepCtx, delay)
					}
					defer func() { rwTx.sleep = nil }()
					server.TestSpanner.PutExecutionTime(testutil.MethodExecuteStreamingSql, testutil.SimulatedExecutionTime{
						Errors: []error{abortedWithRetryDelay(10 * time.Second)},
					})
				case modeCancelMidReplayDML, modeCancelMidReplayQueryNext:
					var calls atomic.Int32
					server.TestSpanner.SetMethodHook(testutil.MethodExecuteStreamingSql, func(srvCtx context.Context, _ interface{}) error {
						if calls.Add(1) == 1 {
							return nil
						}
						txCtx.cancel()
						<-srvCtx.Done()
						return status.FromContextError(srvCtx.Err()).Err()
					})
					server.TestSpanner.PutExecutionTime(testutil.MethodExecuteStreamingSql, testutil.SimulatedExecutionTime{
						Errors: []error{abortedWithRetryDelay(time.Nanosecond)},
					})
				case modeExpiredPreflightOnAbort:
					server.TestSpanner.SetMethodHook(testutil.MethodExecuteStreamingSql, func(_ context.Context, _ interface{}) error {
						txCtx.expire()
						return nil
					})
					server.TestSpanner.PutExecutionTime(testutil.MethodExecuteStreamingSql, testutil.SimulatedExecutionTime{
						Errors: []error{abortedWithRetryDelay(10 * time.Second)},
					})
				}

				_, opErr = c.ExecContext(context.Background(), testutil.UpdateSingersSetLastName, nil)
				server.TestSpanner.SetMethodHook(testutil.MethodExecuteStreamingSql, nil)
				server.TestSpanner.PutExecutionTime(testutil.MethodExecuteStreamingSql, testutil.SimulatedExecutionTime{})

				gotRwTx = rwTx.rwTx
				return nil
			}); err != nil {
				t.Fatalf("dbConn.Raw failed: %v", err)
			}

			assertContextError(t, opErr, tc.wantErr, tc.wantCode)
			if g, w := gotRwTx, (*spanner.ReadWriteStmtBasedTransaction)(nil); g != w {
				t.Errorf("rwTx after retry failure mismatch\nGot:  %v\nWant: %v", g, w)
			}
			assertSingleRollbackRequest(t, server)
		})
	}
}

func TestNonSavepoint_AbortedWithExpiredOpenIterator(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		timeout   bool
		midReplay bool
		wantErr   error
		wantCode  codes.Code
	}{
		{
			name:     "preflight_cancelled",
			wantErr:  context.Canceled,
			wantCode: codes.Canceled,
		},
		{
			name:     "preflight_deadline_exceeded",
			timeout:  true,
			wantErr:  context.DeadlineExceeded,
			wantCode: codes.DeadlineExceeded,
		},
		{
			name:      "mid_replay_cancelled",
			midReplay: true,
			wantErr:   context.Canceled,
			wantCode:  codes.Canceled,
		},
		{
			name:      "mid_replay_deadline_exceeded",
			timeout:   true,
			midReplay: true,
			wantErr:   context.DeadlineExceeded,
			wantCode:  codes.DeadlineExceeded,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			db, server, teardown := setupTestDBConnectionWithParams(t, "minSessions=1;maxSessions=1")
			defer teardown()
			db.SetMaxOpenConns(1)

			const querySQL = "SELECT FOO FROM BAR_NON_SP_OPEN"
			_ = server.TestSpanner.PutStatementResult(querySQL, &testutil.StatementResult{
				Type:      testutil.StatementResultResultSet,
				ResultSet: testutil.CreateSingleColumnInt64ResultSet([]int64{10, 20}, "FOO"),
			})

			ctx := context.Background()
			dbConn, err := db.Conn(ctx)
			if err != nil {
				t.Fatalf("db.Conn failed: %v", err)
			}
			defer silentClose(dbConn)

			queryCtx, failQueryCtx, cleanupQueryCtx := newFailureContext(ctx, tc.timeout)
			defer cleanupQueryCtx()

			var execErr error
			var gotRwTx *spanner.ReadWriteStmtBasedTransaction
			var execSQLCount, rollbackCount int
			if err := dbConn.Raw(func(driverConn any) error {
				c := driverConn.(*conn)
				dtx, err := c.BeginTx(ctx, driver.TxOptions{})
				if err != nil {
					return err
				}
				defer func() { _ = dtx.Rollback() }()

				rows, err := c.QueryContext(queryCtx, querySQL, nil)
				if err != nil {
					return fmt.Errorf("QueryContext failed: %w", err)
				}
				defer func() { _ = rows.Close() }()

				dest := make([]driver.Value, 1)
				if err := rows.Next(dest); err != nil {
					return fmt.Errorf("rows.Next failed: %w", err)
				}
				_ = server.TestSpanner.DrainRequestsFromServer()

				if tc.midReplay {
					var execCalls atomic.Int32
					server.TestSpanner.SetMethodHook(testutil.MethodExecuteStreamingSql, func(srvCtx context.Context, _ interface{}) error {
						if execCalls.Add(1) == 1 {
							return nil
						}
						failQueryCtx()
						<-srvCtx.Done()
						return status.FromContextError(srvCtx.Err()).Err()
					})
				} else {
					failQueryCtx()
				}
				server.TestSpanner.PutExecutionTime(testutil.MethodExecuteStreamingSql, testutil.SimulatedExecutionTime{
					Errors: []error{abortedWithRetryDelay(time.Nanosecond)},
				})

				_, execErr = c.ExecContext(ctx, testutil.UpdateSingersSetLastName, nil)
				server.TestSpanner.SetMethodHook(testutil.MethodExecuteStreamingSql, nil)
				gotRwTx = c.tx.contextTransaction.(*readWriteTransaction).rwTx
				reqs := server.TestSpanner.DrainRequestsFromServer()
				execSQLCount = len(testutil.RequestsOfType(reqs, reflect.TypeOf(&spannerpb.ExecuteSqlRequest{})))
				rollbackCount = len(testutil.RequestsOfType(reqs, reflect.TypeOf(&spannerpb.RollbackRequest{})))
				return nil
			}); err != nil {
				t.Fatalf("dbConn.Raw failed: %v", err)
			}

			assertContextError(t, execErr, tc.wantErr, tc.wantCode)
			if g, w := gotRwTx, (*spanner.ReadWriteStmtBasedTransaction)(nil); g != w {
				t.Errorf("rwTx after retry failure mismatch\nGot:  %v\nWant: %v", g, w)
			}
			wantExecSQLCount := 1
			if tc.midReplay {
				wantExecSQLCount = 2
			}
			if g, w := execSQLCount, wantExecSQLCount; g != w {
				t.Errorf("ExecuteSqlRequest count mismatch\nGot:  %v\nWant: %v", g, w)
			}
			if g, w := rollbackCount, 1; g != w {
				t.Errorf("RollbackRequest count mismatch\nGot:  %v\nWant: %v", g, w)
			}
		})
	}
}

func TestNonSavepoint_AbortedMidQueryReplayThenRecovers(t *testing.T) {
	t.Parallel()

	db, server, teardown := setupTestDBConnection(t)
	defer teardown()

	const querySQL = "SELECT FOO FROM BAR_MID_QUERY_ABORT"
	_ = server.TestSpanner.PutStatementResult(querySQL, &testutil.StatementResult{
		Type:      testutil.StatementResultResultSet,
		ResultSet: testutil.CreateSingleColumnInt64ResultSet([]int64{10, 20, 30}, "FOO"),
	})

	ctx := context.Background()
	tx, err := db.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		t.Fatalf("BeginTx failed: %v", err)
	}
	defer func() { _ = tx.Rollback() }()

	rows, err := tx.QueryContext(ctx, querySQL)
	if err != nil {
		t.Fatalf("QueryContext failed: %v", err)
	}
	defer func() { _ = rows.Close() }()

	if !rows.Next() {
		t.Fatalf("expected first row, err=%v", rows.Err())
	}

	// 1st ExecuteStreamingSql aborts ExecContext; 2nd aborts query replay inside replayAndVerifyRows.
	server.TestSpanner.PutExecutionTime(testutil.MethodExecuteStreamingSql, testutil.SimulatedExecutionTime{
		Errors: []error{
			abortedWithRetryDelay(time.Nanosecond),
			abortedWithRetryDelay(time.Nanosecond),
		},
	})

	if _, err := tx.ExecContext(ctx, testutil.UpdateSingersSetLastName); err != nil {
		t.Fatalf("ExecContext after aborted query replay failed: %v", err)
	}

	var remaining []int64
	for rows.Next() {
		var v int64
		if err := rows.Scan(&v); err != nil {
			t.Fatalf("Scan failed: %v", err)
		}
		remaining = append(remaining, v)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows.Err failed: %v", err)
	}
	if g, w := remaining, []int64{20, 30}; !reflect.DeepEqual(g, w) {
		t.Fatalf("remaining rows mismatch\nGot:  %v\nWant: %v", g, w)
	}
	if err := rows.Close(); err != nil {
		t.Fatalf("rows.Close failed: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("Commit failed: %v", err)
	}
}

func TestNonSavepoint_RowNextAbortedThenContextFailureDuringRetry(t *testing.T) {
	t.Parallel()

	type failTarget int
	const (
		failTxCancel failTarget = iota
		failItCancel
		failItDeadline
	)

	tests := []struct {
		name     string
		target   failTarget
		wantErr  error
		wantCode codes.Code
	}{
		{
			name:     "tx_ctx_cancelled",
			target:   failTxCancel,
			wantErr:  context.Canceled,
			wantCode: codes.Canceled,
		},
		{
			name:     "it_ctx_cancelled",
			target:   failItCancel,
			wantErr:  context.Canceled,
			wantCode: codes.Canceled,
		},
		{
			name:     "it_ctx_deadline_exceeded",
			target:   failItDeadline,
			wantErr:  context.DeadlineExceeded,
			wantCode: codes.DeadlineExceeded,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			db, server, teardown := setupTestDBConnectionWithParams(t, "minSessions=1;maxSessions=1")
			defer teardown()
			db.SetMaxOpenConns(1)

			const querySQL = "SELECT FOO FROM BAR_ROWNEXT_ABORT"
			_ = server.TestSpanner.PutStatementResult(querySQL, &testutil.StatementResult{
				Type:      testutil.StatementResultResultSet,
				ResultSet: testutil.CreateSingleColumnInt64ResultSet([]int64{10, 20}, "FOO"),
			})

			bg := context.Background()
			dbConn, err := db.Conn(bg)
			if err != nil {
				t.Fatalf("db.Conn failed: %v", err)
			}
			defer silentClose(dbConn)

			txCtx := bg
			queryCtx := bg
			var failTargetFn func()
			if tc.target == failTxCancel {
				var cancelTx context.CancelFunc
				txCtx, cancelTx = context.WithCancel(bg)
				defer cancelTx()
				failTargetFn = cancelTx
			} else {
				var cleanupQuery context.CancelFunc
				queryCtx, failTargetFn, cleanupQuery = newFailureContext(bg, tc.target == failItDeadline)
				defer cleanupQuery()
			}

			var nextErr error
			var gotRwTx *spanner.ReadWriteStmtBasedTransaction
			var rollbackCount int
			if err := dbConn.Raw(func(driverConn any) error {
				c := driverConn.(*conn)
				dtx, err := c.BeginTx(txCtx, driver.TxOptions{})
				if err != nil {
					return err
				}
				defer func() { _ = dtx.Rollback() }()

				if _, err := c.ExecContext(bg, testutil.UpdateBarSetFoo, nil); err != nil {
					return fmt.Errorf("initial DML failed: %w", err)
				}
				rows, err := c.QueryContext(queryCtx, querySQL, nil)
				if err != nil {
					return fmt.Errorf("QueryContext failed: %w", err)
				}
				defer func() { _ = rows.Close() }()
				_ = server.TestSpanner.DrainRequestsFromServer()

				var execCalls atomic.Int32
				server.TestSpanner.SetMethodHook(testutil.MethodExecuteStreamingSql, func(srvCtx context.Context, _ interface{}) error {
					if execCalls.Add(1) == 1 {
						return nil
					}
					failTargetFn()
					<-srvCtx.Done()
					return status.FromContextError(srvCtx.Err()).Err()
				})
				server.TestSpanner.PutExecutionTime(testutil.MethodExecuteStreamingSql, testutil.SimulatedExecutionTime{
					Errors: []error{abortedWithRetryDelay(time.Nanosecond)},
				})

				dest := make([]driver.Value, 1)
				nextErr = rows.Next(dest)
				server.TestSpanner.SetMethodHook(testutil.MethodExecuteStreamingSql, nil)
				server.TestSpanner.PutExecutionTime(testutil.MethodExecuteStreamingSql, testutil.SimulatedExecutionTime{})

				gotRwTx = c.tx.contextTransaction.(*readWriteTransaction).rwTx
				reqs := server.TestSpanner.DrainRequestsFromServer()
				rollbackCount = len(testutil.RequestsOfType(reqs, reflect.TypeOf(&spannerpb.RollbackRequest{})))
				return nil
			}); err != nil {
				t.Fatalf("dbConn.Raw failed: %v", err)
			}

			assertContextError(t, nextErr, tc.wantErr, tc.wantCode)
			if g, w := gotRwTx, (*spanner.ReadWriteStmtBasedTransaction)(nil); g != w {
				t.Errorf("rwTx after rows.Next retry failure mismatch\nGot:  %v\nWant: %v", g, w)
			}
			if g, w := rollbackCount, 1; g != w {
				t.Errorf("RollbackRequest count mismatch\nGot:  %v\nWant: %v", g, w)
			}
		})
	}
}

func TestNonSavepoint_BatchDMLAbortedMidRetryThenRecovers(t *testing.T) {
	t.Parallel()

	db, server, teardown := setupTestDBConnection(t)
	defer teardown()
	ctx := context.Background()

	tx, err := db.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		t.Fatalf("BeginTx failed: %v", err)
	}
	defer func() { _ = tx.Rollback() }()

	if _, err := tx.ExecContext(ctx, "START BATCH DML"); err != nil {
		t.Fatalf("START BATCH DML failed: %v", err)
	}
	if _, err := tx.ExecContext(ctx, testutil.UpdateBarSetFoo); err != nil {
		t.Fatalf("batch DML failed: %v", err)
	}
	if _, err := tx.ExecContext(ctx, "RUN BATCH"); err != nil {
		t.Fatalf("RUN BATCH failed: %v", err)
	}

	// Next DML aborts once, and the first replay of ExecuteBatchDml also aborts (1ns),
	// exercising retriableBatchUpdate.retry returning codes.Aborted before recovering.
	server.TestSpanner.PutExecutionTime(testutil.MethodExecuteStreamingSql, testutil.SimulatedExecutionTime{
		Errors: []error{abortedWithRetryDelay(time.Nanosecond)},
	})
	server.TestSpanner.PutExecutionTime(testutil.MethodExecuteBatchDml, testutil.SimulatedExecutionTime{
		Errors: []error{abortedWithRetryDelay(time.Nanosecond)},
	})

	if _, err := tx.ExecContext(ctx, testutil.UpdateSingersSetLastName); err != nil {
		t.Fatalf("ExecContext after aborted batch replay failed: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("Commit failed: %v", err)
	}
}

func TestRetryUntilSuccess_TxContextDeadlineExceededInSynctest(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		txCtx, cancelTx := context.WithTimeout(context.Background(), 20*time.Millisecond)
		defer cancelTx()

		tx := &readWriteTransaction{
			ctx:    txCtx,
			logger: noopLogger,
			retryAborts: func() bool {
				return true
			},
		}

		err := tx.retryUntilSuccess(context.Background(), abortedWithRetryDelay(time.Second))
		assertContextError(t, err, context.DeadlineExceeded, codes.DeadlineExceeded)
	})
}

func TestSavepoint_EnsureReplayed_BufferWriteOpTimeoutRecovers(t *testing.T) {
	t.Parallel()

	db, server, teardown := setupTestDBConnection(t)
	defer teardown()
	ctx := context.Background()

	dbConn, err := db.Conn(ctx)
	if err != nil {
		t.Fatalf("db.Conn failed: %v", err)
	}
	defer silentClose(dbConn)

	opCtx := newControllableDeadlineContext(true)
	defer opCtx.cancel()

	var timeoutErr, recoveryErr, commitErr error
	var gotState readWriteTxState
	if err := dbConn.Raw(func(driverConn any) error {
		c := driverConn.(*conn)
		dtx, err := c.BeginTx(ctx, driver.TxOptions{})
		if err != nil {
			return err
		}
		defer func() { _ = dtx.Rollback() }()

		if _, err := c.ExecContext(ctx, testutil.UpdateBarSetFoo, nil); err != nil {
			return fmt.Errorf("initial DML failed: %w", err)
		}
		if err := c.BufferWrite([]*spanner.Mutation{
			spanner.Insert("T", []string{"K"}, []interface{}{"v1"}),
		}); err != nil {
			return fmt.Errorf("BufferWrite failed: %w", err)
		}
		if err := createAndRollbackConnSavepoint(ctx, c, testutil.UpdateSingersSetLastName); err != nil {
			return err
		}

		failRPCOnHook(server, testutil.MethodExecuteStreamingSql, opCtx.expire)
		_, timeoutErr = c.ExecContext(opCtx, testutil.UpdateSingersSetLastName, nil)
		server.TestSpanner.SetMethodHook(testutil.MethodExecuteStreamingSql, nil)

		gotState = c.tx.contextTransaction.(*readWriteTransaction).state
		_ = server.TestSpanner.DrainRequestsFromServer()

		_, recoveryErr = c.ExecContext(ctx, testutil.UpdateSingersSetLastName, nil)
		if recoveryErr == nil {
			commitErr = dtx.Commit()
		}
		return nil
	}); err != nil {
		t.Fatalf("dbConn.Raw failed: %v", err)
	}

	assertContextError(t, timeoutErr, context.DeadlineExceeded, codes.DeadlineExceeded)
	if g, w := gotState, readWriteTxStateNeedsReplay; g != w {
		t.Errorf("tx.state mismatch\nGot:  %v\nWant: %v", g, w)
	}
	if recoveryErr != nil {
		t.Fatalf("recovery ExecContext failed: %v", recoveryErr)
	}
	if commitErr != nil {
		t.Fatalf("recovery Commit failed: %v", commitErr)
	}

	reqs := server.TestSpanner.DrainRequestsFromServer()
	commitReqs := testutil.RequestsOfType(reqs, reflect.TypeOf(&spannerpb.CommitRequest{}))
	if g, w := len(commitReqs), 1; g != w {
		t.Fatalf("CommitRequest count mismatch\nGot:  %v\nWant: %v", g, w)
	}
	mutations := commitReqs[0].(*spannerpb.CommitRequest).GetMutations()
	if g, w := len(mutations), 1; g != w {
		t.Fatalf("CommitRequest mutations count mismatch\nGot:  %v\nWant: %v", g, w)
	}
	if g, w := mutations[0].GetInsert().GetTable(), "T"; g != w {
		t.Errorf("CommitRequest mutation table mismatch\nGot:  %v\nWant: %v", g, w)
	}
}

func TestSavepoint_EnforceInErrorState_EnsureReplayedContextErrorRecovers(t *testing.T) {
	t.Parallel()

	db, server, teardown := setupTestDBConnectionWithParamsAndDialect(t, "", databasepb.DatabaseDialect_POSTGRESQL)
	defer teardown()
	ctx := context.Background()

	const (
		dml1 = "UPDATE Foo SET Val=1 WHERE Id=1"
		dml2 = "UPDATE Foo SET Val=2 WHERE Id=2"
		dml3 = "UPDATE Foo SET Val=3 WHERE Id=3"
	)
	_ = server.TestSpanner.PutStatementResult(dml1, &testutil.StatementResult{Type: testutil.StatementResultUpdateCount, UpdateCount: 1})
	_ = server.TestSpanner.PutStatementResult(dml2, &testutil.StatementResult{Type: testutil.StatementResultUpdateCount, UpdateCount: 1})
	_ = server.TestSpanner.PutStatementResult(dml3, &testutil.StatementResult{Type: testutil.StatementResultUpdateCount, UpdateCount: 1})

	dbConn, err := db.Conn(ctx)
	if err != nil {
		t.Fatalf("db.Conn failed: %v", err)
	}
	defer silentClose(dbConn)
	setPGInErrorTxBehavior(t, dbConn, connectionstate.InErrorTxBehaviorEnforceInErrorState)

	tx, err := dbConn.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("BeginTx failed: %v", err)
	}
	defer func() { _ = tx.Rollback() }()

	if _, err := tx.ExecContext(ctx, dml1); err != nil {
		t.Fatalf("dml1 failed: %v", err)
	}
	if _, err := tx.ExecContext(ctx, "SAVEPOINT s1"); err != nil {
		t.Fatalf("SAVEPOINT s1 failed: %v", err)
	}
	if _, err := tx.ExecContext(ctx, dml2); err != nil {
		t.Fatalf("dml2 failed: %v", err)
	}
	if _, err := tx.ExecContext(ctx, "ROLLBACK TO SAVEPOINT s1"); err != nil {
		t.Fatalf("ROLLBACK TO SAVEPOINT s1 failed: %v", err)
	}
	_ = server.TestSpanner.DrainRequestsFromServer()

	opCtx := newControllableDeadlineContext(true)
	defer opCtx.cancel()
	failRPCOnHook(server, testutil.MethodExecuteStreamingSql, opCtx.expire)
	_, timeoutErr := tx.ExecContext(opCtx, dml3)
	server.TestSpanner.SetMethodHook(testutil.MethodExecuteStreamingSql, nil)

	assertContextError(t, timeoutErr, context.DeadlineExceeded, codes.DeadlineExceeded)
	if g, w := ToPGSQLState(timeoutErr), "57014"; g != w {
		t.Errorf("PG SQLSTATE mismatch\nGot:  %v\nWant: %v", g, w)
	}
	if g, w := getPGTransactionState(t, dbConn), connectionstate.TransactionStateFailed; g != w {
		t.Fatalf("PG state after ensureReplayed timeout mismatch\nGot:  %v\nWant: %v", g, w)
	}

	var gotState readWriteTxState
	if err := dbConn.Raw(func(driverConn any) error {
		c := driverConn.(*conn)
		gotState = c.tx.contextTransaction.(*readWriteTransaction).state
		return nil
	}); err != nil {
		t.Fatalf("dbConn.Raw failed: %v", err)
	}
	if g, w := gotState, readWriteTxStateNeedsReplay; g != w {
		t.Errorf("rwTx state in PG error state mismatch\nGot:  %v\nWant: %v", g, w)
	}

	if _, err := tx.ExecContext(ctx, dml3); err == nil {
		t.Fatal("expected 25P02 error while in PG error state, got nil")
	} else if g, w := ToPGSQLState(err), "25P02"; g != w {
		t.Errorf("PG SQLSTATE in state 'E' mismatch\nGot:  %v\nWant: %v", g, w)
	}

	if _, err := tx.ExecContext(ctx, "ROLLBACK TO SAVEPOINT s1"); err != nil {
		t.Fatalf("ROLLBACK TO SAVEPOINT s1 recovery failed: %v", err)
	}
	if g, w := getPGTransactionState(t, dbConn), connectionstate.TransactionStateTransaction; g != w {
		t.Fatalf("PG state after savepoint rollback mismatch\nGot:  %v\nWant: %v", g, w)
	}
	_ = server.TestSpanner.DrainRequestsFromServer()

	if _, err := tx.ExecContext(ctx, dml3); err != nil {
		t.Fatalf("recovery dml3 failed: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("Commit failed after recovery: %v", err)
	}
	if g, w := getPGTransactionState(t, dbConn), connectionstate.TransactionStateIdle; g != w {
		t.Fatalf("PG state after commit mismatch\nGot:  %v\nWant: %v", g, w)
	}

	reqs := server.TestSpanner.DrainRequestsFromServer()
	execReqs := testutil.RequestsOfType(reqs, reflect.TypeOf(&spannerpb.ExecuteSqlRequest{}))
	if g, w := len(execReqs), 2; g != w {
		t.Fatalf("recovery ExecuteSqlRequest count mismatch\nGot:  %v\nWant: %v", g, w)
	}
	if g, w := execReqs[0].(*spannerpb.ExecuteSqlRequest).Sql, dml1; g != w {
		t.Errorf("recovery SQL[0] mismatch\nGot:  %v\nWant: %v", g, w)
	}
	if g, w := execReqs[1].(*spannerpb.ExecuteSqlRequest).Sql, dml3; g != w {
		t.Errorf("recovery SQL[1] mismatch\nGot:  %v\nWant: %v", g, w)
	}
}

func TestSavepoint_EnsureReplayed_AbortedThenOpTimeoutRecovers(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		sleepTimeout bool
	}{
		{name: "sleep_timeout_during_aborted_retry", sleepTimeout: true},
		{name: "replay_rpc_timeout_during_aborted_retry"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			db, server, teardown := setupTestDBConnectionWithParams(t, "beginTransactionOption=ExplicitBeginTransaction")
			defer teardown()
			ctx := context.Background()

			dbConn, err := db.Conn(ctx)
			if err != nil {
				t.Fatalf("db.Conn failed: %v", err)
			}
			defer silentClose(dbConn)

			opCtx := newControllableDeadlineContext(true)
			defer opCtx.cancel()

			var timeoutErr, recoveryErr, commitErr error
			var gotState readWriteTxState
			var gotRwTx *spanner.ReadWriteStmtBasedTransaction
			var rollbackCount, recoveryExecSQLCount int
			if err := dbConn.Raw(func(driverConn any) error {
				c := driverConn.(*conn)
				dtx, err := c.BeginTx(ctx, driver.TxOptions{})
				if err != nil {
					return err
				}
				defer func() { _ = dtx.Rollback() }()

				if _, err := c.ExecContext(ctx, testutil.UpdateBarSetFoo, nil); err != nil {
					return fmt.Errorf("initial DML failed: %w", err)
				}
				if err := createAndRollbackConnSavepoint(ctx, c, testutil.UpdateSingersSetLastName); err != nil {
					return err
				}
				_ = server.TestSpanner.DrainRequestsFromServer()

				rwTx := c.tx.contextTransaction.(*readWriteTransaction)
				if tc.sleepTimeout {
					rwTx.sleep = func(sleepCtx context.Context, delay time.Duration) error {
						opCtx.expire()
						return gax.Sleep(sleepCtx, delay)
					}
					server.TestSpanner.PutExecutionTime(testutil.MethodExecuteStreamingSql, testutil.SimulatedExecutionTime{
						Errors: []error{abortedWithRetryDelay(10 * time.Second)},
					})
				} else {
					var execCalls atomic.Int32
					server.TestSpanner.SetMethodHook(testutil.MethodExecuteStreamingSql, func(srvCtx context.Context, _ interface{}) error {
						if execCalls.Add(1) == 1 {
							return nil
						}
						opCtx.expire()
						<-srvCtx.Done()
						return status.FromContextError(srvCtx.Err()).Err()
					})
					server.TestSpanner.PutExecutionTime(testutil.MethodExecuteStreamingSql, testutil.SimulatedExecutionTime{
						Errors: []error{abortedWithRetryDelay(time.Nanosecond)},
					})
				}

				_, timeoutErr = c.ExecContext(opCtx, testutil.UpdateSingersSetLastName, nil)
				rwTx.sleep = nil
				server.TestSpanner.SetMethodHook(testutil.MethodExecuteStreamingSql, nil)
				server.TestSpanner.PutExecutionTime(testutil.MethodExecuteStreamingSql, testutil.SimulatedExecutionTime{})

				gotState = rwTx.state
				gotRwTx = rwTx.rwTx
				failedReqs := server.TestSpanner.DrainRequestsFromServer()
				rollbackCount = len(testutil.RequestsOfType(failedReqs, reflect.TypeOf(&spannerpb.RollbackRequest{})))

				_, recoveryErr = c.ExecContext(ctx, testutil.UpdateSingersSetLastName, nil)
				if recoveryErr == nil {
					commitErr = dtx.Commit()
				}
				reqs := server.TestSpanner.DrainRequestsFromServer()
				recoveryExecSQLCount = len(testutil.RequestsOfType(reqs, reflect.TypeOf(&spannerpb.ExecuteSqlRequest{})))
				return nil
			}); err != nil {
				t.Fatalf("dbConn.Raw failed: %v", err)
			}

			assertContextError(t, timeoutErr, context.DeadlineExceeded, codes.DeadlineExceeded)
			if g, w := gotState, readWriteTxStateNeedsReplay; g != w {
				t.Errorf("tx.state mismatch\nGot:  %v\nWant: %v", g, w)
			}
			if g, w := gotRwTx, (*spanner.ReadWriteStmtBasedTransaction)(nil); g != w {
				t.Errorf("rwTx after failed aborted replay mismatch\nGot:  %v\nWant: %v", g, w)
			}
			if g, w := rollbackCount, 1; g != w {
				t.Errorf("RollbackRequest count mismatch\nGot:  %v\nWant: %v", g, w)
			}
			if recoveryErr != nil {
				t.Fatalf("recovery ExecContext failed: %v", recoveryErr)
			}
			if commitErr != nil {
				t.Fatalf("recovery Commit failed: %v", commitErr)
			}
			if g, w := recoveryExecSQLCount, 2; g != w {
				t.Errorf("recovery ExecuteSqlRequest count mismatch\nGot:  %v\nWant: %v", g, w)
			}
		})
	}
}
