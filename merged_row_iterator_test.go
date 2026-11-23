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
	"errors"
	"io"
	"log/slog"
	"runtime"
	"slices"
	"sync"
	"testing"
	"time"

	"cloud.google.com/go/spanner"
	sppb "cloud.google.com/go/spanner/apiv1/spannerpb"
	"github.com/googleapis/go-sql-spanner/testutil"
	"google.golang.org/api/iterator"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func setupTestPartitionedQuery(t *testing.T, maxPartitions int, rowsPerPartition [][]int64) (*PartitionedQuery, *testutil.MockedSpannerInMemTestServer, func()) {
	t.Helper()

	ctx := context.Background()
	db, server, teardown := setupTestDBConnection(t)
	tx, err := BeginBatchReadOnlyTransaction(ctx, db, BatchReadOnlyTransactionOptions{})
	if err != nil {
		teardown()
		t.Fatalf("failed to begin batch read-only transaction: %v", err)
	}

	rows, err := tx.QueryContext(ctx, testutil.SelectFooFromBar, ExecOptions{
		PartitionedQueryOptions: PartitionedQueryOptions{
			PartitionQuery: true,
			PartitionOptions: spanner.PartitionOptions{
				MaxPartitions: int64(maxPartitions),
			},
		},
	})
	if err != nil {
		_ = tx.Rollback()
		teardown()
		t.Fatalf("failed to partition query: %v", err)
	}
	var pq PartitionedQuery
	if !rows.Next() {
		err := rows.Err()
		_ = rows.Close()
		_ = tx.Rollback()
		teardown()
		t.Fatalf("expected partitioned query row, got none (err=%v)", err)
	}
	if err := rows.Scan(&pq); err != nil {
		_ = rows.Close()
		_ = tx.Rollback()
		teardown()
		t.Fatalf("failed to scan partitioned query: %v", err)
	}
	_ = rows.Close()

	for index, partition := range pq.Partitions {
		var partitionRows []int64
		if index < len(rowsPerPartition) {
			partitionRows = rowsPerPartition[index]
		}
		res := testutil.CreateSingleColumnInt64ResultSet(partitionRows, "FOO")
		if err := server.TestSpanner.PutPartitionResult(
			partition.GetPartitionToken(),
			&testutil.StatementResult{Type: testutil.StatementResultResultSet, ResultSet: res},
		); err != nil {
			_ = tx.Rollback()
			teardown()
			t.Fatalf("failed to put partition result: %v", err)
		}
	}

	cleanup := func() {
		_ = tx.Commit()
		teardown()
	}
	return &pq, server, cleanup
}

func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func newTestPartitionedQuery(numPartitions int) *PartitionedQuery {
	return &PartitionedQuery{
		stmt:       spanner.Statement{SQL: testutil.SelectFooFromBar},
		Partitions: make([]*spanner.Partition, numPartitions),
	}
}

// Verifies that when all producers finish while rows remain buffered, all
// buffered rows are returned before Next reports iterator.Done.
func TestMergedRowIterator_BufferedRowsAfterProducersDone(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	// Total rows (5) fits within the buffer capacity (10), allowing all producers
	// to push every row and finish before Next() is called.
	pq, _, cleanup := setupTestPartitionedQuery(t, 2, [][]int64{{1, 2, 3}, {4, 5}})
	defer cleanup()

	mi := createMergedIterator(discardLogger(), pq, 2)
	if err := mi.run(ctx); err != nil {
		t.Fatalf("run failed: %v", err)
	}

	// Wait until all producers have exited and closed the buffer.
	select {
	case <-mi.producersDone:
	case <-time.After(5 * time.Second):
		mi.Stop()
		t.Fatal("timed out waiting for producersDone")
	}

	// Normal producer completion must not close done (reserved for Stop()),
	// otherwise Next() could select <-done and drop buffered rows.
	select {
	case <-mi.done:
		mi.Stop()
		t.Fatal("done channel closed on normal producer completion before Stop()")
	default:
	}

	var got []int64
	for {
		row, err := mi.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			mi.Stop()
			t.Fatalf("Next failed: %v", err)
		}
		var v int64
		if err := row.Column(0, &v); err != nil {
			mi.Stop()
			t.Fatalf("failed to read column: %v", err)
		}
		got = append(got, v)
	}
	mi.Stop()

	slices.Sort(got)
	if g, w := got, []int64{1, 2, 3, 4, 5}; !slices.Equal(g, w) {
		t.Fatalf("rows mismatch\nGot:  %v\nWant: %v", g, w)
	}
}

// Verifies that running with zero partitions fails with FailedPrecondition and
// calling Stop afterward does not deadlock.
func TestMergedRowIterator_ZeroPartitions(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	mi := createMergedIterator(discardLogger(), newTestPartitionedQuery(0), 2)

	err := mi.run(ctx)
	if g, w := spanner.ErrCode(err), codes.FailedPrecondition; g != w {
		t.Fatalf("error code mismatch\nGot:  %v\nWant: %v", g, w)
	}

	done := make(chan struct{})
	go func() {
		mi.Stop()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Stop() deadlocked when zero partitions were present")
	}
}

type mockPartitionIterator struct {
	ctx         context.Context
	rows        []*spanner.Row
	index       int
	metadata    *sppb.ResultSetMetadata
	metadataErr error
	onMetadata  func()
	nextErr     error
	errAfterRow int
	barrier     *sync.WaitGroup
	waitCh      <-chan struct{}
	blockOnCtx  bool
	onBlock     func()
	onRow       func(index int)
	stopped     bool
	mu          sync.Mutex
}

func (m *mockPartitionIterator) Next() (*spanner.Row, error) {
	if m.barrier != nil && m.index == 0 {
		m.barrier.Done()
		m.barrier.Wait()
	}
	if m.waitCh != nil && m.index == 0 {
		select {
		case <-m.waitCh:
		case <-m.ctx.Done():
			return nil, m.ctx.Err()
		}
	}
	if m.nextErr != nil && m.index >= m.errAfterRow {
		return nil, m.nextErr
	}
	if m.index < len(m.rows) {
		idx := m.index
		row := m.rows[idx]
		m.index++
		if m.onRow != nil {
			m.onRow(idx)
		}
		return row, nil
	}
	if m.blockOnCtx {
		if m.onBlock != nil {
			m.onBlock()
		}
		<-m.ctx.Done()
		return nil, m.ctx.Err()
	}
	return nil, iterator.Done
}

func (m *mockPartitionIterator) Stop() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.stopped = true
}

func (m *mockPartitionIterator) isStopped() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.stopped
}

func (m *mockPartitionIterator) Metadata() (*sppb.ResultSetMetadata, error) {
	if m.onMetadata != nil {
		m.onMetadata()
	}
	return m.metadata, m.metadataErr
}

func (m *mockPartitionIterator) ResultSetStats() *sppb.ResultSetStats {
	return &sppb.ResultSetStats{}
}

// Verifies that Stop unblocks producers waiting on a full row buffer and that
// subsequent Next calls return iterator.Done.
func TestMergedRowIterator_StopWhileProducersBlocked(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	partitionRows := make([][]int64, 3)
	for p := range partitionRows {
		partitionRows[p] = make([]int64, 10)
		for j := range partitionRows[p] {
			partitionRows[p][j] = int64(p*10 + j)
		}
	}
	pq, _, cleanup := setupTestPartitionedQuery(t, 3, partitionRows)
	defer cleanup()

	mi := createMergedIterator(discardLogger(), pq, 3)
	if err := mi.run(ctx); err != nil {
		t.Fatalf("run failed: %v", err)
	}

	if _, err := mi.Next(); err != nil {
		mi.Stop()
		t.Fatalf("first Next failed: %v", err)
	}

	stopped := make(chan struct{})
	go func() {
		mi.Stop()
		close(stopped)
	}()
	select {
	case <-stopped:
	case <-time.After(5 * time.Second):
		t.Fatal("Stop() timed out while producers were blocked on buffer")
	}

	_, err := mi.Next()
	if g, w := err, iterator.Done; g != w {
		t.Errorf("Next after Stop error mismatch\nGot:  %v\nWant: %v", g, w)
	}
}

// Verifies that Stop cancels and unblocks a producer stuck inside a partition
// iterator's Next call without deadlocking.
func TestMergedRowIterator_StopWhileProducerBlockedInNext(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	inNext := make(chan struct{})
	p1Ch := make(chan *mockPartitionIterator, 1)
	var p1Once sync.Once

	pq := newTestPartitionedQuery(2)
	mi := createMergedIterator(discardLogger(), pq, 2)
	mi.executePartition = func(childCtx context.Context, index int) (rowIterator, func() error, error) {
		if index == 0 {
			return &mockPartitionIterator{
				ctx:      childCtx,
				rows:     []*spanner.Row{newRow(t, []string{"FOO"}, []any{int64(1)})},
				metadata: &sppb.ResultSetMetadata{RowType: &sppb.StructType{Fields: []*sppb.StructType_Field{{Name: "FOO"}}}},
			}, nil, nil
		}
		p1 := &mockPartitionIterator{
			ctx:        childCtx,
			blockOnCtx: true,
			onBlock: func() {
				p1Once.Do(func() { close(inNext) })
			},
		}
		p1Ch <- p1
		return p1, nil, nil
	}

	if err := mi.run(ctx); err != nil {
		t.Fatalf("run failed: %v", err)
	}

	select {
	case <-inNext:
	case <-time.After(5 * time.Second):
		mi.Stop()
		t.Fatal("timed out waiting for partition 1 to block in Next")
	}

	stopped := make(chan struct{})
	go func() {
		mi.Stop()
		close(stopped)
	}()
	select {
	case <-stopped:
	case <-time.After(5 * time.Second):
		t.Fatal("Stop() deadlocked while producer was blocked in it.Next()")
	}

	p1 := <-p1Ch
	if g, w := p1.isStopped(), true; g != w {
		t.Errorf("partition 1 Stop() called mismatch\nGot:  %v\nWant: %v", g, w)
	}
}

// Verifies that when every partition yields zero rows, Next returns
// iterator.Done and Metadata still returns the schema.
func TestMergedRowIterator_EmptyPartitions(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	pq, _, cleanup := setupTestPartitionedQuery(t, 3, [][]int64{{}, {}, {}})
	defer cleanup()

	mi := createMergedIterator(discardLogger(), pq, 3)
	if err := mi.run(ctx); err != nil {
		t.Fatalf("run failed: %v", err)
	}
	defer mi.Stop()

	select {
	case <-mi.producersDone:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for producersDone")
	}

	_, err := mi.Next()
	if g, w := err, iterator.Done; g != w {
		t.Errorf("Next error mismatch\nGot:  %v\nWant: %v", g, w)
	}
	metadata, err := mi.Metadata()
	if err != nil {
		t.Fatalf("Metadata failed: %v", err)
	}
	if metadata == nil || metadata.RowType == nil {
		t.Fatalf("expected non-nil metadata with RowType, got %v", metadata)
	}
	if g, w := len(metadata.RowType.Fields), 1; g != w {
		t.Errorf("metadata field count mismatch\nGot:  %v\nWant: %v", g, w)
	}
}

// Verifies that when multiple partitions fail concurrently, the error is
// surfaced cleanly without double-closing channels.
func TestMergedRowIterator_ConcurrentErrors(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	wantErr := status.Error(codes.NotFound, "partition not found")
	var barrier sync.WaitGroup
	barrier.Add(4)

	pq := newTestPartitionedQuery(4)
	mi := createMergedIterator(discardLogger(), pq, 4)
	mi.executePartition = func(childCtx context.Context, index int) (rowIterator, func() error, error) {
		return &mockPartitionIterator{
			ctx:     childCtx,
			barrier: &barrier,
			nextErr: wantErr,
		}, nil, nil
	}

	err := mi.run(ctx)
	if g, w := spanner.ErrCode(err), codes.NotFound; g != w {
		t.Fatalf("run error code mismatch\nGot:  %v\nWant: %v (err=%v)", g, w, err)
	}
	mi.Stop()

	_, nextErr := mi.Next()
	if g, w := spanner.ErrCode(nextErr), codes.NotFound; g != w {
		t.Errorf("Next error code mismatch\nGot:  %v\nWant: %v", g, w)
	}
}

// Verifies that concurrent metadata lookup failures across partitions propagate
// the error without panicking.
func TestMergedRowIterator_ConcurrentMetadataError(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	wantErr := errors.New("metadata error")
	var barrier sync.WaitGroup
	barrier.Add(2)

	pq := newTestPartitionedQuery(2)
	mi := createMergedIterator(discardLogger(), pq, 2)
	mi.executePartition = func(childCtx context.Context, index int) (rowIterator, func() error, error) {
		return &mockPartitionIterator{
			ctx:         childCtx,
			barrier:     &barrier,
			metadataErr: wantErr,
		}, nil, nil
	}

	if g, w := mi.run(ctx), wantErr; !errors.Is(g, w) {
		t.Fatalf("run error mismatch\nGot:  %v\nWant: %v", g, w)
	}
	mi.Stop()

	_, nextErr := mi.Next()
	if g, w := nextErr, wantErr; !errors.Is(g, w) {
		t.Errorf("Next error mismatch\nGot:  %v\nWant: %v", g, w)
	}
}

// Verifies that if Stop runs while a partition's Metadata call is about to fail,
// run and Metadata report Canceled instead of nil.
func TestMergedRowIterator_MetadataErrorRacingWithStop(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	inMetadata := make(chan struct{})
	proceedMetadata := make(chan struct{})
	var once sync.Once

	pq := newTestPartitionedQuery(1)
	mi := createMergedIterator(discardLogger(), pq, 1)
	mi.executePartition = func(childCtx context.Context, index int) (rowIterator, func() error, error) {
		return &mockPartitionIterator{
			ctx:         childCtx,
			metadataErr: errors.New("metadata failed"),
			onMetadata: func() {
				once.Do(func() { close(inMetadata) })
				<-proceedMetadata
			},
		}, nil, nil
	}

	runErrCh := make(chan error, 1)
	go func() {
		runErrCh <- mi.run(ctx)
	}()

	select {
	case <-inMetadata:
	case <-time.After(5 * time.Second):
		close(proceedMetadata)
		mi.Stop()
		t.Fatal("timed out waiting for Metadata() call")
	}

	// Stop while Metadata() is in progress and about to return an error.
	stopped := make(chan struct{})
	go func() {
		mi.Stop()
		close(stopped)
	}()
	select {
	case <-mi.done:
	case <-time.After(5 * time.Second):
		close(proceedMetadata)
		t.Fatal("timed out waiting for Stop() to close done")
	}
	close(proceedMetadata)

	select {
	case <-stopped:
	case <-time.After(5 * time.Second):
		t.Fatal("Stop() timed out")
	}

	var runErr error
	select {
	case runErr = <-runErrCh:
	case <-time.After(5 * time.Second):
		t.Fatal("run() timed out")
	}
	if g, w := spanner.ErrCode(runErr), codes.Canceled; g != w {
		t.Errorf("run error code mismatch\nGot:  %v\nWant: %v (err=%v)", g, w, runErr)
	}
	_, metaErr := mi.Metadata()
	if g, w := spanner.ErrCode(metaErr), codes.Canceled; g != w {
		t.Errorf("Metadata error code mismatch\nGot:  %v\nWant: %v (err=%v)", g, w, metaErr)
	}
}

// Verifies that an error returned when opening a partition is propagated by
// both run and Next.
func TestMergedRowIterator_ExecutePartitionError(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	wantErr := status.Error(codes.Internal, "execute failed")
	pq := newTestPartitionedQuery(2)
	mi := createMergedIterator(discardLogger(), pq, 2)
	mi.executePartition = func(childCtx context.Context, index int) (rowIterator, func() error, error) {
		return nil, nil, wantErr
	}

	if g, w := mi.run(ctx), wantErr; !errors.Is(g, w) {
		t.Fatalf("run error mismatch\nGot:  %v\nWant: %v", g, w)
	}
	_, nextErr := mi.Next()
	if g, w := nextErr, wantErr; !errors.Is(g, w) {
		t.Errorf("Next error mismatch\nGot:  %v\nWant: %v", g, w)
	}
}

// Verifies that an error on one partition after metadata is ready cancels
// sibling partitions blocked in Next.
func TestMergedRowIterator_ErrorAfterMetadataCancelsSiblings(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	wantErr := status.Error(codes.Unavailable, "stream broken")
	p0Blocked := make(chan struct{})
	runDone := make(chan struct{})
	var p0Once sync.Once

	pq := newTestPartitionedQuery(2)
	mi := createMergedIterator(discardLogger(), pq, 2)
	mi.executePartition = func(childCtx context.Context, index int) (rowIterator, func() error, error) {
		if index == 0 {
			return &mockPartitionIterator{
				ctx:        childCtx,
				rows:       []*spanner.Row{newRow(t, []string{"FOO"}, []any{int64(42)})},
				metadata:   &sppb.ResultSetMetadata{RowType: &sppb.StructType{Fields: []*sppb.StructType_Field{{Name: "FOO"}}}},
				blockOnCtx: true,
				onBlock: func() {
					p0Once.Do(func() { close(p0Blocked) })
				},
			}, nil, nil
		}
		return &mockPartitionIterator{
			ctx:     childCtx,
			waitCh:  runDone,
			nextErr: wantErr,
		}, nil, nil
	}

	if err := mi.run(ctx); err != nil {
		t.Fatalf("run failed: %v", err)
	}
	select {
	case <-p0Blocked:
	case <-time.After(5 * time.Second):
		close(runDone)
		mi.Stop()
		t.Fatal("timed out waiting for partition 0 to block in Next")
	}
	close(runDone)

	// Wait until all producers exit (registerErrLocked cancels childCtx so p0 unblocks).
	select {
	case <-mi.producersDone:
	case <-time.After(5 * time.Second):
		mi.Stop()
		t.Fatal("timed out waiting for producersDone after sibling error")
	}

	_, nextErr := mi.Next()
	if g, w := nextErr, wantErr; !errors.Is(g, w) {
		t.Errorf("Next error mismatch\nGot:  %v\nWant: %v", g, w)
	}
	mi.Stop()
}

// Verifies that a caller waiting inside Next on an empty buffer wakes up
// immediately with the error when a producer fails.
func TestMergedRowIterator_NextUnblocksOnProducerError(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	wantErr := status.Error(codes.Unavailable, "mid-stream error")
	releaseErr := make(chan struct{})

	pq := newTestPartitionedQuery(2)
	mi := createMergedIterator(discardLogger(), pq, 2)
	mi.executePartition = func(childCtx context.Context, index int) (rowIterator, func() error, error) {
		if index == 0 {
			return &mockPartitionIterator{
				ctx:      childCtx,
				rows:     []*spanner.Row{newRow(t, []string{"FOO"}, []any{int64(7)})},
				metadata: &sppb.ResultSetMetadata{RowType: &sppb.StructType{Fields: []*sppb.StructType_Field{{Name: "FOO"}}}},
			}, nil, nil
		}
		return &mockPartitionIterator{
			ctx:     childCtx,
			waitCh:  releaseErr,
			nextErr: wantErr,
		}, nil, nil
	}

	if err := mi.run(ctx); err != nil {
		t.Fatalf("run failed: %v", err)
	}
	defer mi.Stop()

	// Consume the single row from partition 0 so buffer is empty.
	row, err := mi.Next()
	if err != nil {
		t.Fatalf("first Next failed: %v", err)
	}
	var gotVal int64
	if err := row.Column(0, &gotVal); err != nil {
		t.Fatalf("failed to read row: %v", err)
	}
	if g, w := gotVal, int64(7); g != w {
		t.Fatalf("row value mismatch\nGot:  %v\nWant: %v", g, w)
	}

	nextErrCh := make(chan error, 1)
	go func() {
		_, nextErr := mi.Next()
		nextErrCh <- nextErr
	}()

	close(releaseErr)
	select {
	case gotErr := <-nextErrCh:
		if g, w := gotErr, wantErr; !errors.Is(g, w) {
			t.Errorf("Next error mismatch\nGot:  %v\nWant: %v", g, w)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Next() timed out waiting for producer error")
	}
}

// Verifies that all partition rows are returned both when partitions outnumber
// workers and when workers outnumber partitions.
func TestMergedRowIterator_Parallelism(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		numPartitions  int
		maxParallelism int
		want           []int64
	}{
		{
			name:           "more partitions than workers",
			numPartitions:  5,
			maxParallelism: 2,
			want:           []int64{0, 1, 2, 3, 4},
		},
		{
			name:           "fewer partitions than workers",
			numPartitions:  2,
			maxParallelism: 8,
			want:           []int64{0, 1},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			ctx := context.Background()
			pq := newTestPartitionedQuery(tc.numPartitions)
			mi := createMergedIterator(discardLogger(), pq, tc.maxParallelism)
			mi.executePartition = func(childCtx context.Context, index int) (rowIterator, func() error, error) {
				return &mockPartitionIterator{
					ctx:      childCtx,
					rows:     []*spanner.Row{newRow(t, []string{"FOO"}, []any{int64(index)})},
					metadata: &sppb.ResultSetMetadata{RowType: &sppb.StructType{Fields: []*sppb.StructType_Field{{Name: "FOO"}}}},
				}, nil, nil
			}

			if err := mi.run(ctx); err != nil {
				t.Fatalf("run failed: %v", err)
			}
			defer mi.Stop()

			var got []int64
			for {
				row, err := mi.Next()
				if err == iterator.Done {
					break
				}
				if err != nil {
					t.Fatalf("Next failed: %v", err)
				}
				var v int64
				if err := row.Column(0, &v); err != nil {
					t.Fatalf("Column failed: %v", err)
				}
				got = append(got, v)
			}
			slices.Sort(got)
			if g, w := got, tc.want; !slices.Equal(g, w) {
				t.Errorf("rows mismatch\nGot:  %v\nWant: %v", g, w)
			}
		})
	}
}

// Verifies that once a partition fails, workers stop claiming any remaining
// unclaimed partitions.
func TestMergedRowIterator_ErrorStopsRemainingPartitions(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	wantErr := status.Error(codes.NotFound, "partition missing")
	var executedCount int
	var mu sync.Mutex

	pq := newTestPartitionedQuery(5)
	mi := createMergedIterator(discardLogger(), pq, 1)
	mi.executePartition = func(childCtx context.Context, index int) (rowIterator, func() error, error) {
		mu.Lock()
		executedCount++
		mu.Unlock()
		return &mockPartitionIterator{
			ctx:     childCtx,
			nextErr: wantErr,
		}, nil, nil
	}

	err := mi.run(ctx)
	if g, w := err, wantErr; !errors.Is(g, w) {
		t.Fatalf("run error mismatch\nGot:  %v\nWant: %v", g, w)
	}
	mi.Stop()

	mu.Lock()
	defer mu.Unlock()
	if g, w := executedCount, 1; g != w {
		t.Errorf("executed partition count mismatch\nGot:  %v\nWant: %v", g, w)
	}
}

// Verifies that each partition's close callback is invoked on normal
// completion, on partition error, and when canceled by a sibling error.
func TestMergedRowIterator_CloseFnCalledOnCompletionErrorAndStop(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	var closedCount int
	var mu sync.Mutex
	var execWG sync.WaitGroup
	execWG.Add(3)
	startErr := make(chan struct{})

	pq := newTestPartitionedQuery(3)
	mi := createMergedIterator(discardLogger(), pq, 3)
	mi.executePartition = func(childCtx context.Context, index int) (rowIterator, func() error, error) {
		execWG.Done()
		closeFn := func() error {
			mu.Lock()
			closedCount++
			mu.Unlock()
			return nil
		}
		switch index {
		case 0:
			return &mockPartitionIterator{
				ctx:      childCtx,
				rows:     []*spanner.Row{newRow(t, []string{"FOO"}, []any{int64(1)})},
				metadata: &sppb.ResultSetMetadata{RowType: &sppb.StructType{Fields: []*sppb.StructType_Field{{Name: "FOO"}}}},
			}, closeFn, nil
		case 1:
			return &mockPartitionIterator{
				ctx:     childCtx,
				waitCh:  startErr,
				nextErr: status.Error(codes.Internal, "partition error"),
			}, closeFn, nil
		default:
			return &mockPartitionIterator{
				ctx:        childCtx,
				blockOnCtx: true,
			}, closeFn, nil
		}
	}

	if err := mi.run(ctx); err != nil {
		close(startErr)
		mi.Stop()
		t.Fatalf("run failed: %v", err)
	}

	// Ensure all 3 partitions have entered executePartition before partition 1 fails.
	execDone := make(chan struct{})
	go func() {
		execWG.Wait()
		close(execDone)
	}()
	select {
	case <-execDone:
	case <-time.After(5 * time.Second):
		close(startErr)
		mi.Stop()
		t.Fatal("timed out waiting for all partitions to start")
	}

	close(startErr)
	mi.Stop()

	mu.Lock()
	defer mu.Unlock()
	if g, w := closedCount, 3; g != w {
		t.Errorf("closeFn call count mismatch\nGot:  %v\nWant: %v", g, w)
	}
}

// Verifies that a producer blocked sending into a full row buffer unblocks
// immediately when a sibling partition registers an error, without calling Stop.
func TestMergedRowIterator_BufferBlockedProducerUnblocksOnSiblingError(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	wantErr := status.Error(codes.Unavailable, "sibling failed")
	p0BufferFull := make(chan struct{})
	runDone := make(chan struct{})
	var p0Once sync.Once

	// Buffer capacity is 10; 11 rows ensures rows 0..9 fill m.buffer and row 10
	// blocks in m.buffer <- row after onRow(10) closes p0BufferFull.
	p0Rows := make([]*spanner.Row, 11)
	for i := range p0Rows {
		p0Rows[i] = newRow(t, []string{"FOO"}, []any{int64(i)})
	}

	pq := newTestPartitionedQuery(2)
	mi := createMergedIterator(discardLogger(), pq, 2)
	mi.executePartition = func(childCtx context.Context, index int) (rowIterator, func() error, error) {
		if index == 0 {
			return &mockPartitionIterator{
				ctx:      childCtx,
				rows:     p0Rows,
				metadata: &sppb.ResultSetMetadata{RowType: &sppb.StructType{Fields: []*sppb.StructType_Field{{Name: "FOO"}}}},
				onRow: func(rowIdx int) {
					if rowIdx == 10 {
						p0Once.Do(func() { close(p0BufferFull) })
					}
				},
			}, nil, nil
		}
		return &mockPartitionIterator{
			ctx:     childCtx,
			waitCh:  runDone,
			nextErr: wantErr,
		}, nil, nil
	}

	if err := mi.run(ctx); err != nil {
		close(runDone)
		t.Fatalf("run failed: %v", err)
	}

	select {
	case <-p0BufferFull:
	case <-time.After(5 * time.Second):
		close(runDone)
		mi.Stop()
		t.Fatal("timed out waiting for partition 0 to fill buffer")
	}
	close(runDone)

	select {
	case <-mi.producersDone:
	case <-time.After(5 * time.Second):
		mi.Stop()
		t.Fatal("timed out waiting for buffer-blocked producer to exit on sibling error")
	}

	_, nextErr := mi.Next()
	if g, w := nextErr, wantErr; !errors.Is(g, w) {
		t.Errorf("Next error mismatch\nGot:  %v\nWant: %v", g, w)
	}
	mi.Stop()
}

// Verifies that a caller waiting inside Next on an empty buffer unblocks with
// iterator.Done when Stop is called.
func TestMergedRowIterator_NextUnblocksOnStop(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	inNext := make(chan struct{})
	var once sync.Once

	pq := newTestPartitionedQuery(2)
	mi := createMergedIterator(discardLogger(), pq, 2)
	mi.executePartition = func(childCtx context.Context, index int) (rowIterator, func() error, error) {
		if index == 0 {
			return &mockPartitionIterator{
				ctx:      childCtx,
				rows:     []*spanner.Row{newRow(t, []string{"FOO"}, []any{int64(1)})},
				metadata: &sppb.ResultSetMetadata{RowType: &sppb.StructType{Fields: []*sppb.StructType_Field{{Name: "FOO"}}}},
			}, nil, nil
		}
		return &mockPartitionIterator{
			ctx:        childCtx,
			blockOnCtx: true,
			onBlock: func() {
				once.Do(func() { close(inNext) })
			},
		}, nil, nil
	}

	if err := mi.run(ctx); err != nil {
		t.Fatalf("run failed: %v", err)
	}

	// Drain partition 0's row so the buffer is empty.
	if _, err := mi.Next(); err != nil {
		mi.Stop()
		t.Fatalf("first Next failed: %v", err)
	}
	select {
	case <-inNext:
	case <-time.After(5 * time.Second):
		mi.Stop()
		t.Fatal("timed out waiting for partition 1 to block in Next")
	}

	nextErrCh := make(chan error, 1)
	go func() {
		_, err := mi.Next()
		nextErrCh <- err
	}()

	mi.Stop()

	select {
	case err := <-nextErrCh:
		if g, w := err, iterator.Done; g != w {
			t.Errorf("Next error mismatch\nGot:  %v\nWant: %v", g, w)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Next() timed out waiting for Stop()")
	}
}

// Verifies that canceling the caller's context mid-stream cancels active
// producers and surfaces context.Canceled from Next.
func TestMergedRowIterator_ParentContextCanceled(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	inNext := make(chan struct{})
	var once sync.Once

	pq := newTestPartitionedQuery(2)
	mi := createMergedIterator(discardLogger(), pq, 2)
	mi.executePartition = func(childCtx context.Context, index int) (rowIterator, func() error, error) {
		if index == 0 {
			return &mockPartitionIterator{
				ctx:      childCtx,
				rows:     []*spanner.Row{newRow(t, []string{"FOO"}, []any{int64(1)})},
				metadata: &sppb.ResultSetMetadata{RowType: &sppb.StructType{Fields: []*sppb.StructType_Field{{Name: "FOO"}}}},
			}, nil, nil
		}
		return &mockPartitionIterator{
			ctx:        childCtx,
			blockOnCtx: true,
			onBlock: func() {
				once.Do(func() { close(inNext) })
			},
		}, nil, nil
	}

	if err := mi.run(ctx); err != nil {
		cancel()
		t.Fatalf("run failed: %v", err)
	}
	defer mi.Stop()

	if _, err := mi.Next(); err != nil {
		cancel()
		t.Fatalf("first Next failed: %v", err)
	}

	select {
	case <-inNext:
	case <-time.After(5 * time.Second):
		cancel()
		t.Fatal("timed out waiting for partition 1 to enter Next")
	}

	cancel()

	select {
	case <-mi.producersDone:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for producersDone after parent ctx cancellation")
	}

	_, err := mi.Next()
	if g, w := err, context.Canceled; !errors.Is(g, w) {
		t.Errorf("Next error mismatch\nGot:  %v\nWant: %v", g, w)
	}
}

// Verifies that a producer blocked sending into a full row buffer unblocks
// immediately when the parent context is canceled and Next surfaces context.Canceled.
func TestMergedRowIterator_BufferBlockedProducerUnblocksOnParentContextCancel(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	bufferFull := make(chan struct{})
	var once sync.Once

	// Buffer capacity is 10; 11 rows ensures rows 0..9 fill m.buffer and row 10
	// blocks in m.buffer <- row after onRow(10) closes bufferFull.
	rows := make([]*spanner.Row, 11)
	for i := range rows {
		rows[i] = newRow(t, []string{"FOO"}, []any{int64(i)})
	}

	pq := newTestPartitionedQuery(1)
	mi := createMergedIterator(discardLogger(), pq, 1)
	mi.executePartition = func(childCtx context.Context, index int) (rowIterator, func() error, error) {
		return &mockPartitionIterator{
			ctx:      childCtx,
			rows:     rows,
			metadata: &sppb.ResultSetMetadata{RowType: &sppb.StructType{Fields: []*sppb.StructType_Field{{Name: "FOO"}}}},
			onRow: func(rowIdx int) {
				if rowIdx == 10 {
					once.Do(func() { close(bufferFull) })
				}
			},
		}, nil, nil
	}

	if err := mi.run(ctx); err != nil {
		t.Fatalf("run failed: %v", err)
	}
	defer mi.Stop()

	select {
	case <-bufferFull:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for producer to fill buffer")
	}

	cancel()

	select {
	case <-mi.producersDone:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for buffer-blocked producer to exit on parent ctx cancellation")
	}

	_, err := mi.Next()
	if g, w := err, context.Canceled; !errors.Is(g, w) {
		t.Errorf("Next error mismatch\nGot:  %v\nWant: %v", g, w)
	}
}

// Verifies that calling Stop before metadata arrives unblocks run with
// Canceled.
func TestMergedRowIterator_StopBeforeMetadata(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	inNext := make(chan struct{})
	var once sync.Once

	pq := newTestPartitionedQuery(1)
	mi := createMergedIterator(discardLogger(), pq, 1)
	mi.executePartition = func(childCtx context.Context, index int) (rowIterator, func() error, error) {
		return &mockPartitionIterator{
			ctx:        childCtx,
			blockOnCtx: true,
			onBlock: func() {
				once.Do(func() { close(inNext) })
			},
		}, nil, nil
	}

	runErrCh := make(chan error, 1)
	go func() {
		runErrCh <- mi.run(ctx)
	}()

	select {
	case <-inNext:
	case <-time.After(5 * time.Second):
		mi.Stop()
		t.Fatal("timed out waiting for producer to enter Next")
	}

	mi.Stop()

	select {
	case err := <-runErrCh:
		if g, w := spanner.ErrCode(err), codes.Canceled; g != w {
			t.Errorf("run error code mismatch\nGot:  %v\nWant: %v (err=%v)", g, w, err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("run() did not return after Stop()")
	}
}

// Verifies that Metadata still returns previously fetched metadata after Stop
// is called.
func TestMergedRowIterator_MetadataAfterStop(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	wantMeta := &sppb.ResultSetMetadata{
		RowType: &sppb.StructType{Fields: []*sppb.StructType_Field{{Name: "FOO"}}},
	}
	pq := newTestPartitionedQuery(1)
	mi := createMergedIterator(discardLogger(), pq, 1)
	mi.executePartition = func(childCtx context.Context, index int) (rowIterator, func() error, error) {
		return &mockPartitionIterator{
			ctx:      childCtx,
			metadata: wantMeta,
		}, nil, nil
	}

	if err := mi.run(ctx); err != nil {
		t.Fatalf("run failed: %v", err)
	}
	mi.Stop()

	gotMeta, err := mi.Metadata()
	if err != nil {
		t.Fatalf("Metadata after Stop failed: %v", err)
	}
	if g, w := gotMeta, wantMeta; g != w {
		t.Errorf("Metadata mismatch\nGot:  %v\nWant: %v", g, w)
	}
}

// Verifies that nil metadata obtained before Stop is still returned as
// (nil, nil) rather than Canceled.
func TestMergedRowIterator_NilMetadataFollowedByStop(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	pq := newTestPartitionedQuery(1)
	mi := createMergedIterator(discardLogger(), pq, 1)
	mi.executePartition = func(childCtx context.Context, index int) (rowIterator, func() error, error) {
		return &mockPartitionIterator{
			ctx:      childCtx,
			metadata: nil,
		}, nil, nil
	}

	if err := mi.run(ctx); err != nil {
		t.Fatalf("run failed: %v", err)
	}
	mi.Stop()

	gotMeta, err := mi.Metadata()
	if err != nil {
		t.Fatalf("Metadata after Stop failed: %v", err)
	}
	if gotMeta != nil {
		t.Errorf("Metadata mismatch\nGot:  %v\nWant: nil", gotMeta)
	}
}

// Verifies that calling run on an already-stopped iterator returns Canceled
// without starting producers.
func TestMergedRowIterator_StopBeforeRun(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	mi := createMergedIterator(discardLogger(), newTestPartitionedQuery(1), 1)
	mi.Stop()
	if g, w := spanner.ErrCode(mi.run(ctx)), codes.Canceled; g != w {
		t.Errorf("run after Stop error code mismatch\nGot:  %v\nWant: %v", g, w)
	}
}

// Verifies that calling run a second time on an already-started iterator fails
// with FailedPrecondition.
func TestMergedRowIterator_DoubleRun(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	mi := createMergedIterator(discardLogger(), newTestPartitionedQuery(1), 1)
	mi.executePartition = func(childCtx context.Context, index int) (rowIterator, func() error, error) {
		return &mockPartitionIterator{
			ctx:      childCtx,
			metadata: &sppb.ResultSetMetadata{},
		}, nil, nil
	}
	if err := mi.run(ctx); err != nil {
		t.Fatalf("first run failed: %v", err)
	}
	defer mi.Stop()
	if g, w := spanner.ErrCode(mi.run(ctx)), codes.FailedPrecondition; g != w {
		t.Errorf("second run error code mismatch\nGot:  %v\nWant: %v", g, w)
	}
}

// Verifies that calling Stop concurrently from multiple goroutines is safe and
// completes without deadlocking.
func TestMergedRowIterator_ConcurrentStop(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	pq := newTestPartitionedQuery(2)
	mi := createMergedIterator(discardLogger(), pq, 2)
	mi.executePartition = func(childCtx context.Context, index int) (rowIterator, func() error, error) {
		return &mockPartitionIterator{
			ctx:        childCtx,
			rows:       []*spanner.Row{newRow(t, []string{"FOO"}, []any{int64(index)})},
			metadata:   &sppb.ResultSetMetadata{},
			blockOnCtx: true,
		}, nil, nil
	}

	if err := mi.run(ctx); err != nil {
		t.Fatalf("run failed: %v", err)
	}

	var wg sync.WaitGroup
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			mi.Stop()
		}()
	}
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("concurrent Stop() calls timed out")
	}
}

// Verifies that non-positive maxParallelism values default to runtime.NumCPU().
func TestMergedRowIterator_DefaultParallelism(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		parallelism int
	}{
		{name: "zero", parallelism: 0},
		{name: "negative", parallelism: -1},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			mi := createMergedIterator(discardLogger(), newTestPartitionedQuery(1), tc.parallelism)
			if g, w := mi.maxParallelism, runtime.NumCPU(); g != w {
				t.Errorf("default maxParallelism mismatch\nGot:  %v\nWant: %v", g, w)
			}
		})
	}
}

// Verifies that ResultSetStats returns a non-nil stats message.
func TestMergedRowIterator_ResultSetStats(t *testing.T) {
	t.Parallel()

	mi := createMergedIterator(discardLogger(), newTestPartitionedQuery(1), 1)
	if stats := mi.ResultSetStats(); stats == nil {
		t.Error("ResultSetStats returned nil, want non-nil")
	}
}
