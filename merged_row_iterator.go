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
	"log/slog"
	"runtime"
	"sync"

	"cloud.google.com/go/spanner"
	sppb "cloud.google.com/go/spanner/apiv1/spannerpb"
	"google.golang.org/api/iterator"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var _ rowIterator = &mergedRowIterator{}

// mergedRowIterator is a row iterator that starts up to maxParallelism
// goroutines and reads data from partitions of a PartitionedQuery. The
// rows that are read from the partitions are put in a buffer. This
// iterator returns the rows from that buffer.
//
// The rows from the underlying partitions are returned in arbitrary
// order.
type mergedRowIterator struct {
	mu           sync.Mutex
	err          error
	pIndex       int
	started      bool
	stopped      bool
	metadataSet  bool
	numProducers int
	metadata     *sppb.ResultSetMetadata
	cancel       context.CancelFunc

	metadataOnce  sync.Once
	buffer        chan *spanner.Row
	metadataReady chan struct{}
	errReady      chan struct{}
	done          chan struct{}
	producersDone chan struct{}

	logger           *slog.Logger
	partitionedQuery *PartitionedQuery
	maxParallelism   int
	executePartition func(ctx context.Context, index int) (rowIterator, func() error, error)
}

func createMergedIterator(logger *slog.Logger, partitionedQuery *PartitionedQuery, maxParallelism int) *mergedRowIterator {
	if maxParallelism <= 0 {
		maxParallelism = runtime.NumCPU()
	}
	return &mergedRowIterator{
		logger:           logger.With("type", "merged_iterator", "stmt", partitionedQuery.stmt.SQL, "parallelism", maxParallelism),
		partitionedQuery: partitionedQuery,
		maxParallelism:   maxParallelism,
		executePartition: func(ctx context.Context, index int) (rowIterator, func() error, error) {
			r, err := partitionedQuery.execute(ctx /*cancel=*/, nil, index)
			if err != nil {
				return nil, nil, err
			}
			return r.it, r.Close, nil
		},
		buffer:        make(chan *spanner.Row, 10),
		done:          make(chan struct{}),
		producersDone: make(chan struct{}),
		errReady:      make(chan struct{}),
		metadataReady: make(chan struct{}),
	}
}

func (m *mergedRowIterator) run(ctx context.Context) error {
	if len(m.partitionedQuery.Partitions) == 0 {
		return spanner.ToSpannerError(status.Error(codes.FailedPrecondition, "partitioned query contains zero partitions"))
	}
	m.logger.DebugContext(ctx, "starting merged row iterator")
	parallelism := m.maxParallelism
	if len(m.partitionedQuery.Partitions) < parallelism {
		parallelism = len(m.partitionedQuery.Partitions)
	}

	m.mu.Lock()
	if m.stopped {
		m.mu.Unlock()
		return spanner.ToSpannerError(status.Error(codes.Canceled, "merged iterator is stopped"))
	}
	if m.started {
		m.mu.Unlock()
		return spanner.ToSpannerError(status.Error(codes.FailedPrecondition, "merged iterator already started"))
	}
	m.started = true
	childCtx, cancel := context.WithCancel(ctx)
	m.cancel = cancel
	m.numProducers = parallelism
	m.mu.Unlock()

	for i := 0; i < parallelism; i++ {
		go m.produceRows(childCtx)
	}
	// Wait until we have metadata or an error.
	_, err := m.Metadata()
	if err != nil {
		m.Stop()
	}
	return err
}

func (m *mergedRowIterator) produceRows(ctx context.Context) {
	m.logger.DebugContext(ctx, "producing rows for merged iterator")
	defer func() {
		m.mu.Lock()
		defer m.mu.Unlock()
		m.numProducers--
		if m.numProducers == 0 {
			m.logger.DebugContext(ctx, "all merged iterator producers done")
			if m.cancel != nil {
				m.cancel()
			}
			close(m.buffer)
			close(m.producersDone)
		}
	}()
	for {
		index, ok := m.nextPartitionIndex()
		if !ok {
			break
		}
		m.produceRowsFromPartition(ctx, index)
	}
}

// nextPartitionIndex claims and returns the next partition index to execute.
// It returns (0, false) when all partitions have been claimed or the iterator
// has stopped or encountered an error.
func (m *mergedRowIterator) nextPartitionIndex() (int, bool) {
	m.logger.Debug("merged iterator moving to next partition index")
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.err != nil || m.stopped || m.pIndex >= len(m.partitionedQuery.Partitions) {
		return 0, false
	}
	index := m.pIndex
	m.pIndex++
	return index, true
}

func (m *mergedRowIterator) produceRowsFromPartition(ctx context.Context, index int) {
	m.logger.DebugContext(ctx, "merged row iterator producing rows from partition", "index", index)
	it, closeFn, err := m.executePartition(ctx, index)
	if err != nil {
		m.registerErr(err)
		return
	}
	if closeFn != nil {
		defer func() { _ = closeFn() }()
	} else {
		defer it.Stop()
	}

	first := true
	for {
		row, err := it.Next()
		if err != nil && err != iterator.Done {
			m.registerErr(err)
			return
		}
		if first {
			first = false
			m.metadataOnce.Do(func() {
				metadata, metadataErr := it.Metadata()
				m.mu.Lock()
				if metadataErr != nil {
					m.logger.DebugContext(ctx, "merged iterator metadata error", "err", metadataErr)
					m.registerErrLocked(metadataErr)
				} else {
					m.metadataSet = true
					m.metadata = metadata
				}
				close(m.metadataReady)
				m.mu.Unlock()
			})
		}
		if row == nil {
			return
		}
		select {
		case m.buffer <- row:
		case <-ctx.Done():
			m.registerErr(ctx.Err())
			return
		}
	}
}

func (m *mergedRowIterator) registerErr(err error) {
	m.logger.Debug("merged row iterator received error", "err", err)
	m.mu.Lock()
	defer m.mu.Unlock()
	m.registerErrLocked(err)
}

func (m *mergedRowIterator) registerErrLocked(err error) {
	if err != nil && m.err == nil && !m.stopped {
		m.err = err
		close(m.errReady)
		if m.cancel != nil {
			m.cancel()
		}
	}
}

func (m *mergedRowIterator) waitReady() {
	select {
	case <-m.metadataReady:
	case <-m.errReady:
	case <-m.done:
	case <-m.producersDone:
	}
}

func (m *mergedRowIterator) Next() (*spanner.Row, error) {
	m.waitReady()
	m.mu.Lock()
	if m.err != nil {
		m.mu.Unlock()
		return nil, m.err
	}
	if m.stopped {
		m.mu.Unlock()
		return nil, iterator.Done
	}
	m.mu.Unlock()

	// When all producers finish normally, m.buffer is closed while m.done stays
	// open (m.done is only closed by Stop). Reading from m.buffer therefore
	// yields all remaining buffered rows before returning iterator.Done.
	select {
	case v, ok := <-m.buffer:
		if !ok {
			return m.finishNext()
		}
		return v, nil
	case <-m.errReady:
		m.mu.Lock()
		defer m.mu.Unlock()
		return nil, m.err
	case <-m.done:
		return m.finishNext()
	}
}

func (m *mergedRowIterator) finishNext() (*spanner.Row, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.err != nil {
		return nil, m.err
	}
	m.logger.Debug("merged iterator is done")
	return nil, iterator.Done
}

func (m *mergedRowIterator) Stop() {
	m.logger.Debug("merged iterator stopped")
	m.mu.Lock()
	m.stopLocked()
	hasProducers := m.numProducers > 0
	m.mu.Unlock()
	if hasProducers {
		// Block until all producers have stopped.
		<-m.producersDone
	}
}

func (m *mergedRowIterator) stopLocked() {
	if !m.stopped {
		m.stopped = true
		close(m.done)
		if m.cancel != nil {
			m.cancel()
		}
	}
}

func (m *mergedRowIterator) Metadata() (*sppb.ResultSetMetadata, error) {
	m.waitReady()
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.err != nil {
		return nil, m.err
	}
	if !m.metadataSet && m.stopped {
		return nil, spanner.ToSpannerError(status.Error(codes.Canceled, "merged iterator is stopped"))
	}
	return m.metadata, nil
}

func (m *mergedRowIterator) ResultSetStats() *sppb.ResultSetStats {
	return &sppb.ResultSetStats{}
}
