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
	stopped      bool
	numProducers int
	metadata     *sppb.ResultSetMetadata

	buffer        chan *spanner.Row
	metadataReady chan struct{}
	errReady      chan struct{}
	done          chan struct{}

	logger           *slog.Logger
	partitionedQuery *PartitionedQuery
	maxParallelism   int
}

func createMergedIterator(logger *slog.Logger, partitionedQuery *PartitionedQuery, maxParallelism int) *mergedRowIterator {
	if maxParallelism <= 0 {
		maxParallelism = runtime.NumCPU()
	}
	return &mergedRowIterator{
		logger:           logger.With("type", "merged_iterator", "stmt", partitionedQuery.stmt.SQL, "parallelism", maxParallelism),
		partitionedQuery: partitionedQuery,
		maxParallelism:   maxParallelism,
		buffer:           make(chan *spanner.Row, 10),
		done:             make(chan struct{}),
		errReady:         make(chan struct{}),
		metadataReady:    make(chan struct{}),
	}
}

func (m *mergedRowIterator) run(ctx context.Context) error {
	m.logger.DebugContext(ctx, "run")
	parallelism := m.maxParallelism
	if len(m.partitionedQuery.Partitions) < parallelism {
		parallelism = len(m.partitionedQuery.Partitions)
	}

	m.numProducers = parallelism
	for i := 0; i < parallelism; i++ {
		go m.produceRows(ctx)
	}
	// Wait until we have metadata or an error.
	_, err := m.Metadata()
	return err
}

func (m *mergedRowIterator) produceRows(ctx context.Context) {
	m.logger.DebugContext(ctx, "produceRows")
	defer func() {
		m.mu.Lock()
		defer m.mu.Unlock()
		m.numProducers--
		if m.numProducers == 0 {
			m.stopLocked()
		}
	}()
	for {
		index := m.nextIndex()
		if index >= len(m.partitionedQuery.Partitions) || m.hasErr() {
			break
		}
		m.produceRowsFromPartition(ctx, index)
	}
}

func (m *mergedRowIterator) nextIndex() int {
	m.logger.Debug("nextIndex")
	m.mu.Lock()
	defer m.mu.Unlock()
	index := m.pIndex
	m.pIndex++

	return index
}

func (m *mergedRowIterator) produceRowsFromPartition(ctx context.Context, index int) {
	m.logger.DebugContext(ctx, "produceRowsFromPartition", "index", index)
	r, err := m.partitionedQuery.execute(ctx, index)
	if err != nil {
		m.registerErr(err)
		return
	}
	defer func() { _ = r.Close() }()

	first := true
	it := r.it
	for {
		row, err := it.Next()
		if err != nil && err != iterator.Done {
			m.registerErr(err)
			return
		}
		if first && (err == iterator.Done || row != nil) {
			first = false
			m.mu.Lock()
			if m.metadata == nil {
				metadata, metadataErr := it.Metadata()
				if metadataErr != nil {
					m.registerErrLocked(metadataErr)
				} else {
					m.metadata = metadata
				}
				m.mu.Unlock()
				close(m.metadataReady)
			} else {
				m.mu.Unlock()
			}
		}
		if row == nil {
			return
		}
		select {
		case m.buffer <- row:
		case <-m.done:
			return
		}
	}
}

func (m *mergedRowIterator) isStopped() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.err != nil
}

func (m *mergedRowIterator) hasErr() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.err != nil
}

func (m *mergedRowIterator) registerErr(err error) {
	m.logger.Warn("registerErr", "err", err)
	m.mu.Lock()
	defer m.mu.Unlock()
	m.registerErrLocked(err)
}

func (m *mergedRowIterator) registerErrLocked(err error) {
	m.logger.Debug("registerErrLocked", "err", err)
	if m.err == nil {
		m.err = err
		close(m.errReady)
	}
}

func (m *mergedRowIterator) Next() (*spanner.Row, error) {
	m.logger.Debug("Next")
	select {
	case <-m.metadataReady:
	case <-m.errReady:
	case <-m.done:
	}
	m.mu.Lock()
	if m.err != nil {
		defer m.mu.Unlock()
		return nil, m.err
	}
	m.mu.Unlock()

	// Pick an element from the buffer if there is any.
	// This prevents the select statement from selecting 'done'
	// when all producers are done, but there are still elements
	// in the buffer.
	if len(m.buffer) > 0 {
		select {
		case v := <-m.buffer:
			if v == nil {
				m.logger.Warn("returning nil row")
			}
			return v, nil
		default:
			// fallthrough
		}
	}

	select {
	case v := <-m.buffer:
		if v == nil {
			m.logger.Warn("returning nil row 2")
		}
		return v, nil
	case <-m.done:
		m.mu.Lock()
		defer m.mu.Unlock()
		if m.err != nil {
			return nil, m.err
		}
		m.logger.Warn("Iterator is done")
		return nil, iterator.Done
	}
}

func (m *mergedRowIterator) Stop() {
	m.logger.Warn("Stop")
	m.mu.Lock()
	defer m.mu.Unlock()
	m.stopLocked()
}

func (m *mergedRowIterator) stopLocked() {
	m.logger.Warn("stopLocked")
	if !m.stopped {
		m.stopped = true
		close(m.done)
	}
}

func (m *mergedRowIterator) Metadata() (*sppb.ResultSetMetadata, error) {
	m.logger.Debug("Metadata")
	select {
	case <-m.metadataReady:
	case <-m.errReady:
	case <-m.done:
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.err != nil {
		return nil, m.err
	}
	return m.metadata, nil
}
