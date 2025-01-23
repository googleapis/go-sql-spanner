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
	"sync"

	"cloud.google.com/go/spanner"
	sppb "cloud.google.com/go/spanner/apiv1/spannerpb"
)

var _ rowIterator = &mergedRowIterator{}

type mergedRowIterator struct {
	mu     sync.Mutex
	buffer []*spanner.Row
	err    error
	pIndex int

	partitionedQuery *PartitionedQuery
	maxParallelism   int
}

func createMergedIterator(partitionedQuery *PartitionedQuery, maxParallelism int) *mergedRowIterator {
	return &mergedRowIterator{
		partitionedQuery: partitionedQuery,
		maxParallelism:   maxParallelism,
		buffer:           make([]*spanner.Row, 0, 10),
	}
}

func (m *mergedRowIterator) run(ctx context.Context) {
	parallelism := m.maxParallelism
	if len(m.partitionedQuery.Partitions) < parallelism {
		parallelism = len(m.partitionedQuery.Partitions)
	}

	for i := 0; i < parallelism; i++ {
		go m.produceRows(ctx)
	}
}

func (m *mergedRowIterator) produceRows(ctx context.Context) {
	for {
		index := m.nextIndex()
		if index > len(m.partitionedQuery.Partitions) {
			break
		}
		m.produceRowsFromPartition(ctx, index)
	}
}

func (m *mergedRowIterator) nextIndex() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	index := m.pIndex
	m.pIndex++

	return index
}

func (m *mergedRowIterator) produceRowsFromPartition(ctx context.Context, index int) {
	rows, err := m.partitionedQuery.execute(ctx, index)
}

func (m *mergedRowIterator) Next() (*spanner.Row, error) {
	//TODO implement me
	panic("implement me")
}

func (m *mergedRowIterator) Stop() {
	//TODO implement me
	panic("implement me")
}

func (m *mergedRowIterator) Metadata() *sppb.ResultSetMetadata {
	//TODO implement me
	panic("implement me")
}
