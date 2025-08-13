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

	"cloud.google.com/go/spanner"
	sppb "cloud.google.com/go/spanner/apiv1/spannerpb"
	"google.golang.org/api/iterator"
)

// prefetchRowIterator wraps a rowIterator and eagerly fetches rows in a
// background goroutine, buffering them in memory so calls to Next() do not
// block on I/O.
type prefetchRowIterator struct {
	u rowIterator

	items chan prefetchItem
	done  chan struct{}
}

type prefetchItem struct {
	row *spanner.Row
	err error
}

func newPrefetchRowIterator(ctx context.Context, u rowIterator) rowIterator {
	it := &prefetchRowIterator{
		u:     u,
		items: make(chan prefetchItem, 64),
		done:  make(chan struct{}),
	}
	go it.runPrefetch(ctx)
	return it
}

func (p *prefetchRowIterator) runPrefetch(ctx context.Context) {
	defer close(p.items)
	for {
		select {
		case <-p.done:
			return
		default:
		}

		row, err := p.u.Next()

		select {
		case <-p.done:
			return
		case p.items <- prefetchItem{row: row, err: err}:
		}

		if err != nil {
			return
		}
	}
}

func (p *prefetchRowIterator) Next() (*spanner.Row, error) {
	it, ok := <-p.items
	if !ok {
		return nil, iterator.Done
	}
	if it.err != nil {
		return nil, it.err
	}
	return it.row, nil
}

func (p *prefetchRowIterator) Stop() {
	select {
	case <-p.done:
		// already stopped
	default:
		close(p.done)
	}
	p.u.Stop()
}

func (p *prefetchRowIterator) Metadata() (*sppb.ResultSetMetadata, error) {
	return p.u.Metadata()
}

func (p *prefetchRowIterator) ResultSetStats() *sppb.ResultSetStats {
	return p.u.ResultSetStats()
}
