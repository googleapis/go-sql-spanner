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
	"cloud.google.com/go/spanner"
	"cloud.google.com/go/spanner/apiv1/spannerpb"
	"google.golang.org/api/iterator"
)

type wrappedRowIterator struct {
	*spanner.RowIterator

	noRows   bool
	firstRow *spanner.Row
}

func (ri *wrappedRowIterator) Next() (*spanner.Row, error) {
	if ri.noRows {
		return nil, iterator.Done
	}
	if ri.firstRow != nil {
		defer func() { ri.firstRow = nil }()
		return ri.firstRow, nil
	}
	return ri.RowIterator.Next()
}

func (ri *wrappedRowIterator) Stop() {
	ri.RowIterator.Stop()
}

func (ri *wrappedRowIterator) Metadata() (*spannerpb.ResultSetMetadata, error) {
	return ri.RowIterator.Metadata, nil
}
