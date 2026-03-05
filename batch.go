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
	"github.com/googleapis/go-sql-spanner/parser"
)

type batch struct {
	tp           parser.BatchType
	statements   []spanner.Statement
	returnValues []int64
	options      *ExecOptions
	automatic    bool
}

func toBatchError(res *result, err error) error {
	if err == nil {
		return err
	}
	if res == nil {
		return &BatchError{
			Err:               err,
			BatchUpdateCounts: []int64{},
		}
	}
	return &BatchError{
		BatchUpdateCounts: res.batchUpdateCounts,
		Err:               err,
	}
}

type BatchError struct {
	BatchUpdateCounts []int64
	Err               error
}

func (be *BatchError) Error() string {
	return be.Err.Error()
}

func (be *BatchError) Unwrap() error {
	return be.Err
}
