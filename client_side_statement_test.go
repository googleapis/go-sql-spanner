// Copyright 2021 Google LLC All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package spannerdriver

import (
	"context"
	"database/sql/driver"
	"io"
	"testing"

	"github.com/google/go-cmp/cmp"
)

func TestStatementExecutor_RetryAbortsInternally(t *testing.T) {
	c := &conn{retryAborts: true}
	s := &statementExecutor{}
	ctx := context.Background()
	for i, test := range []struct {
		wantValue  bool
		setValue   string
		wantSetErr bool
	}{
		{true, "false", false},
		{false, "true", false},
		{true, "FALSE", false},
		{false, "TRUE", false},
		{true, "False", false},
		{false, "True", false},
		{true, "fasle", true},
		{true, "truye", true},
	} {
		it, err := s.ShowRetryAbortsInternally(ctx, c, "", nil)
		if err != nil {
			t.Fatalf("%d: could not get current retry value from connection: %v", i, err)
		}
		cols := it.Columns()
		wantCols := []string{"RetryAbortsInternally"}
		if !cmp.Equal(cols, wantCols) {
			t.Fatalf("%d: column names mismatch\nGot: %v\nWant: %v", i, cols, wantCols)
		}
		values := make([]driver.Value, len(cols))
		if err := it.Next(values); err != nil {
			t.Fatalf("%d: failed to get first row: %v", i, err)
		}
		wantValues := []driver.Value{test.wantValue}
		if !cmp.Equal(values, wantValues) {
			t.Fatalf("%d: retry values mismatch\nGot: %v\nWant: %v", i, values, wantValues)
		}
		if err := it.Next(values); err != io.EOF {
			t.Fatalf("%d: error mismatch\nGot: %v\nWant: %v", i, err, io.EOF)
		}

		// Set the next value.
		res, err := s.SetRetryAbortsInternally(ctx, c, test.setValue, nil)
		if test.wantSetErr {
			if err == nil {
				t.Fatalf("%d: missing expected error for value %q", i, test.setValue)
			}
		} else {
			if err != nil {
				t.Fatalf("%d: could not set new value %q for retry: %v", i, test.setValue, err)
			}
			if res != driver.ResultNoRows {
				t.Fatalf("%d: result mismatch\nGot: %v\nWant: %v", i, res, driver.ResultNoRows)
			}
		}
	}
}

func TestStatementExecutor_AutocommitDmlMode(t *testing.T) {
	c := &conn{}
	s := &statementExecutor{}
	ctx := context.Background()
	for i, test := range []struct {
		wantValue  AutocommitDmlMode
		setValue   string
		wantSetErr bool
	}{
		{Transactional, "'Partitioned_Non_Atomic'", false},
		{PartitionedNonAtomic, "'Transactional'", false},
		{Transactional, "'PARTITIONED_NON_ATOMIC'", false},
		{PartitionedNonAtomic, "'TRANSACTIONAL'", false},
		{Transactional, "'partitioned_non_atomic'", false},
		{PartitionedNonAtomic, "'transactional'", false},
		{Transactional, "'PartitionedNonAtomic'", true},
		{Transactional, "'Transaction'", true},
	} {
		it, err := s.ShowAutocommitDmlMode(ctx, c, "", nil)
		if err != nil {
			t.Fatalf("%d: could not get current autocommit dml mode value from connection: %v", i, err)
		}
		cols := it.Columns()
		wantCols := []string{"AutocommitDmlMode"}
		if !cmp.Equal(cols, wantCols) {
			t.Fatalf("%d: column names mismatch\nGot: %v\nWant: %v", i, cols, wantCols)
		}
		values := make([]driver.Value, len(cols))
		if err := it.Next(values); err != nil {
			t.Fatalf("%d: failed to get first row for autocommit dml mode: %v", i, err)
		}
		wantValues := []driver.Value{test.wantValue.String()}
		if !cmp.Equal(values, wantValues) {
			t.Fatalf("%d: autocommit dml mode values mismatch\nGot: %v\nWant: %v", i, values, wantValues)
		}
		if err := it.Next(values); err != io.EOF {
			t.Fatalf("%d: error mismatch\nGot: %v\nWant: %v", i, err, io.EOF)
		}

		// Set the next value.
		res, err := s.SetAutocommitDmlMode(ctx, c, test.setValue, nil)
		if test.wantSetErr {
			if err == nil {
				t.Fatalf("%d: missing expected error for value %q", i, test.setValue)
			}
		} else {
			if err != nil {
				t.Fatalf("%d: could not set new value %q for autocommit dml mode: %v", i, test.setValue, err)
			}
			if res != driver.ResultNoRows {
				t.Fatalf("%d: result mismatch\nGot: %v\nWant: %v", i, res, driver.ResultNoRows)
			}
		}
	}
}
