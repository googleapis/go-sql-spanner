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
	"time"

	"cloud.google.com/go/spanner"
	"github.com/google/go-cmp/cmp"
	"google.golang.org/grpc/codes"
)

func TestStatementExecutor_StartBatchDdl(t *testing.T) {
	c := &conn{retryAborts: true}
	s := &statementExecutor{}
	ctx := context.Background()

	if c.InDDLBatch() {
		t.Fatal("connection unexpectedly in a DDL batch")
	}
	if _, err := s.StartBatchDdl(ctx, c, "", nil); err != nil {
		t.Fatalf("could not start a DDL batch: %v", err)
	}
	if !c.InDDLBatch() {
		t.Fatal("connection unexpectedly not in a DDL batch")
	}
	if _, err := s.StartBatchDdl(ctx, c, "", nil); spanner.ErrCode(err) != codes.FailedPrecondition {
		t.Fatalf("error mismatch for starting a DDL batch while already in a batch\nGot: %v\nWant: %v", spanner.ErrCode(err), codes.FailedPrecondition)
	}
	if _, err := s.RunBatch(ctx, c, "", nil); err != nil {
		t.Fatalf("could not run empty DDL batch: %v", err)
	}
	if c.InDDLBatch() {
		t.Fatal("connection unexpectedly in a DDL batch")
	}

	// Starting a DDL batch while the connection is in a transaction is not allowed.
	c.tx = &readWriteTransaction{}
	if _, err := s.StartBatchDdl(ctx, c, "", nil); spanner.ErrCode(err) != codes.FailedPrecondition {
		t.Fatalf("error mismatch for starting a DDL batch while in a transaction\nGot: %v\nWant: %v", spanner.ErrCode(err), codes.FailedPrecondition)
	}
}

func TestStatementExecutor_StartBatchDml(t *testing.T) {
	c := &conn{retryAborts: true}
	s := &statementExecutor{}
	ctx := context.Background()

	if c.InDMLBatch() {
		t.Fatal("connection unexpectedly in a DML batch")
	}
	if _, err := s.StartBatchDml(ctx, c, "", nil); err != nil {
		t.Fatalf("could not start a DML batch: %v", err)
	}
	if !c.InDMLBatch() {
		t.Fatal("connection unexpectedly not in a DML batch")
	}
	if _, err := s.StartBatchDml(ctx, c, "", nil); spanner.ErrCode(err) != codes.FailedPrecondition {
		t.Fatalf("error mismatch for starting a DML batch while already in a batch\nGot: %v\nWant: %v", spanner.ErrCode(err), codes.FailedPrecondition)
	}
	if _, err := s.RunBatch(ctx, c, "", nil); err != nil {
		t.Fatalf("could not run empty DML batch: %v", err)
	}
	if c.InDMLBatch() {
		t.Fatal("connection unexpectedly in a DML batch")
	}

	// Starting a DML batch while the connection is in a read-only transaction is not allowed.
	c.tx = &readOnlyTransaction{}
	if _, err := s.StartBatchDml(ctx, c, "", nil); spanner.ErrCode(err) != codes.FailedPrecondition {
		t.Fatalf("error mismatch for starting a DML batch while in a read-only transaction\nGot: %v\nWant: %v", spanner.ErrCode(err), codes.FailedPrecondition)
	}

	// Starting a DML batch while the connection is in a read/write transaction is allowed.
	c.tx = &readWriteTransaction{}
	if _, err := s.StartBatchDml(ctx, c, "", nil); err != nil {
		t.Fatalf("could not start a DML batch while in a read/write transaction: %v", err)
	}
}

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
		wantValue  AutocommitDMLMode
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
		wantCols := []string{"AutocommitDMLMode"}
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

func TestStatementExecutor_ReadOnlyStaleness(t *testing.T) {
	c := &conn{}
	s := &statementExecutor{}
	ctx := context.Background()
	eurWest, err := time.LoadLocation("Europe/Amsterdam")
	if err != nil {
		t.Fatalf("Missing timezone Europe/Amsterdam")
	}
	for i, test := range []struct {
		wantValue  spanner.TimestampBound
		setValue   string
		wantSetErr bool
	}{
		{spanner.ExactStaleness(time.Second), "'Exact_Staleness 1s'", false},
		{spanner.ExactStaleness(10 * time.Millisecond), "'Exact_Staleness 10ms'", false},
		{spanner.MaxStaleness(time.Second), "'Max_Staleness 1s'", false},
		{spanner.MaxStaleness(10 * time.Millisecond), "'Max_Staleness 10ms'", false},
		{spanner.ReadTimestamp(time.Date(2021, 10, 8, 9, 14, 30, 10, time.UTC)), "'Read_Timestamp 2021-10-08T09:14:30.000000010Z'", false},
		{spanner.ReadTimestamp(time.Date(2021, 10, 8, 11, 14, 30, 10, eurWest)), "'Read_Timestamp 2021-10-08T11:14:30.000000010+02:00'", false},
		{spanner.MinReadTimestamp(time.Date(2021, 10, 8, 9, 14, 30, 10, time.UTC)), "'Min_Read_Timestamp 2021-10-08T09:14:30.000000010Z'", false},
		{spanner.MinReadTimestamp(time.Date(2021, 10, 8, 11, 14, 30, 10, eurWest)), "'Min_Read_Timestamp 2021-10-08T11:14:30.000000010+02:00'", false},
		{spanner.StrongRead(), "'Strong'", false},
		{spanner.StrongRead(), "'Non_Existing_Staleness'", true},
		{spanner.StrongRead(), "'Exact_Staleness 1m'", true},
		{spanner.StrongRead(), "'Exact_Staleness 1'", true},
		{spanner.StrongRead(), "'Max_Staleness 1m'", true},
		{spanner.StrongRead(), "'Max_Staleness 1'", true},
		{spanner.StrongRead(), "'Read_Timestamp 2021-10-08T09:14:30.000000010'", true},
		{spanner.StrongRead(), "'Read_Timestamp 2021-10-08T09:14:30'", true},
		{spanner.StrongRead(), "'Read_Timestamp'", true},
		{spanner.StrongRead(), "'Read_Timestamp 2021-10-08 09:14:30Z'", true},
		{spanner.StrongRead(), "'Min_Read_Timestamp 2021-10-08T09:14:30.000000010'", true},
		{spanner.StrongRead(), "'Min_Read_Timestamp 2021-10-08T09:14:30'", true},
		{spanner.StrongRead(), "'Min_Read_Timestamp'", true},
		{spanner.StrongRead(), "'Min_Read_Timestamp 2021-10-08 09:14:30Z'", true},
	} {
		res, err := s.SetReadOnlyStaleness(ctx, c, test.setValue, nil)
		if test.wantSetErr {
			if err == nil {
				t.Fatalf("%d: missing expected error for value %q", i, test.setValue)
			}
		} else {
			if err != nil {
				t.Fatalf("%d: could not set new value %q for read-only staleness: %v", i, test.setValue, err)
			}
			if res != driver.ResultNoRows {
				t.Fatalf("%d: result mismatch\nGot: %v\nWant: %v", i, res, driver.ResultNoRows)
			}
		}

		it, err := s.ShowReadOnlyStaleness(ctx, c, "", nil)
		if err != nil {
			t.Fatalf("%d: could not get current read-only staleness value from connection: %v", i, err)
		}
		cols := it.Columns()
		wantCols := []string{"ReadOnlyStaleness"}
		if !cmp.Equal(cols, wantCols) {
			t.Fatalf("%d: column names mismatch\nGot: %v\nWant: %v", i, cols, wantCols)
		}
		values := make([]driver.Value, len(cols))
		if err := it.Next(values); err != nil {
			t.Fatalf("%d: failed to get first row for read-only staleness: %v", i, err)
		}
		wantValues := []driver.Value{test.wantValue.String()}
		if !cmp.Equal(values, wantValues) {
			t.Fatalf("%d: read-only staleness values mismatch\nGot: %v\nWant: %v", i, values, wantValues)
		}
		if err := it.Next(values); err != io.EOF {
			t.Fatalf("%d: error mismatch\nGot: %v\nWant: %v", i, err, io.EOF)
		}
	}
}
