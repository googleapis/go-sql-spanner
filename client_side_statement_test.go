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
	"fmt"
	"io"
	"strings"
	"testing"
	"time"

	"cloud.google.com/go/spanner"
	"cloud.google.com/go/spanner/admin/database/apiv1/databasepb"
	"cloud.google.com/go/spanner/apiv1/spannerpb"
	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/googleapis/go-sql-spanner/connectionstate"
	"github.com/googleapis/go-sql-spanner/parser"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/types/known/structpb"
)

func TestStatementExecutor_StartBatchDdl(t *testing.T) {
	t.Parallel()

	p, _ := parser.NewStatementParser(databasepb.DatabaseDialect_GOOGLE_STANDARD_SQL, 1000)
	c := &conn{
		logger: noopLogger,
		state:  createInitialConnectionState(connectionstate.TypeNonTransactional, map[string]connectionstate.ConnectionPropertyValue{}),
		parser: p,
	}
	ctx := context.Background()

	if c.InDDLBatch() {
		t.Fatal("connection unexpectedly in a DDL batch")
	}
	if _, err := c.ExecContext(ctx, "start batch ddl", []driver.NamedValue{}); err != nil {
		t.Fatalf("could not start a DDL batch: %v", err)
	}
	if !c.InDDLBatch() {
		t.Fatal("connection unexpectedly not in a DDL batch")
	}
	if _, err := c.ExecContext(ctx, "start batch ddl", []driver.NamedValue{}); spanner.ErrCode(err) != codes.FailedPrecondition {
		t.Fatalf("error mismatch for starting a DDL batch while already in a batch\nGot: %v\nWant: %v", spanner.ErrCode(err), codes.FailedPrecondition)
	}
	if _, err := c.ExecContext(ctx, "run batch", []driver.NamedValue{}); err != nil {
		t.Fatalf("could not run empty DDL batch: %v", err)
	}
	if c.InDDLBatch() {
		t.Fatal("connection unexpectedly in a DDL batch")
	}

	// Starting a DDL batch while the connection is in a transaction is not allowed.
	c.tx = &delegatingTransaction{conn: c, ctx: ctx}
	if _, err := c.ExecContext(ctx, "start batch ddl", []driver.NamedValue{}); spanner.ErrCode(err) != codes.FailedPrecondition {
		t.Fatalf("error mismatch for starting a DDL batch while in a transaction\nGot: %v\nWant: %v", spanner.ErrCode(err), codes.FailedPrecondition)
	}
}

func TestStatementExecutor_StartBatchDml(t *testing.T) {
	t.Parallel()

	p, _ := parser.NewStatementParser(databasepb.DatabaseDialect_GOOGLE_STANDARD_SQL, 1000)
	c := &conn{
		logger: noopLogger,
		state:  createInitialConnectionState(connectionstate.TypeNonTransactional, map[string]connectionstate.ConnectionPropertyValue{}),
		parser: p,
	}
	ctx := context.Background()

	if c.InDMLBatch() {
		t.Fatal("connection unexpectedly in a DML batch")
	}
	if _, err := c.ExecContext(ctx, "start batch dml", []driver.NamedValue{}); err != nil {
		t.Fatalf("could not start a DML batch: %v", err)
	}
	if !c.InDMLBatch() {
		t.Fatal("connection unexpectedly not in a DML batch")
	}
	if _, err := c.ExecContext(ctx, "start batch dml", []driver.NamedValue{}); spanner.ErrCode(err) != codes.FailedPrecondition {
		t.Fatalf("error mismatch for starting a DML batch while already in a batch\nGot: %v\nWant: %v", spanner.ErrCode(err), codes.FailedPrecondition)
	}
	if _, err := c.ExecContext(ctx, "run batch", []driver.NamedValue{}); err != nil {
		t.Fatalf("could not run empty DML batch: %v", err)
	}
	if c.InDMLBatch() {
		t.Fatal("connection unexpectedly in a DML batch")
	}

	// Starting a DML batch while the connection is in a read-only transaction is not allowed.
	c.tx = &delegatingTransaction{conn: c, contextTransaction: &readOnlyTransaction{logger: noopLogger}}
	if _, err := c.ExecContext(ctx, "start batch dml", []driver.NamedValue{}); spanner.ErrCode(err) != codes.FailedPrecondition {
		t.Fatalf("error mismatch for starting a DML batch while in a read-only transaction\nGot: %v\nWant: %v", spanner.ErrCode(err), codes.FailedPrecondition)
	}

	// Starting a DML batch while the connection is in a read/write transaction is allowed.
	c.tx = &delegatingTransaction{conn: c, contextTransaction: &readWriteTransaction{logger: noopLogger}}
	if _, err := c.ExecContext(ctx, "start batch dml", []driver.NamedValue{}); err != nil {
		t.Fatalf("could not start a DML batch while in a read/write transaction: %v", err)
	}
}

func TestStatementExecutor_RetryAbortsInternally(t *testing.T) {
	t.Parallel()

	p, _ := parser.NewStatementParser(databasepb.DatabaseDialect_GOOGLE_STANDARD_SQL, 1000)
	c := &conn{
		logger: noopLogger,
		state:  createInitialConnectionState(connectionstate.TypeNonTransactional, map[string]connectionstate.ConnectionPropertyValue{}),
		parser: p,
	}
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
		it, err := c.QueryContext(ctx, "show variable retry_aborts_internally", []driver.NamedValue{})
		if err != nil {
			t.Fatalf("%d: could not get current retry value from connection: %v", i, err)
		}
		cols := it.Columns()
		wantCols := []string{"retry_aborts_internally"}
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
		res, err := c.ExecContext(ctx, fmt.Sprintf("set retry_aborts_internally = %s", test.setValue), []driver.NamedValue{})
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
	t.Parallel()

	p, _ := parser.NewStatementParser(databasepb.DatabaseDialect_GOOGLE_STANDARD_SQL, 1000)
	c := &conn{
		logger:    noopLogger,
		connector: &connector{},
		state: createInitialConnectionState(connectionstate.TypeNonTransactional, map[string]connectionstate.ConnectionPropertyValue{
			propertyAutocommitDmlMode.Key(): propertyAutocommitDmlMode.CreateTypedInitialValue(Transactional),
		}),
		parser: p,
	}
	_ = c.ResetSession(context.Background())
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
		{Transactional, "'PartitionedNonAtomic'", false},
		{PartitionedNonAtomic, "'Transaction'", true},
	} {
		it, err := c.QueryContext(ctx, "show variable autocommit_dml_mode", []driver.NamedValue{})
		if err != nil {
			t.Fatalf("%d: could not get current autocommit dml mode value from connection: %v", i, err)
		}
		cols := it.Columns()
		wantCols := []string{"autocommit_dml_mode"}
		if !cmp.Equal(cols, wantCols) {
			t.Fatalf("%d: column names mismatch\nGot: %v\nWant: %v", i, cols, wantCols)
		}
		values := make([]driver.Value, len(cols))
		if err := it.Next(values); err != nil {
			t.Fatalf("%d: failed to get first row for autocommit dml mode: %v", i, err)
		}
		wantValues := []driver.Value{test.wantValue.String()}
		if !cmp.Equal(values, wantValues) {
			t.Fatalf("%d: autocommit dml mode values mismatch\n Got: %v\nWant: %v", i, values, wantValues)
		}
		if err := it.Next(values); err != io.EOF {
			t.Fatalf("%d: error mismatch\nGot: %v\nWant: %v", i, err, io.EOF)
		}

		// Set the next value.
		res, err := c.ExecContext(ctx, fmt.Sprintf("set autocommit_dml_mode = %s", test.setValue), []driver.NamedValue{})
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
	t.Parallel()

	p, _ := parser.NewStatementParser(databasepb.DatabaseDialect_GOOGLE_STANDARD_SQL, 1000)
	c := &conn{
		logger: noopLogger,
		state:  createInitialConnectionState(connectionstate.TypeNonTransactional, map[string]connectionstate.ConnectionPropertyValue{}),
		parser: p,
	}
	ctx := context.Background()
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
		{spanner.ReadTimestamp(time.Date(2021, 10, 8, 11, 14, 30, 10, time.UTC)), "'Read_Timestamp 2021-10-08T11:14:30.000000010Z'", false},
		{spanner.MinReadTimestamp(time.Date(2021, 10, 8, 9, 14, 30, 10, time.UTC)), "'Min_Read_Timestamp 2021-10-08T09:14:30.000000010Z'", false},
		{spanner.MinReadTimestamp(time.Date(2021, 10, 8, 11, 14, 30, 10, time.UTC)), "'Min_Read_Timestamp 2021-10-08T11:14:30.000000010Z'", false},
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
		res, err := c.ExecContext(ctx, "set read_only_staleness = "+test.setValue, []driver.NamedValue{})
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

		it, err := c.QueryContext(ctx, "show variable read_only_staleness", []driver.NamedValue{})
		if err != nil {
			t.Fatalf("%d: could not get current read-only staleness value from connection: %v", i, err)
		}
		cols := it.Columns()
		wantCols := []string{"read_only_staleness"}
		if !cmp.Equal(cols, wantCols) {
			t.Fatalf("%d: column names mismatch\n Got: %v\nWant: %v", i, cols, wantCols)
		}
		values := make([]driver.Value, len(cols))
		if err := it.Next(values); err != nil {
			t.Fatalf("%d: failed to get first row for read-only staleness: %v", i, err)
		}
		wantValues := []driver.Value{test.wantValue.String()}
		if !cmp.Equal(values, wantValues) {
			t.Fatalf("%d: read-only staleness values mismatch\n Got: %v\nWant: %v", i, values, wantValues)
		}
		if err := it.Next(values); err != io.EOF {
			t.Fatalf("%d: error mismatch\n Got: %v\nWant: %v", i, err, io.EOF)
		}
	}
}

func TestShowCommitTimestamp(t *testing.T) {
	t.Parallel()

	p, _ := parser.NewStatementParser(databasepb.DatabaseDialect_GOOGLE_STANDARD_SQL, 1000)
	c := &conn{
		logger: noopLogger,
		parser: p,
		state:  createInitialConnectionState(connectionstate.TypeNonTransactional, map[string]connectionstate.ConnectionPropertyValue{}),
	}
	ctx := context.Background()

	ts := time.Now()
	for _, test := range []struct {
		wantValue *time.Time
	}{
		{&ts},
		{nil},
	} {
		if test.wantValue == nil {
			c.clearCommitResponse()
		} else {
			c.setCommitResponse(&spanner.CommitResponse{CommitTs: *test.wantValue})
		}

		it, err := c.QueryContext(ctx, "show variable commit_timestamp", []driver.NamedValue{})
		if err != nil {
			t.Fatalf("could not get current commit timestamp from connection: %v", err)
		}
		cols := it.Columns()
		wantCols := []string{"commit_timestamp"}
		if !cmp.Equal(cols, wantCols) {
			t.Fatalf("column names mismatch\nGot: %v\nWant: %v", cols, wantCols)
		}
		values := make([]driver.Value, len(cols))
		if err := it.Next(values); err != nil {
			t.Fatalf("failed to get first row for commit timestamp: %v", err)
		}
		var wantValues []driver.Value
		if test.wantValue != nil {
			wantValues = []driver.Value{*test.wantValue}
		} else {
			wantValues = []driver.Value{nil}
		}
		if !cmp.Equal(values, wantValues) {
			t.Fatalf("commit timestamp values mismatch\nGot: %v\nWant: %v", values, wantValues)
		}
		if err := it.Next(values); err != io.EOF {
			t.Fatalf("error mismatch\nGot: %v\nWant: %v", err, io.EOF)
		}
	}
}

func TestStatementExecutor_ExcludeTxnFromChangeStreams(t *testing.T) {
	t.Parallel()

	p, _ := parser.NewStatementParser(databasepb.DatabaseDialect_GOOGLE_STANDARD_SQL, 1000)
	c := &conn{
		logger: noopLogger,
		state:  createInitialConnectionState(connectionstate.TypeNonTransactional, map[string]connectionstate.ConnectionPropertyValue{}),
		parser: p,
	}
	ctx := context.Background()
	for i, test := range []struct {
		wantValue  bool
		setValue   string
		wantSetErr bool
	}{
		{false, "false", false},
		{false, "true", false},
		{true, "FALSE", false},
		{false, "TRUE", false},
		{true, "False", false},
		{false, "True", false},
		{true, "fasle", true},
		{true, "truye", true},
	} {
		it, err := c.QueryContext(ctx, "show variable exclude_txn_from_change_streams", []driver.NamedValue{})
		if err != nil {
			t.Fatalf("%d: could not get current exclude value from connection: %v", i, err)
		}
		cols := it.Columns()
		wantCols := []string{"exclude_txn_from_change_streams"}
		if !cmp.Equal(cols, wantCols) {
			t.Fatalf("%d: column names mismatch\nGot: %v\nWant: %v", i, cols, wantCols)
		}
		values := make([]driver.Value, len(cols))
		if err := it.Next(values); err != nil {
			t.Fatalf("%d: failed to get first row: %v", i, err)
		}
		wantValues := []driver.Value{test.wantValue}
		if !cmp.Equal(values, wantValues) {
			t.Fatalf("%d: exclude values mismatch\nGot: %v\nWant: %v", i, values, wantValues)
		}
		if err := it.Next(values); err != io.EOF {
			t.Fatalf("%d: error mismatch\nGot: %v\nWant: %v", i, err, io.EOF)
		}

		// Set the next value.
		res, err := c.ExecContext(ctx, "set exclude_txn_from_change_streams="+test.setValue, []driver.NamedValue{})
		if test.wantSetErr {
			if err == nil {
				t.Fatalf("%d: missing expected error for value %q", i, test.setValue)
			}
		} else {
			if err != nil {
				t.Fatalf("%d: could not set new value %q for exclude: %v", i, test.setValue, err)
			}
			if res != driver.ResultNoRows {
				t.Fatalf("%d: result mismatch\nGot: %v\nWant: %v", i, res, driver.ResultNoRows)
			}
		}
	}
}

func TestStatementExecutor_MaxCommitDelay(t *testing.T) {
	t.Parallel()

	p, _ := parser.NewStatementParser(databasepb.DatabaseDialect_GOOGLE_STANDARD_SQL, 1000)
	c := &conn{
		logger: noopLogger,
		state:  createInitialConnectionState(connectionstate.TypeNonTransactional, map[string]connectionstate.ConnectionPropertyValue{}),
		parser: p,
	}
	ctx := context.Background()
	for i, test := range []struct {
		wantValue  time.Duration
		setValue   string
		wantSetErr bool
	}{
		{time.Second, "'1s'", false},
		{10 * time.Millisecond, "'10ms'", false},
		{20 * time.Microsecond, "'20us'", false},
		{30 * time.Nanosecond, "'30ns'", false},
		{time.Duration(0), "NULL", false},
		{100 * time.Millisecond, "100", false},
		{100 * time.Millisecond, "true", true},
		{100 * time.Millisecond, "ms", true},
		{100 * time.Millisecond, "'ms'", true},
		{20 * time.Millisecond, "20ms", false},
		{10 * time.Millisecond, "10", false},
		{10 * time.Millisecond, "'10ms", true},
		{10 * time.Millisecond, "10ms'", true},
		// Note that setting it to '0s' is different from setting it to NULL.
		{time.Duration(0), "0s", false},
	} {
		_, err := c.ExecContext(ctx, "set max_commit_delay = "+test.setValue, []driver.NamedValue{})
		if test.wantSetErr {
			if err == nil {
				t.Fatalf("%d: missing expected error for value %q", i, test.setValue)
			}
		} else {
			if err != nil {
				t.Fatalf("%d: could not set new value %q for max_commit_delay: %v", i, test.setValue, err)
			}
		}

		it, err := c.QueryContext(ctx, "show variable max_commit_delay", []driver.NamedValue{})
		if err != nil {
			t.Fatalf("%d: could not get current max_commit_delay value from connection: %v", i, err)
		}
		cols := it.Columns()
		wantCols := []string{"max_commit_delay"}
		if !cmp.Equal(cols, wantCols) {
			t.Fatalf("%d: column names mismatch\nGot: %v\nWant: %v", i, cols, wantCols)
		}
		values := make([]driver.Value, len(cols))
		if err := it.Next(values); err != nil {
			t.Fatalf("%d: failed to get first row for max_commit_delay: %v", i, err)
		}
		wantValues := []driver.Value{test.wantValue.String()}
		if strings.EqualFold(test.setValue, "null") {
			// If the value is set to NULL (which means 'set it to the value it would have if no value had been set'),
			// then the returned value is an empty string. This makes it possible to distinguish between having an
			// explicit value of '0s' and having no value at all.
			wantValues[0] = ""
		}
		if !cmp.Equal(values, wantValues) {
			t.Fatalf("%d (%s): max_commit_delay values mismatch\n Got: %v\nWant: %v", i, test.setValue, values, wantValues)
		}
		if err := it.Next(values); err != io.EOF {
			t.Fatalf("%d: error mismatch\nGot: %v\nWant: %v", i, err, io.EOF)
		}
	}
}

func TestStatementExecutor_SetTransactionTag(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	p, _ := parser.NewStatementParser(databasepb.DatabaseDialect_GOOGLE_STANDARD_SQL, 1000)
	for i, test := range []struct {
		wantValue  string
		setValue   string
		wantSetErr bool
	}{
		{"test-tag", "'test-tag'", false},
		{"other-tag", "  'other-tag'\t\n", false},
		{" tag with spaces ", "' tag with spaces '", false},
		{"", "tag-without-quotes", true},
		{"", "tag-with-missing-opening-quote'", true},
		{"", "'tag-with-missing-closing-quote", true},
	} {
		c := &conn{
			logger: noopLogger,
			state:  createInitialConnectionState(connectionstate.TypeNonTransactional, map[string]connectionstate.ConnectionPropertyValue{}),
			parser: p,
		}

		it, err := c.QueryContext(ctx, "show variable transaction_tag", []driver.NamedValue{})
		if err != nil {
			t.Fatalf("%d: could not get current transaction tag value from connection: %v", i, err)
		}
		cols := it.Columns()
		wantCols := []string{"transaction_tag"}
		if !cmp.Equal(cols, wantCols) {
			t.Fatalf("%d: column names mismatch\nGot: %v\nWant: %v", i, cols, wantCols)
		}
		values := make([]driver.Value, len(cols))
		if err := it.Next(values); err != nil {
			t.Fatalf("%d: failed to get first row: %v", i, err)
		}
		wantValues := []driver.Value{""}
		if !cmp.Equal(values, wantValues) {
			t.Fatalf("%d: default transaction tag mismatch\nGot: %v\nWant: %v", i, values, wantValues)
		}
		if err := it.Next(values); err != io.EOF {
			t.Fatalf("%d: error mismatch\nGot: %v\nWant: %v", i, err, io.EOF)
		}

		// Set a transaction tag.
		res, err := c.ExecContext(ctx, "set transaction_tag="+test.setValue, []driver.NamedValue{})
		if test.wantSetErr {
			if err == nil {
				t.Fatalf("%d: missing expected error for value %q", i, test.setValue)
			}
		} else {
			if err != nil {
				t.Fatalf("%d: could not set new value %q for exclude: %v", i, test.setValue, err)
			}
			if res != driver.ResultNoRows {
				t.Fatalf("%d: result mismatch\nGot: %v\nWant: %v", i, res, driver.ResultNoRows)
			}
		}

		// Get the tag that was set
		it, err = c.QueryContext(ctx, "show variable transaction_tag", []driver.NamedValue{})
		if err != nil {
			t.Fatalf("%d: could not get current transaction tag value from connection: %v", i, err)
		}
		if err := it.Next(values); err != nil {
			t.Fatalf("%d: failed to get first row: %v", i, err)
		}
		wantValues = []driver.Value{test.wantValue}
		if !cmp.Equal(values, wantValues) {
			t.Fatalf("%d: transaction tag mismatch\nGot: %v\nWant: %v", i, values, wantValues)
		}
		if err := it.Next(values); err != io.EOF {
			t.Fatalf("%d: error mismatch\nGot: %v\nWant: %v", i, err, io.EOF)
		}

	}
}

func TestStatementExecutor_UsesExecOptions(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	p, _ := parser.NewStatementParser(databasepb.DatabaseDialect_GOOGLE_STANDARD_SQL, 1000)
	c := &conn{
		logger: noopLogger,
		state:  createInitialConnectionState(connectionstate.TypeNonTransactional, map[string]connectionstate.ConnectionPropertyValue{}),
		parser: p,
	}

	clientStmt, err := c.parser.ParseClientSideStatement("show variable read_only_staleness")
	if err != nil {
		t.Fatal(err)
	}
	execStmt, err := createExecutableStatement(clientStmt)
	if err != nil {
		t.Fatal(err)
	}
	it, err := execStmt.queryContext(ctx, c, &ExecOptions{DecodeOption: DecodeOptionProto, ReturnResultSetMetadata: true, ReturnResultSetStats: true})
	if err != nil {
		t.Fatalf("could not get current staleness value from connection: %v", err)
	}
	rows, ok := it.(driver.RowsNextResultSet)
	if !ok {
		t.Fatal("did not get RowsNextResultSet")
	}
	// The first result set contains the metadata.
	cols := rows.Columns()
	wantCols := []string{"metadata"}
	if !cmp.Equal(cols, wantCols) {
		t.Fatalf("column names mismatch\nGot: %v\nWant: %v", cols, wantCols)
	}
	wantValues := []driver.Value{&spannerpb.ResultSetMetadata{
		RowType: &spannerpb.StructType{
			Fields: []*spannerpb.StructType_Field{
				{Name: "read_only_staleness", Type: &spannerpb.Type{Code: spannerpb.TypeCode_STRING}},
			},
		},
	}}
	values := make([]driver.Value, len(cols))
	if err := rows.Next(values); err != nil {
		t.Fatalf("failed to get first row: %v", err)
	}
	if !cmp.Equal(values, wantValues, cmpopts.IgnoreUnexported(spannerpb.ResultSetMetadata{}, spannerpb.StructType{}, spannerpb.StructType_Field{}, spannerpb.Type{})) {
		t.Fatalf("default staleness mismatch\n Got: %v\nWant: %v", values, wantValues)
	}
	if err := rows.Next(values); err != io.EOF {
		t.Fatalf("error mismatch\nGot: %v\nWant: %v", err, io.EOF)
	}

	// Move to the next result set, which should contain the data.
	if !rows.HasNextResultSet() {
		t.Fatal("missing next result set")
	}
	if err := rows.NextResultSet(); err != nil {
		t.Fatalf("error mismatch\n Got: %v\nWant: %v", err, nil)
	}

	cols = rows.Columns()
	wantCols = []string{"read_only_staleness"}
	if !cmp.Equal(cols, wantCols) {
		t.Fatalf("column names mismatch\n Got: %v\nWant: %v", cols, wantCols)
	}
	values = make([]driver.Value, len(cols))
	if err := rows.Next(values); err != nil {
		t.Fatalf("failed to get first row: %v", err)
	}
	// The value that we get should be the raw protobuf value.
	wantValues = []driver.Value{spanner.GenericColumnValue{
		Type:  &spannerpb.Type{Code: spannerpb.TypeCode_STRING},
		Value: &structpb.Value{Kind: &structpb.Value_StringValue{}},
	}}
	if !cmp.Equal(values, wantValues, cmpopts.IgnoreUnexported(spannerpb.Type{}, structpb.Value{})) {
		t.Fatalf("default staleness mismatch\n Got: %v\nWant: %v", values, wantValues)
	}
	if err := rows.Next(values); err != io.EOF {
		t.Fatalf("error mismatch\nGot: %v\nWant: %v", err, io.EOF)
	}

	// Move to the next result set, which should contain the ResultSetStats.
	if !rows.HasNextResultSet() {
		t.Fatal("missing next result set")
	}
	if err := rows.NextResultSet(); err != nil {
		t.Fatalf("error mismatch\n Got: %v\nWant: %v", err, nil)
	}
	cols = rows.Columns()
	wantCols = []string{"stats"}
	if !cmp.Equal(cols, wantCols) {
		t.Fatalf("column names mismatch\nGot: %v\nWant: %v", cols, wantCols)
	}
	wantValues = []driver.Value{&spannerpb.ResultSetStats{}}
	values = make([]driver.Value, len(cols))
	if err := rows.Next(values); err != nil {
		t.Fatalf("failed to get first row: %v", err)
	}
	if !cmp.Equal(values, wantValues, cmpopts.IgnoreUnexported(spannerpb.ResultSetStats{})) {
		t.Fatalf("ResultSetStats mismatch\nGot: %v\nWant: %v", values, wantValues)
	}
	if err := rows.Next(values); err != io.EOF {
		t.Fatalf("error mismatch\nGot: %v\nWant: %v", err, io.EOF)
	}

	// There should be no more result sets.
	if rows.HasNextResultSet() {
		t.Fatal("got unexpected next result set")
	}

}
