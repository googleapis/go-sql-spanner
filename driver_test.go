// Copyright 2021 Google LLC
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
	"database/sql/driver"
	"testing"

	"github.com/google/go-cmp/cmp"
)

func TestExtractDnsParts(t *testing.T) {
	tests := []struct {
		input   string
		want    connectorConfig
		wantErr error
	}{
		{
			input: "projects/p/instances/i/databases/d",
			want: connectorConfig{
				project:  "p",
				instance: "i",
				database: "d",
				params:   map[string]string{},
			},
		},
		{
			input: "projects/DEFAULT_PROJECT_ID/instances/test-instance/databases/test-database",
			want: connectorConfig{
				project:  "DEFAULT_PROJECT_ID",
				instance: "test-instance",
				database: "test-database",
				params:   map[string]string{},
			},
		},
		{
			input: "localhost:9010/projects/p/instances/i/databases/d",
			want: connectorConfig{
				host:     "localhost:9010",
				project:  "p",
				instance: "i",
				database: "d",
				params:   map[string]string{},
			},
		},
		{
			input: "spanner.googleapis.com/projects/p/instances/i/databases/d",
			want: connectorConfig{
				host:     "spanner.googleapis.com",
				project:  "p",
				instance: "i",
				database: "d",
				params:   map[string]string{},
			},
		},
		{
			input: "spanner.googleapis.com/projects/p/instances/i/databases/d?usePlainText=true",
			want: connectorConfig{
				host:     "spanner.googleapis.com",
				project:  "p",
				instance: "i",
				database: "d",
				params: map[string]string{
					"useplaintext": "true",
				},
			},
		},
		{
			input: "spanner.googleapis.com/projects/p/instances/i/databases/d;credentials=/path/to/credentials.json",
			want: connectorConfig{
				host:     "spanner.googleapis.com",
				project:  "p",
				instance: "i",
				database: "d",
				params: map[string]string{
					"credentials": "/path/to/credentials.json",
				},
			},
		},
		{
			input: "spanner.googleapis.com/projects/p/instances/i/databases/d?credentials=/path/to/credentials.json;readonly=true",
			want: connectorConfig{
				host:     "spanner.googleapis.com",
				project:  "p",
				instance: "i",
				database: "d",
				params: map[string]string{
					"credentials": "/path/to/credentials.json",
					"readonly":    "true",
				},
			},
		},
		{
			input: "spanner.googleapis.com/projects/p/instances/i/databases/d?usePlainText=true;",
			want: connectorConfig{
				host:     "spanner.googleapis.com",
				project:  "p",
				instance: "i",
				database: "d",
				params: map[string]string{
					"useplaintext": "true",
				},
			},
		},
	}
	for _, tc := range tests {
		config, err := extractConnectorConfig(tc.input)
		if err != nil {
			t.Errorf("extract failed for %q: %v", tc.input, err)
		} else {
			if !cmp.Equal(config, tc.want, cmp.AllowUnexported(connectorConfig{})) {
				t.Errorf("connector config mismatch for %q\ngot: %v\nwant %v", tc.input, config, tc.want)
			}
		}
	}
}

func TestConnection_NoNestedTransactions(t *testing.T) {
	c := conn{
		tx: &readOnlyTransaction{},
	}
	_, err := c.BeginTx(context.Background(), driver.TxOptions{})
	if err == nil {
		t.Errorf("unexpected nil error for BeginTx call")
	}
}

func TestConn_AbortBatch(t *testing.T) {
	c := &conn{}
	if err := c.StartBatchDdl(); err != nil {
		t.Fatalf("failed to start DDL batch: %v", err)
	}
	if !c.InDdlBatch() {
		t.Fatal("connection did not start DDL batch")
	}
	if err := c.AbortBatch(); err != nil {
		t.Fatalf("failed to abort DDL batch: %v", err)
	}
	if c.InDdlBatch() {
		t.Fatal("connection did not abort DDL batch")
	}

	if err := c.StartBatchDml(); err != nil {
		t.Fatalf("failed to start DML batch: %v", err)
	}
	if !c.InDmlBatch() {
		t.Fatal("connection did not start DML batch")
	}
	if err := c.AbortBatch(); err != nil {
		t.Fatalf("failed to abort DML batch: %v", err)
	}
	if c.InDmlBatch() {
		t.Fatal("connection did not abort DML batch")
	}
}

func TestConn_StartBatchDdl(t *testing.T) {
	for _, test := range []struct {
		name    string
		c       *conn
		wantErr bool
	}{
		{"Default", &conn{}, false},
		{"In DDL batch", &conn{batch: &batch{tp: ddl}}, true},
		{"In DML batch", &conn{batch: &batch{tp: dml}}, true},
		{"In read/write transaction", &conn{tx: &readWriteTransaction{}}, true},
		{"In read-only transaction", &conn{tx: &readOnlyTransaction{}}, true},
		{"In read/write transaction with a DML batch", &conn{tx: &readWriteTransaction{batch: &batch{tp: dml}}}, true},
	} {
		err := test.c.StartBatchDdl()
		if test.wantErr {
			if err == nil {
				t.Fatalf("%s: no error returned for StartDdlBatch: %v", test.name, err)
			}
		} else {
			if err != nil {
				t.Fatalf("%s: failed to start DDL batch: %v", test.name, err)
			}
			if !test.c.InDdlBatch() {
				t.Fatalf("%s: connection did not start DDL batch", test.name)
			}
		}
	}
}

func TestConn_StartBatchDml(t *testing.T) {
	for _, test := range []struct {
		name    string
		c       *conn
		wantErr bool
	}{
		{"Default", &conn{}, false},
		{"In DDL batch", &conn{batch: &batch{tp: ddl}}, true},
		{"In DML batch", &conn{batch: &batch{tp: dml}}, true},
		{"In read/write transaction", &conn{tx: &readWriteTransaction{}}, false},
		{"In read-only transaction", &conn{tx: &readOnlyTransaction{}}, true},
		{"In read/write transaction with a DML batch", &conn{tx: &readWriteTransaction{batch: &batch{tp: dml}}}, true},
	} {
		err := test.c.StartBatchDml()
		if test.wantErr {
			if err == nil {
				t.Fatalf("%s: no error returned for StartDmlBatch: %v", test.name, err)
			}
		} else {
			if err != nil {
				t.Fatalf("%s: failed to start DML batch: %v", test.name, err)
			}
			if !test.c.InDmlBatch() {
				t.Fatalf("%s: connection did not start DML batch", test.name)
			}
		}
	}
}
