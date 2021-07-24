// Copyright 2020 Google Inc. All Rights Reserved.
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
					"usePlainText": "true",
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
