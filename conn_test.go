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
	"database/sql"
	"testing"

	"github.com/googleapis/go-sql-spanner/connectionstate"
	"github.com/googleapis/go-sql-spanner/parser"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestApplyStatementScopedValues(t *testing.T) {
	t.Parallel()

	c := &conn{
		logger: noopLogger,
		state:  createInitialConnectionState(connectionstate.TypeNonTransactional, map[string]connectionstate.ConnectionPropertyValue{}),
	}
	if g, w := propertyIsolationLevel.GetValueOrDefault(c.state), sql.LevelDefault; g != w {
		t.Fatalf("default isolation level mismatch\n Got: %v\nWant: %v", g, w)
	}

	// Add a statement-scoped connection property value for isolation_level.
	cleanup, err := c.applyStatementScopedValues(&ExecOptions{
		PropertyValues: []PropertyValue{
			CreatePropertyValue("isolation_level", "repeatable read"),
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if g, w := propertyIsolationLevel.GetValueOrDefault(c.state), sql.LevelRepeatableRead; g != w {
		t.Fatalf("statement isolation level mismatch\n Got: %v\nWant: %v", g, w)
	}
	// Clean up the statement-scoped connection properties.
	cleanup()

	// The isolation level should now be back to default.
	if g, w := propertyIsolationLevel.GetValueOrDefault(c.state), sql.LevelDefault; g != w {
		t.Fatalf("default isolation level mismatch\n Got: %v\nWant: %v", g, w)
	}
}

func TestApplyStatementScopedValuesWithInvalidValue(t *testing.T) {
	t.Parallel()

	c := &conn{
		logger: noopLogger,
		state:  createInitialConnectionState(connectionstate.TypeNonTransactional, map[string]connectionstate.ConnectionPropertyValue{}),
	}

	// Add a statement-scoped connection property value for isolation_level with an invalid value.
	_, err := c.applyStatementScopedValues(&ExecOptions{
		PropertyValues: []PropertyValue{
			CreatePropertyValue("isolation_level", "not an isolation level"),
		},
	})
	if g, w := status.Code(err), codes.InvalidArgument; g != w {
		t.Fatalf("error mismatch\n Got: %v\nWant: %v", g, w)
	}
	if g, w := propertyIsolationLevel.GetValueOrDefault(c.state), sql.LevelDefault; g != w {
		t.Fatalf("statement isolation level mismatch\n Got: %v\nWant: %v", g, w)
	}
}

func TestApplyStatementScopedValuesWithUnknownProperty(t *testing.T) {
	t.Parallel()

	c := &conn{
		logger: noopLogger,
		state:  createInitialConnectionState(connectionstate.TypeNonTransactional, map[string]connectionstate.ConnectionPropertyValue{}),
	}

	// Add a statement-scoped connection property value for an unknown property without an extension.
	_, err := c.applyStatementScopedValues(&ExecOptions{
		PropertyValues: []PropertyValue{
			CreatePropertyValue("non_existing_property", "some-value"),
		},
	})
	if g, w := status.Code(err), codes.InvalidArgument; g != w {
		t.Fatalf("error mismatch\n Got: %v\nWant: %v", g, w)
	}
	if g, w := propertyIsolationLevel.GetValueOrDefault(c.state), sql.LevelDefault; g != w {
		t.Fatalf("statement isolation level mismatch\n Got: %v\nWant: %v", g, w)
	}
}

func TestApplyStatementScopedValuesWithExtension(t *testing.T) {
	t.Parallel()

	c := &conn{
		logger: noopLogger,
		state:  createInitialConnectionState(connectionstate.TypeNonTransactional, map[string]connectionstate.ConnectionPropertyValue{}),
	}

	// Add a statement-scoped connection property value for an unknown property with an extension.
	// Property values for unknown properties with an extension are allowed.
	propValue := PropertyValue{
		Identifier: parser.Identifier{Parts: []string{"my_extension", "my_property"}},
		Value:      "some-value",
	}
	cleanup, err := c.applyStatementScopedValues(&ExecOptions{
		PropertyValues: []PropertyValue{propValue},
	})
	if g, w := status.Code(err), codes.OK; g != w {
		t.Fatalf("error mismatch\n Got: %v\nWant: %v", g, w)
	}
	val, ok, err := c.state.GetValue("my_extension", "my_property")
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("missing my_extension.my_property")
	}
	if g, w := val, "some-value"; g != w {
		t.Fatalf("value mismatch\n Got: %v\nWant: %v", g, w)
	}
	cleanup()

	// The value should now be gone.
	_, ok, err = c.state.GetValue("my_extension", "my_property")
	if g, w := status.Code(err), codes.InvalidArgument; g != w {
		t.Fatalf("error mismatch\n Got: %v\nWant: %v", g, w)
	}
	if ok {
		t.Fatal("got unexpected value for my_extension.my_property")
	}
}
