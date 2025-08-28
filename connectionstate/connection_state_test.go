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

package connectionstate

import (
	"testing"

	"cloud.google.com/go/spanner"
	"google.golang.org/grpc/codes"
)

func TestSetValueOutsideTransaction(t *testing.T) {
	prop := CreateConnectionProperty("my_property", "Test property", "initial-value", nil, ContextUser)
	properties := map[string]ConnectionProperty{
		"my_property": prop,
	}

	for _, tp := range []Type{TypeTransactional, TypeNonTransactional} {
		for _, setToValue := range []string{"new-value", ""} {
			state, _ := NewConnectionState(tp, properties, map[string]ConnectionPropertyValue{})
			if g, w := prop.GetValueOrDefault(state), prop.defaultValue; g != w {
				t.Fatalf("initial value mismatch\n Got: %v\nWant: %v", g, w)
			}
			if err := prop.SetValue(state, setToValue, ContextUser); err != nil {
				t.Fatal(err)
			}
			if g, w := prop.GetValueOrDefault(state), setToValue; g != w {
				t.Fatalf("value mismatch\n Got: %v\nWant: %v", g, w)
			}
		}
	}
}

func TestSetValueInTransactionAndCommit(t *testing.T) {
	prop := CreateConnectionProperty("my_property", "Test property", "initial-value", nil, ContextUser)
	properties := map[string]ConnectionProperty{
		"my_property": prop,
	}

	for _, tp := range []Type{TypeTransactional, TypeNonTransactional} {
		for _, setToValue := range []string{"new-value", ""} {
			state, _ := NewConnectionState(tp, properties, map[string]ConnectionPropertyValue{})
			if err := state.Begin(); err != nil {
				t.Fatal(err)
			}
			if g, w := prop.GetValueOrDefault(state), prop.defaultValue; g != w {
				t.Fatalf("initial value mismatch\n Got: %v\nWant: %v", g, w)
			}
			if err := prop.SetValue(state, setToValue, ContextUser); err != nil {
				t.Fatal(err)
			}
			if g, w := prop.GetValueOrDefault(state), setToValue; g != w {
				t.Fatalf("value mismatch\n Got: %v\nWant: %v", g, w)
			}

			// Verify that the change is persisted if the transaction is committed.
			if err := state.Commit(); err != nil {
				t.Fatal(err)
			}
			if g, w := prop.GetValueOrDefault(state), setToValue; g != w {
				t.Fatalf("value mismatch\n Got: %v\nWant: %v", g, w)
			}
		}
	}
}

func TestSetValueInTransactionAndRollback(t *testing.T) {
	prop := CreateConnectionProperty("my_property", "Test property", "initial-value", nil, ContextUser)
	properties := map[string]ConnectionProperty{
		"my_property": prop,
	}

	for _, tp := range []Type{TypeTransactional, TypeNonTransactional} {
		for _, setToValue := range []string{"new-value", ""} {
			state, _ := NewConnectionState(tp, properties, map[string]ConnectionPropertyValue{})
			if err := state.Begin(); err != nil {
				t.Fatal(err)
			}
			if g, w := prop.GetValueOrDefault(state), prop.defaultValue; g != w {
				t.Fatalf("initial value mismatch\n Got: %v\nWant: %v", g, w)
			}
			if err := prop.SetValue(state, setToValue, ContextUser); err != nil {
				t.Fatal(err)
			}
			if g, w := prop.GetValueOrDefault(state), setToValue; g != w {
				t.Fatalf("value mismatch\n Got: %v\nWant: %v", g, w)
			}

			// Verify that the change is rolled back if the transaction is rolled back and the connection
			// state is transactional.
			if err := state.Rollback(); err != nil {
				t.Fatal(err)
			}
			var expected string
			if tp == TypeTransactional {
				expected = prop.defaultValue
			} else {
				expected = setToValue
			}
			if g, w := prop.GetValueOrDefault(state), expected; g != w {
				t.Fatalf("value mismatch\n Got: %v\nWant: %v", g, w)
			}
		}
	}
}

func TestResetValueOutsideTransaction(t *testing.T) {
	prop := CreateConnectionProperty("my_property", "Test property", "initial-value", nil, ContextUser)
	properties := map[string]ConnectionProperty{
		"my_property": prop,
	}

	for _, tp := range []Type{TypeTransactional, TypeNonTransactional} {
		state, _ := NewConnectionState(tp, properties, map[string]ConnectionPropertyValue{})
		_ = prop.SetValue(state, "new-value", ContextUser)
		if err := prop.ResetValue(state, ContextUser); err != nil {
			t.Fatal(err)
		}
		if g, w := prop.GetValueOrDefault(state), prop.defaultValue; g != w {
			t.Fatalf("value mismatch\n Got: %v\nWant: %v", g, w)
		}
	}
}

func TestResetValueInTransactionAndCommit(t *testing.T) {
	prop := CreateConnectionProperty("my_property", "Test property", "initial-value", nil, ContextUser)
	properties := map[string]ConnectionProperty{
		"my_property": prop,
	}

	for _, tp := range []Type{TypeTransactional, TypeNonTransactional} {
		state, _ := NewConnectionState(tp, properties, map[string]ConnectionPropertyValue{})
		if err := state.Begin(); err != nil {
			t.Fatal(err)
		}

		// Change the value to something else than the default and commit.
		_ = prop.SetValue(state, "new-value", ContextUser)
		_ = state.Commit()
		if g, w := prop.GetValueOrDefault(state), "new-value"; g != w {
			t.Fatalf("value mismatch\n Got: %v\nWant: %v", g, w)
		}

		// Now reset the value to the default in a new transaction.
		if err := state.Begin(); err != nil {
			t.Fatal(err)
		}
		if err := prop.ResetValue(state, ContextUser); err != nil {
			t.Fatal(err)
		}
		if g, w := prop.GetValueOrDefault(state), prop.defaultValue; g != w {
			t.Fatalf("value mismatch\n Got: %v\nWant: %v", g, w)
		}

		// Verify that the change is persisted if the transaction is committed.
		if err := state.Commit(); err != nil {
			t.Fatal(err)
		}
		if g, w := prop.GetValueOrDefault(state), prop.defaultValue; g != w {
			t.Fatalf("value mismatch\n Got: %v\nWant: %v", g, w)
		}
	}
}

func TestResetValueInTransactionAndRollback(t *testing.T) {
	prop := CreateConnectionProperty("my_property", "Test property", "initial-value", nil, ContextUser)
	properties := map[string]ConnectionProperty{
		"my_property": prop,
	}

	for _, tp := range []Type{TypeTransactional, TypeNonTransactional} {
		state, _ := NewConnectionState(tp, properties, map[string]ConnectionPropertyValue{})
		if err := state.Begin(); err != nil {
			t.Fatal(err)
		}

		// Change the value to something else than the default and commit.
		_ = prop.SetValue(state, "new-value", ContextUser)
		if err := state.Commit(); err != nil {
			t.Fatal(err)
		}
		if g, w := prop.GetValueOrDefault(state), "new-value"; g != w {
			t.Fatalf("value mismatch\n Got: %v\nWant: %v", g, w)
		}

		// Now reset the value to the default in a new transaction.
		if err := state.Begin(); err != nil {
			t.Fatal(err)
		}
		if err := prop.ResetValue(state, ContextUser); err != nil {
			t.Fatal(err)
		}
		if g, w := prop.GetValueOrDefault(state), prop.defaultValue; g != w {
			t.Fatalf("value mismatch\n Got: %v\nWant: %v", g, w)
		}

		// Verify that the change is rolled back if the transaction is rolled back and the connection
		// state is transactional.
		if err := state.Rollback(); err != nil {
			t.Fatal(err)
		}
		// The value should be rolled back to "new-value" if the state is transactional.
		// The value should be "initial-value" if the state is non-transactional, as the Reset is persisted regardless
		// whether the transaction committed or not.
		var expected string
		if tp == TypeTransactional {
			expected = "new-value"
		} else {
			expected = prop.defaultValue
		}
		if g, w := prop.GetValueOrDefault(state), expected; g != w {
			t.Fatalf("value mismatch\n Got: %v\nWant: %v", g, w)
		}
	}
}

func TestSetLocalValue(t *testing.T) {
	prop := CreateConnectionProperty("my_property", "Test property", "initial-value", nil, ContextUser)
	properties := map[string]ConnectionProperty{
		"my_property": prop,
	}

	for _, tp := range []Type{TypeTransactional, TypeNonTransactional} {
		state, _ := NewConnectionState(tp, properties, map[string]ConnectionPropertyValue{})
		if err := state.Begin(); err != nil {
			t.Fatal(err)
		}
		if g, w := prop.GetValueOrDefault(state), prop.defaultValue; g != w {
			t.Fatalf("initial value mismatch\n Got: %v\nWant: %v", g, w)
		}
		if err := prop.SetLocalValue(state, "new-value"); err != nil {
			t.Fatal(err)
		}
		if g, w := prop.GetValueOrDefault(state), "new-value"; g != w {
			t.Fatalf("value mismatch\n Got: %v\nWant: %v", g, w)
		}

		// Verify that the change is no longer visible once the transaction has ended, even if the
		// transaction was committed.
		if err := state.Commit(); err != nil {
			t.Fatal(err)
		}
		if g, w := prop.GetValueOrDefault(state), prop.defaultValue; g != w {
			t.Fatalf("value mismatch\n Got: %v\nWant: %v", g, w)
		}
	}
}

func TestSetLocalValueOutsideTransaction(t *testing.T) {
	prop := CreateConnectionProperty("my_property", "Test property", "initial-value", nil, ContextUser)
	properties := map[string]ConnectionProperty{
		"my_property": prop,
	}

	for _, tp := range []Type{TypeTransactional, TypeNonTransactional} {
		state, _ := NewConnectionState(tp, properties, map[string]ConnectionPropertyValue{})
		// Setting a local value outside a transaction is a no-op.
		if g, w := prop.GetValueOrDefault(state), prop.defaultValue; g != w {
			t.Fatalf("initial value mismatch\n Got: %v\nWant: %v", g, w)
		}
		if err := prop.SetLocalValue(state, "new-value"); err != nil {
			t.Fatal(err)
		}
		if g, w := prop.GetValueOrDefault(state), prop.defaultValue; g != w {
			t.Fatalf("value mismatch\n Got: %v\nWant: %v", g, w)
		}
	}
}

func TestSetLocalValueForStartupProperty(t *testing.T) {
	prop := CreateConnectionProperty("my_property", "Test property", "initial-value", nil, ContextStartup)
	properties := map[string]ConnectionProperty{
		"my_property": prop,
	}

	for _, tp := range []Type{TypeTransactional, TypeNonTransactional} {
		state, _ := NewConnectionState(tp, properties, map[string]ConnectionPropertyValue{})
		err := prop.SetLocalValue(state, "new-value")
		if g, w := spanner.ErrCode(err), codes.FailedPrecondition; g != w {
			t.Fatalf("error code mismatch\n Got: %v\nWant: %v", g, w)
		}
	}
}

func TestSetInTransactionForStartupProperty(t *testing.T) {
	prop := CreateConnectionProperty("my_property", "Test property", "initial-value", nil, ContextStartup)
	properties := map[string]ConnectionProperty{
		"my_property": prop,
	}

	for _, tp := range []Type{TypeTransactional, TypeNonTransactional} {
		state, _ := NewConnectionState(tp, properties, map[string]ConnectionPropertyValue{})
		_ = state.Begin()
		err := prop.SetValue(state, "new-value", ContextUser)
		if g, w := spanner.ErrCode(err), codes.FailedPrecondition; g != w {
			t.Fatalf("error code mismatch\n Got: %v\nWant: %v", g, w)
		}
	}
}

func TestSetStartupProperty(t *testing.T) {
	prop := CreateConnectionProperty("my_property", "Test property", "initial-value", nil, ContextStartup)
	properties := map[string]ConnectionProperty{
		"my_property": prop,
	}

	for _, tp := range []Type{TypeTransactional, TypeNonTransactional} {
		state, _ := NewConnectionState(tp, properties, map[string]ConnectionPropertyValue{})
		err := prop.SetValue(state, "new-value", ContextUser)
		if g, w := spanner.ErrCode(err), codes.FailedPrecondition; g != w {
			t.Fatalf("error code mismatch\n Got: %v\nWant: %v", g, w)
		}
	}
}

func TestSetNormalAndLocalValue(t *testing.T) {
	prop := CreateConnectionProperty("my_property", "Test property", "initial-value", nil, ContextUser)
	properties := map[string]ConnectionProperty{
		"my_property": prop,
	}

	for _, commit := range []bool{true, false} {
		for _, tp := range []Type{TypeTransactional, TypeNonTransactional} {
			state, _ := NewConnectionState(tp, properties, map[string]ConnectionPropertyValue{})
			_ = state.Begin()
			if g, w := prop.GetValueOrDefault(state), prop.defaultValue; g != w {
				t.Fatalf("initial value mismatch\n Got: %v\nWant: %v", g, w)
			}
			// First set a local value.
			if err := prop.SetLocalValue(state, "local-value"); err != nil {
				t.Fatal(err)
			}
			if g, w := prop.GetValueOrDefault(state), "local-value"; g != w {
				t.Fatalf("value mismatch\n Got: %v\nWant: %v", g, w)
			}
			// Then set a 'standard' value in a transaction.
			// This should override the local value.
			if err := prop.SetValue(state, "new-value", ContextUser); err != nil {
				t.Fatal(err)
			}
			if g, w := prop.GetValueOrDefault(state), "new-value"; g != w {
				t.Fatalf("value mismatch\n Got: %v\nWant: %v", g, w)
			}
			// Then set yet another local value. This should take precedence within the current transaction.
			if err := prop.SetLocalValue(state, "second-local-value"); err != nil {
				t.Fatal(err)
			}
			if g, w := prop.GetValueOrDefault(state), "second-local-value"; g != w {
				t.Fatalf("value mismatch\n Got: %v\nWant: %v", g, w)
			}

			// Verify that the last local value is lost when the transaction ends.
			// The value that was set in the transaction should be persisted if
			// the transaction is committed, or if we are using non-transactional state.
			if commit {
				_ = state.Commit()
				if g, w := prop.GetValueOrDefault(state), "new-value"; g != w {
					t.Fatalf("value mismatch\n Got: %v\nWant: %v", g, w)
				}
			} else {
				_ = state.Rollback()
				if tp == TypeNonTransactional {
					// The transaction was rolled back, but this should have no impact on the
					// SET statement in the transaction. So the new-value should be persisted.
					if g, w := prop.GetValueOrDefault(state), "new-value"; g != w {
						t.Fatalf("value mismatch\n Got: %v\nWant: %v", g, w)
					}
				} else {
					// The transaction was rolled back. The value should have been reset to
					// the value it had before the transaction.
					if g, w := prop.GetValueOrDefault(state), "initial-value"; g != w {
						t.Fatalf("value mismatch\n Got: %v\nWant: %v", g, w)
					}
				}
			}
		}
	}
}

func TestSetUnknownProperty(t *testing.T) {
	properties := map[string]ConnectionProperty{}

	for _, tp := range []Type{TypeTransactional, TypeNonTransactional} {
		state, _ := NewConnectionState(tp, properties, map[string]ConnectionPropertyValue{})
		// Try to add a new property without an extension to the connection state.
		// This should fail.
		prop := CreateConnectionProperty("prop", "Test property", "initial-value", nil, ContextUser)
		err := prop.SetValue(state, "new-value", ContextUser)
		if g, w := spanner.ErrCode(err), codes.InvalidArgument; g != w {
			t.Fatalf("error code mismatch\n Got: %v\nWant: %v", g, w)
		}

		// Adding a new property with an extension to the connection state should work.
		propWithExtension := CreateConnectionPropertyWithExtension("spanner", "prop3", "Test property 3", "initial-value-3", nil, ContextUser)
		if err := propWithExtension.SetValue(state, "new-value", ContextUser); err != nil {
			t.Fatal(err)
		}
	}
}

func TestReset(t *testing.T) {
	prop1 := CreateConnectionProperty("prop1", "Test property 1", "initial-value-1", nil, ContextUser)
	prop2 := CreateConnectionProperty("prop2", "Test property 2", "initial-value-2", nil, ContextUser)
	properties := map[string]ConnectionProperty{
		prop1.key: prop1,
		prop2.key: prop2,
	}

	for _, tp := range []Type{TypeTransactional, TypeNonTransactional} {
		state, _ := NewConnectionState(tp, properties, map[string]ConnectionPropertyValue{})
		_ = prop1.SetValue(state, "new-value-1", ContextUser)
		_ = prop2.SetValue(state, "new-value-2", ContextUser)
		// Add a new property to the connection state. This will be removed when the state is reset.
		prop3 := CreateConnectionPropertyWithExtension("spanner", "prop3", "Test property 3", "initial-value-3", nil, ContextUser)
		if err := prop3.SetValue(state, "new-value-3", ContextUser); err != nil {
			t.Fatal(err)
		}

		if g, w := prop1.GetValueOrDefault(state), "new-value-1"; g != w {
			t.Fatalf("value mismatch\n Got: %v\nWant: %v", g, w)
		}
		if g, w := prop2.GetValueOrDefault(state), "new-value-2"; g != w {
			t.Fatalf("value mismatch\n Got: %v\nWant: %v", g, w)
		}
		if g, w := prop3.GetValueOrDefault(state), "new-value-3"; g != w {
			t.Fatalf("value mismatch\n Got: %v\nWant: %v", g, w)
		}

		if err := state.Reset(ContextUser); err != nil {
			t.Fatal(err)
		}
	}
}

func TestResetWithInitialValues(t *testing.T) {
	prop1 := CreateConnectionProperty("prop1", "Test property 1", "default-value-1", nil, ContextUser)
	prop2 := CreateConnectionProperty("prop2", "Test property 2", "default-value-2", nil, ContextUser)
	properties := map[string]ConnectionProperty{
		prop1.key: prop1,
		prop2.key: prop2,
	}
	initialValues := map[string]ConnectionPropertyValue{
		prop2.key: CreateInitialValue(prop2, "initial-value-2"),
	}

	for _, tp := range []Type{TypeTransactional, TypeNonTransactional} {
		state, _ := NewConnectionState(tp, properties, initialValues)
		// Verify that prop1 has the default value of the property, and that prop2 has the initial value that was
		// passed in when the ConnectionState was created.
		if g, w := prop1.GetValueOrDefault(state), "default-value-1"; g != w {
			t.Fatalf("value mismatch\n Got: %v\nWant: %v", g, w)
		}
		if g, w := prop2.GetValueOrDefault(state), "initial-value-2"; g != w {
			t.Fatalf("value mismatch\n Got: %v\nWant: %v", g, w)
		}

		// Verify that updating the values works.
		_ = prop1.SetValue(state, "new-value-1", ContextUser)
		_ = prop2.SetValue(state, "new-value-2", ContextUser)

		if g, w := prop1.GetValueOrDefault(state), "new-value-1"; g != w {
			t.Fatalf("value mismatch\n Got: %v\nWant: %v", g, w)
		}
		if g, w := prop2.GetValueOrDefault(state), "new-value-2"; g != w {
			t.Fatalf("value mismatch\n Got: %v\nWant: %v", g, w)
		}

		// Resetting the values should bring them back to the original values.
		if err := state.Reset(ContextUser); err != nil {
			t.Fatal(err)
		}
		if g, w := prop1.GetValueOrDefault(state), "default-value-1"; g != w {
			t.Fatalf("value mismatch\n Got: %v\nWant: %v", g, w)
		}
		if g, w := prop2.GetValueOrDefault(state), "initial-value-2"; g != w {
			t.Fatalf("value mismatch\n Got: %v\nWant: %v", g, w)
		}
	}
}
