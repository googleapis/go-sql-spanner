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
	"google.golang.org/grpc/status"
)

func noop(v string) (string, error) {
	return v, nil
}

func TestSetValueOutsideTransaction(t *testing.T) {
	prop := CreateConnectionProperty("my_property", "Test property", "initial-value", false, nil, ContextUser, noop)
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
	prop := CreateConnectionProperty("my_property", "Test property", "initial-value", false, nil, ContextUser, noop)
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
	prop := CreateConnectionProperty("my_property", "Test property", "initial-value", false, nil, ContextUser, noop)
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
	prop := CreateConnectionProperty("my_property", "Test property", "initial-value", false, nil, ContextUser, noop)
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
	prop := CreateConnectionProperty("my_property", "Test property", "initial-value", false, nil, ContextUser, noop)
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
	prop := CreateConnectionProperty("my_property", "Test property", "initial-value", false, nil, ContextUser, noop)
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
	prop := CreateConnectionProperty("my_property", "Test property", "initial-value", false, nil, ContextUser, noop)
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

func TestSetStatementValue(t *testing.T) {
	prop := CreateConnectionProperty("my_property", "Test property", "initial-value", false, nil, ContextUser, noop)
	properties := map[string]ConnectionProperty{
		"my_property": prop,
	}

	for _, tp := range []Type{TypeTransactional, TypeNonTransactional} {
		state, _ := NewConnectionState(tp, properties, map[string]ConnectionPropertyValue{})
		if g, w := prop.GetValueOrDefault(state), prop.defaultValue; g != w {
			t.Fatalf("initial value mismatch\n Got: %v\nWant: %v", g, w)
		}
		if err := prop.SetStatementScopedValue(state, "new-value"); err != nil {
			t.Fatal(err)
		}
		if g, w := prop.GetValueOrDefault(state), "new-value"; g != w {
			t.Fatalf("value mismatch\n Got: %v\nWant: %v", g, w)
		}

		// Verify that the change is no longer visible after the statement values have been cleared.
		state.ClearStatementScopedValues()
		if g, w := prop.GetValueOrDefault(state), prop.defaultValue; g != w {
			t.Fatalf("value mismatch\n Got: %v\nWant: %v", g, w)
		}
	}
}

func TestSetLocalValueOutsideTransaction(t *testing.T) {
	prop := CreateConnectionProperty("my_property", "Test property", "initial-value", false, nil, ContextUser, noop)
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
	prop := CreateConnectionProperty("my_property", "Test property", "initial-value", false, nil, ContextStartup, noop)
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
	prop := CreateConnectionProperty("my_property", "Test property", "initial-value", false, nil, ContextStartup, noop)
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
	prop := CreateConnectionProperty("my_property", "Test property", "initial-value", false, nil, ContextStartup, noop)
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
	prop := CreateConnectionProperty("my_property", "Test property", "initial-value", false, nil, ContextUser, noop)
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
		prop := CreateConnectionProperty("prop", "Test property", "initial-value", false, nil, ContextUser, noop)
		err := prop.SetValue(state, "new-value", ContextUser)
		if g, w := spanner.ErrCode(err), codes.InvalidArgument; g != w {
			t.Fatalf("error code mismatch\n Got: %v\nWant: %v", g, w)
		}

		// Adding a new property with an extension to the connection state should work.
		propWithExtension := CreateConnectionPropertyWithExtension("spanner", "prop3", "Test property 3", "initial-value-3", false, nil, ContextUser, noop)
		if err := propWithExtension.SetValue(state, "new-value", ContextUser); err != nil {
			t.Fatal(err)
		}
	}
}

func TestReset(t *testing.T) {
	prop1 := CreateConnectionProperty("prop1", "Test property 1", "initial-value-1", false, nil, ContextUser, noop)
	prop2 := CreateConnectionProperty("prop2", "Test property 2", "initial-value-2", false, nil, ContextUser, noop)
	properties := map[string]ConnectionProperty{
		prop1.key: prop1,
		prop2.key: prop2,
	}

	for _, tp := range []Type{TypeTransactional, TypeNonTransactional} {
		state, _ := NewConnectionState(tp, properties, map[string]ConnectionPropertyValue{})
		_ = prop1.SetValue(state, "new-value-1", ContextUser)
		_ = prop2.SetValue(state, "new-value-2", ContextUser)
		// Add a new property to the connection state. This will be removed when the state is reset.
		prop3 := CreateConnectionPropertyWithExtension("spanner", "prop3", "Test property 3", "initial-value-3", false, nil, ContextUser, noop)
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
	prop1 := CreateConnectionProperty("prop1", "Test property 1", "default-value-1", false, nil, ContextUser, noop)
	prop2 := CreateConnectionProperty("prop2", "Test property 2", "default-value-2", false, nil, ContextUser, noop)
	properties := map[string]ConnectionProperty{
		prop1.key: prop1,
		prop2.key: prop2,
	}
	initialValues := map[string]ConnectionPropertyValue{
		prop2.key: prop2.CreateTypedInitialValue("initial-value-2"),
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

func TestConnectionState_Aliases(t *testing.T) {
	t.Parallel()

	properties := map[string]ConnectionProperty{
		"target_prop": &TypedConnectionProperty[string]{
			key:             "target_prop",
			name:            "target_prop",
			defaultValue:    "default-val",
			hasDefaultValue: true,
			context:         ContextUser,
			converter:       ConvertString,
		},
	}

	state, _ := NewConnectionState(TypeNonTransactional, properties, map[string]ConnectionPropertyValue{})
	state.AddAlias("alias_rw", "target_prop", false)
	state.AddAlias("alias_ro", "target_prop", true)

	// 1. Get through alias
	val, ok, err := state.GetValue("", "alias_rw")
	if err != nil {
		t.Fatal(err)
	}
	if !ok || val != "default-val" {
		t.Errorf("GetValue got %v, %t, want 'default-val', true", val, ok)
	}

	// 2. Set through read-write alias
	if err := state.SetValue("", "alias_rw", "new-val", ContextUser); err != nil {
		t.Fatal(err)
	}
	val, _, _ = state.GetValue("", "target_prop")
	if val != "new-val" {
		t.Errorf("expected target_prop to be updated to 'new-val', got %v", val)
	}

	// 3. Set through read-only alias should fail
	if err := state.SetValue("", "alias_ro", "other-val", ContextUser); err == nil {
		t.Error("expected error when setting read-only alias, got nil")
	}

	// 4. Get through read-only alias should still work and reflect the updated target value
	val, _, _ = state.GetValue("", "alias_ro")
	if val != "new-val" {
		t.Errorf("expected GetValue through read-only alias to return 'new-val', got %v", val)
	}
}

func TestPGTransactionState(t *testing.T) {
	t.Run("DefaultIdleState", func(t *testing.T) {
		state, err := NewConnectionState(TypeTransactional, nil, nil)
		if err != nil {
			t.Fatalf("unexpected error creating state: %v", err)
		}
		if g, w := state.TransactionState(), TransactionStateIdle; g != w {
			t.Errorf("default transaction state mismatch\nGot:  %v\nWant: %v", g, w)
		}
	})

	t.Run("BeginAndCommitTransitions", func(t *testing.T) {
		state, _ := NewConnectionState(TypeTransactional, nil, nil)
		if err := state.Begin(); err != nil {
			t.Fatalf("Begin failed: %v", err)
		}
		if g, w := state.TransactionState(), TransactionStateTransaction; g != w {
			t.Errorf("state after Begin mismatch\nGot:  %v\nWant: %v", g, w)
		}
		if err := state.Commit(); err != nil {
			t.Fatalf("Commit failed: %v", err)
		}
		if g, w := state.TransactionState(), TransactionStateIdle; g != w {
			t.Errorf("state after Commit mismatch\nGot:  %v\nWant: %v", g, w)
		}
	})

	t.Run("SetTransactionFailedAndRollback", func(t *testing.T) {
		state, _ := NewConnectionState(TypeTransactional, nil, nil)
		_ = state.Begin()
		state.SetTransactionFailed()
		if g, w := state.TransactionState(), TransactionStateFailed; g != w {
			t.Errorf("state after SetTransactionFailed mismatch\nGot:  %v\nWant: %v", g, w)
		}
		_ = state.Rollback()
		if g, w := state.TransactionState(), TransactionStateIdle; g != w {
			t.Errorf("state after Rollback mismatch\nGot:  %v\nWant: %v", g, w)
		}
	})

	t.Run("SetTransactionActive", func(t *testing.T) {
		state, _ := NewConnectionState(TypeTransactional, nil, nil)
		if err := state.Begin(); err != nil {
			t.Fatalf("Begin failed: %v", err)
		}
		if g, w := state.TransactionState(), TransactionStateTransaction; g != w {
			t.Errorf("state after Begin mismatch\nGot:  %v\nWant: %v", g, w)
		}
		state.SetTransactionFailed()
		if g, w := state.TransactionState(), TransactionStateFailed; g != w {
			t.Errorf("state after SetTransactionFailed mismatch\nGot:  %v\nWant: %v", g, w)
		}
		state.SetTransactionActive()
		if g, w := state.TransactionState(), TransactionStateTransaction; g != w {
			t.Errorf("state after SetTransactionActive mismatch\nGot:  %v\nWant: %v", g, w)
		}
		if err := state.Commit(); err != nil {
			t.Fatalf("Commit failed: %v", err)
		}
		if g, w := state.TransactionState(), TransactionStateIdle; g != w {
			t.Errorf("state after Commit mismatch\nGot:  %v\nWant: %v", g, w)
		}
	})

	t.Run("SetTransactionActiveOutsideTransaction", func(t *testing.T) {
		state, _ := NewConnectionState(TypeTransactional, nil, nil)
		state.SetTransactionActive()
		if g, w := state.TransactionState(), TransactionStateIdle; g != w {
			t.Errorf("state after SetTransactionActive outside transaction mismatch\nGot:  %v\nWant: %v", g, w)
		}
	})

	t.Run("CheckInErrorTxState_EnforceInErrorState", func(t *testing.T) {
		state, _ := NewConnectionState(TypeTransactional, nil, nil)
		state.SetInErrorTxBehavior(InErrorTxBehaviorEnforceInErrorState)
		_ = state.Begin()
		state.SetTransactionFailed()

		if err := state.CheckInErrorTxState(false); err == nil {
			t.Errorf("expected error when CheckInErrorTxState(false) in state E")
		} else if g, w := status.Code(err), codes.FailedPrecondition; g != w {
			t.Errorf("error code mismatch\nGot:  %v\nWant: %v", g, w)
		}

		if err := state.CheckInErrorTxState(true); err != nil {
			t.Errorf("expected nil error for COMMIT/ROLLBACK in state E, got %v", err)
		}
	})

	t.Run("CheckInErrorTxState_SetTransactionActive", func(t *testing.T) {
		state, _ := NewConnectionState(TypeTransactional, nil, nil)
		state.SetInErrorTxBehavior(InErrorTxBehaviorEnforceInErrorState)
		if err := state.Begin(); err != nil {
			t.Fatalf("Begin failed: %v", err)
		}
		state.SetTransactionFailed()

		if err := state.CheckInErrorTxState(false); err == nil {
			t.Errorf("expected error when CheckInErrorTxState(false) in state E")
		} else if g, w := status.Code(err), codes.FailedPrecondition; g != w {
			t.Errorf("error code mismatch\nGot:  %v\nWant: %v", g, w)
		}

		state.SetTransactionActive()

		if err := state.CheckInErrorTxState(false); err != nil {
			t.Errorf("expected nil error when CheckInErrorTxState(false) in state T, got %v", err)
		}
	})

	t.Run("SetTransactionActive_Idempotent", func(t *testing.T) {
		state, _ := NewConnectionState(TypeTransactional, nil, nil)
		if err := state.Begin(); err != nil {
			t.Fatalf("Begin failed: %v", err)
		}
		state.SetTransactionActive()
		if g, w := state.TransactionState(), TransactionStateTransaction; g != w {
			t.Errorf("state after idempotent SetTransactionActive mismatch\nGot:  %v\nWant: %v", g, w)
		}
	})

	t.Run("SetTransactionActive_PostRollback", func(t *testing.T) {
		state, _ := NewConnectionState(TypeTransactional, nil, nil)
		if err := state.Begin(); err != nil {
			t.Fatalf("Begin failed: %v", err)
		}
		if err := state.Rollback(); err != nil {
			t.Fatalf("Rollback failed: %v", err)
		}
		state.SetTransactionActive()
		if g, w := state.TransactionState(), TransactionStateIdle; g != w {
			t.Errorf("state after SetTransactionActive post-rollback mismatch\nGot:  %v\nWant: %v", g, w)
		}
	})

	t.Run("SetTransactionActive_PostCommit", func(t *testing.T) {
		state, _ := NewConnectionState(TypeTransactional, nil, nil)
		if err := state.Begin(); err != nil {
			t.Fatalf("Begin failed: %v", err)
		}
		if err := state.Commit(); err != nil {
			t.Fatalf("Commit failed: %v", err)
		}
		state.SetTransactionActive()
		if g, w := state.TransactionState(), TransactionStateIdle; g != w {
			t.Errorf("state after SetTransactionActive post-commit mismatch\nGot:  %v\nWant: %v", g, w)
		}
	})
}

func mustCreateInternalConnectionProperty[T comparable](t *testing.T, name string, defaultValue T) *TypedConnectionProperty[T] {
	t.Helper()
	prop, err := CreateInternalConnectionProperty(name, defaultValue)
	if err != nil {
		t.Fatalf("failed to create internal property %q: %v", name, err)
	}
	return prop
}

func mustCreateInternalConnectionPropertyWithContext[T comparable](t *testing.T, name string, defaultValue T, context Context) *TypedConnectionProperty[T] {
	t.Helper()
	prop, err := CreateInternalConnectionPropertyWithContext(name, defaultValue, context)
	if err != nil {
		t.Fatalf("failed to create internal property %q: %v", name, err)
	}
	return prop
}

func TestInternalConnectionProperty_PrimitiveType(t *testing.T) {
	prop, err := CreateInternalConnectionProperty("internal_int", 42)
	if err != nil {
		t.Fatalf("CreateInternalConnectionProperty failed: %v", err)
	}
	if g, w := prop.Internal(), true; g != w {
		t.Errorf("Internal() mismatch\nGot:  %v\nWant: %v", g, w)
	}
	if g, w := prop.Context(), Context(ContextUser); g != w {
		t.Errorf("Context() mismatch\nGot:  %v\nWant: %v", g, w)
	}

	state, err := NewConnectionState(TypeNonTransactional, nil, nil)
	if err != nil {
		t.Fatalf("failed to create connection state: %v", err)
	}

	// Initial value should be defaultValue.
	if g, w := prop.GetValueOrDefault(state), 42; g != w {
		t.Errorf("initial value mismatch\nGot:  %v\nWant: %v", g, w)
	}

	// Set statement-scoped value.
	if err := prop.SetStatementScopedValue(state, 100); err != nil {
		t.Fatalf("SetStatementScopedValue failed: %v", err)
	}
	if g, w := prop.GetValueOrDefault(state), 100; g != w {
		t.Errorf("statement-scoped value mismatch\nGot:  %v\nWant: %v", g, w)
	}

	// Clear statement-scoped value.
	state.ClearStatementScopedValues()
	if g, w := prop.GetValueOrDefault(state), 42; g != w {
		t.Errorf("value after clear mismatch\nGot:  %v\nWant: %v", g, w)
	}

	// Set connection-level value.
	if err := prop.SetValue(state, 200, ContextUser); err != nil {
		t.Fatalf("SetValue failed: %v", err)
	}
	if g, w := prop.GetValueOrDefault(state), 200; g != w {
		t.Errorf("connection value mismatch\nGot:  %v\nWant: %v", g, w)
	}

	// Reset state should clear/reset the value back to default.
	if err := state.Reset(ContextUser); err != nil {
		t.Fatalf("Reset failed: %v", err)
	}
	if g, w := prop.GetValueOrDefault(state), 42; g != w {
		t.Errorf("value after Reset mismatch\nGot:  %v\nWant: %v", g, w)
	}
}

func TestInternalConnectionProperty_PointerType(t *testing.T) {
	type customData struct {
		names []string
		tags  map[string]string
	}

	defaultData := &customData{names: []string{"default"}}
	prop := mustCreateInternalConnectionProperty(t, "internal_ptr", defaultData)

	state, err := NewConnectionState(TypeNonTransactional, nil, nil)
	if err != nil {
		t.Fatalf("failed to create connection state: %v", err)
	}

	if g, w := len(prop.GetValueOrDefault(state).names), 1; g != w {
		t.Errorf("default names length mismatch\nGot:  %v\nWant: %v", g, w)
	}

	scopedData := &customData{
		names: []string{"alice", "bob"},
		tags:  map[string]string{"env": "test"},
	}
	if err := prop.SetStatementScopedValue(state, scopedData); err != nil {
		t.Fatalf("SetStatementScopedValue failed: %v", err)
	}

	val := prop.GetValueOrDefault(state)
	if g, w := len(val.names), 2; g != w {
		t.Errorf("statement-scoped names length mismatch\nGot:  %v\nWant: %v", g, w)
	}
	if g, w := val.tags["env"], "test"; g != w {
		t.Errorf("statement-scoped tag mismatch\nGot:  %v\nWant: %v", g, w)
	}

	state.ClearStatementScopedValues()
	if g, w := len(prop.GetValueOrDefault(state).names), 1; g != w {
		t.Errorf("names length after clear mismatch\nGot:  %v\nWant: %v", g, w)
	}
}

func TestInternalConnectionProperty_InvisibilityToStringAPIs(t *testing.T) {
	prop := mustCreateInternalConnectionProperty(t, "internal_hidden", "secret")
	properties := map[string]ConnectionProperty{
		prop.Key(): prop,
	}

	state, err := NewConnectionState(TypeTransactional, properties, nil)
	if err != nil {
		t.Fatalf("failed to create connection state: %v", err)
	}

	// 1. GetValue should fail.
	if _, _, err := state.GetValue("", "internal_hidden"); err == nil {
		t.Errorf("expected error from GetValue for internal property, got nil")
	} else if g, w := status.Code(err), codes.InvalidArgument; g != w {
		t.Errorf("error code mismatch\nGot:  %v\nWant: %v", g, w)
	}

	// 2. SetValue should fail.
	if err := state.SetValue("", "internal_hidden", "new_secret", ContextUser); err == nil {
		t.Errorf("expected error from SetValue for internal property, got nil")
	} else if g, w := status.Code(err), codes.InvalidArgument; g != w {
		t.Errorf("error code mismatch\nGot:  %v\nWant: %v", g, w)
	}

	// 3. SetLocalValue should fail.
	if err := state.SetLocalValue("", "internal_hidden", "new_secret", false); err == nil {
		t.Errorf("expected error from SetLocalValue for internal property, got nil")
	} else if g, w := status.Code(err), codes.InvalidArgument; g != w {
		t.Errorf("error code mismatch\nGot:  %v\nWant: %v", g, w)
	}

	// 4. SetStatementScopedValue should fail.
	if err := state.SetStatementScopedValue("", "internal_hidden", "new_secret"); err == nil {
		t.Errorf("expected error from SetStatementScopedValue for internal property, got nil")
	} else if g, w := status.Code(err), codes.InvalidArgument; g != w {
		t.Errorf("error code mismatch\nGot:  %v\nWant: %v", g, w)
	}

	// 5. Invisibility after setting statement-scoped value programmatically.
	if err := prop.SetStatementScopedValue(state, "active_secret"); err != nil {
		t.Fatalf("SetStatementScopedValue failed: %v", err)
	}
	if _, _, err := state.GetValue("", "internal_hidden"); err == nil {
		t.Errorf("expected error from GetValue for internal property with active statement scope, got nil")
	}
}

func TestInternalConnectionProperty_TransactionalBehavior(t *testing.T) {
	prop := mustCreateInternalConnectionProperty(t, "internal_tx", "initial")
	state, err := NewConnectionState(TypeTransactional, nil, nil)
	if err != nil {
		t.Fatalf("failed to create connection state: %v", err)
	}

	if err := state.Begin(); err != nil {
		t.Fatalf("Begin failed: %v", err)
	}
	if err := prop.SetValue(state, "in_tx", ContextUser); err != nil {
		t.Fatalf("SetValue in tx failed: %v", err)
	}
	if g, w := prop.GetValueOrDefault(state), "in_tx"; g != w {
		t.Errorf("value in tx mismatch\nGot:  %v\nWant: %v", g, w)
	}
	if err := state.Rollback(); err != nil {
		t.Fatalf("Rollback failed: %v", err)
	}
	if g, w := prop.GetValueOrDefault(state), "initial"; g != w {
		t.Errorf("value after rollback mismatch\nGot:  %v\nWant: %v", g, w)
	}
}

func TestInternalConnectionProperty_DotInNameReturnsError(t *testing.T) {
	prop, err := CreateInternalConnectionProperty("spanner.internal_tag", "secret")
	if err == nil {
		t.Errorf("expected error from CreateInternalConnectionProperty for name with '.', got nil")
	} else if g, w := status.Code(err), codes.InvalidArgument; g != w {
		t.Errorf("error code mismatch\nGot:  %v\nWant: %v", g, w)
	}
	if prop != nil {
		t.Errorf("expected nil property on error\nGot:  %v\nWant: <nil>", prop)
	}
}

func TestInternalConnectionProperty_EmptyNameReturnsError(t *testing.T) {
	tests := []struct {
		name  string
		input string
	}{
		{"empty", ""},
		{"spaces", "   "},
		{"whitespace", "\t\n"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			prop, err := CreateInternalConnectionProperty(tc.input, "secret")
			if err == nil {
				t.Errorf("expected error from CreateInternalConnectionProperty for name %q, got nil", tc.input)
			} else if g, w := status.Code(err), codes.InvalidArgument; g != w {
				t.Errorf("error code mismatch for name %q\nGot:  %v\nWant: %v", tc.input, g, w)
			}
			if prop != nil {
				t.Errorf("expected nil property on error for name %q\nGot:  %v\nWant: <nil>", tc.input, prop)
			}
		})
	}
}

func TestInternalConnectionProperty_TrimsWhitespaceInName(t *testing.T) {
	prop, err := CreateInternalConnectionProperty("  internal_tag  ", "val")
	if err != nil {
		t.Fatalf("CreateInternalConnectionProperty failed: %v", err)
	}
	if g, w := prop.name, "internal_tag"; g != w {
		t.Errorf("name mismatch\nGot:  %v\nWant: %v", g, w)
	}
	if g, w := prop.Key(), "internal_tag"; g != w {
		t.Errorf("Key() mismatch\nGot:  %v\nWant: %v", g, w)
	}
}

func TestInternalConnectionProperty_ExtensionInvisibility(t *testing.T) {
	prop := mustCreateInternalConnectionProperty(t, "internal_tag", "secret")
	properties := map[string]ConnectionProperty{
		prop.Key(): prop,
	}

	state, err := NewConnectionState(TypeTransactional, properties, nil)
	if err != nil {
		t.Fatalf("failed to create connection state: %v", err)
	}

	// Direct access via non-extension SQL returns error (internal property is invisible to SQL)
	if _, _, err := state.GetValue("", "internal_tag"); err == nil {
		t.Errorf("expected error from GetValue for internal property, got nil")
	} else if g, w := status.Code(err), codes.InvalidArgument; g != w {
		t.Errorf("error code mismatch\nGot:  %v\nWant: %v", g, w)
	}

	if err := state.SetValue("", "internal_tag", "override", ContextUser); err == nil {
		t.Errorf("expected error from SetValue for internal property, got nil")
	} else if g, w := status.Code(err), codes.InvalidArgument; g != w {
		t.Errorf("error code mismatch\nGot:  %v\nWant: %v", g, w)
	}

	// An extension property with the same name (e.g. spanner.internal_tag) is completely separate
	if err := state.SetValue("spanner", "internal_tag", "extension_val", ContextUser); err != nil {
		t.Fatalf("SetValue for extension property failed: %v", err)
	}
	val, ok, err := state.GetValue("spanner", "internal_tag")
	if err != nil {
		t.Fatalf("GetValue for extension failed: %v", err)
	}
	if g, w := ok, true; g != w {
		t.Errorf("extension ok mismatch\nGot:  %v\nWant: %v", g, w)
	}
	if g, w := val, "extension_val"; g != w {
		t.Errorf("extension value mismatch\nGot:  %v\nWant: %v", g, w)
	}
	if g, w := prop.GetValueOrDefault(state), "secret"; g != w {
		t.Errorf("internal property corrupted by extension set\nGot:  %v\nWant: %v", g, w)
	}

	// Now test an unregistered internal property set only at statement scope.
	unregisteredState, err := NewConnectionState(TypeTransactional, nil, nil)
	if err != nil {
		t.Fatalf("failed to create connection state: %v", err)
	}
	if err := prop.SetStatementScopedValue(unregisteredState, "active_secret"); err != nil {
		t.Fatalf("SetStatementScopedValue failed: %v", err)
	}

	if _, _, err := unregisteredState.GetValue("", "internal_tag"); err == nil {
		t.Errorf("expected error from GetValue for unregistered statement-scoped internal property, got nil")
	} else if g, w := status.Code(err), codes.InvalidArgument; g != w {
		t.Errorf("error code mismatch\nGot:  %v\nWant: %v", g, w)
	}

	if err := unregisteredState.SetValue("", "internal_tag", "hacked", ContextUser); err == nil {
		t.Errorf("expected error from SetValue for unregistered statement-scoped internal property, got nil")
	} else if g, w := status.Code(err), codes.InvalidArgument; g != w {
		t.Errorf("error code mismatch\nGot:  %v\nWant: %v", g, w)
	}

	if g, w := prop.GetValueOrDefault(unregisteredState), "active_secret"; g != w {
		t.Errorf("statement-scoped secret value corrupted\nGot:  %v\nWant: %v", g, w)
	}

	// Also test local scope in isolation
	unregisteredState.ClearStatementScopedValues()
	if err := unregisteredState.Begin(); err != nil {
		t.Fatalf("Begin failed: %v", err)
	}
	if err := prop.SetLocalValue(unregisteredState, "local_secret"); err != nil {
		t.Fatalf("SetLocalValue failed: %v", err)
	}
	if _, _, err := unregisteredState.GetValue("", "internal_tag"); err == nil {
		t.Errorf("expected error from GetValue for local internal property, got nil")
	} else if g, w := status.Code(err), codes.InvalidArgument; g != w {
		t.Errorf("error code mismatch\nGot:  %v\nWant: %v", g, w)
	}

	// Also test transaction scope in isolation
	unregisteredTxState, err := NewConnectionState(TypeTransactional, nil, nil)
	if err != nil {
		t.Fatalf("failed to create connection state: %v", err)
	}
	if err := unregisteredTxState.Begin(); err != nil {
		t.Fatalf("Begin failed: %v", err)
	}
	if err := prop.SetValue(unregisteredTxState, "tx_secret", ContextUser); err != nil {
		t.Fatalf("SetValue in tx failed: %v", err)
	}
	if _, _, err := unregisteredTxState.GetValue("", "internal_tag"); err == nil {
		t.Errorf("expected error from GetValue for tx internal property, got nil")
	} else if g, w := status.Code(err), codes.InvalidArgument; g != w {
		t.Errorf("error code mismatch\nGot:  %v\nWant: %v", g, w)
	}

	// Test alias resolution for internal property in findProperty
	stateWithAlias, err := NewConnectionState(TypeTransactional, map[string]ConnectionProperty{
		"target_prop": mustCreateInternalConnectionProperty(t, "target_prop", "val"),
	}, nil)
	if err != nil {
		t.Fatalf("NewConnectionState failed: %v", err)
	}
	stateWithAlias.AddAlias("alias_prop", "target_prop", false)
	if _, _, err := stateWithAlias.GetValue("", "alias_prop"); err == nil {
		t.Errorf("expected error from GetValue for alias to internal property, got nil")
	} else if g, w := status.Code(err), codes.InvalidArgument; g != w {
		t.Errorf("error code mismatch\nGot:  %v\nWant: %v", g, w)
	}
}

func TestInternalConnectionProperty_ResetBehavior(t *testing.T) {
	prop := mustCreateInternalConnectionProperty(t, "internal_reset", "default_val")
	state, err := NewConnectionState(TypeTransactional, nil, nil)
	if err != nil {
		t.Fatalf("failed to create connection state: %v", err)
	}

	// Verify hasDefaultValue is true for default value.
	if g, w := prop.CreateDefaultValue().HasValue(), true; g != w {
		t.Errorf("HasValue on default mismatch\nGot:  %v\nWant: %v", g, w)
	}

	// 1. Statement-scoped set and reset
	if err := prop.SetStatementScopedValue(state, "statement_custom"); err != nil {
		t.Fatalf("SetStatementScopedValue failed: %v", err)
	}
	if g, w := prop.GetValueOrDefault(state), "statement_custom"; g != w {
		t.Errorf("value after statement set mismatch\nGot:  %v\nWant: %v", g, w)
	}
	if err := prop.ResetStatementScopedValue(state); err != nil {
		t.Fatalf("ResetStatementScopedValue failed: %v", err)
	}
	if g, w := prop.GetValueOrDefault(state), "default_val"; g != w {
		t.Errorf("value after statement reset mismatch\nGot:  %v\nWant: %v", g, w)
	}
	// Verify GetValueOrError succeeds after statement reset
	if val, hasVal, err := prop.GetValueOrError(state); err != nil {
		t.Fatalf("GetValueOrError after statement reset failed: %v", err)
	} else {
		if g, w := hasVal, true; g != w {
			t.Errorf("GetValueOrError hasVal mismatch\nGot:  %v\nWant: %v", g, w)
		}
		if g, w := val, "default_val"; g != w {
			t.Errorf("GetValueOrError val mismatch\nGot:  %v\nWant: %v", g, w)
		}
	}

	state.ClearStatementScopedValues()

	// 2. Session-level set and reset on unregistered internal property
	if err := prop.SetValue(state, "session_custom", ContextUser); err != nil {
		t.Fatalf("SetValue failed: %v", err)
	}
	if g, w := prop.GetValueOrDefault(state), "session_custom"; g != w {
		t.Errorf("value after session set mismatch\nGot:  %v\nWant: %v", g, w)
	}
	if err := prop.ResetValue(state, ContextUser); err != nil {
		t.Fatalf("ResetValue failed: %v", err)
	}
	if g, w := prop.GetValueOrDefault(state), "default_val"; g != w {
		t.Errorf("value after session reset mismatch\nGot:  %v\nWant: %v", g, w)
	}
	// Verify GetValueOrError succeeds after session reset (not marked as removed)
	if val, hasVal, err := prop.GetValueOrError(state); err != nil {
		t.Fatalf("GetValueOrError after session reset failed: %v", err)
	} else {
		if g, w := hasVal, true; g != w {
			t.Errorf("GetValueOrError hasVal mismatch\nGot:  %v\nWant: %v", g, w)
		}
		if g, w := val, "default_val"; g != w {
			t.Errorf("GetValueOrError val mismatch\nGot:  %v\nWant: %v", g, w)
		}
	}

	// 3. Verify setting a new value after reset clears removed flag and updates value
	if err := prop.SetValue(state, "after_reset", ContextUser); err != nil {
		t.Fatalf("SetValue after reset failed: %v", err)
	}
	if val, hasVal, err := prop.GetValueOrError(state); err != nil {
		t.Fatalf("GetValueOrError after set-after-reset failed: %v", err)
	} else {
		if g, w := hasVal, true; g != w {
			t.Errorf("GetValueOrError hasVal mismatch\nGot:  %v\nWant: %v", g, w)
		}
		if g, w := val, "after_reset"; g != w {
			t.Errorf("GetValueOrError val mismatch\nGot:  %v\nWant: %v", g, w)
		}
	}
}

func TestInternalConnectionProperty_ZeroValueDefaults(t *testing.T) {
	propBool := mustCreateInternalConnectionProperty(t, "enabled", false)
	propInt := mustCreateInternalConnectionProperty(t, "count", 0)
	propStr := mustCreateInternalConnectionProperty(t, "tag", "")

	properties := map[string]ConnectionProperty{
		propBool.Key(): propBool,
		propInt.Key():  propInt,
		propStr.Key():  propStr,
	}
	state, err := NewConnectionState(TypeTransactional, properties, nil)
	if err != nil {
		t.Fatalf("failed to create connection state: %v", err)
	}

	if g, w := propBool.CreateDefaultValue().HasValue(), true; g != w {
		t.Errorf("propBool HasValue mismatch\nGot:  %v\nWant: %v", g, w)
	}
	if val, ok, err := propBool.GetValue(state); err != nil {
		t.Fatalf("propBool GetValue failed: %v", err)
	} else {
		if g, w := ok, true; g != w {
			t.Errorf("propBool ok mismatch\nGot:  %v\nWant: %v", g, w)
		}
		if g, w := val, false; g != w {
			t.Errorf("propBool val mismatch\nGot:  %v\nWant: %v", g, w)
		}
	}

	if g, w := propInt.CreateDefaultValue().HasValue(), true; g != w {
		t.Errorf("propInt HasValue mismatch\nGot:  %v\nWant: %v", g, w)
	}
	if val, ok, err := propInt.GetValue(state); err != nil {
		t.Fatalf("propInt GetValue failed: %v", err)
	} else {
		if g, w := ok, true; g != w {
			t.Errorf("propInt ok mismatch\nGot:  %v\nWant: %v", g, w)
		}
		if g, w := val, 0; g != w {
			t.Errorf("propInt val mismatch\nGot:  %v\nWant: %v", g, w)
		}
	}

	if g, w := propStr.CreateDefaultValue().HasValue(), true; g != w {
		t.Errorf("propStr HasValue mismatch\nGot:  %v\nWant: %v", g, w)
	}
	if val, ok, err := propStr.GetValue(state); err != nil {
		t.Fatalf("propStr GetValue failed: %v", err)
	} else {
		if g, w := ok, true; g != w {
			t.Errorf("propStr ok mismatch\nGot:  %v\nWant: %v", g, w)
		}
		if g, w := val, ""; g != w {
			t.Errorf("propStr val mismatch\nGot:  %v\nWant: %v", g, w)
		}
	}
}

func TestInternalConnectionProperty_CaseNormalization(t *testing.T) {
	prop := mustCreateInternalConnectionProperty(t, "StatementContext", 123)
	if g, w := prop.Key(), "statementcontext"; g != w {
		t.Errorf("prop.Key() mismatch\nGot:  %v\nWant: %v", g, w)
	}

	state, err := NewConnectionState(TypeTransactional, nil, nil)
	if err != nil {
		t.Fatalf("failed to create connection state: %v", err)
	}

	if err := prop.SetStatementScopedValue(state, 456); err != nil {
		t.Fatalf("SetStatementScopedValue failed: %v", err)
	}
	if g, w := prop.GetValueOrDefault(state), 456; g != w {
		t.Errorf("GetValueOrDefault mismatch\nGot:  %v\nWant: %v", g, w)
	}

	// Invisibility via lowercase key
	if _, _, err := state.GetValue("", "statementcontext"); err == nil {
		t.Errorf("expected error from GetValue for internal property, got nil")
	} else if g, w := status.Code(err), codes.InvalidArgument; g != w {
		t.Errorf("error code mismatch\nGot:  %v\nWant: %v", g, w)
	}
}

func TestInternalConnectionProperty_PreRegistered(t *testing.T) {
	prop := mustCreateInternalConnectionPropertyWithContext(t, "pre_reg", "default_val", ContextStartup)
	state, err := NewConnectionState(TypeTransactional, map[string]ConnectionProperty{
		prop.Key(): prop,
	}, nil)
	if err != nil {
		t.Fatalf("failed to create connection state: %v", err)
	}

	if g, w := prop.GetValueOrDefault(state), "default_val"; g != w {
		t.Errorf("initial value mismatch\nGot:  %v\nWant: %v", g, w)
	}

	if err := prop.SetValue(state, "startup_val", ContextStartup); err != nil {
		t.Fatalf("SetValue failed: %v", err)
	}
	if g, w := prop.GetValueOrDefault(state), "startup_val"; g != w {
		t.Errorf("value after set mismatch\nGot:  %v\nWant: %v", g, w)
	}

	// Reset with ContextUser should not reset ContextStartup property
	if err := state.Reset(ContextUser); err != nil {
		t.Fatalf("Reset failed: %v", err)
	}
	if g, w := prop.GetValueOrDefault(state), "startup_val"; g != w {
		t.Errorf("value after user reset mismatch\nGot:  %v\nWant: %v", g, w)
	}

	// Reset with ContextStartup should reset it back to default
	if err := state.Reset(ContextStartup); err != nil {
		t.Fatalf("Reset failed: %v", err)
	}
	if g, w := prop.GetValueOrDefault(state), "default_val"; g != w {
		t.Errorf("value after startup reset mismatch\nGot:  %v\nWant: %v", g, w)
	}
}

func TestInternalConnectionProperty_DefensiveNilChecks(t *testing.T) {
	prop := mustCreateInternalConnectionProperty(t, "nil_test", "default_val")

	if g, w := prop.GetValueOrDefault(nil), "default_val"; g != w {
		t.Errorf("GetValueOrDefault(nil) mismatch\nGot:  %v\nWant: %v", g, w)
	}

	if _, _, err := prop.GetValueOrError(nil); err == nil {
		t.Errorf("expected error from GetValueOrError(nil), got nil")
	} else if g, w := status.Code(err), codes.InvalidArgument; g != w {
		t.Errorf("error code mismatch\nGot:  %v\nWant: %v", g, w)
	}

	if g, w := prop.GetConnectionPropertyValue(nil), (ConnectionPropertyValue)(nil); g != w {
		t.Errorf("GetConnectionPropertyValue(nil) mismatch\nGot:  %v\nWant: %v", g, w)
	}

	// Setter and resetter nil checks
	tests := []struct {
		name string
		fn   func() error
	}{
		{"SetValue", func() error { return prop.SetValue(nil, "v", ContextUser) }},
		{"SetLocalValue", func() error { return prop.SetLocalValue(nil, "v") }},
		{"SetStatementScopedValue", func() error { return prop.SetStatementScopedValue(nil, "v") }},
		{"ResetValue", func() error { return prop.ResetValue(nil, ContextUser) }},
		{"ResetLocalValue", func() error { return prop.ResetLocalValue(nil) }},
		{"ResetStatementScopedValue", func() error { return prop.ResetStatementScopedValue(nil) }},
		{"SetDefaultValue", func() error { return prop.SetDefaultValue(nil, ContextUser) }},
		{"SetLocalDefaultValue", func() error { return prop.SetLocalDefaultValue(nil) }},
		{"SetStatementScopedDefaultValue", func() error { return prop.SetStatementScopedDefaultValue(nil) }},
		{"SetUntypedValue", func() error { return prop.SetUntypedValue(nil, "v", ContextUser) }},
		{"SetLocalUntypedValue", func() error { return prop.SetLocalUntypedValue(nil, "v") }},
		{"SetStatementScopedUntypedValue", func() error { return prop.SetStatementScopedUntypedValue(nil, "v") }},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.fn()
			if err == nil {
				t.Errorf("expected error from %s on nil state, got nil", tc.name)
			} else if g, w := status.Code(err), codes.InvalidArgument; g != w {
				t.Errorf("error code mismatch\nGot:  %v\nWant: %v", g, w)
			}
		})
	}

	state, err := NewConnectionState(TypeTransactional, nil, nil)
	if err != nil {
		t.Fatalf("failed to create connection state: %v", err)
	}
	if g, w := prop.GetValueOrDefault(state), "default_val"; g != w {
		t.Errorf("GetValueOrDefault on unregistered state mismatch\nGot:  %v\nWant: %v", g, w)
	}
	if _, _, err := prop.GetValueOrError(state); err == nil {
		t.Errorf("expected error from GetValueOrError on unregistered property, got nil")
	} else if g, w := status.Code(err), codes.InvalidArgument; g != w {
		t.Errorf("error code mismatch\nGot:  %v\nWant: %v", g, w)
	}
	if g, w := prop.GetConnectionPropertyValue(state), (ConnectionPropertyValue)(nil); g != w {
		t.Errorf("GetConnectionPropertyValue on unregistered state mismatch\nGot:  %v\nWant: %v", g, w)
	}

	if err := prop.SetValue(state, "active_val", ContextUser); err != nil {
		t.Fatalf("SetValue failed: %v", err)
	}
	if propVal := prop.GetConnectionPropertyValue(state); propVal == nil {
		t.Errorf("GetConnectionPropertyValue after set returned nil")
	} else if val, err := propVal.GetValue(); err != nil {
		t.Fatalf("propVal.GetValue failed: %v", err)
	} else if g, w := val, "active_val"; g != w {
		t.Errorf("GetConnectionPropertyValue value mismatch\nGot:  %v\nWant: %v", g, w)
	}
}

func TestInternalConnectionProperty_CopyPreservesRemoved(t *testing.T) {
	prop := CreateConnectionProperty("ext_copy", "desc", "def", true, nil, ContextUser, ConvertString)
	state, err := NewConnectionState(TypeTransactional, map[string]ConnectionProperty{
		prop.Key(): prop,
	}, nil)
	if err != nil {
		t.Fatalf("NewConnectionState failed: %v", err)
	}
	if err := prop.SetValue(state, "custom", ContextUser); err != nil {
		t.Fatalf("SetValue failed: %v", err)
	}
	val := prop.GetConnectionPropertyValue(state)
	if val == nil {
		t.Fatalf("expected non-nil property value")
	}
	cp := val.Copy()
	if g, w := cp.isRemoved(), false; g != w {
		t.Errorf("isRemoved on initial copy mismatch\nGot:  %v\nWant: %v", g, w)
	}
	if typedVal, ok := val.(*connectionPropertyValue[string]); ok {
		typedVal.removed = true
		cpRemoved := typedVal.Copy()
		if g, w := cpRemoved.isRemoved(), true; g != w {
			t.Errorf("isRemoved on removed copy mismatch\nGot:  %v\nWant: %v", g, w)
		}
	} else {
		t.Fatalf("unexpected type for val: %T", val)
	}

	// Verify that a removed property returns an error in GetValueOrError
	extState, err := NewConnectionState(TypeTransactional, nil, nil)
	if err != nil {
		t.Fatalf("NewConnectionState failed: %v", err)
	}
	extProp := &TypedConnectionProperty[string]{
		key:       "custom.dynamic_prop",
		extension: "custom",
		name:      "dynamic_prop",
		context:   ContextUser,
		converter: ConvertString,
	}
	if err := extProp.SetValue(extState, "custom_val", ContextUser); err != nil {
		t.Fatalf("SetValue failed: %v", err)
	}
	// Reset on dynamic extension property sets removeAtReset: true and removed: true
	if err := extProp.ResetValue(extState, ContextUser); err != nil {
		t.Fatalf("ResetValue failed: %v", err)
	}
	if _, _, err := extProp.GetValueOrError(extState); err == nil {
		t.Errorf("expected error from GetValueOrError on removed property, got nil")
	} else if g, w := status.Code(err), codes.InvalidArgument; g != w {
		t.Errorf("error code mismatch\nGot:  %v\nWant: %v", g, w)
	}

	// Setting default value clears the removed flag
	if err := extProp.SetDefaultValue(extState, ContextUser); err != nil {
		t.Fatalf("SetDefaultValue failed: %v", err)
	}
	if val, hasVal, err := extProp.GetValueOrError(extState); err != nil {
		t.Fatalf("GetValueOrError after SetDefaultValue failed: %v", err)
	} else {
		if g, w := hasVal, false; g != w {
			t.Errorf("hasVal mismatch\nGot:  %v\nWant: %v", g, w)
		}
		if g, w := val, ""; g != w {
			t.Errorf("val mismatch\nGot:  %v\nWant: %v", g, w)
		}
	}
}

func TestInternalConnectionProperty_ConvertSafety(t *testing.T) {
	prop := mustCreateInternalConnectionProperty(t, "convert_test", "default_val")

	if _, err := prop.Convert("foo"); err == nil {
		t.Errorf("expected error from Convert on internal property without converter, got nil")
	} else if g, w := status.Code(err), codes.InvalidArgument; g != w {
		t.Errorf("error code mismatch\nGot:  %v\nWant: %v", g, w)
	}
}

func TestInternalConnectionProperty_LocalValueBehavior(t *testing.T) {
	prop := mustCreateInternalConnectionProperty(t, "internal_local", "initial")
	state, err := NewConnectionState(TypeTransactional, nil, nil)
	if err != nil {
		t.Fatalf("failed to create connection state: %v", err)
	}

	if err := state.Begin(); err != nil {
		t.Fatalf("Begin failed: %v", err)
	}
	if err := prop.SetLocalValue(state, "local_val"); err != nil {
		t.Fatalf("SetLocalValue failed: %v", err)
	}
	if g, w := prop.GetValueOrDefault(state), "local_val"; g != w {
		t.Errorf("local value mismatch\nGot:  %v\nWant: %v", g, w)
	}
	if err := state.Commit(); err != nil {
		t.Fatalf("Commit failed: %v", err)
	}
	// Local values are reset when the transaction completes.
	if g, w := prop.GetValueOrDefault(state), "initial"; g != w {
		t.Errorf("value after commit mismatch\nGot:  %v\nWant: %v", g, w)
	}
}

func TestInternalConnectionProperty_UntypedSetters(t *testing.T) {
	prop := mustCreateInternalConnectionProperty(t, "internal_untyped", "default_val")
	state, err := NewConnectionState(TypeTransactional, nil, nil)
	if err != nil {
		t.Fatalf("failed to create connection state: %v", err)
	}

	// Valid untyped session set
	if err := prop.SetUntypedValue(state, "untyped_session", ContextUser); err != nil {
		t.Fatalf("SetUntypedValue failed: %v", err)
	}
	if g, w := prop.GetValueOrDefault(state), "untyped_session"; g != w {
		t.Errorf("untyped session value mismatch\nGot:  %v\nWant: %v", g, w)
	}

	// Invalid untyped session set
	if err := prop.SetUntypedValue(state, 12345, ContextUser); err == nil {
		t.Errorf("expected error from SetUntypedValue with wrong type, got nil")
	} else if g, w := status.Code(err), codes.InvalidArgument; g != w {
		t.Errorf("error code mismatch\nGot:  %v\nWant: %v", g, w)
	}

	// Statement-scoped untyped
	if err := prop.SetStatementScopedUntypedValue(state, "untyped_statement"); err != nil {
		t.Fatalf("SetStatementScopedUntypedValue failed: %v", err)
	}
	if g, w := prop.GetValueOrDefault(state), "untyped_statement"; g != w {
		t.Errorf("untyped statement value mismatch\nGot:  %v\nWant: %v", g, w)
	}
	if err := prop.SetStatementScopedUntypedValue(state, 12345); err == nil {
		t.Errorf("expected error from SetStatementScopedUntypedValue with wrong type, got nil")
	} else if g, w := status.Code(err), codes.InvalidArgument; g != w {
		t.Errorf("error code mismatch\nGot:  %v\nWant: %v", g, w)
	}

	// Clear statement-scoped properties before checking local values
	state.ClearStatementScopedValues()

	// Local untyped
	if err := state.Begin(); err != nil {
		t.Fatalf("Begin failed: %v", err)
	}
	if err := prop.SetLocalUntypedValue(state, "untyped_local"); err != nil {
		t.Fatalf("SetLocalUntypedValue failed: %v", err)
	}
	if g, w := prop.GetValueOrDefault(state), "untyped_local"; g != w {
		t.Errorf("untyped local value mismatch\nGot:  %v\nWant: %v", g, w)
	}
	if err := prop.SetLocalUntypedValue(state, 12345); err == nil {
		t.Errorf("expected error from SetLocalUntypedValue with wrong type, got nil")
	} else if g, w := status.Code(err), codes.InvalidArgument; g != w {
		t.Errorf("error code mismatch\nGot:  %v\nWant: %v", g, w)
	}
	if err := state.Rollback(); err != nil {
		t.Fatalf("Rollback failed: %v", err)
	}
}

func TestInternalConnectionProperty_DefaultSettersAndLocalReset(t *testing.T) {
	prop := mustCreateInternalConnectionProperty(t, "internal_defaults", "default_val")
	state, err := NewConnectionState(TypeTransactional, nil, nil)
	if err != nil {
		t.Fatalf("failed to create connection state: %v", err)
	}

	// Statement-scoped default
	if err := prop.SetStatementScopedValue(state, "statement_changed"); err != nil {
		t.Fatalf("SetStatementScopedValue failed: %v", err)
	}
	if g, w := prop.GetValueOrDefault(state), "statement_changed"; g != w {
		t.Errorf("statement value mismatch\nGot:  %v\nWant: %v", g, w)
	}
	if err := prop.SetStatementScopedDefaultValue(state); err != nil {
		t.Fatalf("SetStatementScopedDefaultValue failed: %v", err)
	}
	if g, w := prop.GetValueOrDefault(state), "default_val"; g != w {
		t.Errorf("value after SetStatementScopedDefaultValue mismatch\nGot:  %v\nWant: %v", g, w)
	}

	state.ClearStatementScopedValues()

	// Local default and reset
	if err := state.Begin(); err != nil {
		t.Fatalf("Begin failed: %v", err)
	}
	if err := prop.SetLocalValue(state, "local_changed"); err != nil {
		t.Fatalf("SetLocalValue failed: %v", err)
	}
	if g, w := prop.GetValueOrDefault(state), "local_changed"; g != w {
		t.Errorf("local value mismatch\nGot:  %v\nWant: %v", g, w)
	}
	if err := prop.ResetLocalValue(state); err != nil {
		t.Fatalf("ResetLocalValue failed: %v", err)
	}
	if g, w := prop.GetValueOrDefault(state), "default_val"; g != w {
		t.Errorf("value after ResetLocalValue mismatch\nGot:  %v\nWant: %v", g, w)
	}

	if err := prop.SetLocalValue(state, "local_changed_again"); err != nil {
		t.Fatalf("SetLocalValue failed: %v", err)
	}
	if err := prop.SetLocalDefaultValue(state); err != nil {
		t.Fatalf("SetLocalDefaultValue failed: %v", err)
	}
	if g, w := prop.GetValueOrDefault(state), "default_val"; g != w {
		t.Errorf("value after SetLocalDefaultValue mismatch\nGot:  %v\nWant: %v", g, w)
	}
	if err := state.Commit(); err != nil {
		t.Fatalf("Commit failed: %v", err)
	}
}

func TestConnectionState_SetLocalValueOutsideTransactionError(t *testing.T) {
	state, err := NewConnectionState(TypeTransactional, nil, nil)
	if err != nil {
		t.Fatalf("failed to create connection state: %v", err)
	}
	if err := state.SetLocalValue("", "some_prop", "val", true); err == nil {
		t.Errorf("expected error from SetLocalValue with isSetTransaction outside tx, got nil")
	} else if g, w := status.Code(err), codes.FailedPrecondition; g != w {
		t.Errorf("error code mismatch\nGot:  %v\nWant: %v", g, w)
	}
}

func TestConnectionState_DynamicExtensionAndUnknownProperties(t *testing.T) {
	state, err := NewConnectionState(TypeTransactional, nil, nil)
	if err != nil {
		t.Fatalf("failed to create connection state: %v", err)
	}

	// Unknown property without extension returns error
	if _, _, err := state.GetValue("", "nonexistent"); err == nil {
		t.Errorf("expected error from GetValue for unknown property, got nil")
	} else if g, w := status.Code(err), codes.InvalidArgument; g != w {
		t.Errorf("error code mismatch\nGot:  %v\nWant: %v", g, w)
	}
	if err := state.SetValue("", "nonexistent", "val", ContextUser); err == nil {
		t.Errorf("expected error from SetValue for unknown property, got nil")
	} else if g, w := status.Code(err), codes.InvalidArgument; g != w {
		t.Errorf("error code mismatch\nGot:  %v\nWant: %v", g, w)
	}

	// Dynamic extension property can be set and read via StatementScoped and Session scopes
	if err := state.SetStatementScopedValue("custom", "scoped_prop", "scoped_val"); err != nil {
		t.Fatalf("SetStatementScopedValue failed: %v", err)
	}
	if val, ok, err := state.GetValue("custom", "scoped_prop"); err != nil {
		t.Fatalf("GetValue failed: %v", err)
	} else if !ok {
		t.Errorf("expected value to be present")
	} else if g, w := val, "scoped_val"; g != w {
		t.Errorf("value mismatch\nGot:  %v\nWant: %v", g, w)
	}

	if err := state.SetValue("custom", "session_prop", "session_val", ContextUser); err != nil {
		t.Fatalf("SetValue failed: %v", err)
	}
	if val, ok, err := state.GetValue("custom", "session_prop"); err != nil {
		t.Fatalf("GetValue failed: %v", err)
	} else if !ok {
		t.Errorf("expected value to be present")
	} else if g, w := val, "session_val"; g != w {
		t.Errorf("value mismatch\nGot:  %v\nWant: %v", g, w)
	}
}

func TestConnectionState_Snapshot(t *testing.T) {
	t.Run("NilState", func(t *testing.T) {
		var state *ConnectionState
		if snap := state.Snapshot(); snap != nil {
			t.Errorf("Snapshot on nil state mismatch\nGot:  %v\nWant: <nil>", snap)
		}
	})

	t.Run("NilPropertyValueInSource", func(t *testing.T) {
		state := &ConnectionState{
			properties: map[string]ConnectionPropertyValue{
				"nil_prop": nil,
			},
		}
		snap := state.Snapshot()
		if snap == nil {
			t.Fatalf("expected non-nil snapshot, got nil")
		}
		if g, w := len(snap.properties), 0; g != w {
			t.Errorf("properties length mismatch\nGot:  %v\nWant: %v", g, w)
		}
	})

	t.Run("IsolationWhenNoStatementScopedProperties", func(t *testing.T) {
		prop := mustCreateInternalConnectionProperty(t, "scoped_prop", "default_val")
		state, err := NewConnectionState(TypeNonTransactional, nil, nil)
		if err != nil {
			t.Fatalf("failed to create connection state: %v", err)
		}

		snap := state.Snapshot()
		if snap == state {
			t.Errorf("expected Snapshot without statement-scoped properties to return a distinct instance for isolation")
		}

		var sink *ConnectionState
		allocs := testing.AllocsPerRun(100, func() {
			sink = state.Snapshot()
		})
		_ = sink
		if g, w := allocs, float64(2); g > w {
			t.Errorf("Snapshot allocation count mismatch\nGot:  %v\nWant: <= %v", g, w)
		}

		// Subsequent statement-scoped value set on state must not bleed into the snapshot
		if err := prop.SetStatementScopedValue(state, "statement_val"); err != nil {
			t.Fatalf("SetStatementScopedValue failed: %v", err)
		}
		if g, w := prop.GetValueOrDefault(snap), "default_val"; g != w {
			t.Errorf("snapshot was polluted by subsequent statement value\nGot:  %v\nWant: %v", g, w)
		}
		if g, w := prop.GetValueOrDefault(state), "statement_val"; g != w {
			t.Errorf("state value mismatch\nGot:  %v\nWant: %v", g, w)
		}
	})

	t.Run("PreservesStatementScopedPropertiesAfterClear", func(t *testing.T) {
		prop := mustCreateInternalConnectionProperty(t, "scoped_prop", "default_val")
		state, err := NewConnectionState(TypeNonTransactional, nil, nil)
		if err != nil {
			t.Fatalf("failed to create connection state: %v", err)
		}

		if err := prop.SetStatementScopedValue(state, "custom_val"); err != nil {
			t.Fatalf("SetStatementScopedValue failed: %v", err)
		}

		snapshot := state.Snapshot()
		if snapshot == state {
			t.Fatalf("expected Snapshot with statement-scoped properties to return a distinct copy")
		}
		if g, w := prop.GetValueOrDefault(snapshot), "custom_val"; g != w {
			t.Errorf("snapshot value before clear mismatch\nGot:  %v\nWant: %v", g, w)
		}
		if g, w := prop.GetValueOrDefault(state), "custom_val"; g != w {
			t.Errorf("state value before clear mismatch\nGot:  %v\nWant: %v", g, w)
		}

		// Clear statement-scoped values on the parent state.
		state.ClearStatementScopedValues()

		// Parent state should be back to default.
		if g, w := prop.GetValueOrDefault(state), "default_val"; g != w {
			t.Errorf("state value after clear mismatch\nGot:  %v\nWant: %v", g, w)
		}
		// Snapshot should still retain the statement-scoped value!
		if g, w := prop.GetValueOrDefault(snapshot), "custom_val"; g != w {
			t.Errorf("snapshot value after clear mismatch\nGot:  %v\nWant: %v", g, w)
		}

		// Setting a new statement-scoped value on parent state does not affect snapshot.
		if err := prop.SetStatementScopedValue(state, "second_val"); err != nil {
			t.Fatalf("SetStatementScopedValue failed: %v", err)
		}
		if g, w := prop.GetValueOrDefault(state), "second_val"; g != w {
			t.Errorf("state value after second set mismatch\nGot:  %v\nWant: %v", g, w)
		}
		if g, w := prop.GetValueOrDefault(snapshot), "custom_val"; g != w {
			t.Errorf("snapshot value after second set mismatch\nGot:  %v\nWant: %v", g, w)
		}
	})

	t.Run("InPlaceMutationIsolationBeforeClear", func(t *testing.T) {
		prop := mustCreateInternalConnectionProperty(t, "scoped_prop", "default_val")
		state, err := NewConnectionState(TypeNonTransactional, nil, nil)
		if err != nil {
			t.Fatalf("failed to create connection state: %v", err)
		}

		if err := prop.SetStatementScopedValue(state, "first_val"); err != nil {
			t.Fatalf("SetStatementScopedValue failed: %v", err)
		}

		snapshot := state.Snapshot()

		// Mutate in place on state WITHOUT calling ClearStatementScopedValues first
		if err := prop.SetStatementScopedValue(state, "mutated_in_place"); err != nil {
			t.Fatalf("SetStatementScopedValue failed: %v", err)
		}

		// Snapshot must retain first_val because Snapshot deep-copied the property value
		if g, w := prop.GetValueOrDefault(snapshot), "first_val"; g != w {
			t.Errorf("snapshot value mutated in-place\nGot:  %v\nWant: %v", g, w)
		}
		if g, w := prop.GetValueOrDefault(state), "mutated_in_place"; g != w {
			t.Errorf("state value mismatch\nGot:  %v\nWant: %v", g, w)
		}
	})

	t.Run("IsolatesSessionProperties", func(t *testing.T) {
		prop := CreateConnectionProperty("session_prop", "Session property description", "default_val", false, nil, ContextUser, noop)
		state, err := NewConnectionState(TypeNonTransactional, map[string]ConnectionProperty{prop.Key(): prop}, nil)
		if err != nil {
			t.Fatalf("failed to create connection state: %v", err)
		}
		if err := prop.SetValue(state, "session_val_1", ContextUser); err != nil {
			t.Fatalf("SetValue failed: %v", err)
		}

		snapshot := state.Snapshot()
		if err := prop.SetValue(state, "session_val_2", ContextUser); err != nil {
			t.Fatalf("SetValue failed: %v", err)
		}

		if g, w := prop.GetValueOrDefault(snapshot), "session_val_1"; g != w {
			t.Errorf("snapshot session property mismatch\nGot:  %v\nWant: %v", g, w)
		}
		if g, w := prop.GetValueOrDefault(state), "session_val_2"; g != w {
			t.Errorf("state session property mismatch\nGot:  %v\nWant: %v", g, w)
		}
	})

	t.Run("IsolatesTransactionAndLocalProperties", func(t *testing.T) {
		prop := CreateConnectionProperty("tx_prop", "Transaction property description", "default_val", false, nil, ContextUser, noop)
		state, err := NewConnectionState(TypeTransactional, map[string]ConnectionProperty{prop.Key(): prop}, nil)
		if err != nil {
			t.Fatalf("failed to create connection state: %v", err)
		}
		if err := state.Begin(); err != nil {
			t.Fatalf("Begin failed: %v", err)
		}
		if err := prop.SetValue(state, "tx_val_1", ContextUser); err != nil {
			t.Fatalf("SetValue failed: %v", err)
		}
		if err := prop.SetLocalValue(state, "local_val_1"); err != nil {
			t.Fatalf("SetLocalValue failed: %v", err)
		}

		snapshot := state.Snapshot()

		// Mutate local property on parent state.
		if err := prop.SetLocalValue(state, "local_val_2"); err != nil {
			t.Fatalf("SetLocalValue failed: %v", err)
		}
		if g, w := prop.GetValueOrDefault(snapshot), "local_val_1"; g != w {
			t.Errorf("snapshot local property mismatch\nGot:  %v\nWant: %v", g, w)
		}
		if g, w := prop.GetValueOrDefault(state), "local_val_2"; g != w {
			t.Errorf("state local property mismatch\nGot:  %v\nWant: %v", g, w)
		}

		// Rollback parent state transaction; snapshot should preserve its snapshotted values.
		if err := state.Rollback(); err != nil {
			t.Fatalf("Rollback failed: %v", err)
		}
		if g, w := prop.GetValueOrDefault(snapshot), "local_val_1"; g != w {
			t.Errorf("snapshot property after state rollback mismatch\nGot:  %v\nWant: %v", g, w)
		}
		if g, w := prop.GetValueOrDefault(state), "default_val"; g != w {
			t.Errorf("state property after rollback mismatch\nGot:  %v\nWant: %v", g, w)
		}
	})

	t.Run("IsolatesReset", func(t *testing.T) {
		prop := CreateConnectionProperty("reset_prop", "Reset property description", "default_val", false, nil, ContextUser, noop)
		state, err := NewConnectionState(TypeNonTransactional, map[string]ConnectionProperty{prop.Key(): prop}, nil)
		if err != nil {
			t.Fatalf("failed to create connection state: %v", err)
		}
		if err := prop.SetValue(state, "custom_val", ContextUser); err != nil {
			t.Fatalf("SetValue failed: %v", err)
		}

		snapshot := state.Snapshot()

		if err := state.Reset(ContextUser); err != nil {
			t.Fatalf("Reset failed: %v", err)
		}

		if g, w := prop.GetValueOrDefault(snapshot), "custom_val"; g != w {
			t.Errorf("snapshot property after reset mismatch\nGot:  %v\nWant: %v", g, w)
		}
		if g, w := prop.GetValueOrDefault(state), "default_val"; g != w {
			t.Errorf("state property after reset mismatch\nGot:  %v\nWant: %v", g, w)
		}
	})

	t.Run("IsolatesAliases", func(t *testing.T) {
		prop1 := CreateConnectionProperty("target_prop1", "Target 1", "val1", true, nil, ContextUser, noop)
		prop2 := CreateConnectionProperty("target_prop2", "Target 2", "val2", true, nil, ContextUser, noop)
		state, err := NewConnectionState(TypeNonTransactional, map[string]ConnectionProperty{
			prop1.Key(): prop1,
			prop2.Key(): prop2,
		}, nil)
		if err != nil {
			t.Fatalf("failed to create connection state: %v", err)
		}
		state.AddAlias("alias1", prop1.Key(), false)

		snapshot := state.Snapshot()

		state.AddAlias("alias2", prop2.Key(), false)

		// State can resolve alias2, snapshot cannot.
		if _, ok, err := state.GetValue("", "alias2"); err != nil || !ok {
			t.Errorf("state failed to resolve alias2: ok=%v, err=%v", ok, err)
		}
		if _, ok, _ := snapshot.GetValue("", "alias2"); ok {
			t.Errorf("expected snapshot to fail resolving alias2 added after snapshot")
		}
	})
}

func TestExtractValues_SkipsInternalProperties(t *testing.T) {
	internalProp := mustCreateInternalConnectionProperty(t, "internal_prop", "default_internal")
	userProp := CreateConnectionProperty("user_prop", "Test user property", "default_user", false, nil, ContextUser, noop)

	properties := map[string]ConnectionProperty{
		internalProp.Key(): internalProp,
		userProp.Key():     userProp,
	}

	values := map[string]string{
		"internal_prop": "attempted_internal_override",
		"user_prop":     "user_override",
	}

	extracted, err := ExtractValues(properties, values)
	if err != nil {
		t.Fatalf("ExtractValues failed: %v", err)
	}

	if _, ok := extracted[internalProp.Key()]; ok {
		t.Errorf("expected internal property to be skipped in ExtractValues, but found key %q", internalProp.Key())
	}
	if userVal, ok := extracted[userProp.Key()]; !ok {
		t.Errorf("expected user property %q to be extracted", userProp.Key())
	} else if val, err := userVal.GetValue(); err != nil {
		t.Fatalf("GetValue failed: %v", err)
	} else if g, w := val, "user_override"; g != w {
		t.Errorf("extracted value mismatch\nGot:  %v\nWant: %v", g, w)
	}
}
