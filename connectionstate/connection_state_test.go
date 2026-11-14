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

func setupSensitivePropertiesTestState(t *testing.T) *ConnectionState {
	secretProp := CreateSensitiveConnectionProperty("secret_key", "Secret property", "initial_secret", true, nil, ContextStartup, noop)
	secretUserProp := CreateSensitiveConnectionProperty("secret_user_key", "Secret user property", "initial_user_secret", true, nil, ContextUser, noop)
	normalProp := CreateConnectionProperty("normal_key", "Normal property", "initial_normal", true, nil, ContextUser, noop)
	secretExtProp := CreateSensitiveConnectionPropertyWithExtension("custom", "sensitive_ext", "Custom extension secret", "initial_ext", true, nil, ContextStartup, noop)

	properties := map[string]ConnectionProperty{
		secretProp.Key():     secretProp,
		secretUserProp.Key(): secretUserProp,
		normalProp.Key():     normalProp,
		secretExtProp.Key():  secretExtProp,
	}

	state, err := NewConnectionState(TypeNonTransactional, properties, map[string]ConnectionPropertyValue{})
	if err != nil {
		t.Fatalf("failed to create connection state: %v", err)
	}
	state.AddAlias("secret_alias", "secret_key", false /* readOnly */)
	state.AddAlias("secret_alias_ro", "secret_key", true /* readOnly */)
	return state
}

func TestConnectionState_FindProperty_Sensitive(t *testing.T) {
	state := setupSensitivePropertiesTestState(t)

	findTests := []struct {
		name          string
		extension     string
		propName      string
		wantSensitive bool
		wantCode      codes.Code
	}{
		{
			name:          "sensitive property is marked sensitive",
			extension:     "",
			propName:      "secret_key",
			wantSensitive: true,
			wantCode:      codes.OK,
		},
		{
			name:          "sensitive property case insensitive",
			extension:     "",
			propName:      "SECRET_KEY",
			wantSensitive: true,
			wantCode:      codes.OK,
		},
		{
			name:          "alias to sensitive property resolves and is sensitive",
			extension:     "",
			propName:      "secret_alias",
			wantSensitive: true,
			wantCode:      codes.OK,
		},
		{
			name:          "read-only alias to sensitive property resolves and is sensitive",
			extension:     "",
			propName:      "secret_alias_ro",
			wantSensitive: true,
			wantCode:      codes.OK,
		},
		{
			name:          "sensitive extension property is marked sensitive",
			extension:     "custom",
			propName:      "sensitive_ext",
			wantSensitive: true,
			wantCode:      codes.OK,
		},
		{
			name:          "normal property is not sensitive",
			extension:     "",
			propName:      "normal_key",
			wantSensitive: false,
			wantCode:      codes.OK,
		},
		{
			name:          "unknown property returns InvalidArgument",
			extension:     "",
			propName:      "unknown_prop",
			wantSensitive: false,
			wantCode:      codes.InvalidArgument,
		},
	}

	for _, tc := range findTests {
		t.Run(tc.name, func(t *testing.T) {
			prop, err := state.FindProperty(tc.extension, tc.propName)
			if g, w := status.Code(err), tc.wantCode; g != w {
				t.Fatalf("error code mismatch\nGot:  %v\nWant: %v", g, w)
			}
			if tc.wantCode == codes.OK {
				if g, w := prop.IsSensitive(), tc.wantSensitive; g != w {
					t.Errorf("IsSensitive mismatch\nGot:  %v\nWant: %v", g, w)
				}
			}
		})
	}
}

func TestConnectionState_GetValue_Sensitive(t *testing.T) {
	state := setupSensitivePropertiesTestState(t)

	getValueTests := []struct {
		name      string
		extension string
		propName  string
		wantCode  codes.Code
		wantVal   any
	}{
		{
			name:      "sensitive property returns value for programmatic access",
			extension: "",
			propName:  "secret_key",
			wantCode:  codes.OK,
			wantVal:   "initial_secret",
		},
		{
			name:      "sensitive property case insensitive returns value",
			extension: "",
			propName:  "SECRET_KEY",
			wantCode:  codes.OK,
			wantVal:   "initial_secret",
		},
		{
			name:      "alias to sensitive property returns value",
			extension: "",
			propName:  "secret_alias",
			wantCode:  codes.OK,
			wantVal:   "initial_secret",
		},
		{
			name:      "sensitive extension property returns value",
			extension: "custom",
			propName:  "sensitive_ext",
			wantCode:  codes.OK,
			wantVal:   "initial_ext",
		},
		{
			name:      "normal property returns value",
			extension: "",
			propName:  "normal_key",
			wantCode:  codes.OK,
			wantVal:   "initial_normal",
		},
		{
			name:      "unknown property returns InvalidArgument",
			extension: "",
			propName:  "unknown_prop",
			wantCode:  codes.InvalidArgument,
		},
	}

	for _, tc := range getValueTests {
		t.Run(tc.name, func(t *testing.T) {
			val, _, err := state.GetValue(tc.extension, tc.propName)
			if g, w := status.Code(err), tc.wantCode; g != w {
				t.Fatalf("error code mismatch\nGot:  %v\nWant: %v", g, w)
			}
			if tc.wantCode == codes.OK {
				if g, w := val, tc.wantVal; g != w {
					t.Errorf("value mismatch\nGot:  %v\nWant: %v", g, w)
				}
			}
		})
	}
}

func TestConnectionState_SetValue_Sensitive(t *testing.T) {
	setValueTests := []struct {
		name      string
		operation func(state *ConnectionState) error
		wantCode  codes.Code
	}{
		{
			name: "sensitive property SetValue in ContextStartup succeeds",
			operation: func(state *ConnectionState) error {
				return state.SetValue("", "secret_key", "new_secret", ContextStartup)
			},
			wantCode: codes.OK,
		},
		{
			name: "sensitive property SetValue in ContextUser fails with PermissionDenied",
			operation: func(state *ConnectionState) error {
				return state.SetValue("", "secret_key", "new_secret", ContextUser)
			},
			wantCode: codes.PermissionDenied,
		},
		{
			name: "sensitive property SetValue NULL in ContextUser fails with PermissionDenied",
			operation: func(state *ConnectionState) error {
				return state.SetValue("", "secret_key", "null", ContextUser)
			},
			wantCode: codes.PermissionDenied,
		},
		{
			name: "sensitive property SetValue DEFAULT in ContextUser fails with PermissionDenied",
			operation: func(state *ConnectionState) error {
				return state.SetValue("", "secret_key", "default", ContextUser)
			},
			wantCode: codes.PermissionDenied,
		},
		{
			name: "sensitive property SetLocalValue fails with PermissionDenied",
			operation: func(state *ConnectionState) error {
				return state.SetLocalValue("", "secret_key", "new_secret", false)
			},
			wantCode: codes.PermissionDenied,
		},
		{
			name: "sensitive property SetStatementScopedValue fails with PermissionDenied",
			operation: func(state *ConnectionState) error {
				return state.SetStatementScopedValue("", "secret_key", "new_secret")
			},
			wantCode: codes.PermissionDenied,
		},
		{
			name: "alias to sensitive property SetValue fails with PermissionDenied",
			operation: func(state *ConnectionState) error {
				return state.SetValue("", "secret_alias", "new_secret", ContextUser)
			},
			wantCode: codes.PermissionDenied,
		},
		{
			name: "read-only alias to sensitive property SetValue fails with PermissionDenied",
			operation: func(state *ConnectionState) error {
				return state.SetValue("", "secret_alias_ro", "new_secret", ContextUser)
			},
			wantCode: codes.PermissionDenied,
		},
		{
			name: "sensitive extension property SetValue fails with PermissionDenied",
			operation: func(state *ConnectionState) error {
				return state.SetValue("custom", "sensitive_ext", "new_secret", ContextUser)
			},
			wantCode: codes.PermissionDenied,
		},
		{
			name: "ContextUser sensitive property SetValue in ContextUser fails with PermissionDenied",
			operation: func(state *ConnectionState) error {
				return state.SetValue("", "secret_user_key", "new_secret", ContextUser)
			},
			wantCode: codes.PermissionDenied,
		},
		{
			name: "ContextUser sensitive property SetValue NULL fails with PermissionDenied",
			operation: func(state *ConnectionState) error {
				return state.SetValue("", "secret_user_key", "null", ContextUser)
			},
			wantCode: codes.PermissionDenied,
		},
		{
			name: "ContextUser sensitive property SetValue DEFAULT fails with PermissionDenied",
			operation: func(state *ConnectionState) error {
				return state.SetValue("", "secret_user_key", "default", ContextUser)
			},
			wantCode: codes.PermissionDenied,
		},
		{
			name: "ContextUser sensitive property SetLocalValue fails with PermissionDenied",
			operation: func(state *ConnectionState) error {
				return state.SetLocalValue("", "secret_user_key", "new_secret", false)
			},
			wantCode: codes.PermissionDenied,
		},
		{
			name: "ContextUser sensitive property SetStatementScopedValue fails with PermissionDenied",
			operation: func(state *ConnectionState) error {
				return state.SetStatementScopedValue("", "secret_user_key", "new_secret")
			},
			wantCode: codes.PermissionDenied,
		},
		{
			name: "sensitive property SET TRANSACTION outside transaction fails with FailedPrecondition",
			operation: func(state *ConnectionState) error {
				return state.SetLocalValue("", "secret_user_key", "new_secret", true)
			},
			wantCode: codes.FailedPrecondition,
		},
		{
			name: "sensitive property SET TRANSACTION inside transaction fails with PermissionDenied",
			operation: func(state *ConnectionState) error {
				if err := state.Begin(); err != nil {
					return err
				}
				return state.SetLocalValue("", "secret_user_key", "new_secret", true)
			},
			wantCode: codes.PermissionDenied,
		},
		{
			name: "normal property SetValue succeeds",
			operation: func(state *ConnectionState) error {
				return state.SetValue("", "normal_key", "new_normal", ContextUser)
			},
			wantCode: codes.OK,
		},
		{
			name: "unknown property SetValue returns InvalidArgument",
			operation: func(state *ConnectionState) error {
				return state.SetValue("", "unknown_prop", "val", ContextUser)
			},
			wantCode: codes.InvalidArgument,
		},
	}

	for _, tc := range setValueTests {
		t.Run(tc.name, func(t *testing.T) {
			state := setupSensitivePropertiesTestState(t)
			err := tc.operation(state)
			if g, w := status.Code(err), tc.wantCode; g != w {
				t.Errorf("error code mismatch\nGot:  %v\nWant: %v", g, w)
			}
		})
	}
}

func TestConnectionState_ExtractValues_Sensitive(t *testing.T) {
	secretProp := CreateSensitiveConnectionProperty("secret_key", "Secret property", "", false, nil, ContextStartup, noop)
	properties := map[string]ConnectionProperty{
		secretProp.Key(): secretProp,
	}

	tests := []struct {
		name      string
		rawValues map[string]string
		wantVal   string
	}{
		{
			name:      "exact key name",
			rawValues: map[string]string{"secret_key": "secret-val-1"},
			wantVal:   "secret-val-1",
		},
		{
			name:      "underscore stripped key name",
			rawValues: map[string]string{"secretkey": "secret-val-2"},
			wantVal:   "secret-val-2",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			extracted, err := ExtractValues(properties, tc.rawValues)
			if err != nil {
				t.Fatalf("ExtractValues failed: %v", err)
			}
			val, ok := extracted[secretProp.Key()]
			if g, w := ok, true; g != w {
				t.Fatalf("extracted map key presence mismatch\nGot:  %v\nWant: %v", g, w)
			}
			gotVal, err := val.GetValue()
			if err != nil {
				t.Fatalf("val.GetValue() failed: %v", err)
			}
			if g, w := gotVal, tc.wantVal; g != w {
				t.Errorf("extracted value mismatch\nGot:  %v\nWant: %v", g, w)
			}
		})
	}
}

func TestConnectionState_Reset_SensitiveScrubbed(t *testing.T) {
	secretProp := CreateSensitiveConnectionProperty("secret_key", "Secret property", "", false, nil, ContextStartup, noop)
	normalProp := CreateConnectionProperty("normal_key", "Normal property", "default_normal", true, nil, ContextUser, noop)
	properties := map[string]ConnectionProperty{
		secretProp.Key(): secretProp,
		normalProp.Key(): normalProp,
	}

	// Simulate scrubbed initialPropertyValues where the sensitive property was deleted.
	state, err := NewConnectionState(TypeTransactional, properties, map[string]ConnectionPropertyValue{})
	if err != nil {
		t.Fatalf("NewConnectionState failed: %v", err)
	}
	if err := state.SetValue("", "normal_key", "changed", ContextUser); err != nil {
		t.Fatalf("SetValue failed: %v", err)
	}
	if err := state.Reset(ContextStartup); err != nil {
		t.Fatalf("Reset(ContextStartup) failed: %v", err)
	}

	val, hasValue, err := state.GetValue("", "secret_key")
	if err != nil {
		t.Fatalf("GetValue(secret_key) failed: %v", err)
	}
	if g, w := hasValue, false; g != w {
		t.Errorf("secret_key hasValue after Reset mismatch\nGot:  %v\nWant: %v", g, w)
	}
	if g, w := val, ""; g != w {
		t.Errorf("secret_key value after Reset mismatch\nGot:  %v\nWant: %v", g, w)
	}
}
