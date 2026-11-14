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

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestCheckValidValue(t *testing.T) {
	type testValue int
	const (
		testValueUnspecified testValue = iota
		testValueTrue
		testValueFalse
	)

	p := &TypedConnectionProperty[testValue]{validValues: []testValue{testValueTrue, testValueFalse}}
	if err := p.checkValidValue(testValueTrue); err != nil {
		t.Fatal(err)
	}
	if err := p.checkValidValue(testValueFalse); err != nil {
		t.Fatal(err)
	}
	if err := p.checkValidValue(testValueUnspecified); err == nil {
		t.Fatalf("expected error for %v", testValueUnspecified)
	} else {
		if g, w := status.Code(err), codes.InvalidArgument; g != w {
			t.Fatalf("error code mismatch\nGot:  %v\nWant: %v", g, w)
		}
	}
}

func stringConverter(value string) (string, error) {
	return value, nil
}

func TestIsSensitive(t *testing.T) {
	normalProp := CreateConnectionProperty("normal_prop", "desc", "default", false, nil, ContextUser, stringConverter)
	if g, w := normalProp.IsSensitive(), false; g != w {
		t.Errorf("IsSensitive mismatch for normal property\nGot:  %v\nWant: %v", g, w)
	}

	normalExtProp := CreateConnectionPropertyWithExtension("ext", "normal_ext_prop", "desc", "default", false, nil, ContextUser, stringConverter)
	if g, w := normalExtProp.IsSensitive(), false; g != w {
		t.Errorf("IsSensitive mismatch for normal extension property\nGot:  %v\nWant: %v", g, w)
	}

	sensitiveProp := CreateSensitiveConnectionProperty("sensitive_prop", "desc", "default", false, nil, ContextStartup, stringConverter)
	if g, w := sensitiveProp.IsSensitive(), true; g != w {
		t.Errorf("IsSensitive mismatch for sensitive property\nGot:  %v\nWant: %v", g, w)
	}

	sensitiveExtProp := CreateSensitiveConnectionPropertyWithExtension("ext", "sensitive_ext_prop", "desc", "default", false, nil, ContextStartup, stringConverter)
	if g, w := sensitiveExtProp.IsSensitive(), true; g != w {
		t.Errorf("IsSensitive mismatch for sensitive extension property\nGot:  %v\nWant: %v", g, w)
	}
}

func TestSensitiveProperty_GetValue(t *testing.T) {
	sensitiveProp := CreateSensitiveConnectionProperty("sensitive_prop", "desc", "default_val", true, nil, ContextStartup, stringConverter)
	properties := map[string]ConnectionProperty{
		sensitiveProp.Key(): sensitiveProp,
	}
	state, err := NewConnectionState(TypeNonTransactional, properties, map[string]ConnectionPropertyValue{})
	if err != nil {
		t.Fatalf("failed to create connection state: %v", err)
	}

	// GetValue succeeds for programmatic access, returning the underlying value.
	val, ok, err := sensitiveProp.GetValue(state)
	if err != nil {
		t.Fatalf("GetValue failed: %v", err)
	}
	if g, w := ok, true; g != w {
		t.Errorf("hasValue mismatch\nGot:  %v\nWant: %v", g, w)
	}
	if g, w := val, "default_val"; g != w {
		t.Errorf("GetValue value mismatch\nGot:  %v\nWant: %v", g, w)
	}

	// GetValueOrError also returns the value.
	val, ok, err = sensitiveProp.GetValueOrError(state)
	if err != nil {
		t.Fatalf("GetValueOrError failed: %v", err)
	}
	if g, w := ok, true; g != w {
		t.Errorf("hasValue mismatch\nGot:  %v\nWant: %v", g, w)
	}
	if g, w := val, "default_val"; g != w {
		t.Errorf("GetValueOrError value mismatch\nGot:  %v\nWant: %v", g, w)
	}
}

func sensitivePropertySetTestCases(prop *TypedConnectionProperty[string]) []struct {
	name      string
	operation func(state *ConnectionState) error
	wantCode  codes.Code
} {
	return []struct {
		name      string
		operation func(state *ConnectionState) error
		wantCode  codes.Code
	}{
		{
			name: "SetValue in ContextStartup succeeds",
			operation: func(state *ConnectionState) error {
				return prop.SetValue(state, "startup_val", ContextStartup)
			},
			wantCode: codes.OK,
		},
		{
			name: "SetValue in ContextUser fails with PermissionDenied",
			operation: func(state *ConnectionState) error {
				return prop.SetValue(state, "user_val", ContextUser)
			},
			wantCode: codes.PermissionDenied,
		},
		{
			name: "SetValue in higher Context fails with PermissionDenied",
			operation: func(state *ConnectionState) error {
				return prop.SetValue(state, "user_val", Context(99))
			},
			wantCode: codes.PermissionDenied,
		},
		{
			name: "SetUntypedValue in ContextStartup succeeds",
			operation: func(state *ConnectionState) error {
				return prop.SetUntypedValue(state, "startup_val", ContextStartup)
			},
			wantCode: codes.OK,
		},
		{
			name: "SetUntypedValue in ContextUser fails with PermissionDenied",
			operation: func(state *ConnectionState) error {
				return prop.SetUntypedValue(state, "user_val", ContextUser)
			},
			wantCode: codes.PermissionDenied,
		},
		{
			name: "SetDefaultValue in ContextStartup succeeds",
			operation: func(state *ConnectionState) error {
				return prop.SetDefaultValue(state, ContextStartup)
			},
			wantCode: codes.OK,
		},
		{
			name: "SetDefaultValue in ContextUser fails with PermissionDenied",
			operation: func(state *ConnectionState) error {
				return prop.SetDefaultValue(state, ContextUser)
			},
			wantCode: codes.PermissionDenied,
		},
		{
			name: "SetLocalValue fails with PermissionDenied",
			operation: func(state *ConnectionState) error {
				return prop.SetLocalValue(state, "local_val")
			},
			wantCode: codes.PermissionDenied,
		},
		{
			name: "SetLocalUntypedValue fails with PermissionDenied",
			operation: func(state *ConnectionState) error {
				return prop.SetLocalUntypedValue(state, "local_val")
			},
			wantCode: codes.PermissionDenied,
		},
		{
			name: "SetLocalDefaultValue fails with PermissionDenied",
			operation: func(state *ConnectionState) error {
				return prop.SetLocalDefaultValue(state)
			},
			wantCode: codes.PermissionDenied,
		},
		{
			name: "SetStatementScopedValue fails with PermissionDenied",
			operation: func(state *ConnectionState) error {
				return prop.SetStatementScopedValue(state, "stmt_val")
			},
			wantCode: codes.PermissionDenied,
		},
		{
			name: "SetStatementScopedUntypedValue fails with PermissionDenied",
			operation: func(state *ConnectionState) error {
				return prop.SetStatementScopedUntypedValue(state, "stmt_val")
			},
			wantCode: codes.PermissionDenied,
		},
		{
			name: "SetStatementScopedDefaultValue fails with PermissionDenied",
			operation: func(state *ConnectionState) error {
				return prop.SetStatementScopedDefaultValue(state)
			},
			wantCode: codes.PermissionDenied,
		},
		{
			name: "ResetValue in ContextStartup succeeds",
			operation: func(state *ConnectionState) error {
				return prop.ResetValue(state, ContextStartup)
			},
			wantCode: codes.OK,
		},
		{
			name: "ResetValue in ContextUser fails with PermissionDenied",
			operation: func(state *ConnectionState) error {
				return prop.ResetValue(state, ContextUser)
			},
			wantCode: codes.PermissionDenied,
		},
		{
			name: "ResetLocalValue fails with PermissionDenied",
			operation: func(state *ConnectionState) error {
				return prop.ResetLocalValue(state)
			},
			wantCode: codes.PermissionDenied,
		},
		{
			name: "ResetStatementScopedValue fails with PermissionDenied",
			operation: func(state *ConnectionState) error {
				return prop.ResetStatementScopedValue(state)
			},
			wantCode: codes.PermissionDenied,
		},
	}
}

func TestSensitiveProperty_SetOperations(t *testing.T) {
	sensitiveProp := CreateSensitiveConnectionProperty("sensitive_prop", "desc", "default_val", true, nil, ContextStartup, stringConverter)
	properties := map[string]ConnectionProperty{
		sensitiveProp.Key(): sensitiveProp,
	}

	for _, testCase := range sensitivePropertySetTestCases(sensitiveProp) {
		t.Run(testCase.name, func(t *testing.T) {
			state, err := NewConnectionState(TypeNonTransactional, properties, map[string]ConnectionPropertyValue{})
			if err != nil {
				t.Fatalf("failed to create connection state: %v", err)
			}
			err = testCase.operation(state)
			if g, w := status.Code(err), testCase.wantCode; g != w {
				t.Errorf("error code mismatch\nGot:  %v\nWant: %v", g, w)
			}
		})
	}
}

func TestSensitiveProperty_ContextUser_SetOperations(t *testing.T) {
	sensitiveUserProp := CreateSensitiveConnectionProperty("sensitive_user_prop", "desc", "default_val", true, nil, ContextUser, stringConverter)
	properties := map[string]ConnectionProperty{
		sensitiveUserProp.Key(): sensitiveUserProp,
	}

	for _, testCase := range sensitivePropertySetTestCases(sensitiveUserProp) {
		t.Run(testCase.name, func(t *testing.T) {
			state, err := NewConnectionState(TypeNonTransactional, properties, map[string]ConnectionPropertyValue{})
			if err != nil {
				t.Fatalf("failed to create connection state: %v", err)
			}
			err = testCase.operation(state)
			if g, w := status.Code(err), testCase.wantCode; g != w {
				t.Errorf("error code mismatch\nGot:  %v\nWant: %v", g, w)
			}
		})
	}
}

func TestSensitiveProperty_UnpopulatedExtensionReset(t *testing.T) {
	sensitiveExtProp := CreateSensitiveConnectionPropertyWithExtension("custom", "ext_secret", "desc", "default_val", true, nil, ContextUser, stringConverter)
	state, err := NewConnectionState(TypeNonTransactional, map[string]ConnectionProperty{}, map[string]ConnectionPropertyValue{})
	if err != nil {
		t.Fatalf("failed to create connection state: %v", err)
	}

	tests := []struct {
		name      string
		operation func() error
	}{
		{
			name:      "ResetValue in ContextUser returns PermissionDenied",
			operation: func() error { return sensitiveExtProp.ResetValue(state, ContextUser) },
		},
		{
			name:      "ResetLocalValue returns PermissionDenied",
			operation: func() error { return sensitiveExtProp.ResetLocalValue(state) },
		},
		{
			name:      "ResetStatementScopedValue returns PermissionDenied",
			operation: func() error { return sensitiveExtProp.ResetStatementScopedValue(state) },
		},
		{
			name:      "SetUntypedValue with invalid type in ContextUser returns PermissionDenied",
			operation: func() error { return sensitiveExtProp.SetUntypedValue(state, 123, ContextUser) },
		},
		{
			name:      "SetLocalUntypedValue with invalid type returns PermissionDenied",
			operation: func() error { return sensitiveExtProp.SetLocalUntypedValue(state, 123) },
		},
		{
			name:      "SetStatementScopedUntypedValue with invalid type returns PermissionDenied",
			operation: func() error { return sensitiveExtProp.SetStatementScopedUntypedValue(state, 123) },
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if g, w := status.Code(tc.operation()), codes.PermissionDenied; g != w {
				t.Errorf("error code mismatch\nGot:  %v\nWant: %v", g, w)
			}
		})
	}
}

func TestNormalProperty_LocalAndStatementScopedOperations(t *testing.T) {
	normalProp := CreateConnectionProperty("normal_prop", "desc", "default_val", true, nil, ContextUser, stringConverter)
	properties := map[string]ConnectionProperty{
		normalProp.Key(): normalProp,
	}

	tests := []struct {
		name      string
		operation func(state *ConnectionState) error
		wantCode  codes.Code
	}{
		{
			name: "SetLocalUntypedValue valid type succeeds in transaction",
			operation: func(state *ConnectionState) error {
				if err := state.Begin(); err != nil {
					return err
				}
				return normalProp.SetLocalUntypedValue(state, "local_val")
			},
			wantCode: codes.OK,
		},
		{
			name: "SetLocalUntypedValue invalid type fails with InvalidArgument",
			operation: func(state *ConnectionState) error {
				return normalProp.SetLocalUntypedValue(state, 123)
			},
			wantCode: codes.InvalidArgument,
		},
		{
			name: "SetStatementScopedUntypedValue valid type succeeds",
			operation: func(state *ConnectionState) error {
				return normalProp.SetStatementScopedUntypedValue(state, "stmt_val")
			},
			wantCode: codes.OK,
		},
		{
			name: "SetStatementScopedUntypedValue invalid type fails with InvalidArgument",
			operation: func(state *ConnectionState) error {
				return normalProp.SetStatementScopedUntypedValue(state, 123)
			},
			wantCode: codes.InvalidArgument,
		},
		{
			name: "ResetLocalValue succeeds in transaction",
			operation: func(state *ConnectionState) error {
				if err := state.Begin(); err != nil {
					return err
				}
				return normalProp.ResetLocalValue(state)
			},
			wantCode: codes.OK,
		},
		{
			name: "ResetStatementScopedValue succeeds",
			operation: func(state *ConnectionState) error {
				return normalProp.ResetStatementScopedValue(state)
			},
			wantCode: codes.OK,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			state, err := NewConnectionState(TypeTransactional, properties, map[string]ConnectionPropertyValue{})
			if err != nil {
				t.Fatalf("failed to create connection state: %v", err)
			}
			if g, w := status.Code(tc.operation(state)), tc.wantCode; g != w {
				t.Errorf("error code mismatch\nGot:  %v\nWant: %v", g, w)
			}
		})
	}
}
