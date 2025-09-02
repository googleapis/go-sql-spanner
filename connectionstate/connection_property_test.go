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
			t.Fatalf("error code mismatch\n Got: %v\nWant: %v", g, w)
		}
	}
}
