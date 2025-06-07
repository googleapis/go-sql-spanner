// Copyright 2025 Google LLC All Rights Reserved.
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
	"testing"

	"cloud.google.com/go/spanner"
	"google.golang.org/grpc/codes"
)

func Test_autoConfigEmulator(t *testing.T) {
	host := "localhost:9010"
	project := "local"
	instance := "main"
	database := "local"
	for range 2 {
		if err := autoConfigEmulator(t.Context(), host, project, instance, database); err != nil {
			if spanner.ErrCode(err) != codes.AlreadyExists {
				t.Errorf("only expect already exists errors, but got: %v", err)
			}
		}
	}
}
