// Copyright 2024 Google LLC
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
	"database/sql/driver"
	"reflect"
	"testing"
)

func TestConvertParam(t *testing.T) {
	check := func(in, want driver.Value) {
		t.Helper()
		got := convertParam(in, false)
		if !reflect.DeepEqual(got, want) {
			t.Errorf("in:%#v want:%#v got:%#v", in, want, got)
		}
	}

	check(uint(197), int64(197))
	check(pointerTo(uint(197)), pointerTo(int64(197)))
	check((*uint)(nil), (*int64)(nil))

	check([]uint{197}, []int64{197})
	check(pointerTo([]uint{197}), []int64{197})
	check([]*uint{pointerTo(uint(197))}, []*int64{pointerTo(int64(197))})
	check(([]*uint)(nil), ([]*int64)(nil))
	check((*[]uint)(nil), ([]int64)(nil))

	check([]int{197}, []int64{197})
	check([]*int{pointerTo(int(197))}, []*int64{pointerTo(int64(197))})
	check(pointerTo([]int{197}), []int64{197})
	check(([]*int)(nil), ([]*int64)(nil))
	check((*[]int)(nil), ([]int64)(nil))

	check(uint64(197), int64(197))
	check(pointerTo(uint64(197)), pointerTo(int64(197)))
	check((*uint64)(nil), (*int64)(nil))

	check([]uint64{197}, []int64{197})
	check(pointerTo([]uint64{197}), []int64{197})
	check([]*uint64{pointerTo(uint64(197))}, []*int64{pointerTo(int64(197))})
	check(([]*uint64)(nil), ([]*int64)(nil))
	check((*[]uint64)(nil), ([]int64)(nil))
}

func pointerTo[T any](v T) *T { return &v }
