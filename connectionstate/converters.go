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

import "strconv"

func ConvertBool(value string) (bool, error) {
	return strconv.ParseBool(value)
}

func ConvertInt(value string) (int, error) {
	return strconv.Atoi(value)
}

func ConvertInt64(value string) (int64, error) {
	return strconv.ParseInt(value, 10, 64)
}

func ConvertUint64(value string) (uint64, error) {
	return strconv.ParseUint(value, 10, 64)
}

func ConvertString(value string) (string, error) {
	return value, nil
}
