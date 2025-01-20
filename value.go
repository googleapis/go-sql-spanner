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
	"database/sql/driver"
	"math/big"
	"reflect"
	"time"

	"cloud.google.com/go/civil"
	"cloud.google.com/go/spanner"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// checkIsValidType returns true for types that do not need extra conversion
// for spanner.
func checkIsValidType(v driver.Value) bool {
	switch v.(type) {
	default:
		return false
	case nil:
	case sql.NullInt64:
	case sql.NullTime:
	case sql.NullString:
	case sql.NullFloat64:
	case sql.NullBool:
	case sql.NullInt32:
	case string:
	case spanner.NullString:
	case []string:
	case []spanner.NullString:
	case *string:
	case []*string:
	case []byte:
	case [][]byte:
	case uint:
	case []uint:
	case *uint:
	case []*uint:
	case *[]uint:
	case int:
	case []int:
	case *int:
	case []*int:
	case *[]int:
	case int64:
	case []int64:
	case spanner.NullInt64:
	case []spanner.NullInt64:
	case *int64:
	case []*int64:
	case uint64:
	case []uint64:
	case *uint64:
	case []*uint64:
	case bool:
	case []bool:
	case spanner.NullBool:
	case []spanner.NullBool:
	case *bool:
	case []*bool:
	case float32:
	case []float32:
	case spanner.NullFloat32:
	case []spanner.NullFloat32:
	case *float32:
	case []*float32:
	case float64:
	case []float64:
	case spanner.NullFloat64:
	case []spanner.NullFloat64:
	case *float64:
	case []*float64:
	case big.Rat:
	case []big.Rat:
	case spanner.NullNumeric:
	case []spanner.NullNumeric:
	case *big.Rat:
	case []*big.Rat:
	case time.Time:
	case []time.Time:
	case spanner.NullTime:
	case []spanner.NullTime:
	case *time.Time:
	case []*time.Time:
	case civil.Date:
	case []civil.Date:
	case spanner.NullDate:
	case []spanner.NullDate:
	case *civil.Date:
	case []*civil.Date:
	case spanner.NullJSON:
	case []spanner.NullJSON:
	case spanner.GenericColumnValue:
	}
	return true
}

/* The following is the same implementation as in google-cloud-go/spanner */

func isStructOrArrayOfStructValue(v interface{}) bool {
	typ := reflect.TypeOf(v)
	if typ.Kind() == reflect.Slice {
		typ = typ.Elem()
	}
	if typ.Kind() == reflect.Ptr {
		typ = typ.Elem()
	}
	return typ.Kind() == reflect.Struct
}

func isAnArrayOfProtoColumn(v interface{}) bool {
	typ := reflect.TypeOf(v)
	if typ.Kind() == reflect.Ptr {
		typ = typ.Elem()
	}
	if typ.Kind() == reflect.Slice {
		typ = typ.Elem()
	}
	return typ.Implements(protoMsgReflectType) || typ.Implements(protoEnumReflectType)
}

var (
	protoMsgReflectType  = reflect.TypeOf((*proto.Message)(nil)).Elem()
	protoEnumReflectType = reflect.TypeOf((*protoreflect.Enum)(nil)).Elem()
)
