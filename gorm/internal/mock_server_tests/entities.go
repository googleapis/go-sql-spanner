// Copyright 2021 Google LLC All Rights Reserved.
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

package mock_server_tests

import (
	"time"

	"cloud.google.com/go/spanner"
)

type Singer struct {
	SingerId  int64 `gorm:"primaryKey:true;autoIncrement:false"`
	FirstName *string
	LastName  string `gorm:"index:idx_singers_last_name"`
	BirthDate spanner.NullDate `gorm:"type:DATE;check:chk_singers_birth_date,birth_date >= DATE '1000-01-01'"`
}

type Album struct {
	AlbumId  int64 `gorm:"primaryKey:true;autoIncrement:false"`
	SingerId *int64 `gorm:"index:idx_albums_title,unique"`
	Singer   *Singer
	Title    string `gorm:"index:idx_albums_title,unique"`
}

type AllBasicTypes struct {
	Id        int64 `gorm:"primaryKey:true;autoIncrement:false"`
	Int64     int64
	Float64   float64
	Numeric   spanner.NullNumeric
	String    string
	Bytes     []byte
	Date      spanner.NullDate
	Timestamp time.Time
	Json      spanner.NullJSON
	Bool      bool
}

type AllSpannerNullableTypes struct {
	Id        int64 `gorm:"primaryKey:true;autoIncrement:false"`
	Int64     spanner.NullInt64
	Float64   spanner.NullFloat64
	Numeric   spanner.NullNumeric
	String    spanner.NullString
	Bytes     []byte
	Date      spanner.NullDate
	Timestamp spanner.NullTime
	Json      spanner.NullJSON
	Bool      spanner.NullBool
}
