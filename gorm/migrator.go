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

package spannergorm

import (
	"database/sql"

	"gorm.io/gorm/migrator"
)

type Migrator struct {
	migrator.Migrator
	Dialector
}

type Column struct {
	name     string
	nullable sql.NullString
	datatype string
	maxLen   sql.NullInt64
}

func (c Column) Name() string {
	return c.name
}

func (c Column) DatabaseTypeName() string {
	return c.datatype
}

func (c Column) Length() (int64, bool) {
	if c.maxLen.Valid {
		return c.maxLen.Int64, c.maxLen.Valid
	}

	return 0, false
}

func (c Column) Nullable() (bool, bool) {
	if c.nullable.Valid {
		return c.nullable.String == "YES", true
	}

	return false, false
}

// DecimalSize return precision int64, scale int64, ok bool
func (c Column) DecimalSize() (int64, int64, bool) {
	if c.datatype == "NUMERIC" {
		return 38, 9, true
	}
	return 0, 0, false
}
