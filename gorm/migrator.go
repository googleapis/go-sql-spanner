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
	"fmt"
	"strings"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
	"gorm.io/gorm/migrator"
	"gorm.io/gorm/schema"
)

type SpannerMigrator interface {
	gorm.Migrator

	StartBatchDDL() error
	RunBatch() error
	AbortBatch() error
}

type spannerMigrator struct {
	migrator.Migrator
	Dialector
}

func (m spannerMigrator) StartBatchDDL() error {
	return m.DB.Exec("START BATCH DDL").Error
}

func (m spannerMigrator) RunBatch() error {
	return m.DB.Exec("RUN BATCH").Error
}

func (m spannerMigrator) AbortBatch() error {
	return m.DB.Exec("ABORT BATCH").Error
}

func (m spannerMigrator) CreateTable(values ...interface{}) error {
	for _, value := range m.ReorderModels(values, false) {
		tx := m.DB.Session(&gorm.Session{})
		if err := m.RunWithValue(value, func(stmt *gorm.Statement) (errr error) {
			var (
				createTableSQL          = "CREATE TABLE ? ("
				values                  = []interface{}{m.CurrentTable(stmt)}
				hasPrimaryKeyInDataType bool
			)

			for _, dbName := range stmt.Schema.DBNames {
				field := stmt.Schema.FieldsByDBName[dbName]
				if !field.IgnoreMigration {
					createTableSQL += "? ?"
					hasPrimaryKeyInDataType = hasPrimaryKeyInDataType || strings.Contains(strings.ToUpper(string(field.DataType)), "PRIMARY KEY")
					values = append(values, clause.Column{Name: dbName}, m.DB.Migrator().FullDataTypeOf(field))
					createTableSQL += ","
				}
			}

			// Indexes should always be created after the table, as Spanner does not support
			// inline index creation.
			for _, idx := range stmt.Schema.ParseIndexes() {
				defer func(value interface{}, name string) {
					if errr == nil {
						errr = tx.Migrator().CreateIndex(value, name)
					}
				}(value, idx.Name)
			}

			for _, rel := range stmt.Schema.Relationships.Relations {
				if !m.DB.DisableForeignKeyConstraintWhenMigrating {
					if constraint := rel.ParseConstraint(); constraint != nil {
						if constraint.Schema == stmt.Schema {
							sql, vars := buildConstraint(constraint)
							createTableSQL += sql + ","
							values = append(values, vars...)
						}
					}
				}
			}

			for _, chk := range stmt.Schema.ParseCheckConstraints() {
				createTableSQL += "CONSTRAINT ? CHECK (?),"
				values = append(values, clause.Column{Name: chk.Name}, clause.Expr{SQL: chk.Constraint})
			}

			createTableSQL = strings.TrimSuffix(createTableSQL, ",")

			createTableSQL += ")"

			if !hasPrimaryKeyInDataType && len(stmt.Schema.PrimaryFields) > 0 {
				createTableSQL += " PRIMARY KEY ?"
				primaryKeys := []interface{}{}
				for _, field := range stmt.Schema.PrimaryFields {
					primaryKeys = append(primaryKeys, clause.Column{Name: field.DBName})
				}

				values = append(values, primaryKeys)
			}

			if tableOption, ok := m.DB.Get("gorm:table_options"); ok {
				createTableSQL += fmt.Sprint(tableOption)
			}

			errr = tx.Exec(createTableSQL, values...).Error
			return errr
		}); err != nil {
			return err
		}
	}
	return nil
}

func buildConstraint(constraint *schema.Constraint) (sql string, results []interface{}) {
	sql = "CONSTRAINT ? FOREIGN KEY ? REFERENCES ??"
	if constraint.OnDelete != "" {
		sql += " ON DELETE " + constraint.OnDelete
	}

	if constraint.OnUpdate != "" {
		sql += " ON UPDATE " + constraint.OnUpdate
	}

	var foreignKeys, references []interface{}
	for _, field := range constraint.ForeignKeys {
		foreignKeys = append(foreignKeys, clause.Column{Name: field.DBName})
	}

	for _, field := range constraint.References {
		references = append(references, clause.Column{Name: field.DBName})
	}
	results = append(results, clause.Table{Name: constraint.Name}, foreignKeys, clause.Table{Name: constraint.ReferenceSchema.Table}, references)
	return
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
