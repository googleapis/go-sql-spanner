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

	_ "github.com/cloudspannerecosystem/go-sql-spanner"
	"gorm.io/gorm"
	"gorm.io/gorm/callbacks"
	"gorm.io/gorm/clause"
	"gorm.io/gorm/logger"
	"gorm.io/gorm/migrator"
	"gorm.io/gorm/schema"
)

type Config struct {
	DriverName string
	DSN        string
	Conn       gorm.ConnPool
}

type Dialector struct {
	*Config
}

func Open(dsn string) gorm.Dialector {
	return &Dialector{Config: &Config{DSN: dsn}}
}

func New(config Config) gorm.Dialector {
	return &Dialector{Config: &config}
}

func (dialector Dialector) Name() string {
	return "spanner"
}

func (dialector Dialector) Initialize(db *gorm.DB) (err error) {
	callbacks.RegisterDefaultCallbacks(db, &callbacks.Config{})
	if dialector.DriverName == "" {
		dialector.DriverName = "spanner"
	}
	// Register an UPDATE callback that will ensure that primary key columns are
	// never included in the SET clause of the statement.
	updateCallback := db.Callback().Update()
	if err := updateCallback.
		After("gorm:before_update").
		Before("gorm:update").
		Register("gorm:spanner:remove_primary_key_from_update", BeforeUpdate); err != nil {
		return err
	}

	if dialector.Conn != nil {
		db.ConnPool = dialector.Conn
	} else {
		db.ConnPool, err = sql.Open(dialector.DriverName, dialector.DSN)
		if err != nil {
			return err
		}
	}

	// Spanner DML does not support 'ON CONFLICT' clauses.
	db.ClauseBuilders[clause.OnConflict{}.Name()] = func(c clause.Clause, builder clause.Builder) {}

	return
}

func BeforeUpdate(db *gorm.DB) {
	// Omit all primary key fields from the SET clause of an UPDATE statement.
	db.Statement.Omit(db.Statement.Schema.PrimaryFieldDBNames...)
}

func (dialector Dialector) DefaultValueOf(field *schema.Field) clause.Expression {
	return clause.Expr{SQL: "NULL"}
}

func (dialector Dialector) Migrator(db *gorm.DB) gorm.Migrator {
	return Migrator{
		Migrator: migrator.Migrator{
			Config: migrator.Config{
				DB:        db,
				Dialector: dialector,
			},
		},
		Dialector: dialector,
	}
}

func (dialector Dialector) BindVarTo(writer clause.Writer, stmt *gorm.Statement, v interface{}) {
	writer.WriteString(fmt.Sprintf("@p%d", len(stmt.Vars)))
}

func (dialector Dialector) QuoteTo(writer clause.Writer, str string) {
	var (
		underQuoted, selfQuoted bool
		continuousBacktick      int8
		shiftDelimiter          int8
	)

	for _, v := range []byte(str) {
		switch v {
		case '`':
			continuousBacktick++
			if continuousBacktick == 2 {
				writer.WriteString("``")
				continuousBacktick = 0
			}
		case '.':
			if continuousBacktick > 0 || !selfQuoted {
				shiftDelimiter = 0
				underQuoted = false
				continuousBacktick = 0
				writer.WriteString("`")
			}
			writer.WriteByte(v)
			continue
		default:
			if shiftDelimiter-continuousBacktick <= 0 && !underQuoted {
				writer.WriteByte('`')
				underQuoted = true
				if selfQuoted = continuousBacktick > 0; selfQuoted {
					continuousBacktick -= 1
				}
			}

			for ; continuousBacktick > 0; continuousBacktick -= 1 {
				writer.WriteString("``")
			}

			writer.WriteByte(v)
		}
		shiftDelimiter++
	}

	if continuousBacktick > 0 && !selfQuoted {
		writer.WriteString("``")
	}
	writer.WriteString("`")
}

func (dialector Dialector) Explain(sql string, vars ...interface{}) string {
	return logger.ExplainSQL(sql, nil, `'`, vars...)
}

func (dialector Dialector) DataTypeOf(field *schema.Field) string {
	switch field.DataType {
	case schema.Bool:
		return "BOOL"
	case schema.Int, schema.Uint:
		return "INT64"
	case schema.Float:
		return "FLOAT64"
	case schema.String:
		var size string
		if field.Size == 0 || field.Size > 2621440 {
			size = "MAX"
		} else {
			size = fmt.Sprintf("%d", field.Size)
		}
		return fmt.Sprintf("STRING(%s)", size)
	case schema.Bytes:
		var size string
		if field.Size == 0 || field.Size > 10485760 {
			size = "MAX"
		} else {
			size = fmt.Sprintf("%d", field.Size)
		}
		return fmt.Sprintf("BYTES(%s)", size)
	case schema.Time:
		return "TIMESTAMP"
	}

	return string(field.DataType)
}
