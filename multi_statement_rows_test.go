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
	"reflect"
	"testing"

	"cloud.google.com/go/spanner/admin/database/apiv1/databasepb"
	"github.com/googleapis/go-sql-spanner/connectionstate"
	"github.com/googleapis/go-sql-spanner/parser"
)

func TestDetermineStatementTypes(t *testing.T) {
	t.Parallel()

	p, _ := parser.NewStatementParser(databasepb.DatabaseDialect_GOOGLE_STANDARD_SQL, 1000)
	c := &conn{
		logger: noopLogger,
		parser: p,
		state:  createInitialConnectionState(connectionstate.TypeNonTransactional, map[string]connectionstate.ConnectionPropertyValue{}),
	}
	type test struct {
		name       string
		statements []string
		want       []*parser.StatementInfo
	}
	for _, test := range []test{
		{
			name:       "Query",
			statements: []string{"SELECT * FROM test"},
			want:       []*parser.StatementInfo{{StatementType: parser.StatementTypeQuery}},
		},
		{
			name:       "DML",
			statements: []string{"insert into my_table (id) values (1)"},
			want:       []*parser.StatementInfo{{StatementType: parser.StatementTypeDml, DmlType: parser.DmlTypeInsert}},
		},
		{
			name:       "DDL",
			statements: []string{"create table test (id int64 primary key)"},
			want:       []*parser.StatementInfo{{StatementType: parser.StatementTypeDdl}},
		},
		{
			name:       "begin",
			statements: []string{"begin transaction"},
			want:       []*parser.StatementInfo{{StatementType: parser.StatementTypeClientSide}},
		},
		{
			name:       "commit",
			statements: []string{"commit"},
			want:       []*parser.StatementInfo{{StatementType: parser.StatementTypeClientSide}},
		},
		{
			name:       "rollback",
			statements: []string{"rollback"},
			want:       []*parser.StatementInfo{{StatementType: parser.StatementTypeClientSide}},
		},
		{
			name:       "unknown",
			statements: []string{"gibberish stuff"},
			want:       []*parser.StatementInfo{{StatementType: parser.StatementTypeUnknown}},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			if g, w := determineStatementTypes(c, test.statements), test.want; !reflect.DeepEqual(g, w) {
				t.Errorf("statement type mismatch\n Got: %v\nWant: %v", g, w)
			}
		})
	}

}

func TestShouldBeginImplicitTransaction(t *testing.T) {
	t.Parallel()

	p, _ := parser.NewStatementParser(databasepb.DatabaseDialect_GOOGLE_STANDARD_SQL, 1000)
	c := &conn{
		logger: noopLogger,
		parser: p,
		state:  createInitialConnectionState(connectionstate.TypeNonTransactional, map[string]connectionstate.ConnectionPropertyValue{}),
	}
	type test struct {
		name         string
		index        int
		statements   []string
		wantReadOnly bool
		wantTxn      bool
	}
	for _, test := range []test{
		{
			name:         "Only queries",
			index:        0,
			statements:   []string{"select * from test1", "select * from test2"},
			wantReadOnly: true,
			wantTxn:      true,
		},
		{
			name:         "Only DML",
			index:        0,
			statements:   []string{"insert into test (id, value) values (1, 'One')", "insert into test (id, value) values (2, 'Two')"},
			wantReadOnly: false,
			wantTxn:      true,
		},
		{
			name:         "Only DDL",
			index:        0,
			statements:   []string{"create table my_table", "create table my_other_table"},
			wantReadOnly: false,
			wantTxn:      false,
		},
		{
			name:         "DML after query",
			index:        0,
			statements:   []string{"select * from test1", "insert into test (id, value) values (1, 'One')"},
			wantReadOnly: false,
			wantTxn:      true,
		},
		{
			name:         "Query after DML",
			index:        0,
			statements:   []string{"insert into test (id, value) values (1, 'One')", "select * from test1"},
			wantReadOnly: false,
			wantTxn:      true,
		},
		{
			name:         "DDL after two queries",
			index:        0,
			statements:   []string{"select * from test1", "select * from test2", "alter table some_table"},
			wantReadOnly: true,
			wantTxn:      true,
		},
		{
			name:         "DDL after one query",
			index:        0,
			statements:   []string{"select * from test1", "alter table some_table"},
			wantReadOnly: true,
			// There is no point in starting a transaction if it would consist of only one statement.
			wantTxn: false,
		},
		{
			name:         "Queries after commit",
			index:        2,
			statements:   []string{"insert into my_table (id) values (1)", "commit", "select * from test1", "select * from test2"},
			wantReadOnly: true,
			wantTxn:      true,
		},
		{
			name:         "DML before commit",
			index:        0,
			statements:   []string{"insert into my_table (id) values (1)", "commit", "update my_table set value='One' where id=1"},
			wantReadOnly: false,
			wantTxn:      true,
		},
		{
			name:         "DML as last statement",
			index:        2,
			statements:   []string{"insert into my_table (id) values (1)", "commit", "update my_table set value='One' where id=1"},
			wantReadOnly: false,
			// There is no point in starting a transaction if it would consist of only one statement.
			wantTxn: false,
		},
		{
			name:         "Queries before commit",
			index:        0,
			statements:   []string{"select * from test1", "select * from test2", "commit"},
			wantReadOnly: true,
			wantTxn:      true,
		},
		{
			name:         "Queries before rollback",
			index:        0,
			statements:   []string{"select * from test1", "select * from test2", "rollback"},
			wantReadOnly: true,
			wantTxn:      true,
		},
		{
			name:         "Queries before begin",
			index:        0,
			statements:   []string{"select * from test1", "select * from test2", "begin transaction"},
			wantReadOnly: false,
			wantTxn:      true,
		},
		{
			name:         "Ends with explicit transaction block",
			index:        0,
			statements:   []string{"select * from test1", "select * from test2", "begin transaction", "select * from test3", "select * from test4"},
			wantReadOnly: false,
			wantTxn:      true,
		},
		{
			name:         "Queries before transaction block with only queries",
			index:        0,
			statements:   []string{"select * from test1", "select * from test2", "begin transaction", "select * from test3", "select * from test4", "commit"},
			wantReadOnly: true,
			wantTxn:      true,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			statementInfo := determineStatementTypes(c, test.statements)
			if g, w := canUseReadOnlyTransaction(c, test.index, test.statements, statementInfo), test.wantReadOnly; g != w {
				t.Errorf("read-only mismatch\n Got: %v\nWant: %v", g, w)
			}
			if g, w := shouldStartImplicitTransaction(c, test.index, test.statements, statementInfo), test.wantTxn; g != w {
				t.Errorf("should start txn mismatch\n Got: %v\nWant: %v", g, w)
			}
		})
	}
}
