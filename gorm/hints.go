package spannergorm

import (
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

type Exprs []clause.Expression

func (exprs Exprs) Build(builder clause.Builder) {
	for idx, expr := range exprs {
		if idx > 0 {
			builder.WriteByte(' ')
		}
		expr.Build(builder)
	}
}

type IndexHint struct {
	Type string
	Key  string
}

func (indexHint IndexHint) ModifyStatement(stmt *gorm.Statement) {
	clause := stmt.Clauses["FROM"]

	if clause.AfterExpression == nil {
		clause.AfterExpression = indexHint
	} else {
		clause.AfterExpression = Exprs{clause.AfterExpression, indexHint}
	}

	stmt.Clauses["FROM"] = clause
}

func (indexHint IndexHint) Build(builder clause.Builder) {
	if indexHint.Key != "" {
		builder.WriteString("@{")
		builder.WriteString(indexHint.Type)
		builder.WriteQuoted(indexHint.Key)
		builder.WriteByte('}')
	}
}

func ForceIndex(name string) IndexHint {
	return IndexHint{Type: "FORCE_INDEX=", Key: name}
}
