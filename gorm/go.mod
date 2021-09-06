module spannergorm

go 1.14

replace github.com/cloudspannerecosystem/go-sql-spanner => ../

require (
	cloud.google.com/go v0.93.3
	cloud.google.com/go/spanner v1.25.0
	github.com/cloudspannerecosystem/go-sql-spanner v0.0.0-00010101000000-000000000000
	github.com/golang/protobuf v1.5.2
	google.golang.org/api v0.54.0
	google.golang.org/genproto v0.0.0-20210821163610-241b8fcbd6c8
	gorm.io/gorm v1.21.13
)
