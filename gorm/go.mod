module spannergorm

go 1.14

replace github.com/cloudspannerecosystem/go-sql-spanner => ../

require (
	cloud.google.com/go v0.88.0 // indirect
	cloud.google.com/go/spanner v1.23.1-0.20210727075241-3d6c6c7873e1
	github.com/cloudspannerecosystem/go-sql-spanner v0.0.0-00010101000000-000000000000
	github.com/golang/protobuf v1.5.2
	google.golang.org/api v0.51.0 // indirect
	google.golang.org/genproto v0.0.0-20210726143408-b02e89920bf0
	gorm.io/gorm v1.21.13
)
