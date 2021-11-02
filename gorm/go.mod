module spannergorm

go 1.14

replace github.com/cloudspannerecosystem/go-sql-spanner => ../

require (
	cloud.google.com/go v0.97.0
	cloud.google.com/go/spanner v1.27.0
	github.com/cloudspannerecosystem/go-sql-spanner v0.0.0-00010101000000-000000000000
	github.com/golang/protobuf v1.5.2
	github.com/google/go-cmp v0.5.6
	google.golang.org/api v0.59.0
	google.golang.org/genproto v0.0.0-20211020151524-b7c3a969101a
	google.golang.org/grpc v1.40.0 // indirect
	gorm.io/gorm v1.21.15
)
