module spannergorm

go 1.14

replace github.com/cloudspannerecosystem/go-sql-spanner => ../

require (
	cloud.google.com/go v0.95.0
	cloud.google.com/go/spanner v1.25.0
	github.com/cloudspannerecosystem/go-sql-spanner v0.0.0-00010101000000-000000000000
	github.com/golang/protobuf v1.5.2
	google.golang.org/api v0.57.0
	google.golang.org/genproto v0.0.0-20210921142501-181ce0d877f6
	gorm.io/gorm v1.21.13
)

replace cloud.google.com/go/spanner => ../../google-cloud-go/spanner
