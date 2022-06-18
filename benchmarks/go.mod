module github.com/googleapis/go-sql-spanner/benchmarks

go 1.14

replace github.com/googleapis/go-sql-spanner => ../

require (
	cloud.google.com/go v0.102.1
	cloud.google.com/go/spanner v1.29.0
	github.com/google/uuid v1.3.0
	github.com/googleapis/go-sql-spanner v0.0.0-00010101000000-000000000000
	google.golang.org/api v0.84.0
	google.golang.org/genproto v0.0.0-20220617124728-180714bec0ad
	google.golang.org/grpc v1.47.0
)
