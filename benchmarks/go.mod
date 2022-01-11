module github.com/googleapis/go-sql-spanner/benchmarks

go 1.14

replace github.com/googleapis/go-sql-spanner => ../

require (
	cloud.google.com/go v0.93.3
	cloud.google.com/go/spanner v1.25.0
	github.com/googleapis/go-sql-spanner v0.0.0-00010101000000-000000000000
	github.com/google/uuid v1.1.2
	google.golang.org/api v0.54.0
	google.golang.org/genproto v0.0.0-20210821163610-241b8fcbd6c8
	google.golang.org/grpc v1.40.0
)
