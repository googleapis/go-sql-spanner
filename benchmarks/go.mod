module github.com/googleapis/go-sql-spanner/benchmarks

go 1.14

replace github.com/googleapis/go-sql-spanner => ../

require (
	cloud.google.com/go v0.100.2
	cloud.google.com/go/spanner v1.32.0
	github.com/googleapis/go-sql-spanner v0.0.0-00010101000000-000000000000
	github.com/google/uuid v1.3.0
	google.golang.org/api v0.68.0
	google.golang.org/genproto 65c12eb4c068
	google.golang.org/grpc v1.44.0
)
