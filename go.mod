module github.com/cloudspannerecosystem/go-sql-spanner

go 1.14

require (
	cloud.google.com/go v0.97.0
	cloud.google.com/go/spanner v1.25.0
	github.com/golang/protobuf v1.5.2
	github.com/google/go-cmp v0.5.6
	google.golang.org/api v0.58.0
	google.golang.org/genproto v0.0.0-20210924002016-3dee208752a0
	google.golang.org/grpc v1.40.0
	google.golang.org/protobuf v1.27.1
)

replace cloud.google.com/go/spanner => ../google-cloud-go/spanner
