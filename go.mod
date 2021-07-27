module github.com/rakyll/go-sql-driver-spanner

go 1.14

require (
	cloud.google.com/go v0.87.0
	cloud.google.com/go/spanner v1.2.1
	github.com/golang/protobuf v1.5.2
	github.com/google/go-cmp v0.5.6 // indirect
	github.com/jinzhu/gorm v1.9.12
	google.golang.org/api v0.50.0
	google.golang.org/genproto v0.0.0-20210719143636-1d5a45f8e492
	google.golang.org/grpc v1.39.0
)

replace cloud.google.com/go/spanner v1.2.1 => /Users/loite/go/src/google-cloud-go/spanner
