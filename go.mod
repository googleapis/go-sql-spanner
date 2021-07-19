module github.com/rakyll/go-sql-driver-spanner

go 1.14

require (
	cloud.google.com/go/spanner v1.2.1
	github.com/google/go-cmp v0.5.6 // indirect
	github.com/jinzhu/gorm v1.9.12
	google.golang.org/api v0.50.0
	google.golang.org/genproto v0.0.0-20210715145939-324b959e9c22
	google.golang.org/grpc v1.39.0
)

replace cloud.google.com/go/spanner v1.2.1 => /home/loite/go/src/cloud.google.com/spanner
