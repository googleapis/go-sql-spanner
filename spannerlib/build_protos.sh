protoc \
  --go_out=/Users/loite/GolandProjects/go-sql-spanner/spannerlib/ \
  --go_opt=paths=source_relative \
  --go-grpc_out=/Users/loite/GolandProjects/go-sql-spanner/spannerlib/ \
  --go-grpc_opt=paths=source_relative \
  google/spannerlib/v1/spannerlib.proto
