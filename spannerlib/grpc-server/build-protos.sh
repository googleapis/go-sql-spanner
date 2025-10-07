PATH="${PATH}:${HOME}/go/bin"
rm -rf googleapis/google/spannerlib || true
cp -r google/spannerlib googleapis/google/spannerlib
cd googleapis || exit 1
protoc \
  --go_out=../ \
  --go_opt=paths=source_relative \
  --go-grpc_out=../ \
  --go-grpc_opt=paths=source_relative \
  google/spannerlib/v1/spannerlib.proto
protoc \
  --java_out=../../wrappers/spannerlib-java/src/main/java/ \
  --plugin=protoc-gen-java-grpc=/Users/loite/protoc-gen-grpc-java-1.75.0-osx-aarch_64.exe \
  --java-grpc_out=../../wrappers/spannerlib-java/src/main/java/ \
  --java-grpc_opt=paths=source_relative \
  google/spannerlib/v1/spannerlib.proto
cd .. || exit 1
rm -rf googleapis/google/spannerlib
