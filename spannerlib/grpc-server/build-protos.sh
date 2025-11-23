PATH="${PATH}:${HOME}/go/bin"
git submodule add git@github.com:googleapis/googleapis.git
ln -sf "${PWD}"/google/spannerlib googleapis/google/spannerlib
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

# dotnet add package Grpc.Tools --version 2.76.0
protoc \
  --csharp_out=../../wrappers/spannerlib-dotnet/spannerlib-dotnet-grpc-v1/ \
  --plugin=protoc-gen-csharp_grpc=/Users/loite/.nuget/packages/grpc.tools/2.76.0/tools/macosx_x64/grpc_csharp_plugin \
  --csharp_grpc_out=../../wrappers/spannerlib-dotnet/spannerlib-dotnet-grpc-v1/ \
  --csharp_opt=file_extension=.g.cs \
  --csharp_grpc_opt=no_server \
  --proto_path=. \
  google/spannerlib/v1/spannerlib.proto
cd .. || exit 1
rm googleapis/google/spannerlib
git rm googleapis -f
rm ../../.gitmodules
