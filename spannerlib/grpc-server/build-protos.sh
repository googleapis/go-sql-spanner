#!/bin/bash

PATH="${PATH}:${HOME}/go/bin"

# 1. Clean up and fetch dependencies
# We DO NOT delete googleapis at the end because Node.js needs it at runtime
if [ ! -d "googleapis" ]; then
    echo "Fetching dependency protos..."
    git clone --depth 1 https://github.com/googleapis/googleapis.git
fi

# Link local proto into the structure
mkdir -p googleapis/google/spannerlib/v1
ln -sf "${PWD}/google/spannerlib/v1/spannerlib.proto" "googleapis/google/spannerlib/v1/spannerlib.proto"

cd googleapis || exit 1

# 2. Go generation
echo "Generating Go..."
protoc \
  --go_out=../ \
  --go_opt=paths=source_relative \
  --go-grpc_out=../ \
  --go-grpc_opt=paths=source_relative \
  google/spannerlib/v1/spannerlib.proto

# 3. Java generation
echo "Generating Java..."
protoc \
  --java_out=../../wrappers/spannerlib-java/src/main/java/ \
  --plugin=protoc-gen-java-grpc=/Users/loite/protoc-gen-grpc-java-1.75.0-osx-aarch_64.exe \
  --java-grpc_out=../../wrappers/spannerlib-java/src/main/java/ \
  --java-grpc_opt=paths=source_relative \
  google/spannerlib/v1/spannerlib.proto

# 4. C# generation
echo "Generating C#..."
protoc \
  --csharp_out=../../wrappers/spannerlib-dotnet/spannerlib-dotnet-grpc-v1/ \
  --plugin=protoc-gen-csharp_grpc=/Users/loite/.nuget/packages/grpc.tools/2.76.0/tools/macosx_x64/grpc_csharp_plugin \
  --csharp_grpc_out=../../wrappers/spannerlib-dotnet/spannerlib-dotnet-grpc-v1/ \
  --csharp_opt=file_extension=.g.cs \
  --csharp_grpc_opt=no_server \
  --proto_path=. \
  google/spannerlib/v1/spannerlib.proto

cd .. || exit 1
echo "Code generation complete for Go, Java, and C#."
echo "Node.js will use dynamic loading via @grpc/proto-loader."
