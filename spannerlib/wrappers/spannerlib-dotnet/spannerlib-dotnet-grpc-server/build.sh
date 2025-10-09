# Builds the gRPC server and the binary .NET wrapper and install the latter in a local nuget repository.

# Determine OS + Arch
mkdir -p binaries/any
mkdir -p binaries/osx-arm64
mkdir -p binaries/linux-x64

# Clear all local nuget cache
rm binaries/any/grpc_server
dotnet nuget locals --clear all
GOOS=darwin GOARCH=arm64 go build -o ../../../grpc-server/grpc_server ../../../grpc-server/server.go
cp ../../../grpc-server/grpc_server binaries/osx-arm64/grpc_server
GOOS=linux GOARCH=amd64 go build -o ../../../grpc-server/grpc_server ../../../grpc-server/server.go
cp ../../../grpc-server/grpc_server binaries/linux-x64/grpc_server

dotnet pack
dotnet nuget remove source local_grpc_server 2>/dev/null
dotnet nuget add source "$PWD"/bin/Release --name local_grpc_server
dotnet restore ../spannerlib-dotnet-grpc-impl
