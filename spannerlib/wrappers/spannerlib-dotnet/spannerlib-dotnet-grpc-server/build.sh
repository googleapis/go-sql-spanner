# Builds the gRPC server and the binary .NET wrapper and install the latter in a local nuget repository.

# Determine OS + Arch
export DEST=binaries/any/grpc_server
mkdir -p binaries/any
mkdir -p binaries/osx-arm64

# Clear all local nuget cache
dotnet nuget locals --clear all
go build -o ../../../grpc-server/grpc_server ../../../grpc-server/server.go
cp ../../../grpc-server/grpc_server $DEST
cp ../../../grpc-server/grpc_server binaries/osx-arm64/grpc_server
dotnet pack
dotnet nuget remove source local_grpc_server 2>/dev/null
dotnet nuget add source "$PWD"/bin/Release --name local_grpc_server
dotnet restore ../spannerlib-dotnet-grpc-impl
