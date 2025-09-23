go build -o grpc_server server.go
chmod +x grpc_server
cp grpc_server ../wrappers/spannerlib-dotnet/spannerlib-dotnet-grpc/binaries/any/grpc_server
cp grpc_server ../wrappers/spannerlib-dotnet/spannerlib-dotnet-grpc/binaries/osx-arm64/grpc_server
