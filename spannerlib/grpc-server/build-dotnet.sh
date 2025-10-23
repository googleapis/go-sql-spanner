# Builds the gRPC server binary for darwin/arm64, linux/x64, and windows/x64
# and copies the binaries to the appropriate folders of the .NET wrapper.
mkdir -p ../wrappers/spannerlib-dotnet/spannerlib-dotnet-grpc-server/binaries/any
rm ../wrappers/spannerlib-dotnet/spannerlib-dotnet-grpc-server/binaries/any/grpc_server

GOOS=darwin GOARCH=arm64 go build -o grpc_server server.go
chmod +x grpc_server
mkdir -p ../wrappers/spannerlib-dotnet/spannerlib-dotnet-grpc-server/binaries/osx-arm64
cp grpc_server ../wrappers/spannerlib-dotnet/spannerlib-dotnet-grpc-server/binaries/osx-arm64/grpc_server

GOOS=linux GOARCH=amd64 go build -o grpc_server server.go
chmod +x grpc_server
mkdir -p ../wrappers/spannerlib-dotnet/spannerlib-dotnet-grpc-server/binaries/linux-x64
cp grpc_server ../wrappers/spannerlib-dotnet/spannerlib-dotnet-grpc-server/binaries/linux-x64/grpc_server

GOOS=windows GOARCH=amd64 go build -o grpc_server.exe server.go
chmod +x grpc_server.exe
mkdir -p ../wrappers/spannerlib-dotnet/spannerlib-dotnet-grpc-server/binaries/win-x64
cp grpc_server.exe ../wrappers/spannerlib-dotnet/spannerlib-dotnet-grpc-server/binaries/win-x64/grpc_server.exe
