# Builds the gRPC server binary for darwin/arm64, linux/x64, and windows/x64
# and copies the binaries to the appropriate folders of the .NET wrapper.

./build-executables.sh

mkdir -p ../wrappers/spannerlib-dotnet/spannerlib-dotnet-grpc-server/binaries/any
rm ../wrappers/spannerlib-dotnet/spannerlib-dotnet-grpc-server/binaries/any/grpc_server 2> /dev/null

mkdir -p ../wrappers/spannerlib-dotnet/spannerlib-dotnet-grpc-server/binaries/osx-arm64
cp binaries/osx-arm64/grpc_server ../wrappers/spannerlib-dotnet/spannerlib-dotnet-grpc-server/binaries/osx-arm64/grpc_server

mkdir -p ../wrappers/spannerlib-dotnet/spannerlib-dotnet-grpc-server/binaries/linux-x64
cp binaries/linux-x64/grpc_server ../wrappers/spannerlib-dotnet/spannerlib-dotnet-grpc-server/binaries/linux-x64/grpc_server

mkdir -p ../wrappers/spannerlib-dotnet/spannerlib-dotnet-grpc-server/binaries/win-x64
cp binaries/win-x64/grpc_server.exe ../wrappers/spannerlib-dotnet/spannerlib-dotnet-grpc-server/binaries/win-x64/grpc_server.exe
