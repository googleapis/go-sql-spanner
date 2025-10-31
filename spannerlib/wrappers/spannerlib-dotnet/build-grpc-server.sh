# Builds the gRPC server binary for darwin/arm64, linux/x64, and windows/x64
# and copies the binaries to the appropriate folders of the .NET wrapper.

cd ../../grpc-server || exit 1
./build-executables.sh
cd ../wrappers/spannerlib-dotnet || exit 1

mkdir -p spannerlib-dotnet-grpc-server/binaries/any
rm spannerlib-dotnet-grpc-server/binaries/any/grpc_server 2> /dev/null

mkdir -p spannerlib-dotnet-grpc-server/binaries/osx-arm64
cp ../../grpc-server/binaries/osx-arm64/grpc_server spannerlib-dotnet-grpc-server/binaries/osx-arm64/grpc_server

mkdir -p spannerlib-dotnet-grpc-server/binaries/linux-x64
cp ../../grpc-server/binaries/linux-x64/grpc_server spannerlib-dotnet-grpc-server/binaries/linux-x64/grpc_server

mkdir -p spannerlib-dotnet-grpc-server/binaries/win-x64
cp ../../grpc-server/binaries/win-x64/grpc_server.exe spannerlib-dotnet-grpc-server/binaries/win-x64/grpc_server.exe
