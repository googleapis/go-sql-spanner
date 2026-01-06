# Builds the socket server binary for darwin/arm64, linux/x64, and windows/x64
# and copies the binaries to the appropriate folders of the .NET wrapper.

cd ../../socket-server || exit 1
./build-executables.sh
cd ../wrappers/spannerlib-dotnet || exit 1

mkdir -p spannerlib-dotnet-socket-server/binaries/any
rm spannerlib-dotnet-socket-server/binaries/any/spannerlib_socket_server 2> /dev/null

mkdir -p spannerlib-dotnet-socket-server/binaries/osx-arm64
cp ../../socket-server/binaries/osx-arm64/spannerlib_socket_server spannerlib-dotnet-socket-server/binaries/osx-arm64/spannerlib_socket_server

mkdir -p spannerlib-dotnet-socket-server/binaries/linux-x64
cp ../../socket-server/binaries/linux-x64/spannerlib_socket_server spannerlib-dotnet-socket-server/binaries/linux-x64/spannerlib_socket_server

mkdir -p spannerlib-dotnet-socket-server/binaries/win-x64
cp ../../socket-server/binaries/win-x64/spannerlib_socket_server.exe spannerlib-dotnet-socket-server/binaries/win-x64/spannerlib_socket_server.exe
