# Builds the socket server binary for darwin/arm64, linux/x64, and windows/x64.
# The binaries are stored in the following files:
# binaries/osx-arm64/spannerlib_socket_server
# binaries/linux-x64/spannerlib_socket_server
# binaries/win-x64/spannerlib_socket_server.exe

mkdir -p binaries/osx-arm64
GOOS=darwin GOARCH=arm64 go build -o binaries/osx-arm64/spannerlib_socket_server server.go
chmod +x binaries/osx-arm64/spannerlib_socket_server

mkdir -p binaries/linux-x64
GOOS=linux GOARCH=amd64 go build -o binaries/linux-x64/spannerlib_socket_server server.go
chmod +x binaries/linux-x64/spannerlib_socket_server

mkdir -p binaries/win-x64
GOOS=windows GOARCH=amd64 go build -o binaries/win-x64/spannerlib_socket_server.exe server.go
chmod +x binaries/win-x64/spannerlib_socket_server.exe
