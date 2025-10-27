# Builds the gRPC server binary for darwin/arm64, linux/x64, and windows/x64.
# The binaries are stored in the following files:
# binaries/osx-arm64/grpc_server
# binaries/linux-x64/grpc_server
# binaries/win-x64/grpc_server.exe

mkdir -p binaries/osx-arm64
GOOS=darwin GOARCH=arm64 go build -o binaries/osx-arm64/grpc_server server.go
chmod +x binaries/osx-arm64/grpc_server

mkdir -p binaries/linux-x64
GOOS=linux GOARCH=amd64 go build -o binaries/linux-x64/grpc_server server.go
chmod +x binaries/linux-x64/grpc_server

mkdir -p binaries/win-x64
GOOS=windows GOARCH=amd64 go build -o binaries/win-x64/grpc_server.exe server.go
chmod +x binaries/win-x64/grpc_server.exe
