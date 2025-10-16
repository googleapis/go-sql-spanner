go build -o grpc_server server.go
chmod +x grpc_server
mkdir -p ../wrappers/spannerlib-java/src/main/resources/darwin-aarch64
cp grpc_server ../wrappers/spannerlib-java/src/main/resources/darwin-aarch64/grpc_server
