go build -o grpc_server server.go
chmod +x grpc_server
cp grpc_server ../wrappers/spannerlib-java/src/main/resources/darwin-aarch64/grpc_server
