README.md
Regenerating protos
cd benchmarks/benchwrapper/proto
protoc --go_out=plugins=grpc:. *.proto