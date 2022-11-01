Benchwrapper
A small gRPC wrapper around the Golang's spanner driver. This allows the benchmarking code to prod at spanner without speaking Go.

Running
cd benchmarks/benchwrapper
export SPANNER_EMULATOR_HOST=localhost:9010
go run *.go --port=8081