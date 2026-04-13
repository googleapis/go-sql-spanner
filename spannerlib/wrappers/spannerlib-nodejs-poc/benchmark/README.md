# Spanner Node.js Wrappers Benchmark

This directory contains a benchmark suite to compare the performance, throughput, and memory usage of three different Node.js wrappers for the Spanner Shared C-Library:
1.  **Koffi** (Fast FFI)
2.  **N-API** (Native C++ Addon)
3.  **IPC** (gRPC Server/Client)

## Prerequisites
Ensure you have the following installed on the machine:
*   **Node.js** (v22 recommended)
*   **Go** (v1.22 recommended)
*   **Build Tools** (`gcc`, `g++`, `make`)

---

## Setup Instructions

### 1. Fetch Google API Protos (For IPC)
The IPC wrapper depends on standard Google API protos. You can fetch them by running the build script in the `grpc-server` directory:

```bash
cd spannerlib/grpc-server
./build-protos.sh
```
*(Note: This script also attempts to generate Go code using `protoc`. If you do not have `protoc` installed, it may show an error on the generation step, but it will still have successfully cloned the `googleapis` folder that Node.js needs).*

### 2. Build the Go Shared Library
Navigate to the shared library directory and build the `.so` file:
```bash
cd spannerlib/shared
go build -o libspanner.so -buildmode=c-shared shared_lib.go
```

### 3. Install Dependencies for Wrappers
Navigate to each wrapper directory and install its specific dependencies:

**Koffi:**
```bash
cd spannerlib/wrappers/spannerlib-nodejs-poc/koffi
npm install
```

**N-API:**
```bash
cd spannerlib/wrappers/spannerlib-nodejs-poc/napi
npm install # This will also automatically build the C++ addon
```

**IPC:**
```bash
cd spannerlib/wrappers/spannerlib-nodejs-poc/ipc
npm install
```

### 4. Start the gRPC Server (Required for IPC)
The IPC wrapper requires the Go gRPC server to be running. 
Navigate to `spannerlib/grpc-server` and run it in the background (or in a `tmux` session):
```bash
cd spannerlib/grpc-server
nohup go run server.go localhost:50051 tcp > server.log 2>&1 &
```

---

## Running the Benchmark

Once all setups are complete, navigate to this directory:

```bash
cd spannerlib/wrappers/spannerlib-nodejs-poc/benchmark
npm install
node --expose-gc benchmark.js
```

## Results
The script will generate a `RESULTS.md` file in this directory containing:
*   Latency statistics from `mitata`.
*   Throughput (ops/sec) and Heap memory delta for 5 runs of each wrapper.
*   A 1-minute cooldown sleep is automatically applied between wrappers to ensure DB cooling and isolation.
