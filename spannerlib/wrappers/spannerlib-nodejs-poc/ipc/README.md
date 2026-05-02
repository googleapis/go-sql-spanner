# Spanner NodeJS gRPC/IPC Wrapper

This directory contains a proof-of-concept for a NodeJS wrapper for the Go Spanner library that uses IPC via a gRPC client to communicate with a Go gRPC server.

## Architecture

The wrapper consists of three main parts:

1.  **Go gRPC Server**: A server written in Go that exposes the Spanner library's functionality over a gRPC interface. This server is located in `spannerlib/grpc-server`. It must be running for this wrapper to function.

2.  **Node.js gRPC Client**: A client written in Node.js that communicates with the Go gRPC server. It uses the `@grpc/grpc-js` and `@grpc/proto-loader` packages to interact with the server. The client is defined in `src/grpc/`.

3.  **JavaScript Wrapper**: A high-level JavaScript API that provides `Pool`, `Connection`, and `Rows` classes. This abstracts away the gRPC communication and provides a clean, async/await-based interface that is identical to the Koffi and N-API POCs.

## How to Run

1.  **Start the Go gRPC server**:
    ```sh
    cd spannerlib/grpc-server
    go run server.go localhost:50051 tcp
    ```

2.  **Install Node.js dependencies**:
    ```sh
    cd spannerlib/wrappers/spannerlib-nodejs-poc/ipc
    npm install
    ```

3.  **Run the test**:
    ```sh
    npm test
    ```
