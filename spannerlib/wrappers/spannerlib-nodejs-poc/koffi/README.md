# Koffi POC for the Real Spanner Shared Library

This directory contains a Proof of Concept (POC) demonstrating how to use [Koffi](https://koffi.dev/)—a fast and lightweight pure JavaScript Foreign Function Interface (FFI)—to interact **directly** with the compiled `go-sql-spanner` C-Shared Library. 

By using Koffi, we can bridge Node.js directly to the Go Driver without writing a single line of C/C++ intermediary code.

## Step-by-Step Implementation

### Step 1: Compile the Go Shared Library (`.so` / `.dylib`)
Instead of using a mock C file, this POC connects to the actual Go codebase. First, the Go code must be exportable as a C-compatible shared library. We achieved this by navigating to `spannerlib/shared` and running:
```bash
go build -buildmode=c-shared -o libspanner.so shared_lib.go
```
*This produces `libspanner.so` (the compiled binary) and `libspanner.h` (the C-Header definitions of your exported Go functions).*

### Step 2: Initialize Koffi in Node.js
We initialized an isolated npm workspace for the Koffi POC and installed the dependency:
```bash
npm install koffi
```

### Step 3: Map Go C-ABI Types to Koffi (`index.js`)
Go's CGO engine translates complex types into C-structs in `libspanner.h`. We used Koffi's `struct` mappings to perfectly mirror Go's memory layouts inside JavaScript:

- **`GoString`**: Go handles C-strings as a struct containing a pointer and a length.
  ```javascript
  const GoString = koffi.struct('GoString', { p: 'string', n: 'size_t' });
  ```
- **`GoSlice`**: Byte slices (used for passing protobuf payloads like SQL Statements) are mapped as:
  ```javascript
  const GoSlice = koffi.struct('GoSlice', { data: 'void*', len: 'int64', cap: 'int64' });
  ```
- **`GoReturnTuple`**: Go functions that return multiple arguments (e.g., `(int64, int32, int64, int32, unsafe.Pointer)`) are compiled by CGO into a single unified C-struct. We mapped this 5-element struct directly in Koffi to easily extract `objectId`, `statusCode`, and `resPointer`.

### Step 4: Define FFI Bindings
We pointed Koffi to `libspanner.so` and registered your exported Go functions natively:
```javascript
const CreatePool = lib.func('CreatePool', GoReturnTuple, [GoString, GoString]);
const Execute = lib.func('Execute', GoReturnTuple, ['int64', 'int64', GoSlice]);
```

### Step 5: Handling Responses & Extracting Memory (`resPointer`)
When the Go driver throws an error or returns protobuf data, it packs bytes into the `resPointer` (returned in `tuple.r4`) and the length in `tuple.r3`. We used Koffi to safely decode that raw memory straight into a Node.js `Buffer`:
```javascript
const bytesArray = koffi.decode(result.r4, 'uint8', result.r3);
const decodedMessage = Buffer.from(bytesArray).toString('utf8');
```
This allowed the JavaScript wrapper to natively throw the exact inner Go error: `Spanner Native Error (Code 3): Invalid CreateSession request.`

### Step 6: Integration Test (`test.js`)
We provided a test script (`node test.js`) to demonstrate the binding. It initializes a Go Session Pool, requests a simulated FFI connection, fires an empty execute payload, and gracefully frees the Go pointer memory (`Release`).

## Key Findings & Advantages
- **No C++ Toolchain (`node-gyp`) Required:** Koffi avoids the complexity of compiling N-API bindings.
- **Perfect Go Memory Introspection:** Koffi natively reads Go's internal `uint8` byte streams (`koffi.decode`), making Protobuf decoding exceptionally fast and seamless.
- **Idiomatic Result Handling:** By mapping Go's C-Tuple returns, the JavaScript developer sees a standard Object-Oriented client interface (`client.createPool(...)`) while Koffi handles all the strict struct-bridging to Go underneath.
