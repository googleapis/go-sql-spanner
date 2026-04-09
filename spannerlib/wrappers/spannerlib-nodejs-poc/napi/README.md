# N-API POC for the Real Spanner Shared Library

This directory contains a Proof of Concept (POC) demonstrating how to use Node.js N-API to build a C++ addon that interacts **directly** with the compiled `go-sql-spanner` C-Shared Library.

By using N-API, we create a high-performance bridge between Node.js and the native Go driver. This involves writing a thin C++ wrapper that is compiled into a `.node` binary.

## Architecture

The wrapper consists of three main parts:

1.  **Go Shared Library (`libspanner.so`)**: The core Go database driver, compiled into a standard C-style shared library.
2.  **C++ Addon (`spanner_napi.node`)**: A C++ layer written using the `node-addon-api` package. It loads `libspanner.so` and exposes its functions to the Node.js runtime. All asynchronous calls are handled using `Napi::AsyncWorker` to prevent blocking the Node.js event loop.
3.  **JavaScript Wrapper**: A high-level JavaScript API that provides `Pool`, `Connection`, and `Rows` classes. This abstracts away the C++ addon and provides a clean, async/await-based interface to the end-user.

## How to Build and Run

### Step 1: Install Dependencies
This step will download the necessary Node.js packages and automatically compile the C++ addon using `node-gyp`.

```bash
# From within the 'napi' directory
npm install
```

The `npm install` command triggers the `build` script in `package.json`, which performs several critical actions:
- It runs `node-gyp rebuild` to compile the C++ source in `src/cpp/addon.cc` into `build/Release/spanner_napi.node`.
- The `binding.gyp` file copies `libspanner.so` into the `build/Release` directory.
- The `build` script then runs `install_name_tool` to permanently link `spanner_napi.node` to `libspanner.so`, making the addon self-contained and portable on macOS without requiring `DYLD_LIBRARY_PATH`.

### Step 2: Set Up the Test Database
The test script requires a Cloud Spanner database. A setup script is provided to create a temporary one for you.

```bash
# This will create a new database and write its name to 'test-db.txt'
npm run setup
```

### Step 3: Run the Test
Execute the test script to verify the full workflow. This will use the compiled addon to connect to Spanner and run a query.

```bash
npm test
```

## Key Findings & Advantages
- **High Performance:** N-API provides a direct, high-performance bridge between the Node.js V8 engine and the native Go code.
- **Robust Asynchronous Operations:** By using `Napi::AsyncWorker`, all blocking I/O (database calls) happens on a background thread, keeping the Node.js event loop free.
- **Self-Contained Binary:** The build process correctly bundles the addon and its `.so` dependency, creating a portable binary that does not require environment variables to run.
