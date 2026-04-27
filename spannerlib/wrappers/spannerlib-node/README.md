# Node-API Wrapper for Spanner Shared Library

This package provides a high-performance Node-API (N-API) bridge to the Go-based Spanner shared library. It offers superior stability and performance compared to traditional FFI approaches.

## Prerequisites

- Node.js >= 20.0.0
- Go compiler (to build the underlying shared library, if not pre-built)
- C++ toolchain (GCC/Clang or MSVC)

## Installation & Building

To build the native addon, run:

```bash
npm install
```

This will trigger `node-gyp` to compile the C++ bridge and link it with `libspanner.so`.

## Usage

```javascript
const { Pool, Connection } = require('spannerlib-node');

async function run() {
    const pool = new Pool('my-user-agent', 'projects/.../instances/.../databases/...');
    await pool.create();
    
    const conn = await pool.createConnection();
    const rows = await conn.executeSql('SELECT 1');
    
    while (await rows.next()) {
        // process rows
    }
    
    await rows.close();
    await conn.close();
    await pool.close();
}
```

## Architecture

The wrapper consists of:
1.  **`src/cpp/addon.cc`**: C++ Node-API bridge that handles thread boundaries and type conversions between V8 and C.
2.  **`src/ffi/utils.js`**: Helper functions to invoke native methods asynchronously using Promises.
3.  **`src/lib/`**: JavaScript classes (`Pool`, `Connection`, `Rows`) that provide a clean object-oriented interface.
