# Node.js Spanner Wrapper Build and Release Architecture

This document describes the compilation pipeline, dual-publishing methodology, and native bridge linking process for the `spannerlib-node` driver.

## The 3-Layer Pipeline

Bridging JavaScript to the native Go SDK involves a sequential compilation pipeline mapping V8 types to Go-compatible memory pointers across three separate layers:

```
  [TypeScript Source] ---> (Babel/TSC) ---> [ESM & CJS JavaScript]
                                                    |
                                             (node-gyp bridge)
                                                    v
                                            [C++ Native Addon]
                                                    |
                                            (cgo linker bridge)
                                                    v
                                            [Go Shared Library]
```

## Compilation Phases

### Phase 1: Compiling the Go Shared Library (CGO Link)

Before building the Node.js Addon, the underlying Go codebase must be compiled into an object format that C/C++ can link against.
*   **Trigger:** Executed via `npm run build:go` which runs `bash scripts/build-shared-lib.sh`.
*   **Action:** The build script invokes the Go compiler with the `-buildmode=c-shared` flag, targeting the primary C-shared entry point located at [shared_lib.go](../../shared/shared_lib.go).
*   **Outputs:** Generates a platform-specific native shared library (e.g., `libspannerlib.dylib` on macOS, `.so` on Linux, `.dll` on Windows) along with the corresponding C header file (`libspannerlib.h`). Both files are placed into the `spannerlib/shared/` directory.

### Phase 2: Compiling the Native Bridge (node-gyp)

Once the Go shared library is generated, the Node.js C++ wrapper is compiled using `node-gyp` to map V8 engine objects into Go pointers.
*   **Trigger:** Executed as part of `npm run build` which invokes `node-gyp rebuild`.
*   **Action:** Reads the `gyp` build instructions in [binding.gyp](./binding.gyp) to locate the Go header files, and dynamically links the bridge against the generated Go shared object. It compiles the bridge source file [addon.cc](./src/cpp/addon.cc) using the local OS C++ compiler toolchain (e.g., Clang on macOS, GCC on Linux, MSVC on Windows).
*   **Output:** Generates the native Node.js binary file at `build/Release/spanner_napi.node`.
*   **Post-build Link Patch (macOS Only):** To ensure portability on macOS without requiring root or global library installs, `npm run postbuild` invokes the `install_name_tool`. This command alters the dynamic linker search path in the `.node` file to use `@loader_path/libspannerlib.dylib`, ensuring Node.js locates the Go dynamic library relatively from the compiled C++ bridge binary path.

### Phase 3: TypeScript Compilation & Dual-Publishing (ESM / CJS)

Finally, the TypeScript layer (which handles user-facing API classes, JavaScript's `FinalizationRegistry` garbage collection mapping, and Protobuf serialization) is compiled for consumer consumption. The build is configured to output both **ES Modules (ESM)** and **CommonJS (CJS)** simultaneously, ensuring compatibility across modern and legacy Node.js environments.
*   **Trigger:** Executed via `npm run compile`.
*   **Action:** 
    1.  **ESM Setup:** `npm run compile:esm` runs the standard `tsc` (TypeScript Compiler) using the primary [tsconfig.json](./tsconfig.json) file. Outputs ES6 modules into `build/esm/`.
    2.  **CJS Setup:** `npm run compile:cjs` compiles the codebase for legacy support using [tsconfig.cjs.json](./tsconfig.cjs.json) and pipes the JS output through `@babel/cli` to translate modern `import`/`export` keywords into standard CommonJS `require()` bindings.
    3.  **Extension Fixer:** Because Node.js requires explicit file extensions when using ESM but Babel strips them or expects `.cjs` in CommonJS modes, the post-compilation script `node scripts/fix-extensions.cjs` rewrites the internal import paths across the `build/cjs/` folder to use `.cjs` extensions.
*   **Output:** Generates two parallel directory trees under `build/esm` and `build/cjs`, making the distributed npm package natively dual-consumable using the `exports` configuration in `package.json`.

## End-to-End Local Builds

To run the entire pipeline end-to-end and generate a fully runnable local build, invoke the top-level script:
```bash
npm run build
```
This builds the underlying Go shared library, links the C++ bridge layer via `node-gyp`, patches dynamic linker paths, and outputs the final dual ESM/CJS JavaScript distributions.

## Platform-Specific Release Pipelines (GitHub Actions)

Releasing the prebuilt native platform packages is managed via a unified manual GitHub Actions workflow:
*   **Workflow:** [release-node-wrapper.yml](../../../.github/workflows/release-node-wrapper.yml)

### Target Platform Packages

| Package Name | Target Platform | Runner |
| :--- | :--- | :--- |
| **`@google-cloud/spannerlib-node-darwin-arm64`** | macOS (Apple Silicon `arm64`) | `macos-latest` |
| **`@google-cloud/spannerlib-node-linux-x64`** | Linux (`x64`) | `ubuntu-latest` |
| **`@google-cloud/spannerlib-node-linux-arm64`** | Linux (`arm64`) | `ubuntu-24.04-arm` |
| **`@google-cloud/spannerlib-node-win32-x64`** | Windows (`x64`) | `windows-latest` |

### Triggering a Release
The workflow uses `workflow_dispatch` and publishes to the Google Wombat registry (`https://wombat-dressing-room.appspot.com`):

1. Go to the **Actions** tab in GitHub.
2. Select **Build and Release Node Wrapper**.
3. Click **Run workflow**.
4. Provide the inputs:
   * **`platform`** *(Required)*: Select `all` (default) to build and release all 4 platforms concurrently, or select a specific target (`darwin-arm64`, `linux-x64`, `linux-arm64`, `win32-x64`).
   * **`npm_tag`** *(Optional, default: `alpha`)*: NPM distribution tag (e.g. `alpha`, `beta`, `latest`).
   * **`npm_token`** *(Optional)*: The authentication token for `//wombat-dressing-room.appspot.com/:_authToken`. If left empty, the workflow automatically runs in **dry-run mode** (builds, packages, and uploads `.tgz` artifacts without publishing).

### Release Execution Steps
For each target platform in the matrix, the workflow:
1. Sets up Go (`1.26.x`) and Node.js (`22`) environments.
2. Masks the supplied `npm_token` and configures `~/.npmrc` for Wombat registry auth.
3. Installs dependencies (`npm install`).
4. Compiles the Go shared library, links the C++ N-API addon, and compiles TypeScript dual outputs (`npm run build`).
5. Sets the target package name (e.g. `@google-cloud/spannerlib-node-linux-x64`) and `os`/`cpu` metadata in `package.json`.
6. Packages the distribution tarball (`npm pack .`) containing both the JavaScript bundles and the compiled native binaries (`spanner_napi.node` and `libspanner.*`).
7. Publishes the generated tarball to Wombat with the specified dist-tag (`npm publish --access=public --tag $NPM_TAG --registry=https://wombat-dressing-room.appspot.com`).
8. Archives and uploads the release `.tgz` as a workflow artifact for auditing and SBOM generation.


