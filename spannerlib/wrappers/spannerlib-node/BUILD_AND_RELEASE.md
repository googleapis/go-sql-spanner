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
                                       [Go Static Archive / DLL]
```

## Compilation Phases

### Phase 1: Compiling the Go Library (CGO Link)

Before building the Node.js Addon, the underlying Go codebase must be compiled into an object format that C/C++ can link against.
*   **Trigger:** Executed via `npm run build:go` which runs `bash scripts/build-shared-lib.sh`.
*   **Action & Outputs:**
    *   **Linux & macOS (`-buildmode=c-archive`):** Invokes Go with `-buildmode=c-archive` targeting [shared_lib.go](../../shared/shared_lib.go) to generate a static archive (`libspanner.a`) and C header (`libspanner.h`). The static archive embeds all Go runtime and driver symbols directly into the final `spanner_napi.node` binary, eliminating dynamic shared library dependencies, `@loader_path` rpaths, and `.so`/`.dylib` file distribution.
    *   **Windows (`-buildmode=c-shared`):** Invokes Go with `-buildmode=c-shared` to generate a dynamic shared library (`libspanner.dll`) along with a companion MSVC-compatible import library (`libspanner.lib`) required by MSVC's linker (`link.exe`).

### Phase 2: Compiling the Native Bridge (node-gyp)

Once the Go library is generated, the Node.js C++ wrapper is compiled using `node-gyp` to map V8 engine objects into Go pointers.
*   **Trigger:** Executed as part of `npm run build` which invokes `node-gyp rebuild`.
*   **Action:** Reads the build instructions in [binding.gyp](./binding.gyp) to locate the Go header files, and links the bridge against the generated Go library. It compiles the bridge source file [addon.cc](./src/cpp/addon.cc) using the local OS C++ compiler toolchain (Clang on macOS, GCC on Linux, MSVC on Windows).
    *   On **Linux and macOS**, `binding.gyp` statically links `libspanner.a` directly into the `.node` binary.
    *   On **Windows**, `binding.gyp` links against `libspanner.lib` and copies `libspanner.dll` adjacent to the addon in `build/Release/`.
*   **Output:** Generates the native Node.js binary file at `build/Release/spanner_napi.node`.

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
This builds the underlying Go library, links the C++ bridge layer via `node-gyp`, and outputs the final dual ESM/CJS JavaScript distributions.

## Platform-Specific Release Pipelines (GitHub Actions)

Releasing the prebuilt native platform packages is managed via a unified manual GitHub Actions workflow:
*   **Workflow:** [release-node-wrapper.yml](../../../.github/workflows/release-node-wrapper.yml)

### Target Platform Packages

| Package Name | Target Platform | Runner OS | glibc / Toolchain |
| :--- | :--- | :--- | :--- |
| **`@google-cloud/spannerlib-node-darwin-arm64`** | macOS (Apple Silicon `arm64`) | `macos-latest` | Apple Clang (`c-archive` static) |
| **`@google-cloud/spannerlib-node-linux-x64`** | Linux (`x64`) | `ubuntu-22.04` | **glibc 2.35** / GCC (`c-archive` static) |
| **`@google-cloud/spannerlib-node-linux-arm64`** | Linux (`arm64`) | `ubuntu-22.04` | **glibc 2.35** / `gcc-aarch64-linux-gnu` (cross-compile) |
| **`@google-cloud/spannerlib-node-win32-x64`** | Windows (`x64`) | `windows-2022` | MSVC 2022 (`c-shared` DLL + `.lib`) |

> **Note on Linux Compatibility:** Compiling on `ubuntu-22.04` dynamically links against **glibc 2.35**, ensuring wide binary compatibility with older and enterprise Linux distributions (such as Debian 12, Ubuntu 22.04+, and RHEL 9). The Linux ARM64 build is cross-compiled on `ubuntu-22.04` using `gcc-aarch64-linux-gnu` rather than running on Ubuntu 24.04 ARM runners to prevent glibc 2.39 lock-in.

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
4. Compiles the Go library (`libspanner.a` / `libspanner.dll`), links the C++ N-API addon, and compiles TypeScript dual outputs (`npm run build`).
5. Sets the target package name (e.g. `@google-cloud/spannerlib-node-linux-x64`) and `os`/`cpu` metadata in `package.json`.
6. Packages the distribution tarball (`npm pack .`) containing both the JavaScript bundles and the compiled native binary (`spanner_napi.node` plus `libspanner.dll` on Windows).
7. Publishes the generated tarball to Wombat with the specified dist-tag (`npm publish --access=public --tag $NPM_TAG --registry=https://wombat-dressing-room.appspot.com`).
8. Archives and uploads the release `.tgz` as a workflow artifact for auditing and SBOM generation.
