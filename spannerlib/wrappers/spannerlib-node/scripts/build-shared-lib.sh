#!/bin/bash
set -e

# Copyright 2026 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Builds the shared library and places it in the shared directory.
# This script handles OS detection to use the correct file extension.

log() {
    echo "[$(date +'%Y-%m-%d %H:%M:%S')] $1"
}

log "Starting Spannerlib Shared Library Build for Node Wrapper..."

# Resolve absolute paths
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SHARED_LIB_DIR="$(cd "$SCRIPT_DIR/../../../shared" && pwd)"

log "Script Directory: $SCRIPT_DIR"
log "Shared Lib Directory: $SHARED_LIB_DIR"

# Auto-detect OS
case "$(uname -s)" in
    Linux*)     OS="Linux";;
    Darwin*)    OS="macOS";;
    CYGWIN*|MINGW*|MSYS*) OS="Windows";;
    *)          OS="Unknown";;
esac
log "Auto-detected OS: $OS"

if [ "$OS" == "macOS" ]; then
    echo "Building for macOS..."
    go build -C "$SHARED_LIB_DIR" -o libspanner.dylib -buildmode=c-shared shared_lib.go
elif [ "$OS" == "Linux" ]; then
    echo "Building for Linux..."
    go build -C "$SHARED_LIB_DIR" -o libspanner.so -buildmode=c-shared shared_lib.go
elif [ "$OS" == "Windows" ]; then
    echo "Building for Windows..."
    (
        cd "$SHARED_LIB_DIR" || exit 1
        # 1. Build DLL with Go
        go build -o libspanner.dll -buildmode=c-shared shared_lib.go
        
        # 2. Extract symbol definitions (dumpbin with gendef fallback)
        if (command -v dumpbin.exe >/dev/null 2>&1 || command -v dumpbin >/dev/null 2>&1); then
            echo "Extracting exports using dumpbin..."
            echo "EXPORTS" > libspanner.def
            dumpbin /EXPORTS libspanner.dll | awk '/ordinal hint/,/Summary/' | awk '{print $4}' | grep -v '^$' | grep -v 'Summary' >> libspanner.def || true
        elif command -v gendef >/dev/null 2>&1; then
            echo "Extracting exports using gendef..."
            gendef libspanner.dll
        fi
        
        # 3. Create MSVC-compatible .lib using MSVC's lib.exe (with dlltool fallback)
        if (command -v lib.exe >/dev/null 2>&1 || command -v lib >/dev/null 2>&1) && [ -s "libspanner.def" ]; then
            echo "Generating MSVC-compatible libspanner.lib with lib.exe..."
            lib.exe /def:libspanner.def /out:libspanner.lib /machine:X64 2>/dev/null || lib /def:libspanner.def /out:libspanner.lib /machine:X64
            rm -f libspanner.exp libspanner.def
        elif command -v dlltool >/dev/null 2>&1 && [ -s "libspanner.def" ]; then
            echo "Generating libspanner.lib using dlltool fallback..."
            dlltool -D libspanner.dll -d libspanner.def -l libspanner.lib -m i386:x86-64
            rm -f libspanner.def
        fi
    )
else
    echo "Unsupported operating system: $OS"
    exit 1
fi

echo "Build complete."
