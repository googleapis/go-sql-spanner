#!/bin/bash

# Builds the shared library and copies the binaries to the appropriate folders for
# the Python wrapper.
# 
# Binaries can be built for
#   linux/x64,
#   darwin/arm64, and
#   windows/x64.
#
# Which ones are actually built depends on the values of the following variables:
#   SKIP_MACOS: If set, will skip the darwin/arm64 build
#   SKIP_LINUX: If set, will skip the linux/x64 build that uses the default C compiler on the system
#   SKIP_LINUX_CROSS_COMPILE: If set, will skip the linux/x64 build that uses the x86_64-unknown-linux-gnu-gcc C compiler.
#                           This compiler is used when compiling for linux/x64 on MacOS.
#   SKIP_WINDOWS: If set, will skip the windows/x64 build.

# Fail execution if any command errors out
set -e

echo -e "Build Spannerlib Shared Lib"

echo -e "RUNNER_OS DIR: $RUNNER_OS"
# Determine which builds to skip when the script runs on GitHub Actions.
if [ "$RUNNER_OS" == "Windows" ]; then
    # Windows does not support any cross-compiling.
    export SKIP_MACOS=true
    export SKIP_LINUX=true
    export SKIP_LINUX_CROSS_COMPILE=true
elif [ "$RUNNER_OS" == "macOS" ]; then
    # When running on macOS, cross-compiling is supported.
    # We skip the 'normal' Linux build (the one that does not explicitly set a C compiler).
    export SKIP_LINUX=true
elif [ "$RUNNER_OS" == "Linux" ]; then
    # Linux does not (yet) support cross-compiling to MacOS.
    # In addition, we use the 'normal' Linux build when we are already running on Linux.
    export SKIP_MACOS=true
    export SKIP_LINUX_CROSS_COMPILE=true
fi

SHARED_LIB_DIR="../../../shared"
TARGET_WRAPPER_DIR="../wrappers/spannerlib-python/spannerlib-python"
ARTIFACTS_DIR="spannerlib-artifacts"

cd "$SHARED_LIB_DIR" || exit 1

./build-binaries.sh

echo -e "PREPARING ARTIFACTS IN: $(pwd)"
# Navigate to the correct wrapper directory
cd "$TARGET_WRAPPER_DIR" || exit 1

echo -e "PREPARING ARTIFACTS IN: $(pwd)"

# Cleanup old artifacts if they exist
if [ -d "$ARTIFACTS_DIR" ]; then
    rm -rf "$ARTIFACTS_DIR"
fi

mkdir -p "$ARTIFACTS_DIR"

if [ -z "$SKIP_MACOS" ]; then
echo "Copying MacOS binaries..."
    mkdir -p "$ARTIFACTS_DIR/osx-arm64"
    cp "$SHARED_LIB_DIR/binaries/osx-arm64/spannerlib.dylib" "$ARTIFACTS_DIR/osx-arm64/spannerlib.dylib"
    cp "$SHARED_LIB_DIR/binaries/osx-arm64/spannerlib.h" "$ARTIFACTS_DIR/osx-arm64/spannerlib.h"
fi

if [ -z "$SKIP_LINUX_CROSS_COMPILE" ] || [ -z "$SKIP_LINUX" ]; then
    echo "Copying Linux binaries..."
    mkdir -p "$ARTIFACTS_DIR/linux-x64"
    cp "$SHARED_LIB_DIR/binaries/linux-x64/spannerlib.so" "$ARTIFACTS_DIR/linux-x64/spannerlib.so"
    cp "$SHARED_LIB_DIR/binaries/linux-x64/spannerlib.h" "$ARTIFACTS_DIR/linux-x64/spannerlib.h"
fi

if [ -z "$SKIP_WINDOWS" ]; then
    echo "Copying Windows binaries..."
    mkdir -p "$ARTIFACTS_DIR/win-x64"
    cp "$SHARED_LIB_DIR/binaries/win-x64/spannerlib.dll" "$ARTIFACTS_DIR/win-x64/spannerlib.dll"
    cp "$SHARED_LIB_DIR/binaries/win-x64/spannerlib.h" "$ARTIFACTS_DIR/win-x64/spannerlib.h"
fi
