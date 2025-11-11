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
echo -e "Build Spannerlib Shared Lib"

export SKIP_MACOS=true
export SKIP_WINDOWS=true
export SKIP_LINUX_CROSS_COMPILE=true

TARGET_WRAPPER_DIR="../wrappers/spannerlib-python" 
ARTIFACTS_DIR="spannerlib-artifacts"

cd ../../shared || exit 1

./build-binaries.sh

# Navigate to the correct wrapper directory
cd "$TARGET_WRAPPER_DIR" || exit 1

echo -e "PREPARING ARTIFACTS IN: $(pwd)"

# Cleanup old artifacts if they exist
if [ -d "$ARTIFACTS_DIR" ]; then
    rm -rf "$ARTIFACTS_DIR" # 2> /dev/null
fi

mkdir -p "$ARTIFACTS_DIR"

if [ -z "$SKIP_MACOS" ]; then
echo "Copying MacOS binaries..."
    mkdir -p "$ARTIFACTS_DIR/osx-arm64"
    cp ../../shared/binaries/osx-arm64/spannerlib.dylib "$ARTIFACTS_DIR/osx-arm64/spannerlib.dylib"
    cp ../../shared/binaries/osx-arm64/spannerlib.h "$ARTIFACTS_DIR/osx-arm64/spannerlib.h"
fi

if [ -z "$SKIP_LINUX_CROSS_COMPILE" ] || [ -z "$SKIP_LINUX" ]; then
    echo "Copying Linux binaries..."
    mkdir -p "$ARTIFACTS_DIR/linux-x64"
    cp ../../shared/binaries/linux-x64/spannerlib.so "$ARTIFACTS_DIR/linux-x64/spannerlib.so"
    cp ../../shared/binaries/linux-x64/spannerlib.h "$ARTIFACTS_DIR/linux-x64/spannerlib.h"
fi

if [ -z "$SKIP_WINDOWS" ]; then
    echo "Copying Windows binaries..."
    mkdir -p "$ARTIFACTS_DIR/win-x64"
    cp ../../shared/binaries/win-x64/spannerlib.dll "$ARTIFACTS_DIR/win-x64/spannerlib.dll"
    cp ../../shared/binaries/win-x64/spannerlib.h "$ARTIFACTS_DIR/win-x64/spannerlib.h"
fi
