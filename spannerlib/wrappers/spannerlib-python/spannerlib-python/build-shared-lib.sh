#!/bin/bash
set -e

# Builds the shared library and copies the binaries to the appropriate folders for
# the Python wrapper.
#
# This script delegates the actual build process to spannerlib/shared/build-binaries.sh
# and then copies the artifacts to google/cloud/spannerlib/internal/lib.

log() {
    echo "[$(date +'%Y-%m-%d %H:%M:%S')] $1"
}

log "Starting Spannerlib Shared Library Build for Python Wrapper..."

# Resolve absolute paths
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SHARED_LIB_DIR="$(cd "$SCRIPT_DIR/../../../shared" && pwd)"
TARGET_LIB_DIR="$SCRIPT_DIR/google/cloud/spannerlib/internal/lib"

log "Script Directory: $SCRIPT_DIR"
log "Shared Lib Directory: $SHARED_LIB_DIR"
log "Target Lib Directory: $TARGET_LIB_DIR"
log "Runner OS: ${RUNNER_OS:-Unknown}"

# Auto-detect RUNNER_OS if not set (e.g. local run)
if [ -z "$RUNNER_OS" ]; then
    case "$(uname -s)" in
        Linux*)     RUNNER_OS="Linux";;
        Darwin*)    RUNNER_OS="macOS";;
        CYGWIN*|MINGW*|MSYS*) RUNNER_OS="Windows";;
        *)          RUNNER_OS="Unknown";;
    esac
    log "Auto-detected RUNNER_OS: $RUNNER_OS"
fi

# Determine which builds to skip/enable based on the runner OS.
# This is primarily for CI (GitHub Actions). Local runs might not have RUNNER_OS set.
if [ -n "$RUNNER_OS" ]; then
    if [[ "$RUNNER_OS" == "Windows" ]] || [[ "$RUNNER_OS" == "Windows_NT" ]]; then
        # Windows runners usually can't cross-compile to Linux/Mac easily without extra setup
        export SKIP_MACOS=true
        export SKIP_LINUX=true
        export SKIP_LINUX_CROSS_COMPILE=true
    elif [[ "$RUNNER_OS" == "macOS" ]]; then
        # macOS can cross-compile to Linux x64 (if toolchain exists) but not usually to Windows or Linux ARM64 easily
        # We skip the 'native' Linux build
        export SKIP_LINUX=true
        # We might want to enable macOS AMD64 build if running on ARM64, or vice versa
        # For now, let's assume we want both if possible, or rely on build-binaries.sh defaults
        export BUILD_MACOS_AMD64=true
    elif [[ "$RUNNER_OS" == "Linux" ]]; then
        # Linux runners
        export SKIP_MACOS=true
        export SKIP_LINUX_CROSS_COMPILE=true
        # Enable Linux ARM64 build if cross-compiler is available (checked in build-binaries.sh)
        export BUILD_LINUX_ARM64=true
    fi
fi

# Execute the shared library build script
log "Executing build-binaries.sh in $SHARED_LIB_DIR..."
pushd "$SHARED_LIB_DIR" > /dev/null
./build-binaries.sh
popd > /dev/null

# Prepare target directory
if [ -d "$TARGET_LIB_DIR" ]; then
    log "Cleaning up existing target directory: $TARGET_LIB_DIR"
    rm -rf "$TARGET_LIB_DIR"
fi
mkdir -p "$TARGET_LIB_DIR"

# Copy artifacts
SOURCE_BINARIES="$SHARED_LIB_DIR/binaries"

if [ -d "$SOURCE_BINARIES" ] && [ "$(ls -A $SOURCE_BINARIES)" ]; then
    log "Copying binaries from $SOURCE_BINARIES to $TARGET_LIB_DIR..."
    cp -r "$SOURCE_BINARIES/"* "$TARGET_LIB_DIR/"
    log "Artifacts copied successfully."
    ls -R "$TARGET_LIB_DIR"
else
    log "WARNING: No binaries found in $SOURCE_BINARIES. The build might have skipped everything or failed silently."
    # We don't exit with error here because sometimes we might run this in a context where
    # we expect no binaries (e.g. linting only), but usually this is an issue.
    # However, since set -e is on, build-binaries.sh should have failed if it errored.
fi

log "Build and copy process completed."
