#!/bin/bash

# Builds the gRPC server binary for darwin/arm64, linux/x64, and windows/x64.
# The binaries are stored in the following files:
# binaries/osx-arm64/spannerlib_grpc_server
# binaries/linux-x64/spannerlib_grpc_server
# binaries/linux-arm64/spannerlib_grpc_server
# binaries/win-x64/spannerlib_grpc_server.exe

set -e

build_target() {
  local goos=$1
  local goarch=$2
  local dir_os=$3
  local dir_arch=$4
  local ext=${5:-}

  local dir="binaries/${dir_os}-${dir_arch}"
  local output="${dir}/spannerlib_grpc_server${ext}"

  echo "Building for ${goos}/${goarch}..."
  mkdir -p "${dir}"
  GOOS=${goos} GOARCH=${goarch} go build -o "${output}" server.go
  chmod +x "${output}"
}

build_target "darwin" "arm64" "osx" "arm64"
build_target "linux" "amd64" "linux" "x64"
build_target "linux" "arm64" "linux" "arm64"
build_target "windows" "amd64" "win" "x64" ".exe"
