# Builds the gRPC server binary for darwin/arm64, linux/x64, and windows/x64
# and copies the binaries to the appropriate folders of the .NET wrapper.

cd ../../grpc-server || exit 1
./build-executables.sh
cd ../wrappers/spannerlib-dotnet || exit 1

copy_binary() {
  local platform=$1 # e.g., osx-arm64
  local ext=${2:-}
  local src_dir="../../grpc-server/binaries/${platform}"
  local dest_dir="spannerlib-dotnet-grpc-server/runtimes/${platform}/native"
  local filename="spannerlib_grpc_server${ext}"

  mkdir -p "${dest_dir}"
  cp "${src_dir}/${filename}" "${dest_dir}/${filename}"
}

mkdir -p spannerlib-dotnet-grpc-server/runtimes/any/native
rm spannerlib-dotnet-grpc-server/runtimes/any/native/spannerlib_grpc_server 2> /dev/null

copy_binary "osx-arm64"
copy_binary "linux-x64"
copy_binary "linux-arm64"
copy_binary "win-x64" ".exe"
