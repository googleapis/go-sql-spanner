
# Builds the .NET wrapper components that depend on the Go shared library.
# This script executes the following steps:
# 1. Determine which binaries should be built (darwin/arm64, linux/x64, win/x64).
#    The default is to build all. This requires the system that is running the script to be able to
#    compile for all those operating systems and architectures.
# 2. Build the binaries for the gRPC server and copy these to the appropriate folders for the .NET wrapper.
# 3. Build the binaries for the shared library and copy these to the appropriate folders for the .NET wrapper.
# 4. Generate a 'snapshot' version number based on the current date/time for the .NET gRPC and shared library wrappers.
# 5. Update all references to the gRPC and shared library wrappers to use the new generated version number.
# 6. Pack the .NET gRPC and shared library wrappers and register the build directories of these as local nuget sources.
#    This allows the other projects in the solution to pick up these 'snapshot' versions locally instead of looking for
#    them in the central nuget repository.
#    Note that we use package references instead of project references for the .NET wrappers that contain the binary
#    files of the shared library, because .NET does not support dynamically loading the correct native library version
#    for project references.

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
echo "Skip macOS: $SKIP_MACOS"
echo "Skip Linux: $SKIP_LINUX"
echo "Skip Linux cross compile: $SKIP_LINUX_CROSS_COMPILE"
echo "Skip windows: $SKIP_WINDOWS"

# Remove existing builds
rm -r ./spannerlib-dotnet-native/libraries 2> /dev/null
rm -r ./spannerlib-dotnet-grpc-server/binaries 2> /dev/null
rm -r ./*/bin 2> /dev/null
rm -r ./*/obj 2> /dev/null
dotnet nuget locals global-packages --clear

# Build gRPC server
echo "Building gRPC server..."
./build-grpc-server.sh

# Build shared library
echo "Building shared library..."
./build-shared-lib.sh

# Build nuget packages
echo "Building .NET packages..."

# Add the build folders as local nuget package sources.
# This allows the references to these generated versions to be picked up locally.

dotnet nuget remove source local-native-build 2>/dev/null
if [ "$RUNNER_OS" == "Windows" ]; then
  # PWD does not work on Windows
  dotnet nuget add source "${GITHUB_WORKSPACE}\spannerlib\wrappers\spannerlib-dotnet\spannerlib-dotnet-native\bin\Release" --name local-native-build
else
  dotnet nuget add source "$PWD"/spannerlib-dotnet-native/bin/Release --name local-native-build
fi
dotnet nuget remove source local-grpc-server-build 2>/dev/null
if [ "$RUNNER_OS" == "Windows" ]; then
  # PWD does not work on Windows
  dotnet nuget add source "${GITHUB_WORKSPACE}\spannerlib\wrappers\spannerlib-dotnet\spannerlib-dotnet-grpc-server\bin\Release" --name local-grpc-server-build
else
  dotnet nuget add source "$PWD"/spannerlib-dotnet-grpc-server/bin/Release --name local-grpc-server-build
fi

# Create packages for the two components that contain the binaries (shared library + gRPC server)
find ./**/bin/Release -type f -name "Alpha*.nupkg" -exec rm {} \;
cd spannerlib-dotnet-native || exit 1
dotnet pack
cd .. || exit 1
cd spannerlib-dotnet-grpc-server || exit 1
dotnet pack
cd .. || exit 1

# Restore the packages of all the projects so they pick up the locally built packages.
dotnet restore
