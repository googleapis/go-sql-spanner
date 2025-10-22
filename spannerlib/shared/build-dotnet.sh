# Builds the shared library and copies the binaries to the appropriate folders for
# the .NET wrapper. Binaries can be built for linux/x64, darwin/arm64, and windows/x64.
# Which ones are actually built depends on the values of the following variables:
# SKIP_MACOS: If set, will skip the darwin/arm64 build
# SKIP_LINUX: If set, will skip the linux/x64 build that uses the default C compiler on the system
# SKIP_LINUX_CROSS_COMPILE: If set, will skip the linux/x64 build that uses the x86_64-unknown-linux-gnu-gcc C compiler.
#                           This compiler is used when compiling for linux/x64 on MacOS.
# SKIP_WINDOWS: If set, will skip the windows/x64 build.

mkdir -p ../wrappers/spannerlib-dotnet/spannerlib-dotnet-native/libraries/any
rm ../wrappers/spannerlib-dotnet/spannerlib-dotnet-native/libraries/any/*

echo "Skip macOS: $SKIP_MACOS"
echo "Skip Linux: $SKIP_LINUX"
echo "Skip Linux cross compile: $SKIP_LINUX_CROSS_COMPILE"
echo "Skip windows: $SKIP_WINDOWS"

if [ -z "$SKIP_MACOS" ]; then
  echo "Building for darwin/arm64"
  GOOS=darwin GOARCH=arm64 CGO_ENABLED=1 go build -o spannerlib.so -buildmode=c-shared shared_lib.go
  mkdir -p ../wrappers/spannerlib-dotnet/spannerlib-dotnet-native/libraries/osx-arm64
  cp spannerlib.so ../wrappers/spannerlib-dotnet/spannerlib-dotnet-native/libraries/osx-arm64/spannerlib.dylib
fi

if [ -z "$SKIP_LINUX_CROSS_COMPILE" ]; then
  # The following software is needed for this build, assuming that the build runs on MacOS.
  #brew tap SergioBenitez/osxct
  #brew install x86_64-unknown-linux-gnu
  echo "Building for linux/x64 (cross-compile)"
  CC=x86_64-unknown-linux-gnu-gcc GOOS=linux GOARCH=amd64 CGO_ENABLED=1 go build -o spannerlib.so -buildmode=c-shared shared_lib.go
  mkdir -p ../wrappers/spannerlib-dotnet/spannerlib-dotnet-native/libraries/linux-x64
  cp spannerlib.so ../wrappers/spannerlib-dotnet/spannerlib-dotnet-native/libraries/linux-x64/spannerlib.so
elif [ -z "$SKIP_LINUX" ]; then
  # The following commands assume that the script is running on linux/x64, or at least on some system that is able
  # to compile to linux/x64 with the default C compiler on the system.
  echo "Building for linux/x64"
  GOOS=linux GOARCH=amd64 CGO_ENABLED=1 go build -o spannerlib.so -buildmode=c-shared shared_lib.go
  mkdir -p ../wrappers/spannerlib-dotnet/spannerlib-dotnet-native/libraries/linux-x64
  cp spannerlib.so ../wrappers/spannerlib-dotnet/spannerlib-dotnet-native/libraries/linux-x64/spannerlib.so
fi

if [ -z "$SKIP_WINDOWS" ]; then
  # This build requires mingw-w64 or a similar Windows compatible C compiler if it is being executed on a
  # non-Windows environment.
  #  brew install mingw-w64
  echo "Building for windows/x64"
  CC=x86_64-w64-mingw32-gcc GOOS=windows GOARCH=amd64 CGO_ENABLED=1 go build -o spannerlib.dll -buildmode=c-shared shared_lib.go
  mkdir -p ../wrappers/spannerlib-dotnet/spannerlib-dotnet-native/libraries/win-x64
  cp spannerlib.dll ../wrappers/spannerlib-dotnet/spannerlib-dotnet-native/libraries/win-x64/spannerlib.dll
fi
