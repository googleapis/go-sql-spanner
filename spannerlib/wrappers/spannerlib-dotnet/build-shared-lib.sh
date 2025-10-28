# Builds the shared library and copies the binaries to the appropriate folders for
# the .NET wrapper. Binaries can be built for linux/x64, darwin/arm64, and windows/x64.
# Which ones are actually built depends on the values of the following variables:
# SKIP_MACOS: If set, will skip the darwin/arm64 build
# SKIP_LINUX: If set, will skip the linux/x64 build that uses the default C compiler on the system
# SKIP_LINUX_CROSS_COMPILE: If set, will skip the linux/x64 build that uses the x86_64-unknown-linux-gnu-gcc C compiler.
#                           This compiler is used when compiling for linux/x64 on MacOS.
# SKIP_WINDOWS: If set, will skip the windows/x64 build.

mkdir -p spannerlib-dotnet-native/libraries/any
rm spannerlib-dotnet-native/libraries/any/* 2> /dev/null

cd ../../shared || exit 1
./build-binaries.sh
cd ../wrappers/spannerlib-dotnet || exit 1

if [ -z "$SKIP_MACOS" ]; then
  mkdir -p spannerlib-dotnet-native/libraries/osx-arm64
  cp ../../shared/binaries/osx-arm64/spannerlib.dylib spannerlib-dotnet-native/libraries/osx-arm64/spannerlib.dylib
fi

if [ -z "$SKIP_LINUX_CROSS_COMPILE" ] || [ -z "$SKIP_LINUX" ]; then
  mkdir -p spannerlib-dotnet-native/libraries/linux-x64
  cp ../../shared/binaries/linux-x64/spannerlib.so spannerlib-dotnet-native/libraries/linux-x64/spannerlib.so
fi

if [ -z "$SKIP_WINDOWS" ]; then
  mkdir -p spannerlib-dotnet-native/libraries/win-x64
  cp ../../shared/binaries/win-x64/spannerlib.dll spannerlib-dotnet-native/libraries/win-x64/spannerlib.dll
fi
