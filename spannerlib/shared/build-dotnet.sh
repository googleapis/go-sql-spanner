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
  #brew tap SergioBenitez/osxct
  #brew install x86_64-unknown-linux-gnu
  echo "Building for linux/x64 (cross-compile)"
  CC=x86_64-unknown-linux-gnu-gcc GOOS=linux GOARCH=amd64 CGO_ENABLED=1 go build -o spannerlib.so -buildmode=c-shared shared_lib.go
  mkdir -p ../wrappers/spannerlib-dotnet/spannerlib-dotnet-native/libraries/linux-x64
  cp spannerlib.so ../wrappers/spannerlib-dotnet/spannerlib-dotnet-native/libraries/linux-x64/spannerlib.so
elif [ -z "$SKIP_LINUX" ]; then
  echo "Building for linux/x64"
  GOOS=linux GOARCH=amd64 CGO_ENABLED=1 go build -o spannerlib.so -buildmode=c-shared shared_lib.go
  mkdir -p ../wrappers/spannerlib-dotnet/spannerlib-dotnet-native/libraries/linux-x64
  cp spannerlib.so ../wrappers/spannerlib-dotnet/spannerlib-dotnet-native/libraries/linux-x64/spannerlib.so
fi

if [ -z "$SKIP_WINDOWS" ]; then
  #  brew install mingw-w64
  echo "Building for windows/x64"
  CC=x86_64-w64-mingw32-gcc GOOS=windows GOARCH=amd64 CGO_ENABLED=1 go build -o spannerlib.dll -buildmode=c-shared shared_lib.go
  mkdir -p ../wrappers/spannerlib-dotnet/spannerlib-dotnet-native/libraries/win-x64
  cp spannerlib.dll ../wrappers/spannerlib-dotnet/spannerlib-dotnet-native/libraries/win-x64/spannerlib.dll
fi
