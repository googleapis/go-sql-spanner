mkdir -p ../wrappers/spannerlib-dotnet/spannerlib-dotnet-native/binaries/any
rm ../wrappers/spannerlib-dotnet/spannerlib-dotnet-grpc-server/binaries/any/spannerlib

GOOS=darwin GOARCH=arm64 CGO_ENABLED=1 go build -o spannerlib.so -buildmode=c-shared shared_lib.go
mkdir -p ../wrappers/spannerlib-dotnet/spannerlib-dotnet-native/binaries/osx-arm64
cp spannerlib.so ../wrappers/spannerlib-dotnet/spannerlib-dotnet-native/binaries/osx-arm64/spannerlib.so

#brew tap SergioBenitez/osxct
#brew install x86_64-unknown-linux-gnu
CC=x86_64-unknown-linux-gnu-gcc GOOS=linux GOARCH=amd64 CGO_ENABLED=1 go build -o spannerlib.so -buildmode=c-shared shared_lib.go
mkdir -p ../wrappers/spannerlib-dotnet/spannerlib-dotnet-native/binaries/linux-x64
cp spannerlib.so ../wrappers/spannerlib-dotnet/spannerlib-dotnet-native/binaries/linux-x64/spannerlib.so
