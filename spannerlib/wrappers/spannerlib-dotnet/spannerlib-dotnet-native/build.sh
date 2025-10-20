# Builds the shared library and the native .NET wrapper and install the latter in a local nuget repository.

# Determine OS + Arch
export DEST=libraries/any/spannerlib
mkdir -p libraries/any

# Clear all local nuget cache
dotnet nuget locals --clear all
go build -o ../../../shared/spannerlib.so -buildmode=c-shared ../../../shared/shared_lib.go
cp ../../../shared/spannerlib.so $DEST
dotnet pack
dotnet nuget remove source local 2>/dev/null
dotnet nuget add source "$PWD"/bin/Release --name local
dotnet restore ../spannerlib-dotnet-native-impl
