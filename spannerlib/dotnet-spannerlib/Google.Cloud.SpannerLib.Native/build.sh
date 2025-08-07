
# Determine OS + Arch
export DEST=libraries/any/spannerlib
mkdir -p libraries/any

# Clear all local nuget cache
dotnet nuget locals --clear all
go build -o ../../spannerlib.so -buildmode=c-shared ../../
cp ../../spannerlib.so $DEST
dotnet pack
dotnet nuget remove source local 2>/dev/null
dotnet nuget add source "$PWD"/bin/Release --name local
