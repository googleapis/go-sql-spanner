# NUGET_API_KEY=secret

# Build gRPC server
echo "Building gRPC server..."
cd ../../grpc-server || exit 1
./build-dotnet.sh

# Build shared library

echo "Building shared library..."
cd ../shared || exit 1
./build-dotnet.sh

# Build and publish nuget packages
cd ../wrappers/spannerlib-dotnet || exit 1

echo "Building .NET packages..."
VERSION=$(date -u +"1.0.0-alpha.%Y%m%d%H%M%S")

echo "Publishing as version $VERSION"
find ./ -type f -name "*.csproj" -exec sed -i "" "s|<Version>.*</Version>|<Version>$VERSION</Version>|g" {} \;
find ./ -type f -name "*.csproj" -exec sed -i "" "s|<PackageReference Include=\(\"Alpha.Google.Cloud.SpannerLib.*\"\) Version=\".*\" />|<PackageReference Include=\1 Version=\"$VERSION\" />|g" {} \;

dotnet nuget remove source local-native-build 2>/dev/null
dotnet nuget add source "$PWD"/spannerlib-dotnet-native/bin/Release --name local-native-build
dotnet nuget remove source local-grpc-server-build 2>/dev/null
dotnet nuget add source "$PWD"/spannerlib-dotnet-grpc-server/bin/Release --name local-grpc-server-build

find ./**/bin/Release -type f -name "Alpha*.nupkg" -exec rm {} \;
cd spannerlib-dotnet-native || exit 1
dotnet pack
cd .. || exit 1
cd spannerlib-dotnet-grpc-server || exit 1
dotnet pack
cd .. || exit 1
dotnet pack
find ./**/bin/Release -type f -name "Alpha*.nupkg" -exec \
  dotnet nuget push \
    {} \
    --api-key $NUGET_API_KEY \
    --source https://api.nuget.org/v3/index.json \;
