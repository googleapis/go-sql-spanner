# Publishes all .NET wrapper projects to nuget central.
# The NUGET_API_KEY variable must contain a valid nuget API key.

# NUGET_API_KEY=secret

VERSION=$(date -u +"1.0.0-alpha.%Y%m%d%H%M%S")

# Update all package references to the new (generated) version.
# Note that sed works slightly differently on Mac than on Linux/Windows,
# which is why we have two different versions below.
echo "Publishing as version $VERSION"
if [ "$RUNNER_OS" == "macOS" ] || [ "$(uname)" == "Darwin" ]; then
  find ./ -type f -name "*.csproj" -exec sed -i "" "s|<Version>.*</Version>|<Version>$VERSION</Version>|g" {} \;
  find ./ -type f -name "*.csproj" -exec sed -i "" "s|<PackageReference Include=\(\"Alpha.Google.Cloud.SpannerLib.*\"\) Version=\".*\" />|<PackageReference Include=\1 Version=\"$VERSION\" />|g" {} \;
else
  find ./ -type f -name "*.csproj" -exec sed -i "s|<Version>.*</Version>|<Version>$VERSION</Version>|g" {} \;
  find ./ -type f -name "*.csproj" -exec sed -i "s|<PackageReference Include=\(\"Alpha.Google.Cloud.SpannerLib.*\"\) Version=\".*\" />|<PackageReference Include=\1 Version=\"$VERSION\" />|g" {} \;
fi

# Build all components
./build.sh

# Remove existing packages to ensure that only the packages that are built in the next step will be pushed to nuget.
find ./**/bin/Release -type f -name "Alpha*.nupkg" -exec rm {} \;

# Pack and publish to nuget
dotnet pack
find ./**/bin/Release -type f -name "Alpha*.nupkg" -exec \
  dotnet nuget push \
    {} \
    --api-key $NUGET_API_KEY \
    --source https://api.nuget.org/v3/index.json \;
