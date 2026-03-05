VERSION=$(date -u +"1.0.0-alpha.%Y%m%d%H%M%S")

# Update all package references to the new (generated) version.
# Note that sed works slightly differently on Mac than on Linux/Windows,
# which is why we have two different versions below.
echo "Building as version $VERSION"
if [ "$RUNNER_OS" == "macOS" ] || [ "$(uname)" == "Darwin" ]; then
  find ./ -type f -name "*.csproj" -exec sed -i "" "s|<Version>.*</Version>|<Version>$VERSION</Version>|g" {} \;
  find ./ -type f -name "*.csproj" -exec sed -i "" "s|<PackageReference Include=\(\"Alpha.Google.Cloud.SpannerLib.*\"\) Version=\".*\" />|<PackageReference Include=\1 Version=\"$VERSION\" />|g" {} \;
else
  find ./ -type f -name "*.csproj" -exec sed -i "s|<Version>.*</Version>|<Version>$VERSION</Version>|g" {} \;
  find ./ -type f -name "*.csproj" -exec sed -i "s|<PackageReference Include=\(\"Alpha.Google.Cloud.SpannerLib.*\"\) Version=\".*\" />|<PackageReference Include=\1 Version=\"$VERSION\" />|g" {} \;
fi
