VERSION=$(date -u +"1.0.0-alpha.%Y%m%d%H%M%S")

echo "Publishing as version $VERSION"
sed -i "" "s|<Version>.*</Version>|<Version>$VERSION</Version>|g" spanner-ado-net.csproj

rm -rf bin/Release
dotnet pack
dotnet nuget push \
  bin/Release/Alpha.Google.Cloud.Spanner.DataProvider.*.nupkg \
  --api-key $NUGET_API_KEY \
  --source https://api.nuget.org/v3/index.json
