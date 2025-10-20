# NUGET_API_KEY=secret

# Build all components
./build.sh

# Pack and publish to nuget
dotnet pack
find ./**/bin/Release -type f -name "Alpha*.nupkg" -exec \
  dotnet nuget push \
    {} \
    --api-key $NUGET_API_KEY \
    --source https://api.nuget.org/v3/index.json \;
