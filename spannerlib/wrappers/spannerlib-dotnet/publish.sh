# Publishes all .NET wrapper projects to nuget central.
# The NUGET_API_KEY variable must contain a valid nuget API key.

# NUGET_API_KEY=secret

# Update all package references to the new (generated) version.
./update-versions.sh

# Build all components.
./build.sh

# Remove existing packages to ensure that only the packages that are built in the next step will be pushed to nuget.
find ./**/bin/Release -type f -name "Alpha*.nupkg" -exec rm {} \;

dotnet build
# Pack and publish to nuget
dotnet pack
find ./**/bin/Release -type f -name "Alpha*.nupkg" -exec \
  dotnet nuget push \
    {} \
    --api-key $NUGET_API_KEY \
    --source https://api.nuget.org/v3/index.json \;
