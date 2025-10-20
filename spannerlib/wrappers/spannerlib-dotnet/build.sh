
# Determine which builds to skip when the script runs on GitHub Actions.
if [ "$RUNNER_OS" == "Windows" ]; then
  # Windows does not support any cross-compiling.
  export SKIP_MACOS=true
  export SKIP_LINUX=true
  export SKIP_LINUX_CROSS_COMPILE=true
elif [ "$RUNNER_OS" == "macOS" ]; then
  # When running on macOS, cross-compiling is supported.
  # We skip the 'normal' Linux build (the one that does not explicitly set a C compiler).
  export SKIP_LINUX=true
elif [ "$RUNNER_OS" == "Linux" ]; then
  # Linux does not (yet) support cross-compiling to MacOS.
  # In addition, we use the 'normal' Linux build when we are already running on Linux.
  export SKIP_MACOS=true
  export SKIP_LINUX_CROSS_COMPILE=true
fi
echo "Skip macOS: $SKIP_MACOS"
echo "Skip Linux: $SKIP_LINUX"
echo "Skip Linux cross compile: $SKIP_LINUX_CROSS_COMPILE"
echo "Skip windows: $SKIP_WINDOWS"

# Build gRPC server
echo "Building gRPC server..."
cd ../../grpc-server || exit 1
./build-dotnet.sh

# Build shared library
echo "Building shared library..."
cd ../shared || exit 1
./build-dotnet.sh

# Build nuget packages
cd ../wrappers/spannerlib-dotnet || exit 1

echo "Building .NET packages..."
VERSION=$(date -u +"1.0.0-alpha.%Y%m%d%H%M%S")

# Update all package references to the new (generated) version.
# Note that sed works slightly differently on Mac than on Linux/Windows,
# which is why we have two different versions below.
echo "Publishing as version $VERSION"
if [ "$RUNNER_OS" == "macOS" ]; then
  find ./ -type f -name "*.csproj" -exec sed -i "" "s|<Version>.*</Version>|<Version>$VERSION</Version>|g" {} \;
  find ./ -type f -name "*.csproj" -exec sed -i "" "s|<PackageReference Include=\(\"Alpha.Google.Cloud.SpannerLib.*\"\) Version=\".*\" />|<PackageReference Include=\1 Version=\"$VERSION\" />|g" {} \;
else
  find ./ -type f -name "*.csproj" -exec sed -i "s|<Version>.*</Version>|<Version>$VERSION</Version>|g" {} \;
  find ./ -type f -name "*.csproj" -exec sed -i "s|<PackageReference Include=\(\"Alpha.Google.Cloud.SpannerLib.*\"\) Version=\".*\" />|<PackageReference Include=\1 Version=\"$VERSION\" />|g" {} \;
fi

# Add the build folders as local nuget package sources.
# This allows the references to these generated versions to be picked up locally.

dotnet nuget remove source local-native-build 2>/dev/null
if [ "$RUNNER_OS" == "Windows" ]; then
  # PWD does not work on Windows
  dotnet nuget add source "${GITHUB_WORKSPACE}\spannerlib\wrappers\spannerlib-dotnet\spannerlib-dotnet-native\bin\Release" --name local-native-build
else
  dotnet nuget add source "$PWD"/spannerlib-dotnet-native/bin/Release --name local-native-build
fi
dotnet nuget remove source local-grpc-server-build 2>/dev/null
if [ "$RUNNER_OS" == "Windows" ]; then
  # PWD does not work on Windows
  dotnet nuget add source "${GITHUB_WORKSPACE}\spannerlib\wrappers\spannerlib-dotnet\spannerlib-dotnet-grpc-server\bin\Release" --name local-grpc-server-build
else
  dotnet nuget add source "$PWD"/spannerlib-dotnet-grpc-server/bin/Release --name local-grpc-server-build
fi

# Create packages for the two components that contain the binaries (shared library + gRPC server)
find ./**/bin/Release -type f -name "Alpha*.nupkg" -exec rm {} \;
cd spannerlib-dotnet-native || exit 1
dotnet pack
cd .. || exit 1
cd spannerlib-dotnet-grpc-server || exit 1
dotnet pack
cd .. || exit 1

# Restore the packages of all the projects so they pick up the locally built packages.
dotnet restore
