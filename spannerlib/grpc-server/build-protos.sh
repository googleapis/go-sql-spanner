PATH="${PATH}:${HOME}/go/bin"

# Detect java grpc plugin
JAVA_GRPC_PLUGIN_ARG=""
if [ -n "${PROTOC_GEN_GRPC_JAVA}" ]; then
  JAVA_GRPC_PLUGIN_ARG="--plugin=protoc-gen-java-grpc=${PROTOC_GEN_GRPC_JAVA}"
elif which protoc-gen-java-grpc > /dev/null 2>&1; then
  # If it is already correctly named on PATH, no plugin arg needed
  JAVA_GRPC_PLUGIN_ARG=""
elif which protoc-gen-grpc-java > /dev/null 2>&1; then
  # If it is on PATH but named protoc-gen-grpc-java
  JAVA_GRPC_PLUGIN_ARG="--plugin=protoc-gen-java-grpc=$(which protoc-gen-grpc-java)"
else
  # Try to find it in the home directory
  MATCH=$(find "$HOME" -maxdepth 1 -name "protoc-gen-grpc-java*" | head -n 1 2>/dev/null)
  if [ -n "$MATCH" ]; then
    JAVA_GRPC_PLUGIN_ARG="--plugin=protoc-gen-java-grpc=${MATCH}"
  fi
fi

# Detect C# grpc plugin
CSHARP_GRPC_PLUGIN_ARG=""
if [ -n "${PROTOC_GEN_CSHARP_GRPC}" ]; then
  CSHARP_GRPC_PLUGIN_ARG="--plugin=protoc-gen-csharp_grpc=${PROTOC_GEN_CSHARP_GRPC}"
elif which grpc_csharp_plugin > /dev/null 2>&1; then
  CSHARP_GRPC_PLUGIN_ARG=""
else
  NUGET_PACKAGES_DIR="${NUGET_PACKAGES:-$HOME/.nuget/packages}"

  EXE_SUFFIX=""
  case "$(uname -s)" in
    CYGWIN*|MINGW*|MSYS*) EXE_SUFFIX=".exe" ;;
  esac

  MATCH=$(find "${NUGET_PACKAGES_DIR}/grpc.tools" -name "grpc_csharp_plugin${EXE_SUFFIX}" | head -n 1 2>/dev/null)
  if [ -z "${MATCH}" ]; then
    echo "grpc.tools not found in NuGet cache. Downloading..."
    TEMP_DIR=$(mktemp -d)
    cd "${TEMP_DIR}" || exit 1
    dotnet new classlib > /dev/null
    dotnet add package Grpc.Tools > /dev/null
    cd - > /dev/null || exit 1
    rm -rf "${TEMP_DIR}"
    MATCH=$(find "${NUGET_PACKAGES_DIR}/grpc.tools" -name "grpc_csharp_plugin${EXE_SUFFIX}" | head -n 1 2>/dev/null)
  fi

  if [ -n "${MATCH}" ]; then
    CSHARP_GRPC_PLUGIN_ARG="--plugin=protoc-gen-csharp_grpc=${MATCH}"
  fi
fi

rm -rf googleapis
git clone --depth=1 https://github.com/googleapis/googleapis.git
ln -sf "${PWD}"/google/spannerlib googleapis/google/spannerlib
cd googleapis || exit 1

echo "Generating Go protobuf..."
protoc \
  --go_out=../ \
  --go_opt=paths=source_relative \
  --go-grpc_out=../ \
  --go-grpc_opt=paths=source_relative \
  google/spannerlib/v1/spannerlib.proto

if [ -n "${JAVA_GRPC_PLUGIN_ARG}" ] || which protoc-gen-grpc-java >/dev/null 2>&1; then
  echo "Generating Java protobuf..."
  protoc \
    --java_out=../../wrappers/spannerlib-java/src/main/java/ \
    ${JAVA_GRPC_PLUGIN_ARG} \
    --java-grpc_out=../../wrappers/spannerlib-java/src/main/java/ \
    --java-grpc_opt=paths=source_relative \
    google/spannerlib/v1/spannerlib.proto
else
  echo "Skipping Java protobuf generation (protoc-gen-grpc-java not found)."
fi

if [ -n "${CSHARP_GRPC_PLUGIN_ARG}" ] || which grpc_csharp_plugin >/dev/null 2>&1; then
  echo "Generating C# protobuf..."
  protoc \
    --csharp_out=../../wrappers/spannerlib-dotnet/spannerlib-dotnet-grpc-v1/ \
    ${CSHARP_GRPC_PLUGIN_ARG} \
    --csharp_grpc_out=../../wrappers/spannerlib-dotnet/spannerlib-dotnet-grpc-v1/ \
    --csharp_opt=file_extension=.g.cs \
    --csharp_grpc_opt=no_server \
    --proto_path=. \
    google/spannerlib/v1/spannerlib.proto
else
  echo "Skipping C# protobuf generation (grpc_csharp_plugin not found)."
fi

cd .. || exit 1
rm -rf googleapis
