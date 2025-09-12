go build -o shared/spannerlib.so -buildmode=c-shared shared/shared_lib.go
mkdir -p dotnet-spannerlib/Google.Cloud.SpannerLib.Native/libraries/any
cp shared/spannerlib.so dotnet-spannerlib/Google.Cloud.SpannerLib.Native/libraries/any/spannerlib.so

go build grpc-server/grpc_server.go
mkdir -p dotnet-spannerlib/Google.Cloud.SpannerLib.Grpc/binaries/any
cp grpc-server/grpc_server dotnet-spannerlib/Google.Cloud.SpannerLib.Grpc/binaries/any/grpc_server

cd dotnet-spannerlib/Google.Cloud.SpannerLib.Native || exit 1
dotnet pack
dotnet nuget add source "$PWD"/bin/Release --name local || true

cd .. || exit 1
dotnet restore
dotnet build --no-restore

cd Google.Cloud.SpannerLib.Tests || exit 1
dotnet test --no-build --verbosity normal

cd ../..
