go build -o spannerlib.so -buildmode=c-shared shared_lib.go
cp spannerlib.so ../wrappers/spannerlib-java/src/main/resources/darwin-aarch64/libspanner.dylib
