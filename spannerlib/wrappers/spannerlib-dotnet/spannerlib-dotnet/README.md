# .NET SpannerLib API

This project contains the .NET API definition of SpannerLib. Drivers and frameworks that use SpannerLib, should depend
on this API, and not on the lower-level native library.

This project only defines the API of the library. In order to use it, you must also include an actual implementation.
This could be:
* `spannerlib-dotnet-native-impl`: This loads SpannerLib as a native library into the current process.
* `spannerlib-dotnet-grpc`: This starts SpannerLib as a child process and communicates with SpannerLib through a gRPC interface. (TODO: Implement)
