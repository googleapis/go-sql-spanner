# SpannerLib .NET Native Wrapper

This is a .NET wrapper around the native SpannerLib shared library.

Note that this project should not be added as a project reference to any other projects due to how .NET loads
native libraries. Instead, use the build.sh file in this folder to build and install the library to a local nuget
repository, and add a reference to that library instead.
