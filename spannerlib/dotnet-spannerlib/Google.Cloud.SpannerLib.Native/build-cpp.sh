
# Clear all local nuget cache
dotnet nuget locals --clear all
dotnet pack
dotnet nuget remove source local 2>/dev/null
dotnet nuget add source "$PWD"/bin/Release --name local
