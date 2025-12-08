// Copyright 2025 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

using System.Runtime.InteropServices;

namespace Google.Cloud.SpannerLib.Native;

/// <summary>
/// This class contains the mapping of functions in SpannerLib to .NET code.
/// </summary>
public static class SpannerLib
{
    private const string SpannerLibName = "spannerlib";

    [DllImport(SpannerLibName, EntryPoint = "Release")]
    [DefaultDllImportSearchPaths(DllImportSearchPath.AssemblyDirectory)]
    public static extern int Release(long pinner);

    [DllImport(SpannerLibName, EntryPoint = "CreatePool")]
    [DefaultDllImportSearchPaths(DllImportSearchPath.AssemblyDirectory)]
    public static extern Message CreatePool(GoString dsn);

    [DllImport(SpannerLibName, EntryPoint = "ClosePool")]
    [DefaultDllImportSearchPaths(DllImportSearchPath.AssemblyDirectory)]
    public static extern Message ClosePool(long poolId);

    [DllImport(SpannerLibName, EntryPoint = "CreateConnection")]
    [DefaultDllImportSearchPaths(DllImportSearchPath.AssemblyDirectory)]
    public static extern Message CreateConnection(long poolId);

    [DllImport(SpannerLibName, EntryPoint = "CloseConnection")]
    [DefaultDllImportSearchPaths(DllImportSearchPath.AssemblyDirectory)]
    public static extern Message CloseConnection(long poolId, long connectionId);

    [DllImport(SpannerLibName, EntryPoint = "WriteMutations")]
    [DefaultDllImportSearchPaths(DllImportSearchPath.AssemblyDirectory)]
    public static extern Message WriteMutations(long poolId, long connectionId, GoSlice mutations);

    [DllImport(SpannerLibName, EntryPoint = "Execute")]
    [DefaultDllImportSearchPaths(DllImportSearchPath.AssemblyDirectory)]
    public static extern Message Execute(long poolId, long connectionId, GoSlice statement);

    [DllImport(SpannerLibName, EntryPoint = "ExecuteBatch")]
    [DefaultDllImportSearchPaths(DllImportSearchPath.AssemblyDirectory)]
    public static extern Message ExecuteBatch(long poolId, long connectionId, GoSlice statements);

    [DllImport(SpannerLibName, EntryPoint = "Metadata")]
    [DefaultDllImportSearchPaths(DllImportSearchPath.AssemblyDirectory)]
    public static extern Message Metadata(long poolId, long connectionId, long rowsId);

    [DllImport(SpannerLibName, EntryPoint = "NextResultSet")]
    [DefaultDllImportSearchPaths(DllImportSearchPath.AssemblyDirectory)]
    public static extern Message NextResultSet(long poolId, long connectionId, long rowsId);

    [DllImport(SpannerLibName, EntryPoint = "ResultSetStats")]
    [DefaultDllImportSearchPaths(DllImportSearchPath.AssemblyDirectory)]
    public static extern Message ResultSetStats(long poolId, long connectionId, long rowsId);

    [DllImport(SpannerLibName, EntryPoint = "Next")]
    [DefaultDllImportSearchPaths(DllImportSearchPath.AssemblyDirectory)]
    public static extern Message Next(long poolId, long connectionId, long rowsId, int numRows, int encodeRowOption);

    [DllImport(SpannerLibName, EntryPoint = "CloseRows")]
    [DefaultDllImportSearchPaths(DllImportSearchPath.AssemblyDirectory)]
    public static extern Message CloseRows(long poolId, long connectionId, long rowsId);

    [DllImport(SpannerLibName, EntryPoint = "BeginTransaction")]
    [DefaultDllImportSearchPaths(DllImportSearchPath.AssemblyDirectory)]
    public static extern Message BeginTransaction(long poolId, long connectionId, GoSlice transactionOptions);

    [DllImport(SpannerLibName, EntryPoint = "Commit")]
    [DefaultDllImportSearchPaths(DllImportSearchPath.AssemblyDirectory)]
    public static extern Message Commit(long poolId, long connectionId);

    [DllImport(SpannerLibName, EntryPoint = "Rollback")]
    [DefaultDllImportSearchPaths(DllImportSearchPath.AssemblyDirectory)]
    public static extern Message Rollback(long poolId, long connectionId);
}