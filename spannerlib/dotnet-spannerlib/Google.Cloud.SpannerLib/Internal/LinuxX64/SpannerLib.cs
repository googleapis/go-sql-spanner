using System.Runtime.InteropServices;

namespace Google.Cloud.SpannerLib.Internal.LinuxX64
{
    internal class SpannerLib : Google.Cloud.SpannerLib.Internal.SpannerLib
    {
        internal override int Release(long pinner)
        {
            return SpannerNativeLib.Release(pinner);
        }

        internal override Message CreatePool(GoString dsn)
        {
            return SpannerNativeLib.CreatePool(dsn);
        }

        internal override Message ClosePool(long poolId)
        {
            return SpannerNativeLib.ClosePool(poolId);
        }

        internal override Message CreateConnection(long poolId)
        {
            return SpannerNativeLib.CreateConnection(poolId);
        }

        internal override Message CloseConnection(long poolId, long connectionId)
        {
            return SpannerNativeLib.CloseConnection(poolId, connectionId);
        }

        internal override Message Apply(long poolId, long connectionId, GoSlice mutations)
        {
            return SpannerNativeLib.Apply(poolId, connectionId, mutations);
        }

        internal override Message BufferWrite(long poolId, long connectionId, long transactionId, GoSlice mutations)
        {
            return SpannerNativeLib.BufferWrite(poolId, connectionId, transactionId, mutations);
        }

        internal override Message Execute(long poolId, long connectionId, GoSlice statement)
        {
            return SpannerNativeLib.Execute(poolId, connectionId, statement);
        }

        internal override Message ExecuteTransaction(long poolId, long connectionId, long txId, GoSlice statement)
        {
            return SpannerNativeLib.ExecuteTransaction(poolId, connectionId, txId, statement);
        }

        internal override Message ExecuteBatchDml(long poolId, long connectionId, GoSlice statements)
        {
            return SpannerNativeLib.ExecuteBatchDml(poolId, connectionId, statements);
        }

        internal override Message Metadata(long poolId, long connectionId, long rowsId)
        {
            return SpannerNativeLib.Metadata(poolId, connectionId, rowsId);
        }

        internal override Message ResultSetStats(long poolId, long connectionId, long rowsId)
        {
            return SpannerNativeLib.ResultSetStats(poolId, connectionId, rowsId);
        }

        internal override Message Next(long poolId, long connectionId, long rowsId)
        {
            return SpannerNativeLib.Next(poolId, connectionId, rowsId);
        }

        internal override Message CloseRows(long poolId, long connectionId, long rowsId)
        {
            return SpannerNativeLib.CloseRows(poolId, connectionId, rowsId);
        }

        internal override Message BeginTransaction(long poolId, long connectionId, GoSlice transactionOptions)
        {
            return SpannerNativeLib.BeginTransaction(poolId, connectionId, transactionOptions);
        }

        internal override Message Commit(long poolId, long connectionId, long txId)
        {
            return SpannerNativeLib.Commit(poolId, connectionId, txId);
        }

        internal override Message Rollback(long poolId, long connectionId, long txId)
        {
            return SpannerNativeLib.Rollback(poolId, connectionId, txId);
        }
    }

    internal static class SpannerNativeLib
    {
        private const string SpannerLibName = "native/linux-x64/spannerlib.so";

        [DllImport(SpannerLibName, EntryPoint = "Release")]
        [DefaultDllImportSearchPaths(DllImportSearchPath.AssemblyDirectory)]
        internal static extern int Release(long pinner);

        [DllImport(SpannerLibName, EntryPoint = "CreatePool")]
        [DefaultDllImportSearchPaths(DllImportSearchPath.AssemblyDirectory)]
        internal static extern Message CreatePool(GoString dsn);

        [DllImport(SpannerLibName, EntryPoint = "ClosePool")]
        [DefaultDllImportSearchPaths(DllImportSearchPath.AssemblyDirectory)]
        internal static extern Message ClosePool(long poolId);

        [DllImport(SpannerLibName, EntryPoint = "CreateConnection")]
        [DefaultDllImportSearchPaths(DllImportSearchPath.AssemblyDirectory)]
        internal static extern Message CreateConnection(long poolId);

        [DllImport(SpannerLibName, EntryPoint = "CloseConnection")]
        [DefaultDllImportSearchPaths(DllImportSearchPath.AssemblyDirectory)]
        internal static extern Message CloseConnection(long poolId, long connectionId);

        [DllImport(SpannerLibName, EntryPoint = "Apply")]
        [DefaultDllImportSearchPaths(DllImportSearchPath.AssemblyDirectory)]
        internal static extern Message Apply(long poolId, long connectionId, GoSlice mutations);

        [DllImport(SpannerLibName, EntryPoint = "BufferWrite")]
        [DefaultDllImportSearchPaths(DllImportSearchPath.AssemblyDirectory)]
        internal static extern Message BufferWrite(long poolId, long connectionId, long transactionId,
            GoSlice mutations);

        [DllImport(SpannerLibName, EntryPoint = "Execute")]
        [DefaultDllImportSearchPaths(DllImportSearchPath.AssemblyDirectory)]
        internal static extern Message Execute(long poolId, long connectionId, GoSlice statement);

        [DllImport(SpannerLibName, EntryPoint = "ExecuteTransaction")]
        [DefaultDllImportSearchPaths(DllImportSearchPath.AssemblyDirectory)]
        internal static extern Message ExecuteTransaction(long poolId, long connectionId, long txId, GoSlice statement);

        [DllImport(SpannerLibName, EntryPoint = "ExecuteBatchDml")]
        [DefaultDllImportSearchPaths(DllImportSearchPath.AssemblyDirectory)]
        internal static extern Message ExecuteBatchDml(long poolId, long connectionId, GoSlice statements);

        [DllImport(SpannerLibName, EntryPoint = "Metadata")]
        [DefaultDllImportSearchPaths(DllImportSearchPath.AssemblyDirectory)]
        internal static extern Message Metadata(long poolId, long connectionId, long rowsId);

        [DllImport(SpannerLibName, EntryPoint = "ResultSetStats")]
        [DefaultDllImportSearchPaths(DllImportSearchPath.AssemblyDirectory)]
        internal static extern Message ResultSetStats(long poolId, long connectionId, long rowsId);

        [DllImport(SpannerLibName, EntryPoint = "Next")]
        [DefaultDllImportSearchPaths(DllImportSearchPath.AssemblyDirectory)]
        internal static extern Message Next(long poolId, long connectionId, long rowsId);

        [DllImport(SpannerLibName, EntryPoint = "CloseRows")]
        [DefaultDllImportSearchPaths(DllImportSearchPath.AssemblyDirectory)]
        internal static extern Message CloseRows(long poolId, long connectionId, long rowsId);

        [DllImport(SpannerLibName, EntryPoint = "BeginTransaction")]
        [DefaultDllImportSearchPaths(DllImportSearchPath.AssemblyDirectory)]
        internal static extern Message BeginTransaction(long poolId, long connectionId, GoSlice transactionOptions);

        [DllImport(SpannerLibName, EntryPoint = "Commit")]
        [DefaultDllImportSearchPaths(DllImportSearchPath.AssemblyDirectory)]
        internal static extern Message Commit(long poolId, long connectionId, long txId);

        [DllImport(SpannerLibName, EntryPoint = "Rollback")]
        [DefaultDllImportSearchPaths(DllImportSearchPath.AssemblyDirectory)]
        internal static extern Message Rollback(long poolId, long connectionId, long txId);
    }
}