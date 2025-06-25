using System.Runtime.InteropServices;

namespace Google.Cloud.SpannerLib.Internal
{

    internal static unsafe class SpannerLib
    {
        private const string SpannerLibName = "spannerlib";
        
        internal struct Message
        {
#pragma warning disable CS0649 // Field is never assigned to, and will always have its default value
            public long Pinner;
            public int Code;
            public long ObjectId;
            public int Length;
            public void* Pointer;
#pragma warning restore CS0649 // Field is never assigned to, and will always have its default value
        }

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