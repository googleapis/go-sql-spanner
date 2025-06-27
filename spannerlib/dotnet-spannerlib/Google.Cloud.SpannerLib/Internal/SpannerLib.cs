using System.Runtime.InteropServices;

namespace Google.Cloud.SpannerLib.Internal
{

    internal abstract class SpannerLib
    {
        private static bool IsOsxArm64()
        {
            return RuntimeInformation.IsOSPlatform(OSPlatform.OSX) &&
                   RuntimeInformation.ProcessArchitecture == Architecture.Arm64;
        }

        private static bool IsLinuxX64()
        {
            return RuntimeInformation.IsOSPlatform(OSPlatform.Linux) &&
                   RuntimeInformation.ProcessArchitecture == Architecture.X64;
        }

        internal static SpannerLib Create()
        {
            if (IsOsxArm64())
            {
                return new OsxArm64.SpannerLib();
            }

            if (IsLinuxX64())
            {
                return new LinuxX64.SpannerLib();
            }
            throw new System.PlatformNotSupportedException("SpannerLib is not supported on this operating system.");
        }

        internal abstract int Release(long pinner);

        internal abstract Message CreatePool(GoString dsn);

        internal abstract Message ClosePool(long poolId);

        internal abstract Message CreateConnection(long poolId);

        internal abstract Message CloseConnection(long poolId, long connectionId);

        internal abstract Message Apply(long poolId, long connectionId, GoSlice mutations);

        internal abstract Message BufferWrite(long poolId, long connectionId, long transactionId, GoSlice mutations);

        internal abstract Message Execute(long poolId, long connectionId, GoSlice statement);

        internal abstract Message ExecuteTransaction(long poolId, long connectionId, long txId, GoSlice statement);

        internal abstract Message ExecuteBatchDml(long poolId, long connectionId, GoSlice statements);

        internal abstract Message Metadata(long poolId, long connectionId, long rowsId);

        internal abstract Message ResultSetStats(long poolId, long connectionId, long rowsId);

        internal abstract Message Next(long poolId, long connectionId, long rowsId);

        internal abstract Message CloseRows(long poolId, long connectionId, long rowsId);

        internal abstract Message BeginTransaction(long poolId, long connectionId, GoSlice transactionOptions);

        internal abstract Message Commit(long poolId, long connectionId, long txId);

        internal abstract Message Rollback(long poolId, long connectionId, long txId);
    }
}