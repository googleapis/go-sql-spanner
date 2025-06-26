using System;
using System.Runtime.CompilerServices;
using Google.Cloud.Spanner.V1;
using Google.Cloud.SpannerLib.Internal;
using Google.Protobuf;
using Google.Protobuf.WellKnownTypes;

namespace Google.Cloud.SpannerLib
{

    public static class Spanner
    {
        private static readonly Internal.SpannerLib SpannerLib = Internal.SpannerLib.Create();

        private static MessageHandler ExecuteLibraryFunction(Func<Message> func)
        {
            var handler = new MessageHandler(SpannerLib, func());
            if (handler.HasError())
            {
                try
                {
                    throw new SpannerException(handler.Code(), handler.Error()!);
                }
                finally
                {
                    handler.Dispose();
                }
            }
            return handler;
        }

        private static void ExecuteAndReleaseLibraryFunction(Func<Message> func)
        {
            using var handler = new MessageHandler(SpannerLib, func());
            if (handler.HasError())
            {
                throw new SpannerException(handler.Code(), handler.Error()!);
            }
        }

        internal static LibPool CreatePool(string dsn)
        {
            using var handler = ExecuteLibraryFunction(() =>
            {
                using var goDsn = new GoString(dsn);
                return SpannerLib.CreatePool(goDsn);
            });
            return new LibPool(handler.ObjectId());
        }

        internal static void ClosePool(LibPool libPool)
        {
            ExecuteAndReleaseLibraryFunction(() => SpannerLib.ClosePool(libPool.Id));
        }

        internal static LibConnection CreateConnection(LibPool libPool)
        {
            using var handler = ExecuteLibraryFunction(() => SpannerLib.CreateConnection(libPool.Id));
            return new LibConnection(libPool, handler.ObjectId());
        }

        internal static void CloseConnection(LibConnection libConnection)
        {
            ExecuteAndReleaseLibraryFunction(() => SpannerLib.CloseConnection(libConnection.LibPool.Id, libConnection.Id));
        }

        internal static CommitResponse Apply(LibConnection libConnection,
            BatchWriteRequest.Types.MutationGroup mutations)
        {
            using var handler = ExecuteLibraryFunction(() =>
            {
                var mutationsBytes = mutations.ToByteArray();
                using var goMutations = DisposableGoSlice.Create(mutationsBytes);
                return SpannerLib.Apply(libConnection.LibPool.Id, libConnection.Id, goMutations.GoSlice);
            });
            return CommitResponse.Parser.ParseFrom(handler.Value());
        }

        internal static void BufferWrite(LibTransaction libTransaction, BatchWriteRequest.Types.MutationGroup mutations)
        {
            ExecuteAndReleaseLibraryFunction(() =>
            {
                var mutationsBytes = mutations.ToByteArray();
                using var goMutations = DisposableGoSlice.Create(mutationsBytes);
                return SpannerLib.BufferWrite(libTransaction.LibConnection.LibPool.Id,
                    libTransaction.LibConnection.Id, libTransaction.Id, goMutations.GoSlice);
            });
        }

        internal static LibRows Execute(LibConnection libConnection, ExecuteSqlRequest statement)
        {
            using var handler = ExecuteLibraryFunction(() =>
            {
                var statementBytes = statement.ToByteArray();
                using var goStatement = DisposableGoSlice.Create(statementBytes);
                return SpannerLib.Execute(libConnection.LibPool.Id, libConnection.Id, goStatement.GoSlice);
            });
            return new LibRows(libConnection, handler.ObjectId());
        }

        internal static LibRows ExecuteTransaction(LibTransaction libTransaction, ExecuteSqlRequest statement)
        {
            using var handler = ExecuteLibraryFunction(() =>
            {
                var statementBytes = statement.ToByteArray();
                using var goStatement = DisposableGoSlice.Create(statementBytes);
                return SpannerLib.ExecuteTransaction(
                    libTransaction.LibConnection.LibPool.Id, libTransaction.LibConnection.Id,
                    libTransaction.Id, goStatement.GoSlice);
            });
            return new LibRows(libTransaction.LibConnection, handler.ObjectId());
        }

        internal static long[] ExecuteBatchDml(LibConnection libConnection, ExecuteBatchDmlRequest statements)
        {
            using var handler = ExecuteLibraryFunction(() =>
            {
                var statementsBytes = statements.ToByteArray();
                using var goStatements = DisposableGoSlice.Create(statementsBytes);
                return SpannerLib.ExecuteBatchDml(libConnection.LibPool.Id, libConnection.Id, goStatements.GoSlice);
            });
            if (handler.Length == 0)
            {
                return Array.Empty<long>();
            }

            var response = ExecuteBatchDmlResponse.Parser.ParseFrom(handler.Value());
            var result = new long[response.ResultSets.Count];
            for (var i = 0; i < result.Length; i++)
            {
                result[i] = response.ResultSets[i].Stats.RowCountExact;
            }

            return result;
        }

        internal static ResultSetMetadata? Metadata(LibRows libRows)
        {
            using var handler = ExecuteLibraryFunction(() => SpannerLib.Metadata(libRows.LibConnection.LibPool.Id, libRows.LibConnection.Id, libRows.Id));
            return handler.Length == 0 ? null : ResultSetMetadata.Parser.ParseFrom(handler.Value());
        }

        internal static ResultSetStats? Stats(LibRows libRows)
        {
            using var handler = ExecuteLibraryFunction(() => SpannerLib.ResultSetStats(libRows.LibConnection.LibPool.Id, libRows.LibConnection.Id, libRows.Id));
            return handler.Length == 0 ? null : ResultSetStats.Parser.ParseFrom(handler.Value());
        }

        internal static ListValue? Next(LibRows libRows)
        {
            using var handler = ExecuteLibraryFunction(() => SpannerLib.Next(libRows.LibConnection.LibPool.Id, libRows.LibConnection.Id, libRows.Id));
            return handler.Length == 0 ? null : ListValue.Parser.ParseFrom(handler.Value());
        }

        internal static void CloseRows(LibRows libRows)
        {
            ExecuteAndReleaseLibraryFunction(() => SpannerLib.CloseRows(libRows.LibConnection.LibPool.Id, libRows.LibConnection.Id, libRows.Id));
        }

        internal static LibTransaction BeginTransaction(LibConnection libConnection, TransactionOptions transactionOptions)
        {
            using var handler = ExecuteLibraryFunction(() =>
            {
                var optionsBytes = transactionOptions.ToByteArray();
                using var goOptions = DisposableGoSlice.Create(optionsBytes);
                return SpannerLib.BeginTransaction(libConnection.LibPool.Id, libConnection.Id, goOptions.GoSlice);
            });
            return new LibTransaction(libConnection, handler.ObjectId());
        }

        internal static void Commit(LibTransaction libTransaction)
        {
            ExecuteAndReleaseLibraryFunction(() => SpannerLib.Commit(libTransaction.LibConnection.LibPool.Id, libTransaction.LibConnection.Id, libTransaction.Id));
        }

        internal static void Rollback(LibTransaction libTransaction)
        {
            ExecuteAndReleaseLibraryFunction(() => SpannerLib.Rollback(libTransaction.LibConnection.LibPool.Id, libTransaction.LibConnection.Id, libTransaction.Id));
        }
    }
}