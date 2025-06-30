using System;
using Google.Cloud.Spanner.V1;
using Google.Cloud.SpannerLib.Native;
using Google.Protobuf;
using Google.Protobuf.WellKnownTypes;

namespace Google.Cloud.SpannerLib
{

    internal static class Spanner
    {
        private static MessageHandler ExecuteLibraryFunction(Func<Message> func)
        {
            var handler = new MessageHandler(func());
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
            using var handler = new MessageHandler(func());
            if (handler.HasError())
            {
                throw new SpannerException(handler.Code(), handler.Error()!);
            }
        }

        internal static Pool CreatePool(string dsn)
        {
            using var handler = ExecuteLibraryFunction(() =>
            {
                using var goDsn = new GoString(dsn);
                return Native.SpannerLib.CreatePool(goDsn);
            });
            return new Pool(handler.ObjectId());
        }

        internal static void ClosePool(Pool pool)
        {
            ExecuteAndReleaseLibraryFunction(() => Native.SpannerLib.ClosePool(pool.Id));
        }

        internal static Connection CreateConnection(Pool pool)
        {
            using var handler = ExecuteLibraryFunction(() => Native.SpannerLib.CreateConnection(pool.Id));
            return new Connection(pool, handler.ObjectId());
        }

        internal static void CloseConnection(Connection connection)
        {
            ExecuteAndReleaseLibraryFunction(() => Native.SpannerLib.CloseConnection(connection.Pool.Id, connection.Id));
        }

        internal static CommitResponse Apply(Connection connection,
            BatchWriteRequest.Types.MutationGroup mutations)
        {
            using var handler = ExecuteLibraryFunction(() =>
            {
                var mutationsBytes = mutations.ToByteArray();
                using var goMutations = DisposableGoSlice.Create(mutationsBytes);
                return Native.SpannerLib.Apply(connection.Pool.Id, connection.Id, goMutations.GoSlice);
            });
            return CommitResponse.Parser.ParseFrom(handler.Value());
        }

        internal static void BufferWrite(Transaction transaction, BatchWriteRequest.Types.MutationGroup mutations)
        {
            ExecuteAndReleaseLibraryFunction(() =>
            {
                var mutationsBytes = mutations.ToByteArray();
                using var goMutations = DisposableGoSlice.Create(mutationsBytes);
                return Native.SpannerLib.BufferWrite(transaction.Connection.Pool.Id,
                    transaction.Connection.Id, transaction.Id, goMutations.GoSlice);
            });
        }

        internal static Rows Execute(Connection connection, ExecuteSqlRequest statement)
        {
            using var handler = ExecuteLibraryFunction(() =>
            {
                var statementBytes = statement.ToByteArray();
                using var goStatement = DisposableGoSlice.Create(statementBytes);
                return Native.SpannerLib.Execute(connection.Pool.Id, connection.Id, goStatement.GoSlice);
            });
            return new Rows(connection, handler.ObjectId());
        }

        internal static Rows ExecuteTransaction(Transaction transaction, ExecuteSqlRequest statement)
        {
            using var handler = ExecuteLibraryFunction(() =>
            {
                var statementBytes = statement.ToByteArray();
                using var goStatement = DisposableGoSlice.Create(statementBytes);
                return Native.SpannerLib.ExecuteTransaction(
                    transaction.Connection.Pool.Id, transaction.Connection.Id,
                    transaction.Id, goStatement.GoSlice);
            });
            return new Rows(transaction.Connection, handler.ObjectId());
        }

        internal static long[] ExecuteBatchDml(Connection connection, ExecuteBatchDmlRequest statements)
        {
            using var handler = ExecuteLibraryFunction(() =>
            {
                var statementsBytes = statements.ToByteArray();
                using var goStatements = DisposableGoSlice.Create(statementsBytes);
                return Native.SpannerLib.ExecuteBatchDml(connection.Pool.Id, connection.Id, goStatements.GoSlice);
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

        internal static ResultSetMetadata? Metadata(Rows rows)
        {
            using var handler = ExecuteLibraryFunction(() => Native.SpannerLib.Metadata(rows.Connection.Pool.Id, rows.Connection.Id, rows.Id));
            return handler.Length == 0 ? null : ResultSetMetadata.Parser.ParseFrom(handler.Value());
        }

        internal static ResultSetStats? Stats(Rows rows)
        {
            using var handler = ExecuteLibraryFunction(() => Native.SpannerLib.ResultSetStats(rows.Connection.Pool.Id, rows.Connection.Id, rows.Id));
            return handler.Length == 0 ? null : ResultSetStats.Parser.ParseFrom(handler.Value());
        }

        internal static ListValue? Next(Rows rows)
        {
            using var handler = ExecuteLibraryFunction(() => Native.SpannerLib.Next(rows.Connection.Pool.Id, rows.Connection.Id, rows.Id));
            return handler.Length == 0 ? null : ListValue.Parser.ParseFrom(handler.Value());
        }

        internal static void CloseRows(Rows rows)
        {
            ExecuteAndReleaseLibraryFunction(() => Native.SpannerLib.CloseRows(rows.Connection.Pool.Id, rows.Connection.Id, rows.Id));
        }

        internal static Transaction BeginTransaction(Connection connection, TransactionOptions transactionOptions)
        {
            using var handler = ExecuteLibraryFunction(() =>
            {
                var optionsBytes = transactionOptions.ToByteArray();
                using var goOptions = DisposableGoSlice.Create(optionsBytes);
                return Native.SpannerLib.BeginTransaction(connection.Pool.Id, connection.Id, goOptions.GoSlice);
            });
            return new Transaction(connection, handler.ObjectId());
        }

        internal static CommitResponse Commit(Transaction transaction)
        {
            using var handler = ExecuteLibraryFunction(() => Native.SpannerLib.Commit(transaction.Connection.Pool.Id, transaction.Connection.Id, transaction.Id));
            return CommitResponse.Parser.ParseFrom(handler.Value());
        }

        internal static void Rollback(Transaction transaction)
        {
            ExecuteAndReleaseLibraryFunction(() => Native.SpannerLib.Rollback(transaction.Connection.Pool.Id, transaction.Connection.Id, transaction.Id));
        }
    }
}