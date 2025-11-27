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

using System;
using System.Threading;
using System.Threading.Tasks;
using Google.Cloud.Spanner.V1;
using Google.Protobuf;
using Google.Protobuf.WellKnownTypes;
using Google.Rpc;

namespace Google.Cloud.SpannerLib.Native.Impl;

/// <summary>
/// An implementation of the generic ISpannerLib interface that loads SpannerLib as a native library into this process.
/// </summary>
public class SharedLibSpanner : ISpannerLib
{
    private MessageHandler ExecuteLibraryFunction(Func<Message> func)
    {
        var handler = new MessageHandler(func());
        if (handler.HasError())
        {
            try
            {
                throw CreateException(handler);
            }
            finally
            {
                handler.Dispose();
            }
        }
        return handler;
    }

    private SpannerException CreateException(MessageHandler handler)
    {
        if (handler.Length > 0)
        {
            var status = Status.Parser.ParseFrom(handler.Value());
            return new SpannerException(status);
        }
        return new SpannerException(new Status {Code = handler.Code(), Message = "Unknown error"});
    }

    private void ExecuteAndReleaseLibraryFunction(Func<Message> func)
    {
        using var handler = new MessageHandler(func());
        if (handler.HasError())
        {
            throw CreateException(handler);
        }
    }

    public Pool CreatePool(string connectionString)
    {
        using var handler = ExecuteLibraryFunction(() =>
        {
            using var goDsn = new GoString(connectionString);
            return SpannerLib.CreatePool(goDsn);
        });
        return new Pool(this, handler.ObjectId());
    }

    public void ClosePool(Pool pool)
    {
        ExecuteAndReleaseLibraryFunction(() => SpannerLib.ClosePool(pool.Id));
    }

    public Connection CreateConnection(Pool pool)
    {
        using var handler = ExecuteLibraryFunction(() => SpannerLib.CreateConnection(pool.Id));
        return new Connection(pool, handler.ObjectId());
    }

    public void CloseConnection(Connection connection)
    {
        ExecuteAndReleaseLibraryFunction(() => SpannerLib.CloseConnection(connection.Pool.Id, connection.Id));
    }

    public CommitResponse? WriteMutations(Connection connection,
        BatchWriteRequest.Types.MutationGroup mutations)
    {
        using var handler = ExecuteLibraryFunction(() =>
        {
            var mutationsBytes = mutations.ToByteArray();
            using var goMutations = DisposableGoSlice.Create(mutationsBytes);
            return SpannerLib.WriteMutations(connection.Pool.Id, connection.Id, goMutations.GoSlice);
        });
        if (handler.Length == 0)
        {
            return null;
        }
        return CommitResponse.Parser.ParseFrom(handler.Value());
    }
    
    public Task<CommitResponse?> WriteMutationsAsync(Connection connection,
        BatchWriteRequest.Types.MutationGroup mutations, CancellationToken cancellationToken = default)
    {
        return Task.Run(() => WriteMutations(connection, mutations), cancellationToken);
    }

    public Rows Execute(Connection connection, ExecuteSqlRequest statement)
    {
        using var handler = ExecuteLibraryFunction(() =>
        {
            var statementBytes = statement.ToByteArray();
            using var goStatement = DisposableGoSlice.Create(statementBytes);
            return SpannerLib.Execute(connection.Pool.Id, connection.Id, goStatement.GoSlice);
        });
        return new Rows(connection, handler.ObjectId());
    }

    public Task<Rows> ExecuteAsync(Connection connection, ExecuteSqlRequest statement, CancellationToken cancellationToken)
    {
        return Task.Run(() => Execute(connection, statement), cancellationToken);
    }

    public long[] ExecuteBatch(Connection connection, ExecuteBatchDmlRequest statements)
    {
        using var handler = ExecuteLibraryFunction(() =>
        {
            var statementsBytes = statements.ToByteArray();
            using var goStatements = DisposableGoSlice.Create(statementsBytes);
            return SpannerLib.ExecuteBatch(connection.Pool.Id, connection.Id, goStatements.GoSlice);
        });
        if (handler.Length == 0)
        {
            return [];
        }

        var response = ExecuteBatchDmlResponse.Parser.ParseFrom(handler.Value());
        var result = new long[response.ResultSets.Count];
        for (var i = 0; i < result.Length; i++)
        {
            if (response.ResultSets[i].Stats.HasRowCountExact)
            {
                result[i] = response.ResultSets[i].Stats.RowCountExact;
            }
            else if (response.ResultSets[i].Stats.HasRowCountLowerBound)
            {
                result[i] = response.ResultSets[i].Stats.RowCountLowerBound;
            }
            else
            {
                result[i] = -1;
            }
        }

        return result;
    }

    public Task<long[]> ExecuteBatchAsync(Connection connection, ExecuteBatchDmlRequest statements, CancellationToken cancellationToken = default)
    {
        return Task.Run(() => ExecuteBatch(connection, statements),  cancellationToken);
    }

    public ResultSetMetadata? Metadata(Rows rows)
    {
        using var handler = ExecuteLibraryFunction(() => SpannerLib.Metadata(rows.SpannerConnection.Pool.Id, rows.SpannerConnection.Id, rows.Id));
        return handler.Length == 0 ? null : ResultSetMetadata.Parser.ParseFrom(handler.Value());
    }

    public async Task<ResultSetMetadata?> MetadataAsync(Rows rows, CancellationToken cancellationToken = default)
    {
        return await Task.Run(() => Metadata(rows), cancellationToken);
    }

    public ResultSetMetadata? NextResultSet(Rows rows)
    {
        using var handler = ExecuteLibraryFunction(() => SpannerLib.NextResultSet(rows.SpannerConnection.Pool.Id, rows.SpannerConnection.Id, rows.Id));
        return handler.Length == 0 ? null : ResultSetMetadata.Parser.ParseFrom(handler.Value());
    }

    public async Task<ResultSetMetadata?> NextResultSetAsync(Rows rows, CancellationToken cancellationToken = default)
    {
        return await Task.Run(() => NextResultSet(rows), cancellationToken);
    }

    public ResultSetStats? Stats(Rows rows)
    {
        using var handler = ExecuteLibraryFunction(() => SpannerLib.ResultSetStats(rows.SpannerConnection.Pool.Id, rows.SpannerConnection.Id, rows.Id));
        return handler.Length == 0 ? null : ResultSetStats.Parser.ParseFrom(handler.Value());
    }

    public ListValue? Next(Rows rows, int numRows, ISpannerLib.RowEncoding encoding)
    {
        using var handler = ExecuteLibraryFunction(() => SpannerLib.Next(rows.SpannerConnection.Pool.Id, rows.SpannerConnection.Id, rows.Id, numRows, (int) encoding));
        return handler.Length == 0 ? null : ListValue.Parser.ParseFrom(handler.Value());
    }

    public async Task<ListValue?> NextAsync(Rows rows, int numRows, ISpannerLib.RowEncoding encoding, CancellationToken cancellationToken = default)
    {
        return await Task.Run(() => Next(rows, numRows, encoding), cancellationToken);
    }

    public void CloseRows(Rows rows)
    {
        ExecuteAndReleaseLibraryFunction(() => SpannerLib.CloseRows(rows.SpannerConnection.Pool.Id, rows.SpannerConnection.Id, rows.Id));
    }

    public Task CloseRowsAsync(Rows rows, CancellationToken cancellationToken = default)
    {
        return Task.Run(() => CloseRows(rows), cancellationToken);
    }

    public void BeginTransaction(Connection connection, TransactionOptions transactionOptions)
    {
        using var handler = ExecuteLibraryFunction(() =>
        {
            var optionsBytes = transactionOptions.ToByteArray();
            using var goOptions = DisposableGoSlice.Create(optionsBytes);
            return SpannerLib.BeginTransaction(connection.Pool.Id, connection.Id, goOptions.GoSlice);
        });
    }

    public CommitResponse? Commit(Connection connection)
    {
        using var handler = ExecuteLibraryFunction(() => SpannerLib.Commit(connection.Pool.Id, connection.Id));
        return handler.Length == 0 ? null : CommitResponse.Parser.ParseFrom(handler.Value());
    }

    public Task<CommitResponse?> CommitAsync(Connection connection, CancellationToken cancellationToken = default)
    {
        return Task.Run(() => Commit(connection), cancellationToken);
    }

    public void Rollback(Connection connection)
    {
        ExecuteAndReleaseLibraryFunction(() => SpannerLib.Rollback(connection.Pool.Id, connection.Id));
    }

    public Task RollbackAsync(Connection connection, CancellationToken cancellationToken = default)
    {
        return Task.Run(() => Rollback(connection), cancellationToken);
    }
}
