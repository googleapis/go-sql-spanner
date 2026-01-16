using System;
using System.Collections.Generic;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;
using Google.Api.Gax;
using Google.Cloud.Spanner.V1;
using Google.Protobuf.WellKnownTypes;
using Grpc.Core;

namespace Google.Cloud.SpannerLib.SocketServer;

public sealed class SocketLibSpanner : ISpannerLib
{
    private readonly Server _server;
    private readonly string _address;
    private bool _disposed;
    private readonly Dictionary<Pool, PoolImpl> _pools = new();
    private readonly Dictionary<Connection, ConnectionImpl> _connections = new();

    public SocketLibSpanner(Server.AddressType addressType = Server.AddressType.UnixDomainSocket)
    {
        _server = new Server();
        _address = _server.Start(addressType: addressType);
    }
    
    ~SocketLibSpanner() => Dispose(false);
    
    public void Dispose()
    {
        Dispose(true);
        GC.SuppressFinalize(this);
    }

    private void Dispose(bool disposing)
    {
        if (_disposed)
        {
            return;
        }
        try
        {
            _server.Dispose();
        }
        finally
        {
            _disposed = true;
        }
    }

    internal Socket CreateSocket()
    {
        var socket = new Socket(AddressFamily.Unix, SocketType.Stream, ProtocolType.Unspecified);
        var endpoint = new UnixDomainSocketEndPoint(_address);
        socket.Connect(endpoint);
        return socket;
    }

    internal async Task<Socket> CreateSocketAsync()
    {
        var socket = new Socket(AddressFamily.Unix, SocketType.Stream, ProtocolType.Unspecified);
        var endpoint = new UnixDomainSocketEndPoint(_address);
        await socket.ConnectAsync(endpoint).ConfigureAwait(false);
        return socket;
    }
    
    public Pool CreatePool(string connectionString)
    {
        var pool = new PoolImpl(this, 0, connectionString);
        _pools.Add(pool, pool);
        return pool;
    }

    public void ClosePool(Pool pool)
    {
        _pools.Remove(pool);
    }

    public Connection CreateConnection(Pool pool)
    {
        var connection = ConnectionImpl.Create(_pools[pool]);
        _connections.Add(connection, connection);
        return connection;
    }

    public void CloseConnection(Connection connection)
    {
        _connections.Remove(connection);
    }

    public Task CloseConnectionAsync(Connection connection, CancellationToken cancellationToken =  default)
    {
        _connections.Remove(connection);
        return Task.CompletedTask;
    }

    public CommitResponse? WriteMutations(Connection connection, BatchWriteRequest.Types.MutationGroup mutations)
    {
        throw new NotImplementedException();
    }
    
    public Task<CommitResponse?> WriteMutationsAsync(Connection connection,
        BatchWriteRequest.Types.MutationGroup mutations, CancellationToken cancellationToken = default)
    {
        throw new NotImplementedException();
    }
    
    public Rows Execute(Connection connection, ExecuteSqlRequest statement, int prefetchRows = 0)
    {
        return ExecuteStreaming(connection, statement, prefetchRows);
    }

    public async Task<Rows> ExecuteAsync(Connection connection, ExecuteSqlRequest statement, int prefetchRows = 0, CancellationToken cancellationToken = default)
    {
        try
        {
            return await ExecuteStreamingAsync(connection, statement, prefetchRows, cancellationToken).ConfigureAwait(false);
        }
        catch (RpcException exception)
        {
            throw SpannerException.ToSpannerException(exception);
        }
    }

    private async Task<StreamingRows> ExecuteStreamingAsync(Connection connection, ExecuteSqlRequest statement, int prefetchRows, CancellationToken cancellationToken = default)
    {
        var client = _clients[Random.Shared.Next(_clients.Length)];
        var stream = TranslateException(() => client.ExecuteStreaming(new ExecuteRequest
        {
            Connection = ToProto(connection),
            ExecuteSqlRequest = statement,
            FetchOptions = new FetchOptions
            {
                NumRows = prefetchRows,
            },
        }));
        return await StreamingRows.CreateAsync(this, connection, stream, cancellationToken).ConfigureAwait(false);
    }
    
    internal AsyncServerStreamingCall<RowData> ContinueStreamingAsync(Connection connection, long rowsId, CancellationToken cancellationToken)
    {
        var client = _clients[Random.Shared.Next(_clients.Length)];
        return TranslateException(() => client.ContinueStreaming(new V1.Rows
        {
            Connection = ToProto(connection),
            Id = rowsId,
        },  cancellationToken: cancellationToken));
    }

    public long[] ExecuteBatch(Connection connection, ExecuteBatchDmlRequest statements)
    {
        var response = TranslateException(() => Client.ExecuteBatch(new ExecuteBatchRequest
        {
            Connection = ToProto(connection),
            ExecuteBatchDmlRequest = statements,
        }));
        return ISpannerLib.ToUpdateCounts(response);
    }

    public async Task<long[]> ExecuteBatchAsync(Connection connection, ExecuteBatchDmlRequest statements, CancellationToken cancellationToken = default)
    {
        try
        {
            var response = await Client.ExecuteBatchAsync(new ExecuteBatchRequest
            {
                Connection = ToProto(connection),
                ExecuteBatchDmlRequest = statements,
            }, cancellationToken: cancellationToken).ConfigureAwait(false);
            return ISpannerLib.ToUpdateCounts(response);
        }
        catch (RpcException exception)
        {
            throw SpannerException.ToSpannerException(exception);
        }
    }

    public ResultSetMetadata? Metadata(Rows rows)
    {
        return TranslateException(() => Client.Metadata(ToProto(rows)));
    }

    public async Task<ResultSetMetadata?> MetadataAsync(Rows rows, CancellationToken cancellationToken = default)
    {
        try
        {
            return await Client.MetadataAsync(ToProto(rows), cancellationToken: cancellationToken).ConfigureAwait(false);
        }
        catch (RpcException exception)
        {
            throw SpannerException.ToSpannerException(exception);
        }
    }

    public ResultSetMetadata? NextResultSet(Rows rows)
    {
        return TranslateException(() => Client.NextResultSet(ToProto(rows)));
    }

    public async Task<ResultSetMetadata?> NextResultSetAsync(Rows rows, CancellationToken cancellationToken = default)
    {
        try
        {
            return await Client.NextResultSetAsync(ToProto(rows), cancellationToken: cancellationToken).ConfigureAwait(false);
        }
        catch (RpcException exception)
        {
            throw SpannerException.ToSpannerException(exception);
        }
    }

    public ResultSetStats? Stats(Rows rows)
    {
        return TranslateException(() => Client.ResultSetStats(ToProto(rows)));
    }

    public ListValue? Next(Rows rows, int numRows, ISpannerLib.RowEncoding encoding)
    {
        var row = TranslateException(() =>Client.Next(new NextRequest
        {
            Rows = ToProto(rows),
            FetchOptions = new FetchOptions
            {
                NumRows = numRows,
                Encoding = (long) encoding,
            },
        }));
        return row.Values.Count == 0 ? null : row;
    }

    public async Task<ListValue?> NextAsync(Rows rows, int numRows, ISpannerLib.RowEncoding encoding, CancellationToken cancellationToken = default)
    {
        try
        {
            return await Client.NextAsync(new NextRequest
            {
                Rows = ToProto(rows),
                FetchOptions = new FetchOptions
                {
                    NumRows = numRows,
                    Encoding = (long) encoding,
                },
            }, cancellationToken: cancellationToken).ConfigureAwait(false);
        }
        catch (RpcException exception)
        {
            throw SpannerException.ToSpannerException(exception);
        }
    }

    public void CloseRows(Rows rows)
    {
        TranslateException(() => Client.CloseRows(ToProto(rows)));
    }

    public async Task CloseRowsAsync(Rows rows, CancellationToken cancellationToken = default)
    {
        try
        {
            await Client.CloseRowsAsync(ToProto(rows), cancellationToken: cancellationToken).ConfigureAwait(false);
        }
        catch (RpcException exception)
        {
            throw SpannerException.ToSpannerException(exception);
        }
    }

    public void BeginTransaction(Connection connection, TransactionOptions transactionOptions)
    {
        TranslateException(() => Client.BeginTransaction(new BeginTransactionRequest
        {
            Connection = ToProto(connection),
            TransactionOptions = transactionOptions,
        }));
    }
    
    public async Task BeginTransactionAsync(Connection connection, TransactionOptions transactionOptions, CancellationToken cancellationToken = default)
    {
        await TranslateException(() => Client.BeginTransactionAsync(new BeginTransactionRequest
        {
            Connection = ToProto(connection),
            TransactionOptions = transactionOptions,
        })).ConfigureAwait(false);
    }
    
    public CommitResponse? Commit(Connection connection)
    {
        var response = TranslateException(() => Client.Commit(ToProto(connection)));
        return response.CommitTimestamp == null ? null : response;
    }

    public async Task<CommitResponse?> CommitAsync(Connection connection, CancellationToken cancellationToken = default)
    {
        try
        {
            var response = await Client.CommitAsync(ToProto(connection), cancellationToken: cancellationToken).ConfigureAwait(false);
            return response.CommitTimestamp == null ? null : response;
        }
        catch (RpcException exception)
        {
            throw new SpannerException(new Status { Code = (int) exception.Status.StatusCode, Message = exception.Status.Detail });
        }
    }

    public void Rollback(Connection connection)
    {
        TranslateException(() => Client.Rollback(ToProto(connection)));
    }

    public async Task RollbackAsync(Connection connection, CancellationToken cancellationToken = default)
    {
        try
        {
            await Client.RollbackAsync(ToProto(connection), cancellationToken:  cancellationToken).ConfigureAwait(false);
        }
        catch (RpcException exception)
        {
            throw SpannerException.ToSpannerException(exception);
        }
    }

    private PoolImpl FromProto(V1.Pool pool) => new(this, pool.Id);

    private static V1.Pool ToProto(PoolImpl poolImpl) => new() { Id = poolImpl.Id };

    private Connection FromProto(PoolImpl poolImpl, V1.Connection proto) =>
        _communicationStyle == CommunicationStyle.ServerStreaming
            ? new Connection(poolImpl, proto.Id)
            : new GrpcBidiConnection(this, poolImpl, proto.Id);

    internal static V1.Connection ToProto(Connection connection) => new() { Id = connection.Id, Pool = ToProto(connection.Pool), };

    private V1.Rows ToProto(Rows rows) => new() { Id = rows.Id, Connection = ToProto(rows.SpannerConnection), };

}