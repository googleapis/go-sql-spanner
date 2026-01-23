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
using System.Net.Http;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;
using Google.Api.Gax;
using Google.Cloud.Spanner.V1;
using Google.Cloud.SpannerLib.V1;
using Google.Protobuf.WellKnownTypes;
using Grpc.Core;
using Grpc.Net.Client;
using BeginTransactionRequest = Google.Cloud.SpannerLib.V1.BeginTransactionRequest;
using Status = Google.Rpc.Status;

namespace Google.Cloud.SpannerLib.Grpc;

public sealed class GrpcLibSpanner : ISpannerLib
{
    
    /// <summary>
    /// Creates a GrpcChannel that uses a Unix domain socket with the given file name.
    /// </summary>
    /// <param name="fileName">The file to use for communication</param>
    /// <returns>A GrpcChannel over a Unix domain socket with the given file name</returns>
    public static GrpcChannel ForUnixSocket(string fileName)
    {
        var endpoint = new UnixDomainSocketEndPoint(fileName);
        return GrpcChannel.ForAddress("http://localhost", new GrpcChannelOptions {
            HttpHandler = new SocketsHttpHandler {
                EnableMultipleHttp2Connections = true,
                ConnectCallback = async (_, cancellationToken) => {
                    var socket = new Socket(AddressFamily.Unix, SocketType.Stream, ProtocolType.Unspecified);
                    try {
                        await socket.ConnectAsync(endpoint, cancellationToken).ConfigureAwait(false);
                        return new NetworkStream(socket, true);
                    } catch {
                        socket.Dispose();
                        throw;
                    }
                }
            },
            MaxReceiveMessageSize = null,
        });
    }

    /// <summary>
    /// Creates a GrpcChannel that connects to the given IP address or host name. The GrpcChannel uses a TCP socket for
    /// communication. The communication does not use encryption.
    /// </summary>
    /// <param name="address">The IP address or host name that the channel should connect to</param>
    /// <returns>A GrpcChannel using a TCP socket for communication</returns>
    public static GrpcChannel ForTcpSocket(string address)
    {
        return GrpcChannel.ForAddress($"http://{address}", new GrpcChannelOptions
        {
            HttpHandler = new SocketsHttpHandler
            {
                EnableMultipleHttp2Connections = true,
            },
            MaxReceiveMessageSize = null,
        });
    }
    
    /// <summary>
    /// This enum is used to configure a connection to use either a single bidirectional gRPC stream for all requests
    /// that are sent to SpannerLib, or to use server streaming RPCs for queries and unary RPCs for all other
    /// operations.
    ///
    /// NOTE: This enum is likely to be removed in a future version.
    /// </summary>
    public enum CommunicationStyle
    {
        ServerStreaming,
        BidiStreaming,
    }
    
    private readonly Server _server;
    private readonly V1.SpannerLib.SpannerLibClient[] _clients;
    private readonly GrpcChannel[] _channels;
    private readonly CommunicationStyle _communicationStyle;
    private bool _disposed;
    
    internal V1.SpannerLib.SpannerLibClient Client => _clients[Random.Shared.Next(_clients.Length)];

    public GrpcLibSpanner(
        CommunicationStyle communicationStyle = CommunicationStyle.BidiStreaming,
        int numChannels = 4,
        Server.AddressType addressType = Server.AddressType.UnixDomainSocket)
    {
        GaxPreconditions.CheckArgument(numChannels > 0, nameof(numChannels), "numChannels must be > 0");
        _server = new Server();
        var file = _server.Start(addressType: addressType);
        _communicationStyle = communicationStyle;

        _channels = new GrpcChannel[numChannels];
        _clients = new V1.SpannerLib.SpannerLibClient[numChannels];
        for (var i = 0; i < numChannels; i++)
        {
            _channels[i] = addressType == Server.AddressType.Tcp ? ForTcpSocket(file) : ForUnixSocket(file);
            _clients[i] = new V1.SpannerLib.SpannerLibClient(_channels[i]);
        }
    }
    
    ~GrpcLibSpanner() => Dispose(false);
    
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
            foreach (var channel in _channels)
            {
                channel.Dispose();
            }
            _server.Dispose();
        }
        finally
        {
            _disposed = true;
        }
    }

    T TranslateException<T>(Func<T> f)
    {
        try
        {
            return f.Invoke();
        }
        catch (RpcException exception)
        {
            throw SpannerException.ToSpannerException(exception);
        }
    }
    
    public Pool CreatePool(string connectionString)
    {
        return FromProto(TranslateException(() => Client.CreatePool(new CreatePoolRequest
        {
            ConnectionString = connectionString,
        })));
    }

    public void ClosePool(Pool pool)
    {
        TranslateException(() => Client.ClosePool(ToProto(pool)));
    }

    public Connection CreateConnection(Pool pool)
    {
        return FromProto(pool, TranslateException(() => Client.CreateConnection(new CreateConnectionRequest
        {
            Pool = ToProto(pool),
        })));
    }

    public void CloseConnection(Connection connection)
    {
        TranslateException(() => Client.CloseConnection(ToProto(connection)));
    }

    public async Task CloseConnectionAsync(Connection connection, CancellationToken cancellationToken =  default)
    {
        try
        {
            await Client.CloseConnectionAsync(ToProto(connection), cancellationToken: cancellationToken).ConfigureAwait(false);
        }
        catch (RpcException exception)
        {
            throw SpannerException.ToSpannerException(exception);
        }
    }

    public CommitResponse? WriteMutations(Connection connection, BatchWriteRequest.Types.MutationGroup mutations)
    {
        var response = TranslateException(() => Client.WriteMutations(new WriteMutationsRequest
        {
            Connection = ToProto(connection),
            Mutations = mutations,
        }));
        return response.CommitTimestamp == null ? null : response;
    }
    
    public async Task<CommitResponse?> WriteMutationsAsync(Connection connection,
        BatchWriteRequest.Types.MutationGroup mutations, CancellationToken cancellationToken = default)
    {
        try
        {
            var response = await Client.WriteMutationsAsync(new WriteMutationsRequest
            {
                Connection = ToProto(connection),
                Mutations = mutations,
            }, cancellationToken: cancellationToken).ConfigureAwait(false);
            return response.CommitTimestamp == null ? null : response;
        }
        catch (RpcException exception)
        {
            throw SpannerException.ToSpannerException(exception);
        }
    }
    
    public Rows Execute(Connection connection, ExecuteSqlRequest statement, int prefetchRows = 0)
    {
        return ExecuteStreaming(connection, statement, prefetchRows);
    }

    private StreamingRows ExecuteStreaming(Connection connection, ExecuteSqlRequest statement, int prefetchRows)
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
        return StreamingRows.Create(this, connection, stream);
    }
    
    internal AsyncServerStreamingCall<RowData> ContinueStreaming(Connection connection, long rowsId)
    {
        var client = _clients[Random.Shared.Next(_clients.Length)];
        return TranslateException(() => client.ContinueStreaming(new V1.Rows
        {
            Connection = ToProto(connection),
            Id = rowsId,
        }));
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
        try
        {
            await Client.BeginTransactionAsync(new BeginTransactionRequest
            {
                Connection = ToProto(connection),
                TransactionOptions = transactionOptions,
            }, cancellationToken: cancellationToken).ConfigureAwait(false);
        }
        catch (RpcException exception)
        {
            throw SpannerException.ToSpannerException(exception);
        }
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

    private Pool FromProto(V1.Pool pool) => new(this, pool.Id);

    private static V1.Pool ToProto(Pool pool) => new() { Id = pool.Id };

    private Connection FromProto(Pool pool, V1.Connection proto) =>
        _communicationStyle == CommunicationStyle.ServerStreaming
            ? new Connection(pool, proto.Id)
            : new GrpcBidiConnection(this, pool, proto.Id);

    internal static V1.Connection ToProto(Connection connection) => new() { Id = connection.Id, Pool = ToProto(connection.Pool), };

    private V1.Rows ToProto(Rows rows) => new() { Id = rows.Id, Connection = ToProto(rows.SpannerConnection), };
}