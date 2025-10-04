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
using Google.Cloud.Spanner.V1;
using Google.Cloud.SpannerLib.V1;
using Google.Protobuf.WellKnownTypes;
using Grpc.Core;
using Grpc.Net.Client;
using BeginTransactionRequest = Google.Cloud.SpannerLib.V1.BeginTransactionRequest;
using Status = Google.Rpc.Status;

namespace Google.Cloud.SpannerLib.Grpc;

public class GrpcLibSpanner : ISpannerLib
{
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
            }
        });
    }

    public static GrpcChannel ForTcpSocket(string address)
    {
        return GrpcChannel.ForAddress($"http://{address}", new GrpcChannelOptions
        {
            HttpHandler = new SocketsHttpHandler
            {
                EnableMultipleHttp2Connections = true,
            }
        });
    }
    
    private readonly Server _server;
    private readonly V1.SpannerLib.SpannerLibClient _client;
    private readonly GrpcChannel _channel;
    private readonly V1.SpannerLib.SpannerLibClient[] _clients;
    private readonly GrpcChannel[] _channels;
    private readonly bool _useStreamingRows;
    private bool _disposed;

    public GrpcLibSpanner(bool useStreamingRows = true, Server.AddressType addressType = Server.AddressType.UnixDomainSocket)
    {
        _server = new Server();
        var file = _server.Start(addressType: addressType);
        _channel = addressType == Server.AddressType.Tcp ? ForTcpSocket(file) : ForUnixSocket(file);
        _client = new V1.SpannerLib.SpannerLibClient(_channel);
        _useStreamingRows = useStreamingRows;

        var numChannels = 1;
        _channels = new GrpcChannel[numChannels];
        _clients = new V1.SpannerLib.SpannerLibClient[numChannels];
        for (var i = 0; i < numChannels; i++)
        {
            _channels[i] = addressType == Server.AddressType.Tcp ? ForTcpSocket(file) : ForUnixSocket(file);
            _clients[i] = new V1.SpannerLib.SpannerLibClient(_channels[i]);
        }
    }
    
    public void Dispose()
    {
        Dispose(true);
        GC.SuppressFinalize(this);
    }

    protected virtual void Dispose(bool disposing)
    {
        if (_disposed)
        {
            return;
        }
        try
        {
            _channel.Dispose();
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
        return FromProto(TranslateException(() => _client.CreatePool(new CreatePoolRequest
        {
            ConnectionString = connectionString,
        })));
    }

    public void ClosePool(Pool pool)
    {
        TranslateException(() => _client.ClosePool(ToProto(pool)));
    }

    public Connection CreateConnection(Pool pool)
    {
        return FromProto(pool, TranslateException(() => _client.CreateConnection(new CreateConnectionRequest
        {
            Pool = ToProto(pool),
        })));
    }

    public void CloseConnection(Connection connection)
    {
        TranslateException(() => _client.CloseConnection(ToProto(connection)));
    }

    public async Task CloseConnectionAsync(Connection connection, CancellationToken cancellationToken =  default)
    {
        try
        {
            await _client.CloseConnectionAsync(ToProto(connection), cancellationToken: cancellationToken);
        }
        catch (RpcException exception)
        {
            throw SpannerException.ToSpannerException(exception);
        }
    }

    public CommitResponse? WriteMutations(Connection connection, BatchWriteRequest.Types.MutationGroup mutations)
    {
        var response = TranslateException(() => _client.WriteMutations(new WriteMutationsRequest
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
            var response = await _client.WriteMutationsAsync(new WriteMutationsRequest
            {
                Connection = ToProto(connection),
                Mutations = mutations,
            });
            return response.CommitTimestamp == null ? null : response;
        }
        catch (RpcException exception)
        {
            throw SpannerException.ToSpannerException(exception);
        }
    }

    public Rows Execute(Connection connection, ExecuteSqlRequest statement)
    {
        if (_useStreamingRows) 
        {
            return ExecuteStreaming(connection, statement);
        }
        return FromProto(connection, TranslateException(() => _client.Execute(new ExecuteRequest
        {
            Connection = ToProto(connection),
            ExecuteSqlRequest = statement,
        })));
    }

    private StreamingRows ExecuteStreaming(Connection connection, ExecuteSqlRequest statement)
    {
        var client = _clients[Random.Shared.Next(_clients.Length)];
        var stream = TranslateException(() => client.ExecuteStreaming(new ExecuteRequest
        {
            Connection = ToProto(connection),
            ExecuteSqlRequest = statement,
        }));
        return StreamingRows.Create(connection, stream);
    }

    public async Task<Rows> ExecuteAsync(Connection connection, ExecuteSqlRequest statement, CancellationToken cancellationToken = default)
    {
        try
        {
            if (_useStreamingRows)
            {
                return await ExecuteStreamingAsync(connection, statement, cancellationToken);
            }
            var rows = await _client.ExecuteAsync(new ExecuteRequest
            {
                Connection = ToProto(connection),
                ExecuteSqlRequest = statement,
            }, cancellationToken: cancellationToken);
            return FromProto(connection, rows);
        }
        catch (RpcException exception)
        {
            throw SpannerException.ToSpannerException(exception);
        }
    }

    private async Task<StreamingRows> ExecuteStreamingAsync(Connection connection, ExecuteSqlRequest statement, CancellationToken cancellationToken = default)
    {
        var client = _clients[Random.Shared.Next(_clients.Length)];
        var stream = TranslateException(() => client.ExecuteStreaming(new ExecuteRequest
        {
            Connection = ToProto(connection),
            ExecuteSqlRequest = statement,
        }));
        return await StreamingRows.CreateAsync(connection, stream, cancellationToken);
    }

    public long[] ExecuteBatch(Connection connection, ExecuteBatchDmlRequest statements)
    {
        var response = TranslateException(() => _client.ExecuteBatch(new ExecuteBatchRequest
        {
            Connection = ToProto(connection),
            ExecuteBatchDmlRequest = statements,
        }));
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

    public async Task<long[]> ExecuteBatchAsync(Connection connection, ExecuteBatchDmlRequest statements, CancellationToken cancellationToken = default)
    {
        try
        {
            var stats = await _client.ExecuteBatchAsync(new ExecuteBatchRequest
            {
                Connection = ToProto(connection),
                ExecuteBatchDmlRequest = statements,
            }, cancellationToken: cancellationToken);
            var result = new long[stats.ResultSets.Count];
            for (var i = 0; i < result.Length; i++)
            {
                result[i] = stats.ResultSets[i].Stats.RowCountExact;
            }
            return result;
        }
        catch (RpcException exception)
        {
            throw SpannerException.ToSpannerException(exception);
        }
    }

    public ResultSetMetadata? Metadata(Rows rows)
    {
        return TranslateException(() => _client.Metadata(ToProto(rows)));
    }

    public async Task<ResultSetMetadata?> MetadataAsync(Rows rows, CancellationToken cancellationToken = default)
    {
        try
        {
            return await _client.MetadataAsync(ToProto(rows), cancellationToken: cancellationToken);
        }
        catch (RpcException exception)
        {
            throw SpannerException.ToSpannerException(exception);
        }
    }

    public ResultSetStats? Stats(Rows rows)
    {
        return TranslateException(() => _client.ResultSetStats(ToProto(rows)));
    }

    public ListValue? Next(Rows rows, int numRows, ISpannerLib.RowEncoding encoding)
    {
        var row = TranslateException(() =>_client.Next(new NextRequest
        {
            Rows = ToProto(rows),
            NumRows = numRows,
            Encoding = (long) encoding,
        }));
        return row.Values.Count == 0 ? null : row;
    }

    public async Task<ListValue?> NextAsync(Rows rows, int numRows, ISpannerLib.RowEncoding encoding, CancellationToken cancellationToken = default)
    {
        try
        {
            return await _client.NextAsync(new NextRequest
            {
                Rows = ToProto(rows),
                NumRows = numRows,
                Encoding = (long)encoding,
            }, cancellationToken: cancellationToken);
        }
        catch (RpcException exception)
        {
            throw SpannerException.ToSpannerException(exception);
        }
    }

    public void CloseRows(Rows rows)
    {
        TranslateException(() => _client.CloseRows(ToProto(rows)));
    }

    public async Task CloseRowsAsync(Rows rows, CancellationToken cancellationToken = default)
    {
        try
        {
            await _client.CloseRowsAsync(ToProto(rows), cancellationToken: cancellationToken);
        }
        catch (RpcException exception)
        {
            throw SpannerException.ToSpannerException(exception);
        }
    }

    public void BeginTransaction(Connection connection, TransactionOptions transactionOptions)
    {
        TranslateException(() => _client.BeginTransaction(new BeginTransactionRequest
        {
            Connection = ToProto(connection),
            TransactionOptions = transactionOptions,
        }));
    }

    public CommitResponse? Commit(Connection connection)
    {
        var response = TranslateException(() => _client.Commit(ToProto(connection)));
        return response.CommitTimestamp == null ? null : response;
    }

    public async Task<CommitResponse?> CommitAsync(Connection connection, CancellationToken cancellationToken = default)
    {
        try
        {
            var response = await _client.CommitAsync(ToProto(connection), cancellationToken: cancellationToken);
            return response.CommitTimestamp == null ? null : response;
        }
        catch (RpcException exception)
        {
            throw new SpannerException(new Status { Code = (int) exception.Status.StatusCode, Message = exception.Status.Detail });
        }
    }

    public void Rollback(Connection connection)
    {
        TranslateException(() => _client.Rollback(ToProto(connection)));
    }

    public async Task RollbackAsync(Connection connection, CancellationToken cancellationToken = default)
    {
        try
        {
            await _client.RollbackAsync(ToProto(connection), cancellationToken:  cancellationToken);
        }
        catch (RpcException exception)
        {
            throw SpannerException.ToSpannerException(exception);
        }
    }

    Pool FromProto(V1.Pool pool) => new(this, pool.Id);

    V1.Pool ToProto(Pool pool) => new() { Id = pool.Id };

    Connection FromProto(Pool pool, V1.Connection proto) => new(pool, proto.Id);

    V1.Connection ToProto(Connection connection) => new() { Id = connection.Id, Pool = ToProto(connection.Pool), };

    Rows FromProto(Connection connection, V1.Rows proto) => new(connection, proto.Id);

    V1.Rows ToProto(Rows rows) => new() { Id = rows.Id, Connection = ToProto(rows.SpannerConnection), };
}