using System.Collections.Generic;
using System.Net.Http.Headers;
using System.Threading.Tasks;
using Google.Cloud.Spanner.V1;
using Google.Cloud.SpannerLib.V1;
using Google.Protobuf.WellKnownTypes;
using Grpc.Core;
using ExecuteBatchDmlRequest = Google.Cloud.Spanner.V1.ExecuteBatchDmlRequest;

namespace Google.Cloud.SpannerLib;

public class GrpcLibSpanner : ISpanner
{
    private readonly Grpc.SpannerLib _lib = new();

    private readonly Dictionary<long, AsyncDuplexStreamingCall<ConnectionStreamRequest, ConnectionStreamResponse>> _streams = new();
    
    public Pool CreatePool(string dsn)
    {
        var pool = _lib.CreatePool(dsn);
        return new Pool(this, pool.Id);
    }

    public void ClosePool(Pool pool)
    {
        _lib.ClosePool(ToProto(pool));
    }

    public Connection CreateConnection(Pool pool)
    {
        var connection = _lib.CreateConnection(ToProto(pool));
        // var stream = CreateConnectionStream();
        // _streams.Add(connection.Id, stream);
        return new Connection(pool, connection.Id);
    }

    public void CloseConnection(Connection connection)
    {
        if (_streams.TryGetValue(connection.Id, out var stream))
        {
            stream.RequestStream.CompleteAsync();
            _streams.Remove(connection.Id);
        }
        _lib.CloseConnection(ToProto(connection));
    }

    private AsyncDuplexStreamingCall<ConnectionStreamRequest,ConnectionStreamResponse> CreateConnectionStream()
    {
        return _lib.CreateStream();
    }

    public CommitResponse Apply(Connection connection, BatchWriteRequest.Types.MutationGroup mutations)
    {
        return _lib.Apply(ToProto(connection), mutations);
    }

    public void BufferWrite(Transaction transaction, BatchWriteRequest.Types.MutationGroup mutations)
    {
        _lib.BufferWrite(ToProto(transaction), mutations);
    }

    public Rows Execute(Connection connection, ExecuteSqlRequest statement)
    {
        var stream = _lib.ExecuteStreaming(ToProto(connection), statement);
        return new Rows(connection, stream);
        
        // var rows = _lib.Execute(ToProto(connection), statement);
        // return new Rows(connection, rows.Id);
    }

    public async Task<Rows> ExecuteAsync(Connection connection, ExecuteSqlRequest statement)
    {
        if (_streams.TryGetValue(connection.Id, out var stream))
        {
            return await ExecuteStreaming(stream, connection, statement);
        }
        return await Task.Run(() => Execute(connection, statement));
    }

    private async Task<Rows> ExecuteStreaming(AsyncDuplexStreamingCall<ConnectionStreamRequest, ConnectionStreamResponse> stream, Connection connection, ExecuteSqlRequest statement)
    {
        await stream.RequestStream.WriteAsync(new ConnectionStreamRequest
        {
            ExecuteRequest = new ExecuteRequest
            {
                Connection = ToProto(connection),
                ExecuteSqlRequest = statement,
            }
        });
        await stream.ResponseStream.MoveNext();
        var response = stream.ResponseStream.Current;
        var rows = new Rows(connection, response.Rows.Id, false);
        await rows.InitMetadataAsync();
        return rows;
    }

    public Rows ExecuteTransaction(Transaction transaction, ExecuteSqlRequest statement)
    {
        var rows = _lib.ExecuteTransaction(ToProto(transaction), statement);
        return new Rows(transaction.Connection, rows.Id);
    }

    public long[] ExecuteBatch(Connection connection, ExecuteBatchDmlRequest statements)
    {
        var response = _lib.ExecuteBatchDml(ToProto(connection), statements);
        var result = new long[response.ResultSets.Count];
        for (var i = 0; i < result.Length; i++)
        {
            result[i] = response.ResultSets[i].Stats.RowCountExact;
        }
        return result;
    }

    public Task<long[]> ExecuteBatchAsync(Connection connection, ExecuteBatchDmlRequest statements)
    {
        throw new System.NotImplementedException();
    }

    public ResultSetMetadata? Metadata(Rows rows)
    {
        return _lib.Metadata(ToProto(rows));
    }

    public async Task<ResultSetMetadata?> MetadataAsync(Rows rows)
    {
        if (_streams.TryGetValue(rows.Connection.Id, out var stream))
        {
            return await MetadataStreaming(stream, rows);
        }
        return await Task.Run(() => Metadata(rows));
    }

    private async Task<ResultSetMetadata?> MetadataStreaming(AsyncDuplexStreamingCall<ConnectionStreamRequest, ConnectionStreamResponse> stream, Rows rows)
    {
        await stream.RequestStream.WriteAsync(new ConnectionStreamRequest
        {
            MetadataRequest = new MetadataRequest
            {
                Rows = ToProto(rows),
            }
        });
        await stream.ResponseStream.MoveNext();
        var response = stream.ResponseStream.Current;
        return response.Metadata;
    }

    public ResultSetStats? Stats(Rows rows)
    {
        return _lib.ResultSetStats(ToProto(rows));
    }

    public ListValue? Next(Rows rows, int numRows, ISpanner.RowEncoding encoding)
    {
        var row = _lib.Next(ToProto(rows));
        if (row.Values.Count == 0)
        {
            return null;
        }
        return row;
    }

    public async Task<ListValue?> NextAsync(Rows rows, int numRows, ISpanner.RowEncoding encoding)
    {
        if (_streams.TryGetValue(rows.Connection.Id, out var stream))
        {
            return await NextStreaming(stream, rows);
        }
        return await Task.Run(() => Next(rows, numRows, encoding));
    }

    private async Task<ListValue?> NextStreaming(AsyncDuplexStreamingCall<ConnectionStreamRequest, ConnectionStreamResponse> stream, Rows rows)
    {
        await stream.RequestStream.WriteAsync(new ConnectionStreamRequest
        {
            NextRequest = new NextRequest
            {
                Rows = ToProto(rows),
            }
        });
        await stream.ResponseStream.MoveNext();
        var response = stream.ResponseStream.Current;
        if (response.Row.Values.Count == 0)
        {
            return null;
        }
        return response.Row;
    }

    public void CloseRows(Rows rows)
    {
        _lib.CloseRows(ToProto(rows));
    }

    public Transaction BeginTransaction(Connection connection, TransactionOptions transactionOptions)
    {
        var transaction = _lib.BeginTransaction(ToProto(connection), transactionOptions);
        return new Transaction(connection, transaction.Id);
    }

    public CommitResponse Commit(Transaction transaction)
    {
        return _lib.Commit(ToProto(transaction));
    }

    public void Rollback(Transaction transaction)
    {
        _lib.Rollback(ToProto(transaction));
    }
    
    private static V1.Pool ToProto(Pool pool) => new() {Id = pool.Id};
    
    private static V1.Connection ToProto(Connection connection) => new() {Pool = ToProto(connection.Pool), Id = connection.Id};
    
    private static V1.Transaction ToProto(Transaction transaction) => new() {Connection = ToProto(transaction.Connection), Id = transaction.Id};
    
    private static V1.Rows ToProto(Rows rows) => new() {Connection = ToProto(rows.Connection), Id = rows.Id};
}