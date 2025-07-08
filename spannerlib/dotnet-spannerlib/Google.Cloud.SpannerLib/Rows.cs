using System;
using System.Threading.Tasks;
using Google.Cloud.Spanner.V1;
using Google.Protobuf.WellKnownTypes;
using Grpc.Core;

namespace Google.Cloud.SpannerLib;

public class Rows : AbstractLibObject
{
    private Lazy<ResultSetStats?> _stats;

    internal Connection Connection { get; private set; }

    private readonly AsyncServerStreamingCall<ResultSet>? _stream;

    public ResultSetMetadata? Metadata { get; private set; }

    private ResultSetStats? Stats => _stats.Value;

    public long UpdateCount
    {
        get
        {
            var stats = Stats;
            if (stats == null)
            {
                return -1;
            }
            if (stats.HasRowCountExact)
            {
                return (int)stats.RowCountExact;
            }
            if (stats.HasRowCountLowerBound)
            {
                return (int)stats.RowCountLowerBound;
            }
            return -1;
        }
    }

    internal Rows(Connection connection, long id) : this(connection, id, true)
    {
    }

    internal Rows(Connection connection, long id, bool initMetadata) : base(connection.Spanner, id)
    {
        Connection = connection;
        if (initMetadata)
        {
            Metadata = Spanner.Metadata(this);
        }
        _stats = new(() => Spanner.Stats(this));
        _stream = null;
    }

    internal Rows(Connection connection, AsyncServerStreamingCall<ResultSet> stream) : base(connection.Spanner, -1)
    {
        Connection = connection;
        _stats = new(() => Spanner.Stats(this));
        _stream = stream;
    }

    public async Task InitMetadataAsync()
    {
        Metadata = await Spanner.MetadataAsync(this);
    }

    public ListValue? Next()
    {
        if (_stream == null)
        {
            var res = Spanner.Next(this);
            if (res == null && !_stats.IsValueCreated)
            {
                // initialize stats.
                _ = _stats.Value;
            }
            return res;
        }
        if (_stream.ResponseStream.MoveNext().Result)
        {
            var row = _stream.ResponseStream.Current;
            if (row.Rows.Count == 0)
            {
                return null;
            }
            Metadata ??= row.Metadata;
            return row.Rows[0];
        }
        return null;
    }

    public async Task<ListValue?> NextAsync()
    {
        if (_stream == null)
        {
            return await Spanner.NextAsync(this);
        }
        if (await _stream.ResponseStream.MoveNext())
        {
            var row = _stream.ResponseStream.Current;
            if (row.Rows.Count == 0)
            {
                return null;
            }
            Metadata ??= row.Metadata;
            return row.Rows[0];
        }
        return null;
        // var res = await Spanner.NextAsync(this);
        // if (res == null && !_stats.IsValueCreated)
        // {
        //     // initialize stats.
        //     _ = _stats.Value;
        // }
        // return res;
    }

    protected override void CloseLibObject()
    {
        _stream?.Dispose();
        Spanner.CloseRows(this);
    }
}