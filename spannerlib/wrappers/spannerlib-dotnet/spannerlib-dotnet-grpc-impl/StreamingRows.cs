using System.Threading;
using System.Threading.Tasks;
using Google.Api.Gax;
using Google.Cloud.Spanner.V1;
using Google.Cloud.SpannerLib.V1;
using Google.Protobuf.WellKnownTypes;
using Grpc.Core;
using Status = Google.Rpc.Status;

namespace Google.Cloud.SpannerLib.Grpc;

public class StreamingRows : Rows
{
    private readonly AsyncServerStreamingCall<RowData> _stream;
    private ListValue? _pendingRow;
    private ResultSetMetadata? _metadata;
    private ResultSetStats? _stats;
    private bool _done;

    protected override ResultSetStats? Stats => _stats;

    public override ResultSetMetadata? Metadata => _metadata;

    public static StreamingRows Create(Connection connection, AsyncServerStreamingCall<RowData> stream)
    {
        var rows = new StreamingRows(connection, stream);
        rows._pendingRow = rows.Next();
        return rows;
    }

    public static async Task<StreamingRows> CreateAsync(Connection connection, AsyncServerStreamingCall<RowData> stream, CancellationToken cancellationToken = default)
    {
        var rows = new StreamingRows(connection, stream);
        rows._pendingRow = await rows.NextAsync(cancellationToken);
        return rows;
    }

    private StreamingRows(Connection connection, AsyncServerStreamingCall<RowData> stream) : base(connection, 0, initMetadata: false)
    {
        _stream = stream;
    }

    protected override void Dispose(bool disposing)
    {
        if (!_done)
        {
            MarkDone();
        }
        _stream.Dispose();
        base.Dispose(disposing);
    }
    
    private void MarkDone()
    {
        _done = true;
    }

    public override ListValue? Next()
    {
        if (_pendingRow != null) {
            var row = _pendingRow;
            _pendingRow = null;
            return row;
        }
        try
        {
            var hasNext = Task.Run(() => _stream.ResponseStream.MoveNext()).ResultWithUnwrappedExceptions();
            if (!hasNext)
            {
                MarkDone();
                return null;
            }
            var rowData = _stream.ResponseStream.Current;
            if (rowData.Metadata != null)
            {
                _metadata = rowData.Metadata;
            }
            if (rowData.Stats != null)
            {
                _stats = rowData.Stats;
            }
            if (rowData.Data.Count == 0)
            {
                MarkDone();
                return null;
            }
            return rowData.Data[0];
        }
        catch (RpcException exception)
        {
            throw new SpannerException(new Status { Code = (int) exception.Status.StatusCode, Message = exception.Status.Detail });
        }
    }

    public override async Task<ListValue?> NextAsync(CancellationToken cancellationToken = default)
    {
        if (_pendingRow != null) {
            var row = _pendingRow;
            _pendingRow = null;
            return row;
        }
        try
        {
            var hasNext = await _stream.ResponseStream.MoveNext(cancellationToken);
            if (!hasNext)
            {
                MarkDone();
                return null;
            }
            var rowData = _stream.ResponseStream.Current;
            if (rowData.Metadata != null)
            {
                _metadata = rowData.Metadata;
            }
            if (rowData.Stats != null)
            {
                _stats = rowData.Stats;
            }
            if (rowData.Data.Count == 0)
            {
                MarkDone();
                return null;
            }
            return rowData.Data[0];
        }
        catch (RpcException exception)
        {
            throw SpannerException.ToSpannerException(exception);
        }
    }
}