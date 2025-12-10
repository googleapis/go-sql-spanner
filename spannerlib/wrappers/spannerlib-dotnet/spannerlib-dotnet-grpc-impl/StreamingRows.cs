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
    private bool _pendingNextResultSetCall;

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
        rows._pendingRow = await rows.NextAsync(cancellationToken).ConfigureAwait(false);
        return rows;
    }

    private StreamingRows(Connection connection, AsyncServerStreamingCall<RowData> stream) : base(connection, 0, initMetadata: false)
    {
        _stream = stream;
    }

    protected override void Dispose(bool disposing)
    {
        Cleanup();
    }

    protected override ValueTask DisposeAsyncCore()
    {
        Cleanup();
        return ValueTask.CompletedTask;
    }
    
    private void Cleanup()
    {
        if (!_done)
        {
            MarkDone();
        }
        _stream.Dispose();
    }
    
    private void MarkDone()
    {
        _done = true;
    }

    public override ListValue? Next()
    {
        // TODO: Combine sync and async methods
        if (_pendingNextResultSetCall)
        {
            return null;
        }
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
                if (rowData.HasMoreResults)
                {
                    _pendingNextResultSetCall = true;
                }
                else
                {
                    MarkDone();
                }
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
        if (_pendingNextResultSetCall)
        {
            return null;
        }
        if (_pendingRow != null) {
            var row = _pendingRow;
            _pendingRow = null;
            return row;
        }
        try
        {
            var hasNext = await _stream.ResponseStream.MoveNext(cancellationToken).ConfigureAwait(false);
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
                if (rowData.HasMoreResults)
                {
                    _pendingNextResultSetCall = true;
                }
                else
                {
                    MarkDone();
                }
                return null;
            }
            return rowData.Data[0];
        }
        catch (RpcException exception)
        {
            throw SpannerException.ToSpannerException(exception);
        }
    }

    /// <summary>
    /// Moves the cursor to the next result set in this Rows object.
    /// </summary>
    /// <returns>True if there was another result set, and false otherwise</returns>
    public override bool NextResultSet()
    {
        if (_done)
        {
            return false;
        }
        // Read data until we reach the next result set.
        ReadUntilEnd();
        
        var hasNextResultSet = HasNextResultSet();
        _pendingRow = Next();
        return hasNextResultSet;
    }

    /// <summary>
    /// Moves the cursor to the next result set in this Rows object.
    /// </summary>
    /// <returns>True if there was another result set, and false otherwise</returns>
    public override async Task<bool> NextResultSetAsync(CancellationToken cancellationToken = default)
    {
        if (_done)
        {
            return false;
        }
        // Read data until we reach the next result set.
        await ReadUntilEndAsync(cancellationToken).ConfigureAwait(false);
        
        var hasNextResultSet = HasNextResultSet();
        _pendingRow = await NextAsync(cancellationToken).ConfigureAwait(false);
        return hasNextResultSet;
    }

    private bool HasNextResultSet()
    {
        if (_pendingNextResultSetCall)
        {
            _stats = null;
            _metadata = null;
            _pendingNextResultSetCall = false;
            return true;
        }
        return false;
    }

    private void ReadUntilEnd()
    {
        // Read the remaining rows in the current result set.
        while (!_pendingNextResultSetCall && Next() != null)
        {
        }
    }

    private async Task ReadUntilEndAsync(CancellationToken cancellationToken)
    {
        // Read the remaining rows in the current result set.
        while (!_pendingNextResultSetCall && await NextAsync(cancellationToken).ConfigureAwait(false) != null)
        {
        }
    }
}