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
using Google.Api.Gax;
using Google.Cloud.Spanner.V1;
using Google.Cloud.SpannerLib.V1;
using Google.Protobuf.WellKnownTypes;
using Google.Rpc;
using Grpc.Core;
using Status = Google.Rpc.Status;

namespace Google.Cloud.SpannerLib.Grpc;

public class StreamingRows : Rows
{
    private readonly GrpcLibSpanner _spanner;
    private readonly ExecuteResponse? _executeResponse;
    private AsyncServerStreamingCall<RowData>? _stream;
    private ListValue? _pendingRow;
    private ResultSetMetadata? _metadata;
    private ResultSetStats? _stats;
    private bool _done;
    private bool _pendingNextResultSetCall;

    private bool HasOnlyInMemResults => !_executeResponse?.HasMoreResults ?? false;
    private bool HasMoreInMemRows =>
        _executeResponse != null
        && _currentResultSetIndex < _executeResponse.ResultSets.Count
        && (_currentResultSetIndex < _executeResponse.ResultSets.Count-1 || _currentRowIndex < _executeResponse.ResultSets[_currentResultSetIndex].Rows.Count-1);
    private bool IsPositionedAtInMemResultSet =>
        _executeResponse != null
        && _currentResultSetIndex < _executeResponse.ResultSets.Count;
    private bool IsPositionedAtInMemResultSetWithAllData =>
        IsPositionedAtInMemResultSet
        && (_currentResultSetIndex < _executeResponse!.ResultSets.Count - 1 || !_executeResponse.HasMoreResults);
    private ResultSet CurrentInMemResultSet => _executeResponse!.ResultSets[_currentResultSetIndex];

    private int _currentResultSetIndex;
    private int _currentRowIndex = -1;
    
    private AsyncServerStreamingCall<RowData> Stream => _stream!;

    protected override ResultSetStats? Stats => IsPositionedAtInMemResultSetWithAllData ? CurrentInMemResultSet.Stats : _stats;

    public override ResultSetMetadata? Metadata => IsPositionedAtInMemResultSet ? CurrentInMemResultSet.Metadata : _metadata;

    internal static StreamingRows Create(GrpcLibSpanner spanner, Connection connection, AsyncServerStreamingCall<RowData> stream)
    {
        var rows = new StreamingRows(spanner, connection, stream);
        rows._pendingRow = rows.Next();
        return rows;
    }

    internal static StreamingRows Create(GrpcLibSpanner spanner, Connection connection, ExecuteResponse response)
    {
        var rows = new StreamingRows(spanner, connection, response);
        rows._pendingRow = rows.Next();
        return rows;
    }

    internal static async Task<StreamingRows> CreateAsync(GrpcLibSpanner spanner, Connection connection, AsyncServerStreamingCall<RowData> stream, CancellationToken cancellationToken)
    {
        var rows = new StreamingRows(spanner, connection, stream);
        rows._pendingRow = await rows.NextAsync(cancellationToken).ConfigureAwait(false);
        return rows;
    }

    internal static async Task<StreamingRows> CreateAsync(GrpcLibSpanner spanner, Connection connection, ExecuteResponse response, CancellationToken cancellationToken)
    {
        var rows = new StreamingRows(spanner, connection, response);
        rows._pendingRow = await rows.NextAsync(cancellationToken).ConfigureAwait(false);
        return rows;
    }

    private StreamingRows(GrpcLibSpanner spanner, Connection connection, AsyncServerStreamingCall<RowData> stream) : base(connection, 0, initMetadata: false)
    {
        _spanner = spanner;
        _stream = stream;
        _executeResponse = null;
    }

    private StreamingRows(GrpcLibSpanner spanner, Connection connection, ExecuteResponse response) : base(connection, response.Rows.Id, initMetadata: false)
    {
        _spanner = spanner;
        _stream = null;
        _executeResponse = response;
    }

    protected override void Dispose(bool disposing)
    {
        Cleanup();
        if (_stream == null && (_executeResponse?.HasMoreResults ?? true))
        {
            base.Dispose(disposing);
        }
    }

    protected override ValueTask DisposeAsyncCore()
    {
        Cleanup();
        if (_stream == null && (_executeResponse?.HasMoreResults ?? true))
        {
            return base.DisposeAsyncCore();
        }
        return ValueTask.CompletedTask;
    }

    private void Cleanup()
    {
        if (!_done)
        {
            MarkDone();
        }
        _stream?.Dispose();
    }
    
    private void MarkDone()
    {
        _done = true;
    }

    private bool TryNextCached(out ListValue? result, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        if (_pendingNextResultSetCall || _done)
        {
            result = null;
            return true;
        }
        if (_pendingRow != null) {
            result = _pendingRow;
            _pendingRow = null;
            return true;
        }
        if (HasOnlyInMemResults || HasMoreInMemRows)
        {
            result = NextInMem();
            return true;
        }
        result = null;
        return false;
    }

    public override ListValue? Next()
    {
        if (TryNextCached(out var result, CancellationToken.None))
        {
            return result;
        }
        _stream ??= _spanner.ContinueStreaming(SpannerConnection, Id);
        try
        {
            var hasNext = Task.Run(() => Stream.ResponseStream.MoveNext()).GetAwaiter().GetResult();
            if (!hasNext)
            {
                MarkDone();
                return null;
            }
            var rowData = Stream.ResponseStream.Current;
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
        if (TryNextCached(out var result, cancellationToken))
        {
            return result;
        }
        _stream ??= _spanner.ContinueStreamingAsync(SpannerConnection, Id, cancellationToken);
        try
        {
            var hasNext = await Stream.ResponseStream.MoveNext(cancellationToken).ConfigureAwait(false);
            if (!hasNext)
            {
                MarkDone();
                return null;
            }
            var rowData = Stream.ResponseStream.Current;
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
    /// Returns the next row based on the cached in-memory results.
    /// This method assumes that the cursor is positioned at an in-memory result.
    /// </summary>
    private ListValue? NextInMem()
    {
        GaxPreconditions.CheckNotNull(_executeResponse, nameof(_executeResponse));
        if (_currentResultSetIndex == _executeResponse!.ResultSets.Count)
        {
            return null;
        }
        _currentRowIndex = Math.Min(_currentRowIndex + 1, CurrentInMemResultSet.Rows.Count);
        return _currentRowIndex == CurrentInMemResultSet.Rows.Count ? null : CurrentInMemResultSet.Rows[_currentRowIndex];
    }

    private bool TryNextResultSetInMem(out bool result)
    {
        if (HasOnlyInMemResults)
        {
            result = NextResultSetInMem();
            return true;
        }
        if (_executeResponse != null && _currentResultSetIndex < _executeResponse.ResultSets.Count-1)
        {
            result = NextResultSetInMem();
            return true;
        }
        result = false;
        return false;
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
        if (TryNextResultSetInMem(out var result))
        {
            return result;
        }
        _stream ??= _spanner.ContinueStreaming(SpannerConnection, Id);
        
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
        if (TryNextResultSetInMem(out var result))
        {
            return result;
        }
        _stream ??= _spanner.ContinueStreaming(SpannerConnection, Id);
        
        // Read data until we reach the next result set.
        await ReadUntilEndAsync(cancellationToken).ConfigureAwait(false);
        
        var hasNextResultSet = HasNextResultSet();
        _pendingRow = await NextAsync(cancellationToken).ConfigureAwait(false);
        return hasNextResultSet;
    }

    private bool NextResultSetInMem()
    {
        GaxPreconditions.CheckNotNull(_executeResponse, nameof(_executeResponse));
        if (_currentResultSetIndex == _executeResponse!.ResultSets.Count - 1)
        {
            if (_executeResponse.Status != null && _executeResponse.Status.Code != (int)Code.Ok)
            {
                throw new SpannerException(_executeResponse.Status);
            }
            return false;
        }
        _currentResultSetIndex++;
        _currentRowIndex = -1;
        return true;
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
