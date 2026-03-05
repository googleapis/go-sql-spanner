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
using Google.Protobuf.WellKnownTypes;

namespace Google.Cloud.SpannerLib;

/// <summary>
/// Rows is the result that is returned for all SQL statements that are executed on a connection. The contents of a Rows
/// object depends on the type of SQL statement that was returned.
/// </summary>
public class Rows : AbstractLibObject
{
    public Connection SpannerConnection { get; private set; }
    
    private ResultSetMetadata? _metadata;

    public virtual ResultSetMetadata? Metadata => _metadata ??= Spanner.Metadata(this);

    private Lazy<ResultSetStats?> _stats;

    /// <summary>
    /// The ResultSetStats of the SQL statement. This is only available once all data rows have been read.
    /// </summary>
    protected virtual ResultSetStats? Stats => _stats.Value;

    /// <summary>
    /// The update count of the SQL statement. This is only available once all data rows have been read.
    /// </summary>
    public long UpdateCount
    {
        get
        {
            var stats = Stats;
            if (stats == null)
            {
                return -1L;
            }
            if (stats.HasRowCountExact)
            {
                return stats.RowCountExact;
            }
            if (stats.HasRowCountLowerBound)
            {
                return stats.RowCountLowerBound;
            }
            return -1L;
        }
    }
    
    private bool _hasReadAllResults;

    public Rows(Connection connection, long id, bool initMetadata = true) : base(connection.Spanner, id)
    {
        SpannerConnection = connection;
        if (initMetadata)
        {
            _metadata = Spanner.Metadata(this);
        }
        _stats = new(() => Spanner.Stats(this));
    }

    /// <summary>
    /// Returns the next data row from this Rows object.
    /// </summary>
    /// <returns>The next data row or null if there are no more data</returns>
    public virtual ListValue? Next()
    {
        var res = Spanner.Next(this, 1, ISpannerLib.RowEncoding.Proto);
        if (res == null && !_stats.IsValueCreated)
        {
            // initialize stats.
            _ = _stats.Value;
        }
        return res;
    }

    /// <summary>
    /// Returns the next data row from this Rows object.
    /// </summary>
    /// <returns>The next data row or null if there are no more data</returns>
    public virtual async Task<ListValue?> NextAsync(CancellationToken cancellationToken = default)
    {
        return await Spanner.NextAsync(this, 1, ISpannerLib.RowEncoding.Proto, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Gets the total update count in this Rows object. This consumes all data in all the result sets.
    /// This method should only be called if the caller is only interested in the update count, and not in any of the
    /// rows in the result sets.
    /// </summary>
    /// <returns>
    /// The total update count of all the result sets in this Rows object.
    /// If the SQL string only contained non-DML statements, then the update count is -1. If the SQL string contained at
    /// least one DML statement, then the returned value is the sum of all update counts of the DML statements in the
    /// SQL string that was executed.
    /// </returns>
    public long GetTotalUpdateCount()
    {
        long result = -1;
        var hasUpdateCount = false;
        do
        {
            var updateCount = UpdateCount;
            if (updateCount > -1)
            {
                if (!hasUpdateCount)
                {
                    hasUpdateCount = true;
                    result = 0;
                }
                result += updateCount;
            }
        } while (NextResultSet());
        return result;
    }

    /// <summary>
    /// Gets the total update count in this Rows object. This consumes all data in all the result sets.
    /// This method should only be called if the caller is only interested in the update count, and not in any of the
    /// rows in the result sets.
    /// </summary>
    /// <returns>
    /// The total update count of all the result sets in this Rows object.
    /// If the SQL string only contained non-DML statements, then the update count is -1. If the SQL string contained at
    /// least one DML statement, then the returned value is the sum of all update counts of the DML statements in the
    /// SQL string that was executed.
    /// </returns>
    public async Task<long> GetTotalUpdateCountAsync(CancellationToken cancellationToken = default)
    {
        long result = -1;
        var hasUpdateCount = false;
        do
        {
            var updateCount = UpdateCount;
            if (updateCount > -1)
            {
                if (!hasUpdateCount)
                {
                    hasUpdateCount = true;
                    result = 0;
                }
                result += updateCount;
            }
        } while (await NextResultSetAsync(cancellationToken).ConfigureAwait(false));
        return result;
    }

    /// <summary>
    /// Moves the cursor to the next result set in this Rows object.
    /// </summary>
    /// <returns>True if there was another result set, and false otherwise</returns>
    public virtual bool NextResultSet()
    {
        if (_hasReadAllResults)
        {
            return false;
        }
        return NextResultSet(Spanner.NextResultSet(this));
    }

    /// <summary>
    /// Moves the cursor to the next result set in this Rows object.
    /// </summary>
    /// <returns>True if there was another result set, and false otherwise</returns>
    public virtual async Task<bool> NextResultSetAsync(CancellationToken cancellationToken = default)
    {
        if (_hasReadAllResults)
        {
            return false;
        }
        return NextResultSet(await Spanner.NextResultSetAsync(this, cancellationToken).ConfigureAwait(false));
    }

    private bool NextResultSet(ResultSetMetadata? metadata)
    {
        if (metadata == null)
        {
            _hasReadAllResults = true;
            return false;
        }
        _metadata = metadata;
        _stats = new(() => Spanner.Stats(this));
        return true;
    }

    /// <summary>
    /// Closes this Rows object.
    /// </summary>
    protected override void CloseLibObject()
    {
        Spanner.CloseRows(this);
    }

    protected override async ValueTask CloseLibObjectAsync()
    {
        await Spanner.CloseRowsAsync(this).ConfigureAwait(false);
    }
    
}
