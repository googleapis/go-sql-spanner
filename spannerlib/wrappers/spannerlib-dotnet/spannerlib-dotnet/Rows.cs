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

    public ResultSetMetadata? Metadata => _metadata ??= Spanner.Metadata(this);

    private readonly Lazy<ResultSetStats?> _stats;

    /// <summary>
    /// The ResultSetStats of the SQL statement. This is only available once all data rows have been read.
    /// </summary>
    private ResultSetStats? Stats => _stats.Value;

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
    public ListValue? Next()
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
    public async Task<ListValue?> NextAsync()
    {
        return await Spanner.NextAsync(this, 1, ISpannerLib.RowEncoding.Proto);
    }

    /// <summary>
    /// Closes this Rows object.
    /// </summary>
    protected override void CloseLibObject()
    {
        Spanner.CloseRows(this);
    }
}
