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
using System.Data.Common;
using System.Diagnostics.CodeAnalysis;
using System.Threading;
using System.Threading.Tasks;
using Google.Api.Gax;

namespace Google.Cloud.Spanner.DataProvider;

public class SpannerDataSource : DbDataSource
{
    private readonly SpannerConnectionStringBuilder _connectionStringBuilder;
        
    [AllowNull]
    public sealed override string ConnectionString => _connectionStringBuilder.ConnectionString;

    public static SpannerDataSource Create(string connectionString)
    {
        GaxPreconditions.CheckNotNull(connectionString, nameof(connectionString));
        return Create(new SpannerConnectionStringBuilder(connectionString));
    }

    public static SpannerDataSource Create(SpannerConnectionStringBuilder connectionStringBuilder)
    {
        return new SpannerDataSource(connectionStringBuilder);
    }

    private SpannerDataSource(SpannerConnectionStringBuilder connectionStringBuilder)
    {
        GaxPreconditions.CheckNotNull(connectionStringBuilder, nameof(connectionStringBuilder));
        connectionStringBuilder.CheckValid();
        _connectionStringBuilder = connectionStringBuilder;
    }
    
    public new SpannerConnection CreateConnection() => (base.CreateConnection() as SpannerConnection)!;
    
    public new SpannerConnection OpenConnection() => (base.OpenDbConnection() as SpannerConnection)!;

    public new async ValueTask<SpannerConnection> OpenConnectionAsync(CancellationToken cancellationToken = default)
    {
        var connection = CreateConnection();
        try
        {
            await connection.OpenAsync(cancellationToken).ConfigureAwait(false);
            return connection;
        }
        catch
        {
            await connection.DisposeAsync().ConfigureAwait(false);
            throw;
        }
    }

    protected override DbConnection CreateDbConnection()
    {
        return new SpannerConnection(_connectionStringBuilder);
    }
}
