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

namespace Google.Cloud.Spanner.DataProvider;

public class SpannerFactory : DbProviderFactory, IServiceProvider
{
    /// <summary>
    /// Gets an instance of the <see cref="SpannerFactory"/>.
    /// This can be used to retrieve strongly typed data objects.
    /// </summary>
    public static readonly SpannerFactory Instance = new();

    SpannerFactory() {}

    /// <summary>
    /// Returns a strongly typed <see cref="DbCommand"/> instance.
    /// </summary>
    public override DbCommand CreateCommand() => new SpannerCommand();

    /// <summary>
    /// Returns a strongly typed <see cref="DbConnection"/> instance.
    /// </summary>
    public override DbConnection CreateConnection() => new SpannerConnection();

    /// <summary>
    /// Returns a strongly typed <see cref="DbParameter"/> instance.
    /// </summary>
    public override DbParameter CreateParameter() => new SpannerParameter();

    /// <summary>
    /// Returns a strongly typed <see cref="DbConnectionStringBuilder"/> instance.
    /// </summary>
    public override DbConnectionStringBuilder CreateConnectionStringBuilder() => new SpannerConnectionStringBuilder();

    /// <summary>
    /// Returns a strongly typed <see cref="DbCommandBuilder"/> instance.
    /// </summary>
    public override DbCommandBuilder CreateCommandBuilder() => new SpannerCommandBuilder();

    /// <summary>
    /// Returns a strongly typed <see cref="DbDataAdapter"/> instance.
    /// </summary>
    public override DbDataAdapter CreateDataAdapter() => new SpannerDataAdapter();

    /// <summary>
    /// Specifies whether the specific <see cref="DbProviderFactory"/> supports the <see cref="DbDataAdapter"/> class.
    /// </summary>
    public override bool CanCreateDataAdapter => true;

    /// <summary>
    /// Specifies whether the specific <see cref="DbProviderFactory"/> supports the <see cref="DbCommandBuilder"/> class.
    /// </summary>
    public override bool CanCreateCommandBuilder => true;

    /// <inheritdoc/>
    public override bool CanCreateBatch => true;

    /// <inheritdoc/>
    public override DbBatch CreateBatch() => new SpannerBatch();

    /// <inheritdoc/>
    public override DbBatchCommand CreateBatchCommand() => new SpannerBatchCommand();

    /// <inheritdoc/>
    public override DbDataSource CreateDataSource(string connectionString)
        => SpannerDataSource.Create(connectionString);

    #region IServiceProvider Members

    /// <summary>
    /// Gets the service object of the specified type.
    /// </summary>
    /// <param name="serviceType">An object that specifies the type of service object to get.</param>
    /// <returns>A service object of type serviceType, or null if there is no service object of type serviceType.</returns>
    public object? GetService(System.Type serviceType) => null;

    #endregion
    
}