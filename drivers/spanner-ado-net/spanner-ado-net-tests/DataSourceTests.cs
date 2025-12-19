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

using System.Data;
using System.Data.Common;
using System.Diagnostics.CodeAnalysis;
using Google.Cloud.SpannerLib.MockServer;

namespace Google.Cloud.Spanner.DataProvider.Tests;

public class DataSourceTests : AbstractMockServerTests
{
    [Test]
    public async Task CreateConnection()
    {
        await using var dataSource = SpannerDataSource.Create(ConnectionString);
        await using var connection = dataSource.CreateConnection();
        Assert.That(connection.State, Is.EqualTo(ConnectionState.Closed));

        await connection.OpenAsync();
        Assert.That(await connection.ExecuteScalarAsync("SELECT 1"), Is.EqualTo(1));
    }

    [Test]
    [SuppressMessage("ReSharper", "MethodHasAsyncOverload")]
    public async Task OpenConnection([Values] bool async)
    {
        await using var dataSource = SpannerDataSource.Create(ConnectionString);
        await using var connection = async
            ? await dataSource.OpenConnectionAsync()
            : dataSource.OpenConnection();

        Assert.That(connection.State, Is.EqualTo(ConnectionState.Open));
        Assert.That(await connection.ExecuteScalarAsync("SELECT 1"), Is.EqualTo(1));
    }

    [Test]
    [SuppressMessage("ReSharper", "MethodHasAsyncOverload")]
    public async Task ExecuteScalarOnConnectionlessCommand([Values] bool async)
    {
        await using var dataSource = SpannerDataSource.Create(ConnectionString);
        await using var command = dataSource.CreateCommand();
        command.CommandText = "SELECT 1";

        if (async)
        {
            Assert.That(await command.ExecuteScalarAsync(), Is.EqualTo(1));
        }
        else
        {
            Assert.That(command.ExecuteScalar(), Is.EqualTo(1));
        }
    }

    [Test]
    [SuppressMessage("ReSharper", "MethodHasAsyncOverload")]
    public async Task ExecuteNonQueryOnConnectionlessCommand([Values] bool async)
    {
        await using var dataSource = SpannerDataSource.Create(ConnectionString);
        await using var command = dataSource.CreateCommand();
        command.CommandText = "SELECT 1";

        if (async)
        {
            Assert.That(await command.ExecuteNonQueryAsync(), Is.EqualTo(-1));
        }
        else
        {
            Assert.That(command.ExecuteNonQuery(), Is.EqualTo(-1));
        }
    }

    [Test]
    [SuppressMessage("ReSharper", "MethodHasAsyncOverload")]
    public async Task ExecuteReaderOnConnectionlessCommand([Values] bool async)
    {
        await using var dataSource = SpannerDataSource.Create(ConnectionString);
        await using var command = dataSource.CreateCommand();
        command.CommandText = "SELECT 1";

        await using var reader = async ? await command.ExecuteReaderAsync() : command.ExecuteReader();
        Assert.That(reader.Read());
        Assert.That(reader.GetInt32(0), Is.EqualTo(1));
    }

    [Ignore("Requires support for batching queries")]
    [Test]
    [SuppressMessage("ReSharper", "MethodHasAsyncOverload")]
    public async Task ExecuteScalarOnConnectionlessBatch([Values] bool async)
    {
        await using var dataSource = SpannerDataSource.Create(ConnectionString);
        await using var batch = dataSource.CreateBatch();
        batch.AddSpannerBatchCommand("SELECT 1");
        batch.AddSpannerBatchCommand("SELECT 2");

        if (async)
        {
            Assert.That(await batch.ExecuteScalarAsync(), Is.EqualTo(1));
        }
        else
        {
            Assert.That(batch.ExecuteScalar(), Is.EqualTo(1));
        }
    }

    [Ignore("Requires support for batching queries")]
    [Test]
    [SuppressMessage("ReSharper", "MethodHasAsyncOverload")]
    public async Task ExecuteNonQueryOnConnectionlessBatch([Values] bool async)
    {
        await using var dataSource = SpannerDataSource.Create(ConnectionString);
        await using var batch = dataSource.CreateBatch();
        batch.AddSpannerBatchCommand("SELECT 1");
        batch.AddSpannerBatchCommand("SELECT 2");

        if (async)
        {
            Assert.That(await batch.ExecuteNonQueryAsync(), Is.EqualTo(-1));
        }
        else
        {
            Assert.That(batch.ExecuteNonQuery(), Is.EqualTo(-1));
        }
    }

    [Ignore("Requires support for batching queries")]
    [Test]
    [SuppressMessage("ReSharper", "MethodHasAsyncOverload")]
    public async Task ExecuteReaderOnConnectionlessBatch([Values] bool async)
    {
        await using var dataSource = SpannerDataSource.Create(ConnectionString);
        await using var batch = dataSource.CreateBatch();
        batch.AddSpannerBatchCommand("SELECT 1");
        batch.AddSpannerBatchCommand("SELECT 2");

        await using var reader = async ? await batch.ExecuteReaderAsync() : batch.ExecuteReader();
        Assert.That(reader.Read());
        Assert.That(reader.GetInt32(0), Is.EqualTo(1));
        Assert.That(reader.NextResult());
        Assert.That(reader.Read());
        Assert.That(reader.GetInt32(0), Is.EqualTo(2));
    }

    [Test]
    [SuppressMessage("ReSharper", "MethodHasAsyncOverload")]
    public async Task ExecuteScalarOnConnectionlessDmlBatch([Values] bool async)
    {
        const string sql = "insert into my_table (id) values (default)";
        Fixture.SpannerMock.AddOrUpdateStatementResult(sql, StatementResult.CreateUpdateCount(1));
        
        await using var dataSource = SpannerDataSource.Create(ConnectionString);
        await using var batch = dataSource.CreateBatch();
        batch.AddSpannerBatchCommand(sql);
        batch.AddSpannerBatchCommand(sql);

        if (async)
        {
            Assert.ThrowsAsync<NotImplementedException>(async () => await batch.ExecuteScalarAsync());
        }
        else
        {
            Assert.Throws<NotImplementedException>(() => batch.ExecuteScalar());
        }
    }

    [Test]
    [SuppressMessage("ReSharper", "MethodHasAsyncOverload")]
    public async Task ExecuteNonQueryOnConnectionlessDmlBatch([Values] bool async)
    {
        const string sql = "insert into my_table (id) values (default)";
        Fixture.SpannerMock.AddOrUpdateStatementResult(sql, StatementResult.CreateUpdateCount(1));
        
        await using var dataSource = SpannerDataSource.Create(ConnectionString);
        await using var batch = dataSource.CreateBatch();
        batch.AddSpannerBatchCommand(sql);
        batch.AddSpannerBatchCommand(sql);

        if (async)
        {
            Assert.That(await batch.ExecuteNonQueryAsync(), Is.EqualTo(2));
        }
        else
        {
            Assert.That(batch.ExecuteNonQuery(), Is.EqualTo(2));
        }
    }

    [Test]
    [SuppressMessage("ReSharper", "MethodHasAsyncOverload")]
    public async Task ExecuteReaderOnConnectionlessDmlBatch([Values] bool async)
    {
        const string sql = "insert into my_table (id) values (default)";
        Fixture.SpannerMock.AddOrUpdateStatementResult(sql, StatementResult.CreateUpdateCount(1));
        
        await using var dataSource = SpannerDataSource.Create(ConnectionString);
        await using var batch = dataSource.CreateBatch();
        batch.AddSpannerBatchCommand(sql);
        batch.AddSpannerBatchCommand(sql);

        if (async)
        {
            Assert.ThrowsAsync<NotImplementedException>(async () => await batch.ExecuteReaderAsync());
        }
        else
        {
            Assert.Throws<NotImplementedException>(() => batch.ExecuteReader());
        }
    }

    [Test]
    public void Dispose()
    {
        var dataSource = SpannerDataSource.Create(ConnectionString);
        var connection1 = dataSource.OpenConnection();
        var connection2 = dataSource.OpenConnection();
        connection1.Close();

        // SpannerDataSource does not contain any state, so disposing it is a no-op.
        dataSource.Dispose();
        using var connection3 = dataSource.OpenConnection();

        connection2.Close();
    }

    [Test]
    public async Task DisposeAsync()
    {
        var dataSource = SpannerDataSource.Create(ConnectionString);
        var connection1 = await dataSource.OpenConnectionAsync();
        var connection2 = await dataSource.OpenConnectionAsync();
        await connection1.CloseAsync();

        // SpannerDataSource does not contain any state, so disposing it is a no-op.
        await dataSource.DisposeAsync();
        await using var connection3 = await dataSource.OpenConnectionAsync();

        await connection2.CloseAsync();
    }

    [Test]
    public async Task CannotAccessConnectionTransactionOnDataSourceCommand()
    {
        await using var command = DataSource.CreateCommand();
    
        Assert.That(() => command.Connection, Throws.Exception.TypeOf<NotSupportedException>());
        Assert.That(() => command.Connection = null, Throws.Exception.TypeOf<NotSupportedException>());
        Assert.That(() => command.Transaction, Throws.Exception.TypeOf<NotSupportedException>());
        Assert.That(() => command.Transaction = null, Throws.Exception.TypeOf<NotSupportedException>());
    
        Assert.That(() => command.Prepare(), Throws.Exception.TypeOf<NotSupportedException>());
        Assert.That(() => command.PrepareAsync(), Throws.Exception.TypeOf<NotSupportedException>());
    }
    
    [Test]
    public async Task CannotAccessConnectionTransactionOnDataSourceBatch()
    {
        await using var batch = DataSource.CreateBatch();
    
        Assert.That(() => batch.Connection, Throws.Exception.TypeOf<NotSupportedException>());
        Assert.That(() => batch.Connection = null, Throws.Exception.TypeOf<NotSupportedException>());
        Assert.That(() => batch.Transaction, Throws.Exception.TypeOf<NotSupportedException>());
        Assert.That(() => batch.Transaction = null, Throws.Exception.TypeOf<NotSupportedException>());
    
        Assert.That(() => batch.Prepare(), Throws.Exception.TypeOf<NotSupportedException>());
        Assert.That(() => batch.PrepareAsync(), Throws.Exception.TypeOf<NotSupportedException>());
    }
    
    [Test]
    [SuppressMessage("ReSharper", "MethodHasAsyncOverload")]
    public async Task AsDbDataSource([Values] bool async)
    {
        await using DbDataSource dataSource = SpannerDataSource.Create(ConnectionString);
        await using var connection = async
            ? await dataSource.OpenConnectionAsync()
            : dataSource.OpenConnection();
        Assert.That(connection.State, Is.EqualTo(ConnectionState.Open));

        await using var command = dataSource.CreateCommand("SELECT 1");

        Assert.That(async
            ? await command.ExecuteScalarAsync()
            : command.ExecuteScalar(), Is.EqualTo(1));
    }
    
}