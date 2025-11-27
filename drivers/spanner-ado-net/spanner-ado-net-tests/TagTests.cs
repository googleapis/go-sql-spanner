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

using Google.Cloud.Spanner.V1;
using Google.Cloud.SpannerLib.MockServer;

namespace Google.Cloud.Spanner.DataProvider.Tests;

public class TagTests : AbstractMockServerTests
{
    [Test]
    public async Task TestRequestTag([Values] bool async)
    {
        const string sql = "insert into my_table (id, value) values (1, 'One')";
        Fixture.SpannerMock.AddOrUpdateStatementResult(sql, StatementResult.CreateUpdateCount(1L));

        await using var connection = new SpannerConnection();
        connection.ConnectionString = ConnectionString;
        await connection.OpenAsync();
        
        await using var command = connection.CreateCommand();
        command.CommandText = sql;
        command.Tag = "my_tag";
        if (async)
        {
            await command.ExecuteNonQueryAsync();
        }
        else
        {
            command.ExecuteNonQuery();
        }
        
        var requests = Fixture.SpannerMock.Requests.ToList();
        var request = requests.OfType<ExecuteSqlRequest>().First();
        Assert.That(request, Is.Not.Null);
        Assert.That(request.RequestOptions, Is.Not.Null);
        Assert.That(request.RequestOptions.RequestTag, Is.EqualTo("my_tag"));
        Assert.That(request.RequestOptions.TransactionTag, Is.EqualTo(""));
    }
    
    [Test]
    public async Task TestTransactionTag([Values] bool async)
    {
        const string sql = "insert into my_table (id, value) values (1, 'One')";
        Fixture.SpannerMock.AddOrUpdateStatementResult(sql, StatementResult.CreateUpdateCount(1L));

        await using var connection = new SpannerConnection();
        connection.ConnectionString = ConnectionString;
        await connection.OpenAsync();
        
        await using var transaction = await connection.BeginTransactionAsync();
        transaction.Tag = "my_tx_tag";
        
        await using var command = connection.CreateCommand();
        command.Transaction =  transaction;
        command.CommandText = sql;
        command.Tag = "my_tag1";
        if (async)
        {
            await command.ExecuteNonQueryAsync();
        }
        else
        {
            command.ExecuteNonQuery();
        }

        command.Tag = "my_tag2";
        if (async)
        {
            await command.ExecuteNonQueryAsync();
        }
        else
        {
            command.ExecuteNonQuery();
        }

        var requests = Fixture.SpannerMock.Requests.ToList();
        var request = requests.OfType<ExecuteSqlRequest>().First();
        Assert.That(request, Is.Not.Null);
        Assert.That(request.RequestOptions, Is.Not.Null);
        Assert.That(request.RequestOptions.RequestTag, Is.EqualTo("my_tag1"));
        Assert.That(request.RequestOptions.TransactionTag, Is.EqualTo("my_tx_tag"));
        
        request = requests.OfType<ExecuteSqlRequest>().ToList()[1];
        Assert.That(request.RequestOptions.RequestTag, Is.EqualTo("my_tag2"));
        Assert.That(request.RequestOptions.TransactionTag, Is.EqualTo("my_tx_tag"));
    }
    
    [Test]
    public async Task TestRequestTagBatch([Values] bool async)
    {
        const string sql = "insert into my_table (id, value) values (1, 'One')";
        Fixture.SpannerMock.AddOrUpdateStatementResult(sql, StatementResult.CreateUpdateCount(1L));

        await using var connection = new SpannerConnection();
        connection.ConnectionString = ConnectionString;
        await connection.OpenAsync();

        await using var batch = connection.CreateBatch();
        batch.Tag = "my_tag";
        batch.BatchCommands.Add(sql);
        batch.BatchCommands.Add(sql);
        if (async)
        {
            await batch.ExecuteNonQueryAsync();
        }
        else
        {
            // ReSharper disable once MethodHasAsyncOverload
            batch.ExecuteNonQuery();
        }

        var requests = Fixture.SpannerMock.Requests.ToList();
        var request = requests.OfType<ExecuteBatchDmlRequest>().First();
        Assert.That(request, Is.Not.Null);
        Assert.That(request.RequestOptions, Is.Not.Null);
        Assert.That(request.RequestOptions.RequestTag, Is.EqualTo("my_tag"));
        Assert.That(request.RequestOptions.TransactionTag, Is.EqualTo(""));
    }
    
    [Test]
    public async Task TestTransactionTagBatch([Values] bool async)
    {
        const string sql = "insert into my_table (id, value) values (1, 'One')";
        Fixture.SpannerMock.AddOrUpdateStatementResult(sql, StatementResult.CreateUpdateCount(1L));

        await using var connection = new SpannerConnection();
        connection.ConnectionString = ConnectionString;
        await connection.OpenAsync();
        
        await using var transaction = await connection.BeginTransactionAsync();
        transaction.Tag = "my_tx_tag";

        await using var batch = connection.CreateBatch();
        batch.Transaction = transaction;
        batch.Tag = "my_tag";
        batch.BatchCommands.Add(sql);
        batch.BatchCommands.Add(sql);
        if (async)
        {
            await batch.ExecuteNonQueryAsync();
        }
        else
        {
            // ReSharper disable once MethodHasAsyncOverload
            batch.ExecuteNonQuery();
        }

        var requests = Fixture.SpannerMock.Requests.ToList();
        var request = requests.OfType<ExecuteBatchDmlRequest>().First();
        Assert.That(request, Is.Not.Null);
        Assert.That(request.RequestOptions, Is.Not.Null);
        Assert.That(request.RequestOptions.RequestTag, Is.EqualTo("my_tag"));
        Assert.That(request.RequestOptions.TransactionTag, Is.EqualTo("my_tx_tag"));
    }

    [Test]
    public async Task TestMultipleStatements([Values] bool async, [Values] bool batchFirst)
    {
        const string dml = "insert into my_table (id, value) values (1, 'One')";
        Fixture.SpannerMock.AddOrUpdateStatementResult(dml, StatementResult.CreateUpdateCount(1L));
        const string query = "select * from my_table";
        Fixture.SpannerMock.AddOrUpdateStatementResult(query, StatementResult.CreateSelect1ResultSet());

        await using var connection = new SpannerConnection();
        connection.ConnectionString = ConnectionString;
        await connection.OpenAsync();
        
        await using var transaction = await connection.BeginTransactionAsync();
        transaction.Tag = "my_tx_tag";

        if (batchFirst)
        {
            await using var firstBatch = connection.CreateBatch();
            firstBatch.Transaction = transaction;
            firstBatch.Tag = "first_batch";
            firstBatch.BatchCommands.Add(dml);
            firstBatch.BatchCommands.Add(dml);
            if (async)
            {
                await firstBatch.ExecuteNonQueryAsync();
            }
            else
            {
                // ReSharper disable once MethodHasAsyncOverload
                firstBatch.ExecuteNonQuery();
            }
        }
        
        await using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = query;
        command.Tag = "my_query";
        if (async)
        {
            await using var reader = await command.ExecuteReaderAsync();
        }
        else
        {
            // ReSharper disable once MethodHasAsyncOverload
            await using var reader = command.ExecuteReader();
        }
        
        command.Tag = "my_scalar";
        if (async)
        {
            await command.ExecuteScalarAsync();
        }
        else
        {
            command.ExecuteScalar();
        }

        command.Tag = "my_dml";
        command.CommandText = dml;
        if (async)
        {
            await command.ExecuteNonQueryAsync();
        }
        else
        {
            command.ExecuteNonQuery();
        }

        await using var batch = connection.CreateBatch();
        batch.Transaction = transaction;
        batch.Tag = "my_batch";
        batch.BatchCommands.Add(dml);
        batch.BatchCommands.Add(dml);
        if (async)
        {
            await batch.ExecuteNonQueryAsync();
        }
        else
        {
            // ReSharper disable once MethodHasAsyncOverload
            batch.ExecuteNonQuery();
        }

        if (async)
        {
            await transaction.CommitAsync();
        }
        else
        {
            // ReSharper disable once MethodHasAsyncOverload
            transaction.Commit();
        }

        var requests = Fixture.SpannerMock.Requests.ToList();
        if (batchFirst)
        {
            var firstBatchRequest = requests.OfType<ExecuteBatchDmlRequest>().First();
            Assert.That(firstBatchRequest, Is.Not.Null);
            Assert.That(firstBatchRequest.RequestOptions, Is.Not.Null);
            Assert.That(firstBatchRequest.RequestOptions.RequestTag, Is.EqualTo("first_batch"));
            Assert.That(firstBatchRequest.RequestOptions.TransactionTag, Is.EqualTo("my_tx_tag"));
        }

        var batchRequest = requests.OfType<ExecuteBatchDmlRequest>().Last();
        Assert.That(batchRequest, Is.Not.Null);
        Assert.That(batchRequest.RequestOptions, Is.Not.Null);
        Assert.That(batchRequest.RequestOptions.RequestTag, Is.EqualTo("my_batch"));
        Assert.That(batchRequest.RequestOptions.TransactionTag, Is.EqualTo("my_tx_tag"));
        
        var executeRequests = requests.OfType<ExecuteSqlRequest>().ToList();
        Assert.That(executeRequests, Has.Count.EqualTo(3));
        var queryRequest = executeRequests[0];
        Assert.That(queryRequest, Is.Not.Null);
        Assert.That(queryRequest.RequestOptions, Is.Not.Null);
        Assert.That(queryRequest.RequestOptions.RequestTag, Is.EqualTo("my_query"));
        Assert.That(queryRequest.RequestOptions.TransactionTag, Is.EqualTo("my_tx_tag"));
        
        var scalarRequest = executeRequests[1];
        Assert.That(scalarRequest, Is.Not.Null);
        Assert.That(scalarRequest.RequestOptions, Is.Not.Null);
        Assert.That(scalarRequest.RequestOptions.RequestTag, Is.EqualTo("my_scalar"));
        Assert.That(scalarRequest.RequestOptions.TransactionTag, Is.EqualTo("my_tx_tag"));
        
        var dmlRequest = executeRequests[2];
        Assert.That(dmlRequest, Is.Not.Null);
        Assert.That(dmlRequest.RequestOptions, Is.Not.Null);
        Assert.That(dmlRequest.RequestOptions.RequestTag, Is.EqualTo("my_dml"));
        Assert.That(dmlRequest.RequestOptions.TransactionTag, Is.EqualTo("my_tx_tag"));
        
        var commitRequest = requests.OfType<CommitRequest>().First();
        Assert.That(commitRequest, Is.Not.Null);
        Assert.That(commitRequest.RequestOptions, Is.Not.Null);
        Assert.That(commitRequest.RequestOptions.TransactionTag, Is.EqualTo("my_tx_tag"));
    }
}
