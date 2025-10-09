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

public class TransactionTests : AbstractMockServerTests
{
    [Test]
    public async Task TestReadWriteTransaction()
    {
        const string sql = "update my_table set my_column=@value where id=@id";
        Fixture.SpannerMock.AddOrUpdateStatementResult(sql, StatementResult.CreateUpdateCount(1L));
        
        await using var connection = new SpannerConnection();
        connection.ConnectionString = ConnectionString;
        await connection.OpenAsync();
        await using var transaction = await connection.BeginTransactionAsync();
        await using var command = connection.CreateCommand();
        command.CommandText = sql;
        var paramId = command.CreateParameter();
        paramId.ParameterName = "id";
        paramId.Value = 1;
        command.Parameters.Add(paramId);
        var paramValue = command.CreateParameter();
        paramValue.ParameterName = "value";
        paramValue.Value = "One";
        command.Parameters.Add(paramValue);
        var updateCount = await command.ExecuteNonQueryAsync();
        await transaction.CommitAsync();
        
        Assert.That(updateCount, Is.EqualTo(1));
        var requests = Fixture.SpannerMock.Requests.ToList();
        // The transaction should use inline-begin.
        Assert.That(requests.OfType<BeginTransactionRequest>().Count(), Is.EqualTo(0));
        Assert.That(requests.OfType<ExecuteSqlRequest>().Count(), Is.EqualTo(1));
        Assert.That(requests.OfType<CommitRequest>().Count(),  Is.EqualTo(1));
        var executeRequest = requests.OfType<ExecuteSqlRequest>().First();
        Assert.That(executeRequest.Transaction, Is.EqualTo(new TransactionSelector
        {
            Begin = new TransactionOptions
            {
                ReadWrite = new TransactionOptions.Types.ReadWrite(),
            }
        }));
    }

    [Test]
    public async Task TestReadOnlyTransaction()
    {
        const string sql = "select value from my_table where id=@id";
        Fixture.SpannerMock.AddOrUpdateStatementResult(sql, StatementResult.CreateSingleColumnResultSet(new V1.Type{Code = V1.TypeCode.String}, "value", "One"));
        
        await using var connection = new SpannerConnection();
        connection.ConnectionString = ConnectionString;
        await connection.OpenAsync();
        await using var transaction = connection.BeginReadOnlyTransaction();
        await using var command = connection.CreateCommand();
        command.CommandText = sql;
        var paramId = command.CreateParameter();
        paramId.ParameterName = "id";
        paramId.Value = 1;
        command.Parameters.Add(paramId);
        await using var reader = await command.ExecuteReaderAsync();
        Assert.That(await reader.ReadAsync());
        Assert.That(reader.FieldCount, Is.EqualTo(1));
        Assert.That(reader.GetValue(0), Is.EqualTo("One"));
        Assert.That(await reader.ReadAsync(), Is.False);
        
        // We must commit the transaction in order to end it.
        await transaction.CommitAsync();
        
        var requests = Fixture.SpannerMock.Requests.ToList();
        // The transaction should use inline-begin.
        Assert.That(requests.OfType<BeginTransactionRequest>().Count(), Is.EqualTo(0));
        Assert.That(requests.OfType<ExecuteSqlRequest>().Count(), Is.EqualTo(1));
        // Committing a read-only transaction is a no-op on Spanner.
        Assert.That(requests.OfType<CommitRequest>().Count(),  Is.EqualTo(0));
        var executeRequest = requests.OfType<ExecuteSqlRequest>().First();
        Assert.That(executeRequest.Transaction, Is.EqualTo(new TransactionSelector
        {
            Begin = new TransactionOptions
            {
                ReadOnly = new TransactionOptions.Types.ReadOnly
                {
                    Strong = true,
                    ReturnReadTimestamp = true,
                },
            }
        }));
    }

    [Ignore("Needs a fix in SpannerLib")]
    [Test]
    public async Task TestTransactionTag()
    {
        const string select = "select value from my_table where id=@id";
        Fixture.SpannerMock.AddOrUpdateStatementResult(select, StatementResult.CreateSingleColumnResultSet(new V1.Type{Code = V1.TypeCode.String}, "value", "one"));
        const string update = "update my_table set my_column=@value where id=@id";
        Fixture.SpannerMock.AddOrUpdateStatementResult(update, StatementResult.CreateUpdateCount(1L));
        
        await using var connection = new SpannerConnection();
        connection.ConnectionString = ConnectionString;
        await connection.OpenAsync();
        await using var setTagCommand = connection.CreateCommand();
        setTagCommand.CommandText = "set transaction_tag='test_tag'";
        await setTagCommand.ExecuteNonQueryAsync();
        await using var transaction = await connection.BeginTransactionAsync();
        
        await using var command = connection.CreateCommand();
        command.CommandText = select;
        var selectParamId = command.CreateParameter();
        selectParamId.ParameterName = "id";
        selectParamId.Value = 1;
        command.Parameters.Add(selectParamId);
        await using var reader = await command.ExecuteReaderAsync();
        Assert.That(await reader.ReadAsync());
        Assert.That(reader.FieldCount, Is.EqualTo(1));
        Assert.That(reader.GetValue(0), Is.EqualTo("one"));
        Assert.That(await reader.ReadAsync(), Is.False);
        
        await using var updateCommand = connection.CreateCommand();
        updateCommand.CommandText = update;
        var paramId = updateCommand.CreateParameter();
        paramId.ParameterName = "id";
        paramId.Value = 1;
        updateCommand.Parameters.Add(paramId);
        var paramValue = updateCommand.CreateParameter();
        paramValue.ParameterName = "value";
        paramValue.Value = "One";
        updateCommand.Parameters.Add(paramValue);
        var updateCount = await updateCommand.ExecuteNonQueryAsync();
        await transaction.CommitAsync();
        
        Assert.That(updateCount, Is.EqualTo(1));
        var requests = Fixture.SpannerMock.Requests.ToList();
        // The transaction should use inline-begin.
        Assert.That(requests.OfType<BeginTransactionRequest>().Count(), Is.EqualTo(0));
        Assert.That(requests.OfType<ExecuteSqlRequest>().Count(), Is.EqualTo(2));
        Assert.That(requests.OfType<CommitRequest>().Count(),  Is.EqualTo(1));
        var selectRequest = requests.OfType<ExecuteSqlRequest>().First();
        Assert.That(selectRequest.Transaction, Is.EqualTo(new TransactionSelector
        {
            Begin = new TransactionOptions
            {
                ReadWrite = new TransactionOptions.Types.ReadWrite(),
            }
        }));
        Assert.That(selectRequest.RequestOptions.TransactionTag, Is.EqualTo("test_tag"));
        var updateRequest = requests.OfType<ExecuteSqlRequest>().Single(request => request.Sql == update);
        Assert.That(updateRequest.RequestOptions.TransactionTag, Is.EqualTo("test_tag"));
        var commitRequest = requests.OfType<CommitRequest>().Single();
        Assert.That(commitRequest.RequestOptions.TransactionTag, Is.EqualTo("test_tag"));
        
        // The next transaction should not use the tag.
        await using var tx2 = await connection.BeginTransactionAsync();
        await using var command2 = connection.CreateCommand();
        command2.CommandText = update;
        command2.Parameters.Add(paramId);
        command2.Parameters.Add(paramValue);
        await command2.ExecuteNonQueryAsync();
        await tx2.CommitAsync();
        
        var lastRequest = requests.OfType<ExecuteSqlRequest>().Last(request => request.Sql == update);
        Assert.That(lastRequest.RequestOptions.TransactionTag, Is.Null);
        var lastCommitRequest = requests.OfType<CommitRequest>().Last();
        Assert.That(lastCommitRequest.RequestOptions.TransactionTag, Is.Null);
    }
}