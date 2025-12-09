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
using Google.Cloud.Spanner.V1;
using Google.Cloud.SpannerLib.MockServer;
using Google.Protobuf.WellKnownTypes;
using Grpc.Core;

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

        foreach (var options in new TransactionOptions.Types.ReadOnly?[]
                 {
                     null,
                     new() { Strong = true },
                     new() { ExactStaleness = Duration.FromTimeSpan(TimeSpan.FromSeconds(25)) },
                     new() { ReadTimestamp = Timestamp.FromDateTime(DateTime.UtcNow) },
                 })
        {
            await using var transaction = options == null 
                ? connection.BeginReadOnlyTransaction()
                : connection.BeginReadOnlyTransaction(options);
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
            Fixture.SpannerMock.ClearRequests();
            
            // The transaction should use inline-begin.
            Assert.That(requests.OfType<BeginTransactionRequest>().Count(), Is.EqualTo(0));
            Assert.That(requests.OfType<ExecuteSqlRequest>().Count(), Is.EqualTo(1));
            // Committing a read-only transaction is a no-op on Spanner.
            Assert.That(requests.OfType<CommitRequest>().Count(), Is.EqualTo(0));
            var executeRequest = requests.OfType<ExecuteSqlRequest>().First();
            if (options == null)
            {
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
            else
            {
                var expectedOptions = options;
                expectedOptions.ReturnReadTimestamp = true;
                Assert.That(executeRequest.Transaction, Is.EqualTo(new TransactionSelector
                {
                    Begin = new TransactionOptions
                    {
                        ReadOnly = expectedOptions,
                    }
                }));
            }
        }
    }

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
        
        requests = Fixture.SpannerMock.Requests.ToList();
        var lastRequest = requests.OfType<ExecuteSqlRequest>().Last(request => request.Sql == update);
        Assert.That(lastRequest.RequestOptions.TransactionTag, Is.EqualTo(""));
        var lastCommitRequest = requests.OfType<CommitRequest>().Last();
        Assert.That(lastCommitRequest.RequestOptions.TransactionTag, Is.EqualTo(""));
    }

    [Test]
    public void TestChangeTransactionTagAfterStart()
    {
        const string update = "update my_table set my_column='test' where id=1";
        Fixture.SpannerMock.AddOrUpdateStatementResult(update, StatementResult.CreateUpdateCount(1L));
        
        using var connection = new SpannerConnection();
        connection.ConnectionString = ConnectionString;
        connection.Open();
        
        using var transaction = connection.BeginTransaction();
        transaction.Tag = "first_tag";
        using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = update;
        
        // We can still change the transaction tag as long as no statement has been executed.
        transaction.Tag = "second_tag";
        // Execute a statement. From this point on the transaction tag can no longer be changed.
        command.ExecuteNonQuery();
        
        Assert.Throws<InvalidOperationException>(() => transaction.Tag = "third_tag");
        transaction.Commit();
        
        var requests = Fixture.SpannerMock.Requests.ToList();
        var executeRequest = requests.OfType<ExecuteSqlRequest>().Single();
        Assert.That(executeRequest.RequestOptions.TransactionTag, Is.EqualTo("second_tag"));
        var commitRequest = requests.OfType<CommitRequest>().Single();
        Assert.That(commitRequest.RequestOptions.TransactionTag, Is.EqualTo("second_tag"));
    }
    
    [Test]
    [SuppressMessage("ReSharper", "MethodHasAsyncOverload")]
    public async Task Commit([Values(PrepareOrNot.NotPrepared, PrepareOrNot.Prepared)] PrepareOrNot prepare)
    {
        const string insertSql = "INSERT INTO my_table (name) VALUES ('X')";
        const string selectCountSql = "SELECT COUNT(*) FROM my_table";
        Fixture.SpannerMock.AddOrUpdateStatementResult(insertSql, StatementResult.CreateUpdateCount(1L));
        Fixture.SpannerMock.AddOrUpdateStatementResult(selectCountSql, StatementResult.CreateSelect1ResultSet());
        
        await using var conn = await OpenConnectionAsync();

        var tx = await conn.BeginTransactionAsync();
        await using (tx)
        {
            var cmd = new SpannerCommand(insertSql, conn, tx);
            if (prepare == PrepareOrNot.Prepared)
            {
                cmd.Prepare();
            }
            cmd.ExecuteNonQuery();
            Assert.That(conn.ExecuteScalar("SELECT COUNT(*) FROM my_table"), Is.EqualTo(1));
            tx.Commit();
            Assert.That(tx.IsCompleted);
            Assert.That(() => tx.Connection, Throws.Nothing);
            Assert.That(await conn.ExecuteScalarAsync("SELECT COUNT(*) FROM my_table"), Is.EqualTo(1));
        }
        Assert.That(() => tx.Connection, Throws.Exception.TypeOf<ObjectDisposedException>());
    }
    
    [Test]
    [SuppressMessage("ReSharper", "MethodHasAsyncOverload")]
    public async Task CommitAsync([Values(PrepareOrNot.NotPrepared, PrepareOrNot.Prepared)] PrepareOrNot prepare)
    {
        const string insertSql = "INSERT INTO my_table (name) VALUES ('X')";
        const string selectCountSql = "SELECT COUNT(*) FROM my_table";
        Fixture.SpannerMock.AddOrUpdateStatementResult(insertSql, StatementResult.CreateUpdateCount(1L));
        Fixture.SpannerMock.AddOrUpdateStatementResult(selectCountSql, StatementResult.CreateSelect1ResultSet());
        
        await using var conn = await OpenConnectionAsync();

        var tx = await conn.BeginTransactionAsync();
        await using (tx)
        {
            var cmd = new SpannerCommand(insertSql, conn, tx);
            if (prepare == PrepareOrNot.Prepared)
            {
                cmd.Prepare();
            }
            await cmd.ExecuteNonQueryAsync();
            Assert.That(conn.ExecuteScalar(selectCountSql), Is.EqualTo(1));
            await tx.CommitAsync();
            Assert.That(tx.IsCompleted);
            Assert.That(() => tx.Connection, Throws.Nothing);
            Assert.That(await conn.ExecuteScalarAsync(selectCountSql), Is.EqualTo(1));
        }
        Assert.That(() => tx.Connection, Throws.Exception.TypeOf<ObjectDisposedException>());
    }

    [Test]
    [SuppressMessage("ReSharper", "MethodHasAsyncOverload")]
    public async Task Rollback([Values(PrepareOrNot.NotPrepared, PrepareOrNot.Prepared)] PrepareOrNot prepare)
    {
        const string insertSql = "INSERT INTO my_table (name) VALUES ('X')";
        const string selectCountSql = "SELECT COUNT(*) FROM my_table";
        Fixture.SpannerMock.AddOrUpdateStatementResult(insertSql, StatementResult.CreateUpdateCount(1L));
        
        await using var conn = await OpenConnectionAsync();

        var tx = await conn.BeginTransactionAsync();
        await using (tx)
        {
            var cmd = new SpannerCommand(insertSql, conn, tx);
            if (prepare == PrepareOrNot.Prepared)
            {
                cmd.Prepare();
            }
            cmd.ExecuteNonQuery();
            Fixture.SpannerMock.AddOrUpdateStatementResult(selectCountSql, StatementResult.CreateSelect1ResultSet());
            
            Assert.That(conn.ExecuteScalar(selectCountSql), Is.EqualTo(1));
            tx.Rollback();
            Fixture.SpannerMock.AddOrUpdateStatementResult(selectCountSql, StatementResult.CreateSelectZeroResultSet());
            
            Assert.That(tx.IsCompleted);
            Assert.That(() => tx.Connection, Throws.Nothing);
            Assert.That(await conn.ExecuteScalarAsync(selectCountSql), Is.EqualTo(0));
        }
        Assert.That(() => tx.Connection, Throws.Exception.TypeOf<ObjectDisposedException>());
    }

    [Test]
    [SuppressMessage("ReSharper", "MethodHasAsyncOverload")]
    public async Task RollbackAsync([Values(PrepareOrNot.NotPrepared, PrepareOrNot.Prepared)] PrepareOrNot prepare)
    {
        const string insertSql = "INSERT INTO my_table (name) VALUES ('X')";
        const string selectCountSql = "SELECT COUNT(*) FROM my_table";
        Fixture.SpannerMock.AddOrUpdateStatementResult(insertSql, StatementResult.CreateUpdateCount(1L));
        
        await using var conn = await OpenConnectionAsync();

        var tx = await conn.BeginTransactionAsync();
        await using (tx)
        {
            var cmd = new SpannerCommand(insertSql, conn, tx);
            if (prepare == PrepareOrNot.Prepared)
            {
                cmd.Prepare();
            }
            await cmd.ExecuteNonQueryAsync();
            Fixture.SpannerMock.AddOrUpdateStatementResult(selectCountSql, StatementResult.CreateSelect1ResultSet());
            
            Assert.That(conn.ExecuteScalar(selectCountSql), Is.EqualTo(1));
            await tx.RollbackAsync();
            Fixture.SpannerMock.AddOrUpdateStatementResult(selectCountSql, StatementResult.CreateSelectZeroResultSet());
            
            Assert.That(tx.IsCompleted);
            Assert.That(() => tx.Connection, Throws.Nothing);
            Assert.That(await conn.ExecuteScalarAsync(selectCountSql), Is.EqualTo(0));
        }
        Assert.That(() => tx.Connection, Throws.Exception.TypeOf<ObjectDisposedException>());
    }

    [Test]
    public async Task RollbackOnDispose()
    {
        const string insertSql = "INSERT INTO my_table (name) VALUES ('X')";
        Fixture.SpannerMock.AddOrUpdateStatementResult(insertSql, StatementResult.CreateUpdateCount(1L));
        
        await using var conn = await OpenConnectionAsync();
        await using (var tx = await conn.BeginTransactionAsync())
        {
            await conn.ExecuteNonQueryAsync(insertSql, tx: tx);
        }
        
        // The rollback that is initiated by disposing the transaction is an async shoot-and-forget rollback request.
        // So we need to wait a bit for it to show up on the mock server.
        Fixture.SpannerMock.WaitForRequestsToContain(request => request is RollbackRequest, TimeSpan.FromSeconds(1));
        var requests = Fixture.SpannerMock.Requests
            .Where(request => request is ExecuteSqlRequest or RollbackRequest or CommitRequest)
            .Select(request => request.GetType());
        Assert.That(requests, Is.EqualTo(new[]{typeof(ExecuteSqlRequest), typeof(RollbackRequest)}));
    }

    [Test]
    [SuppressMessage("ReSharper", "UseAwaitUsing")]
    public async Task RollbackOnClose()
    {
        const string insertSql = "INSERT INTO my_table (name) VALUES ('X')";
        Fixture.SpannerMock.AddOrUpdateStatementResult(insertSql, StatementResult.CreateUpdateCount(1L));
        
        using (var conn = await OpenConnectionAsync())
        {
            var tx = await conn.BeginTransactionAsync();
            await conn.ExecuteNonQueryAsync(insertSql, tx);
        }
        
        // The rollback that is initiated by closing the connection is an async shoot-and-forget rollback request.
        // So we need to wait a bit for it to show up on the mock server.
        Fixture.SpannerMock.WaitForRequestsToContain(request => request is RollbackRequest, TimeSpan.FromSeconds(1));
        var requests = Fixture.SpannerMock.Requests
            .Where(request => request is ExecuteSqlRequest or RollbackRequest or CommitRequest)
            .Select(request => request.GetType());
        Assert.That(requests, Is.EqualTo(new[]{typeof(ExecuteSqlRequest), typeof(RollbackRequest)}));
    }
    
    [Test]
    [SuppressMessage("ReSharper", "MethodHasAsyncOverload")]
    public async Task RollbackFailedTransaction()
    {
        const string insertSql = "INSERT INTO my_table (name) VALUES ('X')";
        const string badQuery = "BAD QUERY";
        Fixture.SpannerMock.AddOrUpdateStatementResult(insertSql, StatementResult.CreateUpdateCount(1L));
        Fixture.SpannerMock.AddOrUpdateStatementResult(badQuery, StatementResult.CreateException(new RpcException(new Status(StatusCode.InvalidArgument, "Invalid SQL"))));
        
        await using var conn = await OpenConnectionAsync();
    
        await using var tx = await conn.BeginTransactionAsync();
        await conn.ExecuteNonQueryAsync(insertSql, tx: tx);
        Assert.That(async () => await conn.ExecuteNonQueryAsync(badQuery), Throws.Exception.TypeOf<SpannerDbException>());
        tx.Rollback();
        Assert.That(tx.IsCompleted);
        
        var requests = Fixture.SpannerMock.Requests
            .Where(request => request is ExecuteSqlRequest or RollbackRequest or CommitRequest)
            .Select(request => request.GetType());
        Assert.That(requests, Is.EqualTo(new[]
        {
            typeof(ExecuteSqlRequest),
            typeof(ExecuteSqlRequest),
            typeof(RollbackRequest)
        }));
    }
    
    [Test]
    [SuppressMessage("ReSharper", "MethodHasAsyncOverload")]
    public async Task EmptyCommit()
    {
        await using var conn = await OpenConnectionAsync();
        await conn.BeginTransaction().CommitAsync();
        
        // Empty transactions are a no-op.
        var requests = Fixture.SpannerMock.Requests
            .Where(request => request is BeginTransactionRequest or RollbackRequest or CommitRequest)
            .Select(request => request.GetType());
        Assert.That(requests, Is.Empty);
    }

    [Test]
    [SuppressMessage("ReSharper", "MethodHasAsyncOverload")]
    public async Task EmptyRollback()
    {
        await using var conn = await OpenConnectionAsync();
        await conn.BeginTransaction().RollbackAsync();
        
        // Empty transactions are a no-op.
        var requests = Fixture.SpannerMock.Requests
            .Where(request => request is BeginTransactionRequest or RollbackRequest or CommitRequest)
            .Select(request => request.GetType());
        Assert.That(requests, Is.Empty);
    }

    [Test]
    [SuppressMessage("ReSharper", "MethodHasAsyncOverload")]
    [SuppressMessage("ReSharper", "UseAwaitUsing")]
    public async Task EmptyDispose()
    {
        await using var dataSource = CreateDataSource();

        using (var conn = await dataSource.OpenConnectionAsync())
        using (conn.BeginTransaction())
        { }

        using (var conn = await dataSource.OpenConnectionAsync())
        {
            // Make sure the BeginTransaction from the previous connection did not carry over to the new connection.
            Assert.That(async () => await conn.BeginTransactionAsync(), Throws.Nothing);
        }
    }

    [Test]
    public async Task DbConnectionIsolationLevel()
    {
        const string insertSql = "INSERT INTO my_table (name) VALUES ('X')";
        Fixture.SpannerMock.AddOrUpdateStatementResult(insertSql, StatementResult.CreateUpdateCount(1L));
        const string selectSql = "select value from my_table where id=@id";
        Fixture.SpannerMock.AddOrUpdateStatementResult(selectSql, StatementResult.CreateSingleColumnResultSet(new V1.Type{Code = V1.TypeCode.String}, "value", "One"));
        
        var cancellationToken = CancellationToken.None;
        await using var conn = await OpenConnectionAsync();
        DbConnection dbConn = conn;
        await using var transaction = await dbConn.BeginTransactionAsync(IsolationLevel.RepeatableRead, cancellationToken);
        await using var cmd = dbConn.CreateCommand();
        cmd.CommandText = "set local transaction_tag = 'spanner-lib'";
        await cmd.ExecuteNonQueryAsync(cancellationToken);

        for (var i = 0; i < 3; i++)
        {
            await using var selectCommand = dbConn.CreateCommand();
            selectCommand.CommandText = selectSql;
            selectCommand.Transaction = transaction;
            await using var reader = await selectCommand.ExecuteReaderAsync(cancellationToken);
            var foundRows = 0;
            while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
            {
                foundRows++;
            }

            if (foundRows != 1)
            {
                throw new InvalidOperationException("Unexpected found rows: " + foundRows);
            }

            await using var updateCommand = dbConn.CreateCommand();
            updateCommand.CommandText = insertSql;
            updateCommand.Transaction = transaction;
            var updated = await updateCommand.ExecuteNonQueryAsync(cancellationToken);
            if (updated != 1)
            {
                throw new InvalidOperationException("Unexpected affected rows: " + updated);
            }
        }
        await transaction.CommitAsync(cancellationToken);

        var firstRequest = Fixture.SpannerMock.Requests.OfType<ExecuteSqlRequest>().First();
        Assert.That(firstRequest.Transaction.Begin.IsolationLevel, Is.EqualTo(TransactionOptions.Types.IsolationLevel.RepeatableRead));
        var requests = Fixture.SpannerMock.Requests;
        Assert.That(requests.OfType<ExecuteSqlRequest>().Count(), Is.EqualTo(6));
    }

    [Test]
    [TestCase(IsolationLevel.RepeatableRead,  TransactionOptions.Types.IsolationLevel.RepeatableRead, false)]
    [TestCase(IsolationLevel.Serializable,    TransactionOptions.Types.IsolationLevel.Serializable, false)]
    [TestCase(IsolationLevel.Snapshot,        TransactionOptions.Types.IsolationLevel.RepeatableRead, false)]
    [TestCase(IsolationLevel.Unspecified,     TransactionOptions.Types.IsolationLevel.Unspecified, false)]
    [TestCase(IsolationLevel.RepeatableRead,  TransactionOptions.Types.IsolationLevel.RepeatableRead, true)]
    [TestCase(IsolationLevel.Serializable,    TransactionOptions.Types.IsolationLevel.Serializable, true)]
    [TestCase(IsolationLevel.Snapshot,        TransactionOptions.Types.IsolationLevel.RepeatableRead, true)]
    [TestCase(IsolationLevel.Unspecified,     TransactionOptions.Types.IsolationLevel.Unspecified, true)]
    [SuppressMessage("ReSharper", "MethodHasAsyncOverload")]
    public async Task SupportedIsolationLevels(IsolationLevel level, TransactionOptions.Types.IsolationLevel expectedSpannerLevel, bool async)
    {
        const string insertSql = "INSERT INTO my_table (name) VALUES ('X')";
        Fixture.SpannerMock.AddOrUpdateStatementResult(insertSql, StatementResult.CreateUpdateCount(1L));
        
        await using var conn = await OpenConnectionAsync();
        var tx = async ? await conn.BeginTransactionAsync(level) : conn.BeginTransaction(level);
        var cmd = conn.CreateCommand("set local transaction_tag='test'");
        cmd.Transaction = tx;
        await cmd.ExecuteNonQueryAsync();
        await conn.ExecuteNonQueryAsync(insertSql, tx: tx);
        
        // TODO: Add support for this to the shared lib.
        // Assert.That(conn.ExecuteScalar("SHOW TRANSACTION ISOLATION LEVEL"), Is.EqualTo(expectedSpannerLevel.ToString()));
        if (async)
        {
            await tx.CommitAsync();
        }
        else
        {
            tx.Commit();
        }
        
        var request = Fixture.SpannerMock.Requests.OfType<ExecuteSqlRequest>().First();
        Assert.That(request.Transaction.Begin.IsolationLevel, Is.EqualTo(expectedSpannerLevel));
    }

    [Test]
    [TestCase(IsolationLevel.Chaos)]
    [TestCase(IsolationLevel.ReadUncommitted)]
    [TestCase(IsolationLevel.ReadCommitted)]
    public async Task UnsupportedIsolationLevels(IsolationLevel level)
    {
        await using var conn = await OpenConnectionAsync();
        Assert.That(() => conn.BeginTransaction(level), Throws.Exception.TypeOf<NotSupportedException>());
    }

    [Test]
    [SuppressMessage("ReSharper", "MethodHasAsyncOverload")]
    public async Task RollbackTwice()
    {
        await using var conn = await OpenConnectionAsync();
        var transaction = conn.BeginTransaction();
        transaction.Rollback();
        Assert.That(() => transaction.Rollback(), Throws.Exception.TypeOf<InvalidOperationException>());
    }

    [Test]
    [SuppressMessage("ReSharper", "MethodHasAsyncOverload")]
    public async Task DefaultIsolationLevel()
    {
        await using var conn = await OpenConnectionAsync();
        var tx = conn.BeginTransaction();
        Assert.That(tx.IsolationLevel, Is.EqualTo(IsolationLevel.Unspecified));
        tx.Rollback();

        tx = conn.BeginTransaction(IsolationLevel.Unspecified);
        Assert.That(tx.IsolationLevel, Is.EqualTo(IsolationLevel.Unspecified));
        tx.Rollback();
    }

    [Test]
    public async Task ViaSql()
    {
        const string insertSql = "INSERT INTO my_table (name) VALUES ('X')";
        Fixture.SpannerMock.AddOrUpdateStatementResult(insertSql, StatementResult.CreateUpdateCount(1L));
        
        await using var conn = await OpenConnectionAsync();

        await conn.ExecuteNonQueryAsync("BEGIN");
        await conn.ExecuteNonQueryAsync(insertSql);
        await conn.ExecuteNonQueryAsync("ROLLBACK");
        
        var requests = Fixture.SpannerMock.Requests
            .Where(request => request is ExecuteSqlRequest or RollbackRequest or CommitRequest)
            .Select(request => request.GetType());
        Assert.That(requests, Is.EqualTo(new[]
        {
            typeof(ExecuteSqlRequest),
            typeof(RollbackRequest)
        }));
        var executeRequest = Fixture.SpannerMock.Requests.OfType<ExecuteSqlRequest>().First();
        Assert.That(executeRequest.Transaction?.Begin?.ReadWrite, Is.Not.Null);
        Assert.That(executeRequest.Transaction.Begin.IsolationLevel, Is.EqualTo(TransactionOptions.Types.IsolationLevel.Unspecified));
    }

    [Test]
    [SuppressMessage("ReSharper", "MethodHasAsyncOverload")]
    public async Task Nested()
    {
        await using var conn = await OpenConnectionAsync();
        conn.BeginTransaction();
        Assert.That(() => conn.BeginTransaction(), Throws.TypeOf<InvalidOperationException>());
    }

    [Test]
    public void BeginTransactionOnClosedConnectionThrows()
    {
        using var conn = new SpannerConnection();
        Assert.That(() => conn.BeginTransaction(), Throws.Exception.TypeOf<InvalidOperationException>());
    }

    [Test]
    [SuppressMessage("ReSharper", "MethodHasAsyncOverload")]
    public async Task IsCompletedCommit()
    {
        const string insertSql = "INSERT INTO my_table (name) VALUES ('X')";
        Fixture.SpannerMock.AddOrUpdateStatementResult(insertSql, StatementResult.CreateUpdateCount(1L));
        
        await using var conn = await OpenConnectionAsync();
        var tx = conn.BeginTransaction();
        Assert.That(!tx.IsCompleted);
        await conn.ExecuteNonQueryAsync(insertSql, tx: tx);
        Assert.That(!tx.IsCompleted);
        await tx.CommitAsync();
        Assert.That(tx.IsCompleted);
    }

    [Test]
    [SuppressMessage("ReSharper", "MethodHasAsyncOverload")]
    public async Task IsCompletedRollback()
    {
        const string insertSql = "INSERT INTO my_table (name) VALUES ('X')";
        Fixture.SpannerMock.AddOrUpdateStatementResult(insertSql, StatementResult.CreateUpdateCount(1L));
        
        await using var conn = await OpenConnectionAsync();
        var tx = conn.BeginTransaction();
        Assert.That(!tx.IsCompleted);
        await conn.ExecuteNonQueryAsync(insertSql, tx: tx);
        Assert.That(!tx.IsCompleted);
        tx.Rollback();
        Assert.That(tx.IsCompleted);
    }

    [Test]
    [SuppressMessage("ReSharper", "MethodHasAsyncOverload")]
    public async Task IsCompletedRollbackFailedTransaction()
    {
        const string insertSql = "INSERT INTO my_table (name) VALUES ('X')";
        const string badQuery = "BAD QUERY";
        Fixture.SpannerMock.AddOrUpdateStatementResult(insertSql, StatementResult.CreateUpdateCount(1L));
        Fixture.SpannerMock.AddOrUpdateStatementResult(badQuery, StatementResult.CreateException(new RpcException(new Status(StatusCode.InvalidArgument, "Invalid SQL"))));
        
        await using var conn = await OpenConnectionAsync();
        var tx = conn.BeginTransaction();
        Assert.That(!tx.IsCompleted);
        await conn.ExecuteNonQueryAsync(insertSql, tx: tx);
        Assert.That(!tx.IsCompleted);
        Assert.That(async () => await conn.ExecuteNonQueryAsync(badQuery), Throws.Exception.TypeOf<SpannerDbException>());
        Assert.That(!tx.IsCompleted);
        tx.Rollback();
        Assert.That(tx.IsCompleted);
    }

    [Test]
    [SuppressMessage("ReSharper", "UseAwaitUsing")]
    public async Task DisposeTransactionRollbackOnOnlyFailedStatement()
    {
        const string sql = "SELECT * FROM unknown_table";
        Fixture.SpannerMock.AddOrUpdateStatementResult(sql, StatementResult.CreateException(new RpcException(new Status(StatusCode.NotFound, "Invalid table"))));
        
        using var conn = await OpenConnectionAsync();
        // Execute a read/write transaction with only a failed statement.
        // This will only lead to a single ExecuteSqlRequest being sent to Spanner.
        // That request fails to return a transaction ID, which again means that no Rollback will be sent to Spanner.
        await using (var tx = await conn.BeginTransactionAsync())
        {
            Assert.That(async () => await conn.ExecuteScalarAsync(sql, tx: tx),
                Throws.Exception.TypeOf<SpannerDbException>());
        }
        
        var requests = Fixture.SpannerMock.Requests
            .Where(request => request is ExecuteSqlRequest or BeginTransactionRequest or RollbackRequest or CommitRequest)
            .Select(request => request.GetType());
        Assert.That(requests, Is.EqualTo(new[]{typeof(ExecuteSqlRequest)}));
    }

    [Test]
    [SuppressMessage("ReSharper", "MethodHasAsyncOverload")]
    public async Task DisposeConnectionRollbackOnOnlyFailedStatement()
    {
        const string sql = "SELECT * FROM unknown_table";
        Fixture.SpannerMock.AddOrUpdateStatementResult(sql, StatementResult.CreateException(new RpcException(new Status(StatusCode.NotFound, "Invalid table"))));
        
        // Execute a read/write transaction with only a failed statement.
        // This will only lead to a single ExecuteSqlRequest being sent to Spanner.
        // That request fails to return a transaction ID, which again means that no Rollback will be sent to Spanner.
        var conn = await OpenConnectionAsync();
        var tx = conn.BeginTransaction();
        Assert.That(async () => await conn.ExecuteScalarAsync(sql, tx: tx), Throws.Exception.TypeOf<SpannerDbException>());

        await conn.DisposeAsync();
        
        var requests = Fixture.SpannerMock.Requests
            .Where(request => request is ExecuteSqlRequest or BeginTransactionRequest or RollbackRequest or CommitRequest)
            .Select(request => request.GetType());
        Assert.That(requests, Is.EqualTo(new[]{typeof(ExecuteSqlRequest)}));
    }

    [Test]
    [SuppressMessage("ReSharper", "MethodHasAsyncOverload")]
    public async Task DisposeConnectionRollbackFailedAndSucceededStatement()
    {
        const string insertSql = "INSERT INTO my_table (name) VALUES ('X')";
        const string sql = "SELECT * FROM unknown_table";
        Fixture.SpannerMock.AddOrUpdateStatementResult(sql, StatementResult.CreateException(new RpcException(new Status(StatusCode.NotFound, "Invalid table"))));
        Fixture.SpannerMock.AddOrUpdateStatementResult(insertSql, StatementResult.CreateUpdateCount(1L));
        
        // Execute a read/write transaction with only a failed statement.
        // This will only lead to a single ExecuteSqlRequest being sent to Spanner.
        // That request fails to return a transaction ID, which again means that no Rollback will be sent to Spanner.
        var conn = await OpenConnectionAsync();
        var tx = conn.BeginTransaction();
        Assert.That(async () => await conn.ExecuteScalarAsync(sql, tx: tx), Throws.Exception.TypeOf<SpannerDbException>());
        await conn.ExecuteNonQueryAsync(insertSql, tx: tx);

        await conn.DisposeAsync();
        
        // The rollback that is initiated by disposing the transaction is an async shoot-and-forget rollback request.
        // So we need to wait a bit for it to show up on the mock server.
        Fixture.SpannerMock.WaitForRequestsToContain(request => request is RollbackRequest, TimeSpan.FromSeconds(1));
        var requests = Fixture.SpannerMock.Requests
            .Where(request => request is ExecuteSqlRequest or BeginTransactionRequest or RollbackRequest or CommitRequest)
            .Select(request => request.GetType());
        Assert.That(requests, Is.EqualTo(new[]
        {
            typeof(ExecuteSqlRequest), // This statement failed
            typeof(BeginTransactionRequest), // This is sent because the first ExecuteSqlRequest failed
            typeof(ExecuteSqlRequest), // This is a retry of the first statement in order to include it in the tx
            typeof(ExecuteSqlRequest), // This is the successful insert
            typeof(RollbackRequest), // This is the shoot-and-forget rollback from closing the connection
        }));
    }
    
    [Test]
    [TestCase(true)]
    [TestCase(false)]
    public async Task Bug3306(bool inTransactionBlock)
    {
        var conn = await OpenConnectionAsync();
        var tx = await conn.BeginTransactionAsync();
        await conn.ExecuteNonQueryAsync("SELECT 1", tx);
        if (!inTransactionBlock)
        {
            await tx.RollbackAsync();
        }
        await conn.CloseAsync();

        conn = await OpenConnectionAsync();
        var tx2 = await conn.BeginTransactionAsync();

        await tx.DisposeAsync();

        Assert.That(tx.IsDisposed, Is.True);
        Assert.That(tx2.IsDisposed, Is.False);

        await conn.DisposeAsync();
    }

    [Test]
    [SuppressMessage("ReSharper", "MethodHasAsyncOverload")]
    public async Task AccessConnectionOnCompletedTransaction()
    {
        await using var conn = await OpenConnectionAsync();
        await using var tx = await conn.BeginTransactionAsync();
        tx.Commit();
        Assert.That(tx.Connection, Is.Null);
    }
    
    [Test]
    public async Task CanAccessConnectionAfterCommit()
    {
        await using var dataSource = CreateDataSource();
        await using var conn = await dataSource.OpenConnectionAsync();
        await using var tx = await conn.BeginTransactionAsync();
        await conn.ExecuteNonQueryAsync("SELECT 1", tx);
        await tx.CommitAsync();
        await conn.CloseAsync();
        Assert.DoesNotThrow(() =>
        {
            _ = tx.Connection;
        });
    }

}