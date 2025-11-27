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
using System.Text;
using System.Text.Json;
using Google.Cloud.Spanner.V1;
using Google.Cloud.SpannerLib.MockServer;
using Google.Protobuf.WellKnownTypes;
using Grpc.Core;
using Status = Grpc.Core.Status;
using TypeCode = Google.Cloud.Spanner.V1.TypeCode;

namespace Google.Cloud.Spanner.DataProvider.Tests;

public class CommandTests : AbstractMockServerTests
{
    [Test]
    public async Task TestAllParameterTypes()
    {
        var insert = "insert into my_table values (@p1, @p2, @p3, @p4, @p5, @p6, @p7, @p8, @p9, @p10, @p11, @p12)";
        Fixture.SpannerMock.AddOrUpdateStatementResult(insert, StatementResult.CreateUpdateCount(1));
        
        await using var connection = new SpannerConnection();
        connection.ConnectionString = ConnectionString;
        await connection.OpenAsync();
        
        await using var command = connection.CreateCommand();
        command.CommandText = insert;
        // TODO:
        //   - PROTO
        //   - STRUCT
        AddParameter(command, "p1", true);
        AddParameter(command, "p2", new byte[] {1, 2, 3});
        AddParameter(command, "p3", new DateOnly(2025, 10, 2));
        AddParameter(command, "p4", new TimeSpan(1, 2, 3, 4, 5, 6));
        AddParameter(command, "p5", JsonDocument.Parse("{\"key\": \"value\"}"));
        AddParameter(command, "p6", 9.99m);
        AddParameter(command, "p7", "test");
        AddParameter(command, "p8", new DateTime(2025, 10, 2, 15, 57, 31, 999, DateTimeKind.Utc));
        AddParameter(command, "p9", Guid.Parse("5555990c-b259-4539-bd22-5a9293cf10ac"));
        AddParameter(command, "p10", 3.14d);
        AddParameter(command, "p11", 3.14f);
        AddParameter(command, "p12", DBNull.Value);
        
        AddParameter(command, "p13", new bool?[]{true, false, null});
        AddParameter(command, "p14", new byte[]?[]{ [1,2,3], null });
        AddParameter(command, "p15", new DateOnly?[] { new DateOnly(2025, 10, 2), null });
        AddParameter(command, "p16", new TimeSpan?[] { new TimeSpan(1, 2, 3, 4, 5, 6), null });
        AddParameter(command, "p17", new [] { JsonDocument.Parse("{\"key\": \"value\"}"), null });
        AddParameter(command, "p18", new decimal?[] { 9.99m, null });
        AddParameter(command, "p19", new [] { "test", null });
        AddParameter(command, "p20", new DateTime?[] { new DateTime(2025, 10, 2, 15, 57, 31, 999, DateTimeKind.Utc), null });
        AddParameter(command, "p21", new Guid?[] { Guid.Parse("5555990c-b259-4539-bd22-5a9293cf10ac"), null });
        AddParameter(command, "p22", new double?[] { 3.14d, null });
        AddParameter(command, "p23", new float?[] { 3.14f, null });
        
        await command.ExecuteNonQueryAsync();

        var requests = Fixture.SpannerMock.Requests.ToList();
        Assert.That(requests.OfType<ExecuteSqlRequest>().Count, Is.EqualTo(1));
        Assert.That(requests.OfType<CommitRequest>().Count, Is.EqualTo(1));
        var request = requests.OfType<ExecuteSqlRequest>().Single();
        // The driver does not send any parameter types, unless it is explicitly asked to do so.
        Assert.That(request.ParamTypes.Count, Is.EqualTo(0));
        Assert.That(request.Params.Fields.Count, Is.EqualTo(23));
        var fields = request.Params.Fields;
        Assert.That(fields["p1"].HasBoolValue, Is.True);
        Assert.That(fields["p1"].BoolValue, Is.True);
        Assert.That(fields["p2"].HasStringValue, Is.True);
        Assert.That(fields["p2"].StringValue, Is.EqualTo(Convert.ToBase64String(new byte[]{1,2,3})));
        Assert.That(fields["p3"].HasStringValue, Is.True);
        Assert.That(fields["p3"].StringValue, Is.EqualTo("2025-10-02"));
        Assert.That(fields["p4"].HasStringValue, Is.True);
        Assert.That(fields["p4"].StringValue, Is.EqualTo("P1DT2H3M4.005006S"));
        Assert.That(fields["p5"].HasStringValue, Is.True);
        Assert.That(fields["p5"].StringValue, Is.EqualTo("{\"key\": \"value\"}"));
        Assert.That(fields["p6"].HasStringValue, Is.True);
        Assert.That(fields["p6"].StringValue, Is.EqualTo("9.99"));
        Assert.That(fields["p7"].HasStringValue, Is.True);
        Assert.That(fields["p7"].StringValue, Is.EqualTo("test"));
        Assert.That(fields["p8"].HasStringValue, Is.True);
        Assert.That(fields["p8"].StringValue, Is.EqualTo("2025-10-02T15:57:31.9990000Z"));
        Assert.That(fields["p9"].HasStringValue, Is.True);
        Assert.That(fields["p9"].StringValue, Is.EqualTo("5555990c-b259-4539-bd22-5a9293cf10ac"));
        Assert.That(fields["p10"].HasNumberValue, Is.True);
        Assert.That(fields["p10"].NumberValue, Is.EqualTo(3.14d));
        Assert.That(fields["p11"].HasNumberValue, Is.True);
        Assert.That(fields["p11"].NumberValue, Is.EqualTo(3.14f));
        Assert.That(fields["p12"].HasNullValue, Is.True);
        
        Assert.That(fields["p13"].KindCase, Is.EqualTo(Value.KindOneofCase.ListValue));
        Assert.That(fields["p13"].ListValue.Values.Count, Is.EqualTo(3));
        Assert.That(fields["p13"].ListValue.Values[0].HasBoolValue, Is.True);
        Assert.That(fields["p13"].ListValue.Values[0].BoolValue, Is.True);
        Assert.That(fields["p13"].ListValue.Values[1].HasBoolValue, Is.True);
        Assert.That(fields["p13"].ListValue.Values[1].BoolValue, Is.False);
        Assert.That(fields["p13"].ListValue.Values[2].HasNullValue, Is.True);
        
        Assert.That(fields["p14"].KindCase, Is.EqualTo(Value.KindOneofCase.ListValue));
        Assert.That(fields["p14"].ListValue.Values.Count, Is.EqualTo(2));
        Assert.That(fields["p14"].ListValue.Values[0].HasStringValue, Is.True);
        Assert.That(fields["p14"].ListValue.Values[0].StringValue, Is.EqualTo(Convert.ToBase64String(new byte[]{1,2,3})));
        Assert.That(fields["p14"].ListValue.Values[1].HasNullValue, Is.True);
        
        Assert.That(fields["p15"].KindCase, Is.EqualTo(Value.KindOneofCase.ListValue));
        Assert.That(fields["p15"].ListValue.Values.Count, Is.EqualTo(2));
        Assert.That(fields["p15"].ListValue.Values[0].HasStringValue, Is.True);
        Assert.That(fields["p15"].ListValue.Values[0].StringValue, Is.EqualTo("2025-10-02"));
        Assert.That(fields["p15"].ListValue.Values[1].HasNullValue, Is.True);
        
        Assert.That(fields["p16"].KindCase, Is.EqualTo(Value.KindOneofCase.ListValue));
        Assert.That(fields["p16"].ListValue.Values.Count, Is.EqualTo(2));
        Assert.That(fields["p16"].ListValue.Values[0].HasStringValue, Is.True);
        Assert.That(fields["p16"].ListValue.Values[0].StringValue, Is.EqualTo("P1DT2H3M4.005006S"));
        Assert.That(fields["p16"].ListValue.Values[1].HasNullValue, Is.True);
        
        Assert.That(fields["p17"].KindCase, Is.EqualTo(Value.KindOneofCase.ListValue));
        Assert.That(fields["p17"].ListValue.Values.Count, Is.EqualTo(2));
        Assert.That(fields["p17"].ListValue.Values[0].HasStringValue, Is.True);
        Assert.That(fields["p17"].ListValue.Values[0].StringValue, Is.EqualTo("{\"key\": \"value\"}"));
        Assert.That(fields["p17"].ListValue.Values[1].HasNullValue, Is.True);
        
        Assert.That(fields["p18"].KindCase, Is.EqualTo(Value.KindOneofCase.ListValue));
        Assert.That(fields["p18"].ListValue.Values.Count, Is.EqualTo(2));
        Assert.That(fields["p18"].ListValue.Values[0].HasStringValue, Is.True);
        Assert.That(fields["p18"].ListValue.Values[0].StringValue, Is.EqualTo("9.99"));
        Assert.That(fields["p18"].ListValue.Values[1].HasNullValue, Is.True);
        
        Assert.That(fields["p19"].KindCase, Is.EqualTo(Value.KindOneofCase.ListValue));
        Assert.That(fields["p19"].ListValue.Values.Count, Is.EqualTo(2));
        Assert.That(fields["p19"].ListValue.Values[0].HasStringValue, Is.True);
        Assert.That(fields["p19"].ListValue.Values[0].StringValue, Is.EqualTo("test"));
        Assert.That(fields["p19"].ListValue.Values[1].HasNullValue, Is.True);
        
        Assert.That(fields["p20"].KindCase, Is.EqualTo(Value.KindOneofCase.ListValue));
        Assert.That(fields["p20"].ListValue.Values.Count, Is.EqualTo(2));
        Assert.That(fields["p20"].ListValue.Values[0].HasStringValue, Is.True);
        Assert.That(fields["p20"].ListValue.Values[0].StringValue, Is.EqualTo("2025-10-02T15:57:31.9990000Z"));
        Assert.That(fields["p20"].ListValue.Values[1].HasNullValue, Is.True);
        
        Assert.That(fields["p21"].KindCase, Is.EqualTo(Value.KindOneofCase.ListValue));
        Assert.That(fields["p21"].ListValue.Values.Count, Is.EqualTo(2));
        Assert.That(fields["p21"].ListValue.Values[0].HasStringValue, Is.True);
        Assert.That(fields["p21"].ListValue.Values[0].StringValue, Is.EqualTo("5555990c-b259-4539-bd22-5a9293cf10ac"));
        Assert.That(fields["p21"].ListValue.Values[1].HasNullValue, Is.True);
        
        Assert.That(fields["p22"].KindCase, Is.EqualTo(Value.KindOneofCase.ListValue));
        Assert.That(fields["p22"].ListValue.Values.Count, Is.EqualTo(2));
        Assert.That(fields["p22"].ListValue.Values[0].HasNumberValue, Is.True);
        Assert.That(fields["p22"].ListValue.Values[0].NumberValue, Is.EqualTo(3.14d));
        Assert.That(fields["p22"].ListValue.Values[1].HasNullValue, Is.True);
        
        Assert.That(fields["p23"].KindCase, Is.EqualTo(Value.KindOneofCase.ListValue));
        Assert.That(fields["p23"].ListValue.Values.Count, Is.EqualTo(2));
        Assert.That(fields["p23"].ListValue.Values[0].HasNumberValue, Is.True);
        Assert.That(fields["p23"].ListValue.Values[0].NumberValue, Is.EqualTo(3.14f));
        Assert.That(fields["p23"].ListValue.Values[1].HasNullValue, Is.True);
    }

    [Test]
    public async Task TestExecuteNonQueryWithSelect()
    {
        const string sql = "select * from my_table";
        Fixture.SpannerMock.AddOrUpdateStatementResult(sql, StatementResult.CreateSelect1ResultSet());
        await using var conn = await OpenConnectionAsync();
        await using var cmd = new SpannerCommand(conn);
        cmd.CommandText = sql;
        
        var result = await cmd.ExecuteNonQueryAsync();
        Assert.That(result, Is.EqualTo(-1));
    }

    [Test]
    public async Task TestExecuteNonQueryWithError()
    {
        const string sql = "select * from my_table";
        Fixture.SpannerMock.AddOrUpdateStatementResult(sql, StatementResult.CreateException(new RpcException(new Status(StatusCode.NotFound, "Table not found"))));
        await using var conn = await OpenConnectionAsync();
        await using var cmd = new SpannerCommand(conn);
        cmd.CommandText = sql;
        
        Assert.ThrowsAsync<SpannerDbException>(async () => await cmd.ExecuteNonQueryAsync());
    }
    
    [Test]
    [TestCase(new[] { true }, TestName = "SingleQuery")]
    [TestCase(new[] { false }, TestName = "SingleNonQuery")]
    [TestCase(new[] { true, true }, TestName = "TwoQueries")]
    [TestCase(new[] { false, false }, TestName = "TwoNonQueries")]
    [TestCase(new[] { false, true }, TestName = "NonQueryQuery")]
    [TestCase(new[] { true, false }, TestName = "QueryNonQuery")]
    public async Task MultipleStatements(bool[] queries)
    {
        const string update = "UPDATE my_table SET name='yo' WHERE 1=0;";
        const string select = "SELECT 1;";
        Fixture.SpannerMock.AddOrUpdateStatementResult(update, StatementResult.CreateUpdateCount(0));
        Fixture.SpannerMock.AddOrUpdateStatementResult(update[..^1], StatementResult.CreateUpdateCount(0));
        Fixture.SpannerMock.AddOrUpdateStatementResult(select, StatementResult.CreateSelect1ResultSet());
        
        await using var conn = await OpenConnectionAsync();
        var sb = new StringBuilder();
        foreach (var query in queries)
        {
            sb.Append(query ? select : update);
        }
        var sql = sb.ToString();
        foreach (var prepare in new[] { false, true })
        {
            await using var cmd = conn.CreateCommand();
            cmd.CommandText = sql;
            if (prepare)
            {
                await cmd.PrepareAsync();
            }
            await using var reader = await cmd.ExecuteReaderAsync();
            var numResultSets = queries.Length;
            for (var i = 0; i < numResultSets; i++)
            {
                Assert.That(await reader.ReadAsync(), Is.EqualTo(queries[i]));
                if (queries[i])
                {
                    Assert.That(reader[0], Is.EqualTo(1));
                }
                Assert.That(await reader.NextResultAsync(), Is.EqualTo(i != numResultSets - 1));
            }
        }
    }

    [Test]
    public async Task MultipleStatementsWithParameters([Values(PrepareOrNot.NotPrepared, PrepareOrNot.Prepared)] PrepareOrNot prepare)
    {
        Fixture.SpannerMock.AddOrUpdateStatementResult("SELECT @p1", StatementResult.CreateResultSet(
            new List<Tuple<TypeCode, string>>([Tuple.Create(TypeCode.Int64, "p1")]),
            new List<object[]>([[8L]])));
        Fixture.SpannerMock.AddOrUpdateStatementResult(" SELECT @p2", StatementResult.CreateResultSet(
            new List<Tuple<TypeCode, string>>([Tuple.Create(TypeCode.String, "p2")]),
            new List<object[]>([["foo"]])));
        
        await using var conn = await OpenConnectionAsync();
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT @p1; SELECT @p2";
        var p1 = new SpannerParameter{ParameterName = "p1"};
        var p2 = new SpannerParameter{ParameterName = "p2"};
        cmd.Parameters.Add(p1);
        cmd.Parameters.Add(p2);
        if (prepare == PrepareOrNot.Prepared)
        {
            await cmd.PrepareAsync();
        }
        p1.Value = 8;
        p2.Value = "foo";
        await using var reader = await cmd.ExecuteReaderAsync();
        Assert.That(await reader.ReadAsync(), Is.True);
        Assert.That(reader.GetInt32(0), Is.EqualTo(8));
        Assert.That(await reader.NextResultAsync(), Is.True);
        Assert.That(await reader.ReadAsync(), Is.True);
        Assert.That(reader.GetString(0), Is.EqualTo("foo"));
        Assert.That(await reader.NextResultAsync(), Is.False);
    }
    
    [Test]
    public async Task SingleRowMultipleStatements([Values(PrepareOrNot.NotPrepared, PrepareOrNot.Prepared)] PrepareOrNot prepare)
    {
        await using var conn = await OpenConnectionAsync();
        await using var cmd = new SpannerCommand("SELECT 1; SELECT 2", conn);
        if (prepare == PrepareOrNot.Prepared)
        {
            await cmd.PrepareAsync();
        }
        await using var reader = await cmd.ExecuteReaderAsync(CommandBehavior.SingleRow);
        Assert.That(await reader.ReadAsync(), Is.True);
        Assert.That(reader.GetInt32(0), Is.EqualTo(1));
        Assert.That(await reader.ReadAsync(), Is.False);
        Assert.That(await reader.NextResultAsync(), Is.False);
    }

    [Test]
    [Ignore("Requires support for statement_timeout in the shared library")]
    public async Task Timeout()
    {
        Fixture.SpannerMock.AddOrUpdateExecutionTime(nameof(Fixture.SpannerMock.ExecuteStreamingSql), ExecutionTime.FromMillis(10, 0));
        
        await using var dataSource = CreateDataSource(csb => csb.CommandTimeout = 1);
        await using var conn = await dataSource.OpenConnectionAsync();
        await using var cmd = new SpannerCommand("SELECT 1", conn);
        Assert.That(() => cmd.ExecuteScalar(), Throws.Exception
            .TypeOf<SpannerDbException>()
            .With.InnerException.TypeOf<TimeoutException>()
        );
        Assert.That(conn.State, Is.EqualTo(ConnectionState.Open));
    }
    
    [Test]
    [Ignore("Requires support for statement_timeout in the shared library")]
    public async Task TimeoutAsync()
    {
        Fixture.SpannerMock.AddOrUpdateExecutionTime(nameof(Fixture.SpannerMock.ExecuteStreamingSql), ExecutionTime.FromMillis(10, 0));
        
        await using var dataSource = CreateDataSource(csb => csb.CommandTimeout = 1);
        await using var conn = await dataSource.OpenConnectionAsync();
        await using var cmd = new SpannerCommand("SELECT 1", conn);
        Assert.That(async () => await cmd.ExecuteScalarAsync(),
            Throws.Exception
                .TypeOf<SpannerDbException>()
                .With.InnerException.TypeOf<TimeoutException>());
        Assert.That(conn.State, Is.EqualTo(ConnectionState.Open));
    }

    [Test]
    public async Task TimeoutSwitchConnection()
    {
        var csb = new SpannerConnectionStringBuilder(ConnectionString);
        Assert.That(csb.CommandTimeout, Is.EqualTo(0));

        await using var dataSource1 = CreateDataSource(ConnectionString + ";CommandTimeout=100");
        await using var c1 = dataSource1.CreateConnection();
        await using var cmd = c1.CreateCommand();
        Assert.That(cmd.CommandTimeout, Is.EqualTo(100));
        await using var dataSource2 = CreateDataSource(ConnectionString + ";CommandTimeout=101");
        await using (var c2 = dataSource2.CreateConnection())
        {
            cmd.Connection = c2;
            Assert.That(cmd.CommandTimeout, Is.EqualTo(101));
        }
        cmd.CommandTimeout = 102;
        await using (var c2 = dataSource2.CreateConnection())
        {
            cmd.Connection = c2;
            Assert.That(cmd.CommandTimeout, Is.EqualTo(102));
        }
    }
    
    [Test]
    [Ignore("Requires support for cancel in the shared library")]
    public async Task Cancel()
    {
        var sql = "insert into my_table (id, value) values (1, 'one')";
        Fixture.SpannerMock.AddOrUpdateStatementResult(sql, StatementResult.CreateUpdateCount(1L));
        Fixture.SpannerMock.AddOrUpdateExecutionTime(nameof(Fixture.SpannerMock.ExecuteStreamingSql), ExecutionTime.FromMillis(50, 0));
        
        await using var conn = await OpenConnectionAsync();
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;

        // ReSharper disable once AccessToDisposedClosure
        var queryTask = Task.Run(() => cmd.ExecuteNonQuery());
        // Wait until the request is on the mock server.
        Fixture.SpannerMock.WaitForRequestsToContain(message => message is ExecuteSqlRequest request && request.Sql == sql);
        cmd.Cancel();
        Assert.That(async () => await queryTask, Throws
            .TypeOf<OperationCanceledException>()
            .With.InnerException.TypeOf<SpannerDbException>()
            .With.InnerException.Property(nameof(SpannerDbException.ErrorCode)).EqualTo(StatusCode.Cancelled)
        );
    }
        
    [Test]
    [SuppressMessage("ReSharper", "MethodHasAsyncOverload")]
    public async Task CloseConnection()
    {
        await using var conn = await OpenConnectionAsync();
        await using (var cmd = new SpannerCommand("SELECT 1", conn))
        await using (var reader = await cmd.ExecuteReaderAsync(CommandBehavior.CloseConnection))
        {
            while (reader.Read())
            {
            }
        }
        Assert.That(conn.State, Is.EqualTo(ConnectionState.Closed));
    }
    
    [Test]
    [SuppressMessage("ReSharper", "MethodHasAsyncOverload")]
    public async Task CloseDuringRead()
    {
        await using var dataSource = CreateDataSource();
        await using var conn = await dataSource.OpenConnectionAsync();
        await using (var cmd = new SpannerCommand("SELECT 1", conn))
        await using (var reader = await cmd.ExecuteReaderAsync())
        {
            reader.Read();
            conn.Close();
            Assert.That(conn.State, Is.EqualTo(ConnectionState.Closed));
            // Closing a SpannerConnection does not close the related readers.
            Assert.False(reader.IsClosed);
        }

        conn.Open();
        Assert.That(conn.State, Is.EqualTo(ConnectionState.Open));
        Assert.That(await conn.ExecuteScalarAsync("SELECT 1"), Is.EqualTo(1));
    }
    
    [Test]
    public async Task CloseConnectionWithException()
    {
        const string sql = "select * from non_existing_table";
        Fixture.SpannerMock.AddOrUpdateStatementResult(sql, StatementResult.CreateException(new RpcException(new Status(StatusCode.NotFound, "Table not found"))));
        
        await using var conn = await OpenConnectionAsync();
        await using (var cmd = new SpannerCommand(sql, conn))
        {
            Assert.That(() => cmd.ExecuteReaderAsync(CommandBehavior.CloseConnection),
                Throws.Exception.TypeOf<SpannerDbException>());
        }
        Assert.That(conn.State, Is.EqualTo(ConnectionState.Closed));
    }
    
    [Test]
    [SuppressMessage("ReSharper", "MethodHasAsyncOverload")]
    public async Task SingleRow([Values(PrepareOrNot.NotPrepared, PrepareOrNot.Prepared)] PrepareOrNot prepare)
    {
        const string sql = "SELECT 1, 2 UNION SELECT 3, 4";
        Fixture.SpannerMock.AddOrUpdateStatementResult(sql, StatementResult.CreateResultSet(
            new List<Tuple<TypeCode, string>>([Tuple.Create(TypeCode.Int64, "c1"), Tuple.Create(TypeCode.Int64, "c2")]),
            new List<object[]>([[1L, 2L], [3L, 4L]])));
        
        await using var conn = await OpenConnectionAsync();
        await using var cmd = new SpannerCommand(sql, conn);
        if (prepare == PrepareOrNot.Prepared)
        {
            cmd.Prepare();
        }

        await using var reader = await cmd.ExecuteReaderAsync(CommandBehavior.SingleRow);
        Assert.That(() => reader.GetInt32(0), Throws.Exception.TypeOf<InvalidOperationException>());
        Assert.That(reader.Read(), Is.True);
        Assert.That(reader.GetInt32(0), Is.EqualTo(1));
        Assert.That(reader.Read(), Is.False);
    }
    
    [Test]
    public async Task CommandTextNotSet()
    {
        await using var conn = await OpenConnectionAsync();
        await using (var cmd = new SpannerCommand())
        {
            cmd.Connection = conn;
            Assert.That(cmd.ExecuteNonQueryAsync, Throws.Exception.TypeOf<InvalidOperationException>());
            cmd.CommandText = null;
            Assert.That(cmd.ExecuteNonQueryAsync, Throws.Exception.TypeOf<InvalidOperationException>());
            cmd.CommandText = "";
        }

        await using (var cmd = conn.CreateCommand())
        {
            Assert.That(cmd.ExecuteNonQueryAsync, Throws.Exception.TypeOf<InvalidOperationException>());
        }
    }
    
    [Test]
    public async Task ExecuteScalar()
    {
        const string sql = "select name from my_table";
        Fixture.SpannerMock.AddOrUpdateStatementResult(sql, StatementResult.CreateResultSet(
            new List<Tuple<TypeCode, string>>([Tuple.Create(TypeCode.String, "name")]),
            new List<object[]>([])));
        
        await using var conn = await OpenConnectionAsync();
        await using var command = new SpannerCommand(sql, conn);
        Assert.That(command.ExecuteScalarAsync, Is.Null);

        Fixture.SpannerMock.AddOrUpdateStatementResult(sql, StatementResult.CreateResultSet(
            new List<Tuple<TypeCode, string>>([Tuple.Create(TypeCode.String, "name")]),
            new List<object[]>([[DBNull.Value]])));
        Assert.That(command.ExecuteScalarAsync, Is.EqualTo(DBNull.Value));

        Fixture.SpannerMock.AddOrUpdateStatementResult(sql, StatementResult.CreateResultSet(
            new List<Tuple<TypeCode, string>>([Tuple.Create(TypeCode.String, "name")]),
            new List<object[]>([["X1"], ["X2"]])));
        Assert.That(command.ExecuteScalarAsync, Is.EqualTo("X1"));
    }

    [Test]
    public async Task ExecuteNonQuery()
    {
        const string insertOneRow = "insert into my_table (name) values ('Test')";
        Fixture.SpannerMock.AddOrUpdateStatementResult(insertOneRow, StatementResult.CreateUpdateCount(1L));
        
        await using var conn = await OpenConnectionAsync();
        await using var cmd = conn.CreateCommand();

        // Insert one row
        cmd.CommandText = insertOneRow;
        Assert.That(cmd.ExecuteNonQueryAsync, Is.EqualTo(1));

        // Insert two rows in one batch using a SQL string that contains two statements.
        cmd.CommandText = $"{insertOneRow}; {insertOneRow}";
        Assert.That(cmd.ExecuteNonQueryAsync, Is.EqualTo(2));

        // Execute a large SQL string.
        var value = TestUtils.GenerateRandomString(10_000_000);
        cmd.CommandText = $"insert into my_table (name) values ('{value}')";
        Fixture.SpannerMock.AddOrUpdateStatementResult(cmd.CommandText, StatementResult.CreateUpdateCount(1L));
        Assert.That(cmd.ExecuteNonQueryAsync, Is.EqualTo(1));
    }
    
    [Test]
    public async Task Dispose()
    {
        await using var conn = await OpenConnectionAsync();
        var cmd = new SpannerCommand("SELECT 1", conn);
        cmd.Dispose();
        Assert.That(() => cmd.ExecuteScalarAsync(), Throws.Exception.TypeOf<ObjectDisposedException>());
        Assert.That(() => cmd.ExecuteNonQueryAsync(), Throws.Exception.TypeOf<ObjectDisposedException>());
        Assert.That(() => cmd.ExecuteReaderAsync(), Throws.Exception.TypeOf<ObjectDisposedException>());
        Assert.That(() => cmd.PrepareAsync(), Throws.Exception.TypeOf<ObjectDisposedException>());
    }
    
    [Test]
    public async Task DisposeDesNotCloseReader()
    {
        await using var conn = await OpenConnectionAsync();
        var cmd = new SpannerCommand("SELECT 1", conn);
        await using var reader1 = await cmd.ExecuteReaderAsync();
        cmd.Dispose();
        cmd = new SpannerCommand("SELECT 1", conn);
        await using var reader2 = await cmd.ExecuteReaderAsync();
        Assert.That(reader2, Is.Not.Null);
        Assert.That(reader1.IsClosed, Is.False);
        Assert.That(await reader1.ReadAsync(), Is.True);
    }
    
    [Test]
    [TestCase(CommandBehavior.Default)]
    [TestCase(CommandBehavior.SequentialAccess)]
    public async Task StatementMappedOutputParameters(CommandBehavior behavior)
    {
        const string sql = "select 3, 4 as param1, 5 as param2, 6";
        Fixture.SpannerMock.AddOrUpdateStatementResult(sql, StatementResult.CreateResultSet(
            new List<Tuple<TypeCode, string>>([
                Tuple.Create(TypeCode.Int64, "c1"),
                Tuple.Create(TypeCode.Int64, "param1"),
                Tuple.Create(TypeCode.Int64, "param2"),
                Tuple.Create(TypeCode.Int64, "c2")]),
            new List<object[]>([[3, 4, 5, 6]])));
        
        await using var conn = await OpenConnectionAsync();
        var command = new SpannerCommand(sql, conn);

        var p = new SpannerParameter
        {
            ParameterName = "param2",
            Direction = ParameterDirection.Output,
            Value = -1,
            DbType = DbType.Int64,
        };
        command.Parameters.Add(p);

        p = new SpannerParameter
        {
            ParameterName = "param1",
            Direction = ParameterDirection.Output,
            Value = -1,
            DbType = DbType.Int64,
        };
        command.Parameters.Add(p);

        p = new SpannerParameter
        {
            ParameterName = "p",
            Direction = ParameterDirection.Output,
            Value = -1,
            DbType = DbType.Int64,
        };
        command.Parameters.Add(p);

        await using var reader = await command.ExecuteReaderAsync(behavior);

        // TODO: Enable if we decide to support output parameters in the same way as npgsql.
        // Assert.That(command.Parameters["param1"].Value, Is.EqualTo(4));
        // Assert.That(command.Parameters["param2"].Value, Is.EqualTo(5));

        await reader.ReadAsync();

        Assert.That(reader.GetInt32(0), Is.EqualTo(3));
        Assert.That(reader.GetInt32(1), Is.EqualTo(4));
        Assert.That(reader.GetInt32(2), Is.EqualTo(5));
        Assert.That(reader.GetInt32(3), Is.EqualTo(6));
    }

    [Test]
    public async Task TableDirect()
    {
        const string sql = "select * from my_table";
        Fixture.SpannerMock.AddOrUpdateStatementResult(sql, StatementResult.CreateResultSet(
            new List<Tuple<TypeCode, string>>([Tuple.Create(TypeCode.String, "name")]),
            new List<object[]>([["foo"]])));
        await using var conn = await OpenConnectionAsync();

        await using var cmd = new SpannerCommand("my_table", conn) { CommandType = CommandType.TableDirect };
        await using var reader = await cmd.ExecuteReaderAsync();
        Assert.That(await reader.ReadAsync(), Is.True);
        Assert.That(reader["name"], Is.EqualTo("foo"));
    }
    
    [Test]
    public async Task InvalidUtf8()
    {
        const string sql = "SELECT 'abc\uD801\uD802d'";
        Fixture.SpannerMock.AddOrUpdateStatementResult("SELECT 'abc��d'", StatementResult.CreateResultSet(
            new List<Tuple<TypeCode, string>>([Tuple.Create(TypeCode.String, "c")]),
            new List<object[]>([["abc��d"]])));
        
        await using var dataSource = CreateDataSource();
        await using var conn = await dataSource.OpenConnectionAsync();
        var value = await conn.ExecuteScalarAsync(sql);
        Assert.That(value, Is.EqualTo("abc��d"));
    }
    
    [Test]
    public async Task UseAcrossConnectionChange([Values(PrepareOrNot.Prepared, PrepareOrNot.NotPrepared)] PrepareOrNot prepare)
    {
        await using var conn1 = await OpenConnectionAsync();
        await using var conn2 = await OpenConnectionAsync();
        await using var cmd = new SpannerCommand("SELECT 1", conn1);
        if (prepare == PrepareOrNot.Prepared)
        {
            await cmd.PrepareAsync();
        }
        cmd.Connection = conn2;
        if (prepare == PrepareOrNot.Prepared)
        {
            await cmd.PrepareAsync();
        }
        Assert.That(await cmd.ExecuteScalarAsync(), Is.EqualTo(1));
    }
    
    [Test]
    public async Task CreateCommandBeforeConnectionOpen()
    {
        await using var conn = new SpannerConnection(ConnectionString);
        var cmd = new SpannerCommand("SELECT 1", conn);
        conn.Open();
        Assert.That(await cmd.ExecuteScalarAsync(), Is.EqualTo(1));
    }
    
    [Test]
    public void ConnectionNotSetThrows()
    {
        var cmd = new SpannerCommand { CommandText = "SELECT 1" };
        Assert.That(() => cmd.ExecuteScalarAsync(), Throws.Exception.TypeOf<InvalidOperationException>());
    }

    [Test]
    public void ConnectionNotOpen_OpensConnection()
    {
        using var conn = new SpannerConnection(ConnectionString);
        var cmd = new SpannerCommand("SELECT 1", conn);
        Assert.That(() => cmd.ExecuteScalarAsync(), Throws.Nothing);
    }
    
    [Test]
    public async Task ExecuteNonQueryThrowsSpannerDbException([Values] bool async)
    {
        const string sql = "insert into my_table (ref) values (1) returning ref";
        Fixture.SpannerMock.AddOrUpdateStatementResult(sql, StatementResult.CreateException(new RpcException(new Status(StatusCode.FailedPrecondition, "Foreign key constraint violation"))));
        await using var conn = await OpenConnectionAsync();

        var ex = async
            ? Assert.ThrowsAsync<SpannerDbException>(async () => await conn.ExecuteNonQueryAsync(sql))
            : Assert.Throws<SpannerDbException>(() => conn.ExecuteNonQuery(sql));
        Assert.That(ex!.Status.Code, Is.EqualTo((int) StatusCode.FailedPrecondition));
    }

    [Test]
    public async Task ExecuteScalarThrowsSpannerDbException([Values] bool async)
    {
        const string sql = "insert into my_table (ref) values (1) returning ref";
        Fixture.SpannerMock.AddOrUpdateStatementResult(sql, StatementResult.CreateException(new RpcException(new Status(StatusCode.FailedPrecondition, "Foreign key constraint violation"))));
        await using var conn = await OpenConnectionAsync();

        var ex = async
            ? Assert.ThrowsAsync<SpannerDbException>(async () => await conn.ExecuteScalarAsync(sql))
            : Assert.Throws<SpannerDbException>(() => conn.ExecuteScalar(sql));
        Assert.That(ex!.Status.Code, Is.EqualTo((int) StatusCode.FailedPrecondition));
    }

    [Test]
    [SuppressMessage("ReSharper", "MethodHasAsyncOverload")]
    public async Task ExecuteReaderThrowsSpannerDbException([Values] bool async)
    {
        const string sql = "insert into my_table (ref) values (1) returning ref";
        Fixture.SpannerMock.AddOrUpdateStatementResult(sql, StatementResult.CreateException(new RpcException(new Status(StatusCode.FailedPrecondition, "Foreign key constraint violation"))));
        await using var conn = await OpenConnectionAsync();
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;

        var ex = async
            ? Assert.ThrowsAsync<SpannerDbException>(async () => await cmd.ExecuteReaderAsync())
            : Assert.Throws<SpannerDbException>(() => cmd.ExecuteReader());
        Assert.That(ex!.Status.Code, Is.EqualTo((int) StatusCode.FailedPrecondition));
    }
    
    [Test]
    public void CommandIsNotRecycled()
    {
        const string sql = "select @p1";
        Fixture.SpannerMock.AddOrUpdateStatementResult(sql, StatementResult.CreateSingleColumnResultSet(new V1.Type{Code = TypeCode.Int64}, "p1", 8L));
        
        using var conn = new SpannerConnection(ConnectionString);
        var cmd1 = conn.CreateCommand();
        cmd1.CommandText = sql;
        var tx = conn.BeginTransaction();
        cmd1.Transaction = tx;
        AddParameter(cmd1, "p1", 8);
        _ = cmd1.ExecuteScalar();
        cmd1.Dispose();

        var cmd2 = conn.CreateCommand();
        Assert.That(cmd2, Is.Not.SameAs(cmd1));
        Assert.That(cmd2.CommandText, Is.Empty);
        Assert.That(cmd2.CommandType, Is.EqualTo(CommandType.Text));
        Assert.That(cmd2.Transaction, Is.Null);
        Assert.That(cmd2.Parameters, Is.Empty);
    }

    [Test]
    public async Task ManyParameters([Values(PrepareOrNot.NotPrepared, PrepareOrNot.Prepared)] PrepareOrNot prepare)
    {
        await using var conn = await OpenConnectionAsync();
        await using var cmd = new SpannerCommand(conn);
        var sb = new StringBuilder($"INSERT INTO my_table (some_column) VALUES ");
        var numParams = ushort.MaxValue;
        for (var i = 0; i < numParams; i++)
        {
            var paramName = "p" + i;
            AddParameter(cmd, paramName, i);
            if (i > 0)
                sb.Append(", ");
            sb.Append($"(@{paramName})");
        }
        cmd.CommandText = sb.ToString();
        Fixture.SpannerMock.AddOrUpdateStatementResult(cmd.CommandText, StatementResult.CreateUpdateCount(numParams));

        if (prepare == PrepareOrNot.Prepared)
        {
            await cmd.PrepareAsync();
        }
        await cmd.ExecuteNonQueryAsync();
    }
    
    [Test]
    public async Task ManyParametersAcrossStatements()
    {
        var result = StatementResult.CreateSelect1ResultSet();
        // Create a command with 100 statements which have 7 params each
        const int numStatements = 100;
        const int numParamsPerStatement = 7;
        
        await using var conn = await OpenConnectionAsync();
        await using var cmd = new SpannerCommand(conn);
        var paramIndex = 0;
        var sb = new StringBuilder();
        for (var statementIndex = 0; statementIndex < numStatements; statementIndex++)
        {
            if (statementIndex > 0)
                sb.Append(";");
            var statement = new StringBuilder();
            statement.Append("SELECT ");
            var startIndex = paramIndex;
            var endIndex = paramIndex + numParamsPerStatement;
            for (; paramIndex < endIndex; paramIndex++)
            {
                var paramName = "p" + paramIndex;
                AddParameter(cmd, paramName, paramIndex);
                if (paramIndex > startIndex)
                    statement.Append(", ");
                statement.Append('@');
                statement.Append(paramName);
            }
            sb.Append(statement);
            Fixture.SpannerMock.AddOrUpdateStatementResult(statement.ToString(), result);
        }
        cmd.CommandText = sb.ToString();
        await cmd.ExecuteNonQueryAsync();
    }
    
    [Test]
    public async Task SameCommandDifferentParamValues()
    {
        const string sql = "select @p";
        Fixture.SpannerMock.AddOrUpdateStatementResult(sql, StatementResult.CreateSingleColumnResultSet(new V1.Type{Code = TypeCode.Int64}, "p", 8L));
        
        await using var conn = await OpenConnectionAsync();
        await using var cmd = new SpannerCommand(sql, conn);
        AddParameter(cmd, "p", 8);
        await cmd.ExecuteNonQueryAsync();
        var request = Fixture.SpannerMock.Requests.First(r => r is ExecuteSqlRequest { Sql: sql }) as ExecuteSqlRequest;
        Assert.That(request, Is.Not.Null);
        Assert.That(request.Params.Fields["p"].StringValue, Is.EqualTo("8"));

        Fixture.SpannerMock.ClearRequests();
        Fixture.SpannerMock.AddOrUpdateStatementResult(sql, StatementResult.CreateSingleColumnResultSet(new V1.Type{Code = TypeCode.Int64}, "p", 9L));
        
        cmd.Parameters[0].Value = 9;
        Assert.That(await cmd.ExecuteScalarAsync(), Is.EqualTo(9));
        request = Fixture.SpannerMock.Requests.First(r => r is ExecuteSqlRequest { Sql: sql }) as ExecuteSqlRequest;
        Assert.That(request, Is.Not.Null);
        Assert.That(request.Params.Fields["p"].StringValue, Is.EqualTo("9"));
    }
    
    [Test]
    public async Task SameCommandDifferentParamInstances()
    {
        const string sql = "select @p";
        Fixture.SpannerMock.AddOrUpdateStatementResult(sql, StatementResult.CreateSingleColumnResultSet(new V1.Type{Code = TypeCode.Int64}, "p", 8L));
        
        await using var conn = await OpenConnectionAsync();
        await using var cmd = new SpannerCommand(sql, conn);
        AddParameter(cmd, "p", 8);
        await cmd.ExecuteNonQueryAsync();
        var request = Fixture.SpannerMock.Requests.First(r => r is ExecuteSqlRequest { Sql: sql }) as ExecuteSqlRequest;
        Assert.That(request, Is.Not.Null);
        Assert.That(request.Params.Fields["p"].StringValue, Is.EqualTo("8"));

        Fixture.SpannerMock.ClearRequests();
        Fixture.SpannerMock.AddOrUpdateStatementResult(sql, StatementResult.CreateSingleColumnResultSet(new V1.Type{Code = TypeCode.Int64}, "p", 9L));
        
        cmd.Parameters.RemoveAt(0);
        AddParameter(cmd, "p", 9);
        Assert.That(await cmd.ExecuteScalarAsync(), Is.EqualTo(9));
        request = Fixture.SpannerMock.Requests.First(r => r is ExecuteSqlRequest { Sql: sql }) as ExecuteSqlRequest;
        Assert.That(request, Is.Not.Null);
        Assert.That(request.Params.Fields["p"].StringValue, Is.EqualTo("9"));
    }
    
    [Test]
    public async Task CancelWhileReadingFromLongRunningQuery()
    {
        const string sql = "select id from my_table";
        Fixture.SpannerMock.AddOrUpdateStatementResult(sql, StatementResult.CreateSingleColumnResultSet(
            new V1.Type{Code = TypeCode.Int64},
            "id",
            [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20]));
        await using var conn = await OpenConnectionAsync();

        await using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;

        using (var cts = new CancellationTokenSource())
        await using (var reader = await cmd.ExecuteReaderAsync(cts.Token))
        {
            Assert.ThrowsAsync<OperationCanceledException>(async () =>
            {
                var i = 0;
                while (await reader.ReadAsync(cts.Token))
                {
                    i++;
                    if (i == 10)
                    {
                        await cts.CancelAsync();
                    }
                }
            });
        }

        cmd.CommandText = "SELECT 1";
        Assert.That(await cmd.ExecuteScalarAsync(CancellationToken.None), Is.EqualTo(1));
    }
    
    [Test]
    public async Task CompletedTransactionThrows([Values] bool commit)
    {
        await using var conn = await OpenConnectionAsync();
        await using var tx = await conn.BeginTransactionAsync();
        await using var cmd = conn.CreateCommand();

        if (commit)
        {
            await tx.CommitAsync();
        }
        else
        {
            await tx.RollbackAsync();
        }
        Assert.Throws<InvalidOperationException>(() => cmd.Transaction = tx);
    }

    private void AddParameter(DbCommand command, string name, object? value)
    {
        var parameter = command.CreateParameter();
        parameter.ParameterName = name;
        parameter.Value = value;
        command.Parameters.Add(parameter);
    }
}