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
using System.Data;
using System.Data.Common;
using System.Linq;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;
using Google.Cloud.Spanner.V1;
using Google.Cloud.SpannerLib.MockServer;
using Google.Protobuf.WellKnownTypes;

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
    [TestCase(new[] { true }, TestName = "SingleQuery")]
    [TestCase(new[] { false }, TestName = "SingleNonQuery")]
    [TestCase(new[] { true, true }, TestName = "TwoQueries")]
    [TestCase(new[] { false, false }, TestName = "TwoNonQueries")]
    [TestCase(new[] { false, true }, TestName = "NonQueryQuery")]
    [TestCase(new[] { true, false }, TestName = "QueryNonQuery")]
    [Ignore("Requires support for multi-statements strings in the shared library")]
    public async Task MultipleStatements(bool[] queries)
    {
        const string update = "UPDATE my_table SET name='yo' WHERE 1=0;";
        const string select = "SELECT 1;";
        Fixture.SpannerMock.AddOrUpdateStatementResult(update, StatementResult.CreateUpdateCount(0));
        
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
            var numResultSets = queries.Count(q => q);
            for (var i = 0; i < numResultSets; i++)
            {
                Assert.That(await reader.ReadAsync(), Is.True);
                Assert.That(reader[0], Is.EqualTo(1));
                Assert.That(await reader.NextResultAsync(), Is.EqualTo(i != numResultSets - 1));
            }
        }
    }

    [Test]
    [Ignore("Requires support for multi-statements strings in the shared library")]
    public async Task MultipleStatementsWithParameters([Values(PrepareOrNot.NotPrepared, PrepareOrNot.Prepared)] PrepareOrNot prepare)
    {
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
    [Ignore("Requires support for multi-statements strings in the shared library")]
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
        await using var conn = await dataSource.OpenConnectionAsync() as SpannerConnection;
        await using var cmd = new SpannerCommand("SELECT 1", conn!);
        Assert.That(() => cmd.ExecuteScalar(), Throws.Exception
            .TypeOf<SpannerDbException>()
            .With.InnerException.TypeOf<TimeoutException>()
        );
        Assert.That(conn!.State, Is.EqualTo(ConnectionState.Open));
    }
    
    private void AddParameter(DbCommand command, string name, object? value)
    {
        var parameter = command.CreateParameter();
        parameter.ParameterName = name;
        parameter.Value = value;
        command.Parameters.Add(parameter);
    }
}