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

using System.Data.Common;
using System.Text.Json;
using Google.Cloud.Spanner.V1;
using Google.Cloud.SpannerLib.MockServer;
using Google.Protobuf.WellKnownTypes;

namespace Google.Cloud.Spanner.DataProvider.Tests;

public class BatchTests : AbstractMockServerTests
{
    [TestCase(1, false, false)]
    [TestCase(2, false, false)]
    [TestCase(5, false, false)]
    [TestCase(1, true, false)]
    [TestCase(2, true, false)]
    [TestCase(5, true, false)]
    [TestCase(1, false, true)]
    [TestCase(2, false, true)]
    [TestCase(5, false, true)]
    [TestCase(1, true, true)]
    [TestCase(2, true, true)]
    [TestCase(5, true, true)]
    public async Task TestAllParameterTypes(int numCommands, bool executeAsync, bool useTransaction)
    {
        await using var connection = new SpannerConnection();
        connection.ConnectionString = ConnectionString;
        await connection.OpenAsync();

        SpannerTransaction? transaction = null;
        if (useTransaction)
        {
            if (executeAsync)
            {
                transaction = await connection.BeginTransactionAsync();
            }
            else
            {
                // ReSharper disable once MethodHasAsyncOverload
                transaction = connection.BeginTransaction();
            }
        }

        const string insert = "insert into my_table values " +
                              "(@p1, @p2, @p3, @p4, @p5, @p6, @p7, @p8, @p9, @p10, @p11, @p12, @p13, @p14, @p15, @p16, @p17, @p18, @p19, @p20, @p21, @p22, @p23)";
        Fixture.SpannerMock.AddOrUpdateStatementResult(insert, StatementResult.CreateUpdateCount(1));
        
        await using var batch = connection.CreateBatch();
        if (transaction != null)
        {
            batch.Transaction = transaction;
        }

        for (var i = 0; i < numCommands; i++)
        {
            var command = batch.CreateBatchCommand();
            command.CommandText = insert;
            // TODO:
            //   - PROTO
            //   - STRUCT
            AddParameter(command, "p1", true);
            AddParameter(command, "p2", new byte[] { 1, 2, 3 });
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

            AddParameter(command, "p13", new bool?[] { true, false, null });
            AddParameter(command, "p14", new byte[]?[] { [1, 2, 3], null });
            AddParameter(command, "p15", new DateOnly?[] { new DateOnly(2025, 10, 2), null });
            AddParameter(command, "p16", new TimeSpan?[] { new TimeSpan(1, 2, 3, 4, 5, 6), null });
            AddParameter(command, "p17", new[] { JsonDocument.Parse("{\"key\": \"value\"}"), null });
            AddParameter(command, "p18", new decimal?[] { 9.99m, null });
            AddParameter(command, "p19", new[] { "test", null });
            AddParameter(command, "p20",
                new DateTime?[] { new DateTime(2025, 10, 2, 15, 57, 31, 999, DateTimeKind.Utc), null });
            AddParameter(command, "p21", new Guid?[] { Guid.Parse("5555990c-b259-4539-bd22-5a9293cf10ac"), null });
            AddParameter(command, "p22", new double?[] { 3.14d, null });
            AddParameter(command, "p23", new float?[] { 3.14f, null });

            batch.BatchCommands.Add(command);
        }

        int affected;
        if (executeAsync)
        {
            affected = await batch.ExecuteNonQueryAsync();
        }
        else
        {
            // ReSharper disable once MethodHasAsyncOverload
            affected = batch.ExecuteNonQuery();
        }
        Assert.That(affected, Is.EqualTo(numCommands));
        foreach (var command in batch.BatchCommands)
        {
            Assert.That(command.RecordsAffected, Is.EqualTo(1));
        }
        if (transaction != null)
        {
            if (executeAsync)
            {
                await transaction.CommitAsync();
            }
            else
            {
                // ReSharper disable once MethodHasAsyncOverload
                transaction.Commit();
            }
        }

        var requests = Fixture.SpannerMock.Requests.ToList();
        Assert.That(requests.OfType<ExecuteBatchDmlRequest>().Count, Is.EqualTo(1));
        Assert.That(requests.OfType<CommitRequest>().Count, Is.EqualTo(1));
        var request = requests.OfType<ExecuteBatchDmlRequest>().Single();
        Assert.That(request.Statements.Count, Is.EqualTo(numCommands));
        foreach (var statement in request.Statements)
        {
            // The driver does not send any parameter types, unless it is explicitly asked to do so.
            Assert.That(statement.ParamTypes.Count, Is.EqualTo(0));
            Assert.That(statement.Params.Fields.Count, Is.EqualTo(23));
            var fields = statement.Params.Fields;
            Assert.That(fields["p1"].HasBoolValue, Is.True);
            Assert.That(fields["p1"].BoolValue, Is.True);
            Assert.That(fields["p2"].HasStringValue, Is.True);
            Assert.That(fields["p2"].StringValue, Is.EqualTo(Convert.ToBase64String(new byte[] { 1, 2, 3 })));
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
            Assert.That(fields["p14"].ListValue.Values[0].StringValue,
                Is.EqualTo(Convert.ToBase64String(new byte[] { 1, 2, 3 })));
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
            Assert.That(fields["p21"].ListValue.Values[0].StringValue,
                Is.EqualTo("5555990c-b259-4539-bd22-5a9293cf10ac"));
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
        var commitRequest = requests.OfType<CommitRequest>().First();
        Assert.That(commitRequest, Is.Not.Null);
    }

    [TestCase(true)]
    [TestCase(false)]
    public async Task TestEmptyBatch(bool executeAsync)
    {
        await using var connection = new SpannerConnection();
        connection.ConnectionString = ConnectionString;
        await connection.OpenAsync();
        
        await using var batch = connection.CreateBatch();
        int affected;
        if (executeAsync)
        {
            affected = await batch.ExecuteNonQueryAsync();
        }
        else
        {
            // ReSharper disable once MethodHasAsyncOverload
            affected = batch.ExecuteNonQuery();
        }
        Assert.That(affected, Is.EqualTo(0));
    }

    [TestCase(true)]
    [TestCase(false)]
    public async Task TestExecuteReader(bool executeAsync)
    {
        await using var connection = new SpannerConnection();
        connection.ConnectionString = ConnectionString;
        await connection.OpenAsync();
        
        await using var batch = connection.CreateBatch();
        var command = batch.CreateBatchCommand();
        command.CommandText = "select * from my_table";
        if (executeAsync)
        {
            Assert.ThrowsAsync<NotImplementedException>(() => batch.ExecuteReaderAsync());
        }
        else
        {
            Assert.Throws<NotImplementedException>(() => batch.ExecuteReader());
        }
    }

    [TestCase(true)]
    [TestCase(false)]
    public async Task TestExecuteScalar(bool executeAsync)
    {
        await using var connection = new SpannerConnection();
        connection.ConnectionString = ConnectionString;
        await connection.OpenAsync();
        
        await using var batch = connection.CreateBatch();
        var command = batch.CreateBatchCommand();
        command.CommandText = "select * from my_table";
        if (executeAsync)
        {
            Assert.ThrowsAsync<NotImplementedException>(() => batch.ExecuteScalarAsync());
        }
        else
        {
            Assert.Throws<NotImplementedException>(() => batch.ExecuteScalar());
        }
    }

    private static void AddParameter(DbBatchCommand command, string name, object? value)
    {
        var parameter = command.CreateParameter();
        parameter.ParameterName = name;
        parameter.Value = value;
        command.Parameters.Add(parameter);
    }
}