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
using Google.Cloud.Spanner.Admin.Database.V1;
using Google.Cloud.Spanner.V1;
using Google.Cloud.SpannerLib.MockServer;
using TypeCode = Google.Cloud.Spanner.V1.TypeCode;

namespace Google.Cloud.Spanner.DataProvider.Tests;

public class CommandParameterTests : AbstractMockServerTests
{
    [Test]
    [TestCase(CommandBehavior.Default)]
    [TestCase(CommandBehavior.SequentialAccess)]
    public async Task InputAndOutputParameters(CommandBehavior behavior)
    {
        const string sql = "SELECT @c-1 AS c, @a+2 AS b";
        Fixture.SpannerMock.AddOrUpdateStatementResult(sql, StatementResult.CreateResultSet(
            new List<Tuple<TypeCode, string>>([
                Tuple.Create(TypeCode.Int64, "c"),
                Tuple.Create(TypeCode.Int64, "b"),
            ]),
            new List<object[]>([[3, 5]])));
        
        await using var conn = await OpenConnectionAsync();
        await using var cmd = new SpannerCommand(sql, conn);
        cmd.AddParameter("a", 3);
        var b = new SpannerParameter { ParameterName = "b", Direction = ParameterDirection.Output };
        cmd.Parameters.Add(b);
        var c = new SpannerParameter { ParameterName = "c", Direction = ParameterDirection.InputOutput, Value = 4 };
        cmd.Parameters.Add(c);
        await using (await cmd.ExecuteReaderAsync(behavior))
        {
            // TODO: Enable if we decide to support output parameters in the same way as npgsql.
            // Assert.That(b.Value, Is.EqualTo(5));
            // Assert.That(c.Value, Is.EqualTo(3));
        }
        var request = Fixture.SpannerMock.Requests.Single(r => r is ExecuteSqlRequest { Sql: sql }) as ExecuteSqlRequest;
        Assert.That(request, Is.Not.Null);
        Assert.That(request.Params.Fields.Count, Is.EqualTo(3));
        Assert.That(request.Params.Fields["a"].StringValue, Is.EqualTo("3"));
        Assert.That(request.Params.Fields["b"].HasNullValue);
        Assert.That(request.Params.Fields["c"].StringValue, Is.EqualTo("4"));
    }
    
    [Test]
    public async Task SendWithoutType([Values(PrepareOrNot.NotPrepared, PrepareOrNot.Prepared)] PrepareOrNot prepare)
    {
        const string sql = "select cast(@p as timestamp)";
        Fixture.SpannerMock.AddOrUpdateStatementResult(sql, StatementResult.CreateSingleColumnResultSet(
            new V1.Type{Code = TypeCode.Timestamp}, "p", "2025-10-30T10:00:00.000000000Z"));
        
        await using var conn = await OpenConnectionAsync();
        await using var cmd = new SpannerCommand(sql, conn);
        cmd.AddParameter("p", "2025-10-30T10:00:00Z");
        if (prepare == PrepareOrNot.Prepared)
        {
            await cmd.PrepareAsync();
        }

        await using var reader = await cmd.ExecuteReaderAsync();
        await reader.ReadAsync();
        Assert.That(reader.GetValue(0), Is.EqualTo(new DateTime(2025, 10, 30,  10, 0, 0, DateTimeKind.Utc)));

        var request = Fixture.SpannerMock.Requests.First(r => r is ExecuteSqlRequest { Sql: sql }) as ExecuteSqlRequest;
        Assert.That(request, Is.Not.Null);
        Assert.That(request.Params.Fields.Count, Is.EqualTo(1));
        Assert.That(request.Params.Fields["p"].StringValue, Is.EqualTo("2025-10-30T10:00:00Z"));
        Assert.That(request.ParamTypes.Count, Is.EqualTo(0));

        var expectedCount = prepare == PrepareOrNot.Prepared ? 2 : 1;
        Assert.That(Fixture.SpannerMock.Requests.Count(r => r is ExecuteSqlRequest { Sql: sql }), Is.EqualTo(expectedCount));
    }
    
    [Test]
    public async Task PositionalParameter()
    {
        // Set the database dialect to PostgreSQL to enable the use of PostgreSQL-style positional parameters.
        Fixture.SpannerMock.AddDialectResult(DatabaseDialect.Postgresql);
        const string sql = "SELECT $1";
        Fixture.SpannerMock.AddOrUpdateStatementResult(sql, StatementResult.CreateSingleColumnResultSet(
            new V1.Type{Code = TypeCode.Int64}, "c", 8L));
        
        await using var conn = await OpenConnectionAsync();
        await using var cmd = new SpannerCommand(sql, conn);
        cmd.Parameters.Add(new SpannerParameter { Value = 8 });
        Assert.That(await cmd.ExecuteScalarAsync(), Is.EqualTo(8));
        
        var request = Fixture.SpannerMock.Requests.Single(r => r is ExecuteSqlRequest { Sql: sql }) as ExecuteSqlRequest;
        Assert.That(request, Is.Not.Null);
        Assert.That(request.Params.Fields.Count, Is.EqualTo(1));
        Assert.That(request.Params.Fields["p1"].StringValue, Is.EqualTo("8"));
        Assert.That(request.ParamTypes.Count, Is.EqualTo(0));
    }

    [Test]
    public async Task UnreferencedNamedParameterIsIgnored()
    {
        await using var conn = await OpenConnectionAsync();
        await using var cmd = new SpannerCommand("SELECT 1", conn);
        cmd.AddParameter("not_used", 8);
        Assert.That(await cmd.ExecuteScalarAsync(), Is.EqualTo(1));
        
        var request = Fixture.SpannerMock.Requests.Single(r => r is ExecuteSqlRequest { Sql: "SELECT 1" }) as ExecuteSqlRequest;
        Assert.That(request, Is.Not.Null);
        Assert.That(request.Params.Fields.Count, Is.EqualTo(1));
        Assert.That(request.Params.Fields["not_used"].StringValue, Is.EqualTo("8"));
        Assert.That(request.ParamTypes.Count, Is.EqualTo(0));
    }

    [Test]
    public async Task UnreferencedPositionalParameterIsIgnored()
    {
        // Set the database dialect to PostgreSQL to enable the use of PostgreSQL-style positional parameters.
        Fixture.SpannerMock.AddDialectResult(DatabaseDialect.Postgresql);
        await using var conn = await OpenConnectionAsync();
        await using var cmd = new SpannerCommand("SELECT 1", conn);
        cmd.Parameters.Add(new SpannerParameter { Value = 8 });
        Assert.That(await cmd.ExecuteScalarAsync(), Is.EqualTo(1));
        
        var request = Fixture.SpannerMock.Requests.Single(r => r is ExecuteSqlRequest { Sql: "SELECT 1" }) as ExecuteSqlRequest;
        Assert.That(request, Is.Not.Null);
        Assert.That(request.Params.Fields.Count, Is.EqualTo(1));
        Assert.That(request.Params.Fields["p1"].StringValue, Is.EqualTo("8"));
        Assert.That(request.ParamTypes.Count, Is.EqualTo(0));
    }

    [Test]
    public void ParameterName()
    {
        var command = new SpannerCommand();

        // Add parameters.
        command.Parameters.Add(new SpannerParameter{ ParameterName = "@Parameter1", DbType = DbType.Boolean, Value = true });
        command.Parameters.Add(new SpannerParameter{ ParameterName = "@Parameter2", DbType = DbType.Int32, Value = 1 });
        command.Parameters.Add(new SpannerParameter{ ParameterName = "Parameter3", DbType = DbType.DateTime, Value = DBNull.Value });
        command.Parameters.Add(new SpannerParameter{ ParameterName = "Parameter4", DbType = DbType.Binary, Value = DBNull.Value });

        var parameter = command.Parameters["@Parameter1"];
        Assert.That(parameter, Is.Not.Null);
        command.Parameters[0].Value = 1;

        Assert.That(command.Parameters["@Parameter1"].ParameterName, Is.EqualTo("@Parameter1"));
        Assert.That(command.Parameters["@Parameter2"].ParameterName, Is.EqualTo("@Parameter2"));
        Assert.That(command.Parameters["Parameter3"].ParameterName, Is.EqualTo("Parameter3"));
        Assert.That(command.Parameters["Parameter4"].ParameterName, Is.EqualTo("Parameter4"));

        Assert.That(command.Parameters[0].ParameterName, Is.EqualTo("@Parameter1"));
        Assert.That(command.Parameters[1].ParameterName, Is.EqualTo("@Parameter2"));
        Assert.That(command.Parameters[2].ParameterName, Is.EqualTo("Parameter3"));
        Assert.That(command.Parameters[3].ParameterName, Is.EqualTo("Parameter4"));
        
        // Verify that the '@' is stripped before being sent to Spanner.
        var statement = command.BuildStatement();
        Assert.That(statement, Is.Not.Null);
        Assert.That(statement.Params.Fields.Count, Is.EqualTo(4));
        Assert.That(statement.Params.Fields["Parameter1"].StringValue, Is.EqualTo("1"));
        Assert.That(statement.Params.Fields["Parameter2"].StringValue, Is.EqualTo("1"));
        Assert.That(statement.Params.Fields["Parameter3"].HasNullValue);
        Assert.That(statement.Params.Fields["Parameter4"].HasNullValue);
        
        Assert.That(statement.ParamTypes.Count, Is.EqualTo(4));
        Assert.That(statement.ParamTypes["Parameter1"].Code, Is.EqualTo(TypeCode.Bool));
        Assert.That(statement.ParamTypes["Parameter2"].Code, Is.EqualTo(TypeCode.Int64));
        Assert.That(statement.ParamTypes["Parameter3"].Code, Is.EqualTo(TypeCode.Timestamp));
        Assert.That(statement.ParamTypes["Parameter4"].Code, Is.EqualTo(TypeCode.Bytes));
    }

    [Test]
    public async Task SameParamMultipleTimes()
    {
        const string sql = "SELECT @p1, @p1";
        Fixture.SpannerMock.AddOrUpdateStatementResult(sql, StatementResult.CreateResultSet(
            new List<Tuple<TypeCode, string>>([Tuple.Create(TypeCode.Int64, "p1"), Tuple.Create(TypeCode.Int64, "p1")]),
            new List<object[]>([[8, 8]])));
        
        await using var conn = await OpenConnectionAsync();
        await using var cmd = new SpannerCommand(sql, conn);
        cmd.AddParameter("@p1", 8);
        await using var reader = await cmd.ExecuteReaderAsync();
        await reader.ReadAsync();
        Assert.That(reader[0], Is.EqualTo(8));
        Assert.That(reader[1], Is.EqualTo(8));
        
        var request = Fixture.SpannerMock.Requests.Single(r => r is ExecuteSqlRequest { Sql: sql }) as ExecuteSqlRequest;
        Assert.That(request, Is.Not.Null);
        Assert.That(request.Params.Fields.Count, Is.EqualTo(1));
        Assert.That(request.Params.Fields["p1"].StringValue, Is.EqualTo("8"));
        Assert.That(request.ParamTypes.Count, Is.EqualTo(0));
    }

    [Test]
    public async Task ParameterMustBeSet()
    {
        await using var conn = await OpenConnectionAsync();
        await using var cmd = new SpannerCommand("SELECT @p1::TEXT", conn);
        cmd.Parameters.Add(new SpannerParameter{ ParameterName = "@p1" });

        Assert.That(async () => await cmd.ExecuteReaderAsync(),
            Throws.Exception
                .TypeOf<InvalidOperationException>()
                .With.Message.EqualTo("Parameter @p1 has no value"));
    }

}