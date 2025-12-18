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

using Google.Cloud.Spanner.Admin.Database.V1;
using Google.Cloud.Spanner.DataProvider.Samples.Snippets;
using Google.Cloud.Spanner.V1;
using Google.Cloud.SpannerLib.MockServer;
using TypeCode = Google.Cloud.Spanner.V1.TypeCode;

namespace Google.Cloud.Spanner.DataProvider.Samples.Tests;

public class SamplesMockServerTests : AbstractMockServerTests
{
    private StringWriter? _writer;
    private TextWriter _output;
    
    private StringWriter Writer => _writer ??= new StringWriter();

    [SetUp]
    public void SetupOutput()
    {
        _output = Console.Out;
        _writer = new StringWriter();
        Console.SetOut(_writer);
    }
    
    [TearDown]
    public void CleanupOutput()
    {
        _writer?.Dispose();
        Console.SetOut(_output);
    }

    [Test]
    public async Task TestHelloWorldSample()
    {
        const string sql = "SELECT 'Hello World' as Message";
        Fixture.SpannerMock.AddOrUpdateStatementResult(sql,
            StatementResult.CreateResultSet([Tuple.Create(TypeCode.String, "Message")], [["Hello World"]]));
        
        await HelloWorldSample.Run(ConnectionString);
        
        Assert.That(Writer.ToString(), Is.EqualTo($"Greeting from Spanner: Hello World{Environment.NewLine}"));
        var requests = Fixture.SpannerMock.Requests.OfType<ExecuteSqlRequest>().ToList();
        Assert.That(requests, Has.Count.EqualTo(1));
        var request = requests[0];
        Assert.That(request.Transaction, Is.EqualTo(new TransactionSelector
        {
            SingleUse = new TransactionOptions
            {
                ReadOnly = new TransactionOptions.Types.ReadOnly
                {
                    Strong = true,
                    ReturnReadTimestamp = true,
                }
            }
        }));
    }

    [Test]
    public async Task TestCommitTimestamp()
    {
        const string sql = "INSERT INTO Singers (SingerId, FirstName, LastName) VALUES (@id, @first, @last)";
        Fixture.SpannerMock.AddOrUpdateStatementResult(sql, StatementResult.CreateUpdateCount(1L));
        
        await CommitTimestampSample.Run(ConnectionString);

        Assert.That(Writer.ToString(), Does.StartWith($"Inserted 1 singer(s){Environment.NewLine}Transaction committed at "));
        var requests = Fixture.SpannerMock.Requests.OfType<ExecuteSqlRequest>().ToList();
        Assert.That(requests, Has.Count.EqualTo(1));
        var request = requests[0];
        Assert.That(request.Transaction, Is.EqualTo(new TransactionSelector
        {
            Begin = new TransactionOptions
            {
                ReadWrite = new TransactionOptions.Types.ReadWrite(),
            }
        }));
        Assert.That(request.Params.Fields, Has.Count.EqualTo(3));
        Assert.That(request.ParamTypes, Has.Count.EqualTo(0));
        Assert.That(Fixture.SpannerMock.Requests.OfType<ExecuteSqlRequest>().ToList(), Has.Count.EqualTo(1));
    }

    [Test]
    public async Task TestCustomConfigurationSample()
    {
        const string sql = "SELECT @greeting as Message";
        Fixture.SpannerMock.AddOrUpdateStatementResult(sql,
            StatementResult.CreateResultSet([Tuple.Create(TypeCode.String, "Message")], [["Hello from Spanner"]]));
        
        await CustomConfigurationSample.Run(ConnectionString);
        
        Assert.That(Writer.ToString(), Is.EqualTo($"Greeting: Hello from Spanner{Environment.NewLine}"));
    }

    [Test]
    public async Task TestDataTypesSample()
    {
        const string insert = "insert or update into AllTypes " +
                           "(id, col_bool, col_bytes, col_date, col_float32, col_float64, col_int64, /*col_interval,*/ col_json, col_numeric, col_string, col_timestamp) " +
                           "values (@id, @bool, @bytes, @date, @float32, @float64, @int64, /*@interval,*/ @json, @numeric, @string, @timestamp)";
        Fixture.SpannerMock.AddOrUpdateStatementResult(insert, StatementResult.CreateUpdateCount(1L));
        const string query = "select * from AllTypes order by id";
        Fixture.SpannerMock.AddOrUpdateStatementResult(query, StatementResult.CreateResultSet(
            [
                Tuple.Create(TypeCode.Int64, "id"),
                Tuple.Create(TypeCode.Bool, "col_bool"),
                Tuple.Create(TypeCode.Bytes, "col_bytes"),
                Tuple.Create(TypeCode.Date, "col_date"),
                Tuple.Create(TypeCode.Float32, "col_float32"),
                Tuple.Create(TypeCode.Float64, "col_float64"),
                Tuple.Create(TypeCode.Int64, "col_int64"),
                Tuple.Create(TypeCode.Json, "col_json"),
                Tuple.Create(TypeCode.Numeric, "col_numeric"),
                Tuple.Create(TypeCode.String, "col_string"),
                Tuple.Create(TypeCode.Timestamp, "col_timestamp"),
            ],
                [[
                    1L,
                    true,
                    Convert.ToBase64String(new byte[]{1,2,3}),
                    DateOnly.FromDateTime(DateTime.Now),
                    3.14f,
                    3.14d,
                    100L,
                    "{\"key\":\"value\"}",
                    3.14m,
                    "test-string",
                    DateTime.Now,
                ]]));
        
        await DataTypesSample.Run(ConnectionString);
        
        Assert.That(Writer.ToString(), Does.StartWith($"Inserted: 1{Environment.NewLine}id: 1{Environment.NewLine}col_bool: True{Environment.NewLine}"));
        var request = Fixture.SpannerMock.Requests.OfType<ExecuteSqlRequest>().First();
        Assert.That(request.Sql, Is.EqualTo(insert));
        Assert.That(request.Params.Fields, Has.Count.EqualTo(11));
        Assert.That(request.ParamTypes, Has.Count.EqualTo(0));
        Assert.That(request.Params.Fields["id"].StringValue, Is.EqualTo("1"));
        Assert.That(request.Params.Fields["bool"].BoolValue, Is.True);
        Assert.That(request.Params.Fields["bytes"].StringValue, Is.EqualTo(Convert.ToBase64String(new byte[]{1,2,3})));
        Assert.That(request.Params.Fields["date"].StringValue, Is.Not.Null);
        Assert.That(request.Params.Fields["float32"].NumberValue, Is.EqualTo(3.14f));
        Assert.That(request.Params.Fields["float64"].NumberValue, Is.EqualTo(3.14d));
        Assert.That(request.Params.Fields["int64"].StringValue, Is.EqualTo("100"));
        Assert.That(request.Params.Fields["json"].StringValue, Is.EqualTo("{\"key\": \"value\"}"));
        Assert.That(request.Params.Fields["numeric"].StringValue, Is.EqualTo("3.14"));
        Assert.That(request.Params.Fields["string"].StringValue, Is.EqualTo("test-string"));
        Assert.That(request.Params.Fields["timestamp"].StringValue, Is.Not.Null);
    }

    [Test]
    public async Task TestDdlBatchSample()
    {
        await DdlBatchSample.Run(ConnectionString);
        
        Assert.That(Writer.ToString(), Is.EqualTo(
            $"Executed ADO.NET batch{Environment.NewLine}" +
            $"Executed DDL batch{Environment.NewLine}" +
            $"Executed a single SQL string with multiple DDL statements as one batch.{Environment.NewLine}"));

        var requests = Fixture.DatabaseAdminMock.Requests.OfType<UpdateDatabaseDdlRequest>().ToList();
        Assert.That(requests.Count, Is.EqualTo(3));
        foreach (var request in requests)
        {
            Assert.That(request.Statements.Count, Is.EqualTo(2));
        }
    }
    
    [Test]
    public async Task TestDmlBatchSample()
    {
        const string insert =
            "insert or update into Singers (SingerId, FirstName, LastName) values (@Id, @FirstName, @LastName)";
        Fixture.SpannerMock.AddOrUpdateStatementResult(insert, StatementResult.CreateUpdateCount(1L));
        const string update = "update Singers set BirthDate = NULL where BirthDate < DATE '1900-01-01'";
        Fixture.SpannerMock.AddOrUpdateStatementResult(update, StatementResult.CreateUpdateCount(10L));
        
        await DmlBatchSample.Run(ConnectionString);
        
        Assert.That(Writer.ToString(), Is.EqualTo(
            $"Executed ADO.NET batch{Environment.NewLine}" +
            $"Affected: 13{Environment.NewLine}{Environment.NewLine}" +
            $"Executed DML batch{Environment.NewLine}" +
            $"Affected: -1{Environment.NewLine}{Environment.NewLine}"));
        
        var executeRequests = Fixture.SpannerMock.Requests.OfType<ExecuteSqlRequest>().ToList();
        Assert.That(executeRequests, Is.Empty);
        var requests = Fixture.SpannerMock.Requests.OfType<ExecuteBatchDmlRequest>().ToList();
        Assert.That(requests.Count, Is.EqualTo(2));
        foreach (var request in requests)
        {
            Assert.That(request.Statements.Count, Is.EqualTo(4));
        }
    }

    [Test]
    public async Task TestMutationsSample()
    {
        await MutationsSample.Run(ConnectionString);
        
        Assert.That(Writer.ToString(), Is.EqualTo($"Inserted data using mutations. Affected: 1{Environment.NewLine}{Environment.NewLine}"));
        var beginRequests = Fixture.SpannerMock.Requests.OfType<BeginTransactionRequest>().ToList();
        Assert.That(beginRequests, Has.Count.EqualTo(1));
        var beginRequest = beginRequests[0];
        Assert.That(beginRequest.MutationKey?.Insert?.Table, Is.Not.Null);
        Assert.That(beginRequest.MutationKey.Insert.Table, Is.EqualTo("Singers"));
        Assert.That(beginRequest.Options?.ReadWrite, Is.Not.Null);
        var commitRequests = Fixture.SpannerMock.Requests.OfType<CommitRequest>().ToList();
        Assert.That(commitRequests, Has.Count.EqualTo(1));
        var commitRequest = commitRequests[0];
        Assert.That(commitRequest.Mutations, Has.Count.EqualTo(1));
    }

    [Test]
    public async Task TestPartitionedDmlSample()
    {
        const string sql = "update Singers set BirthDate=date '1900-01-01' where BirthDate is null";
        Fixture.SpannerMock.AddOrUpdateStatementResult(sql, StatementResult.CreateUpdateCount(100L));
        
        await PartitionedDmlSample.Run(ConnectionString);
        
        Assert.That(Writer.ToString(), Is.EqualTo($"Executed a Partitioned DML statement. Affected: 100{Environment.NewLine}{Environment.NewLine}"));
        var beginRequests = Fixture.SpannerMock.Requests.OfType<BeginTransactionRequest>().ToList();
        Assert.That(beginRequests, Has.Count.EqualTo(1));
        var beginRequest = beginRequests[0];
        Assert.That(beginRequest.Options?.PartitionedDml, Is.Not.Null);
        var executeRequests = Fixture.SpannerMock.Requests.OfType<ExecuteSqlRequest>().ToList();
        Assert.That(executeRequests, Has.Count.EqualTo(1));
        var executeRequest = executeRequests[0];
        Assert.That(executeRequest.Transaction?.Id, Is.Not.Null);
        Assert.That(Fixture.SpannerMock.Requests.OfType<CommitRequest>(), Is.Empty);
    }
    
    [Test]
    public async Task TestQueryParametersSample()
    {
        const string namedParameters =
            "SELECT SingerId, FullName FROM Singers WHERE LastName LIKE @lastName    OR FirstName LIKE @firstName ORDER BY LastName, FirstName";
        const string positionalParameters =
            "SELECT SingerId, FullName FROM Singers WHERE LastName LIKE @p1    OR FirstName LIKE @p2 ORDER BY LastName, FirstName";
        var result = StatementResult.CreateResultSet(
            [Tuple.Create(TypeCode.Int64, "SingerId"), Tuple.Create(TypeCode.String, "FullName")],
            [[1L, "Pete Allison"], [2L, "Alice Peterson"]]);
        Fixture.SpannerMock.AddOrUpdateStatementResult(namedParameters, result);
        Fixture.SpannerMock.AddOrUpdateStatementResult(positionalParameters, result);
        
        await QueryParametersSample.Run(ConnectionString);
        
        Assert.That(Writer.ToString(), Is.EqualTo(
            $"Found singer with named parameters: Pete Allison{Environment.NewLine}" +
            $"Found singer with named parameters: Alice Peterson{Environment.NewLine}" +
            $"Found singer with positional parameters: Pete Allison{Environment.NewLine}" +
            $"Found singer with positional parameters: Alice Peterson{Environment.NewLine}"));
        
        var requests = Fixture.SpannerMock.Requests.OfType<ExecuteSqlRequest>().ToList();
        Assert.That(requests, Has.Count.EqualTo(2));
        var index = 0;
        foreach (var request in requests)
        {
            Assert.That(request.Transaction?.SingleUse?.ReadOnly, Is.Not.Null);
            Assert.That(request.ParamTypes, Is.Empty);
            Assert.That(request.Params.Fields, Has.Count.EqualTo(2));
            if (index == 0)
            {
                Assert.That(request.Params.Fields["firstName"].StringValue, Is.EqualTo("A%"));
                Assert.That(request.Params.Fields["lastName"].StringValue, Is.EqualTo("R%"));
            }
            else
            {
                Assert.That(request.Params.Fields["p1"].StringValue, Is.EqualTo("R%"));
                Assert.That(request.Params.Fields["p2"].StringValue, Is.EqualTo("A%"));
            }
            index++;
        }
    }

    [Test]
    public async Task TestReadOnlyTransactionSample()
    {
        const string sql =
            "SELECT SingerId, FullName FROM Singers WHERE LastName LIKE @lastName    OR FirstName LIKE @firstName ORDER BY LastName, FirstName";
        var result = StatementResult.CreateResultSet(
            [Tuple.Create(TypeCode.Int64, "SingerId"), Tuple.Create(TypeCode.String, "FullName")],
            [[1L, "Pete Allison"], [2L, "Alice Peterson"]]);
        Fixture.SpannerMock.AddOrUpdateStatementResult(sql, result);
        
        await ReadOnlyTransactionSample.Run(ConnectionString);
        
        Assert.That(Writer.ToString(), Is.EqualTo(
            $"Found singer: Pete Allison{Environment.NewLine}" +
            $"Found singer: Alice Peterson{Environment.NewLine}"));
        
        Assert.That(Fixture.SpannerMock.Requests.OfType<BeginTransactionRequest>(), Is.Empty);
        Assert.That(Fixture.SpannerMock.Requests.OfType<CommitRequest>(), Is.Empty);
        var requests = Fixture.SpannerMock.Requests.OfType<ExecuteSqlRequest>().ToList();
        Assert.That(requests, Has.Count.EqualTo(1));
        var request = requests[0];
        Assert.That(request.Transaction?.Begin?.ReadOnly, Is.Not.Null);
    }

    [Test]
    public async Task TestStaleReadSample()
    {
        var now = DateTime.UtcNow;
        const string sql = "SELECT SingerId, FullName FROM Singers ORDER BY LastName, FirstName";
        var result = StatementResult.CreateResultSet(
            [Tuple.Create(TypeCode.Int64, "SingerId"), Tuple.Create(TypeCode.String, "FullName")],
            [[1L, "Pete Allison"], [2L, "Alice Peterson"]]);
        Fixture.SpannerMock.AddOrUpdateStatementResult(sql, result);
        const string currentTimestamp = "SELECT CURRENT_TIMESTAMP";
        Fixture.SpannerMock.AddOrUpdateStatementResult(currentTimestamp, StatementResult.CreateResultSet(
            [Tuple.Create(TypeCode.Timestamp, "CURRENT_TIMESTAMP")],
            [[now]]));
        
        await StaleReadSample.Run(ConnectionString);
        
        Assert.That(Writer.ToString(), Is.EqualTo(
            $"Found singer using a single stale query: Pete Allison{Environment.NewLine}" +
            $"Found singer using a single stale query: Alice Peterson{Environment.NewLine}" +
            $"Found singer using a stale read-only transaction: Pete Allison{Environment.NewLine}" +
            $"Found singer using a stale read-only transaction: Alice Peterson{Environment.NewLine}"));
        
        Assert.That(Fixture.SpannerMock.Requests.OfType<BeginTransactionRequest>(), Is.Empty);
        Assert.That(Fixture.SpannerMock.Requests.OfType<CommitRequest>(), Is.Empty);
        var requests = Fixture.SpannerMock.Requests.OfType<ExecuteSqlRequest>().ToList();
        Assert.That(requests, Has.Count.EqualTo(3));
        Assert.That(requests[0].Transaction?.SingleUse?.ReadOnly?.MaxStaleness?.Seconds ?? -1, Is.EqualTo(10));
        Assert.That(requests[1].Transaction?.SingleUse?.ReadOnly, Is.Not.Null);
        Assert.That(requests[1].Transaction?.SingleUse?.ReadOnly?.MaxStaleness, Is.Null);
        Assert.That(requests[2].Transaction?.Begin?.ReadOnly?.ReadTimestamp, Is.Not.Null);

        var actual =
            TimeSpan.FromTicks(TimeSpan.TicksPerSecond * requests[2].Transaction.Begin.ReadOnly.ReadTimestamp.Seconds +
                               requests[2].Transaction.Begin.ReadOnly.ReadTimestamp.Nanos /
                               TimeSpan.NanosecondsPerTick);
        var expected = TimeSpan.FromTicks(now.Ticks - DateTime.UnixEpoch.Ticks);
        Assert.That(actual, Is.EqualTo(expected));
    }

    [Test]
    public async Task TestTagsSample()
    {
        const string sql = "SELECT 'Hello World' as Message";
        Fixture.SpannerMock.AddOrUpdateStatementResult(sql,
            StatementResult.CreateResultSet([Tuple.Create(TypeCode.String, "Message")], [["Hello World"]]));
        
        await TagsSample.Run(ConnectionString);
        
        Assert.That(Fixture.SpannerMock.Requests.OfType<BeginTransactionRequest>(), Is.Empty);
        var executeRequest = Fixture.SpannerMock.Requests.OfType<ExecuteSqlRequest>().Single();
        Assert.That(executeRequest.Transaction?.Begin?.ReadWrite, Is.Not.Null);
        Assert.That(executeRequest.RequestOptions?.TransactionTag ?? "", Is.EqualTo("my_transaction_tag"));
        Assert.That(executeRequest.RequestOptions?.RequestTag ?? "", Is.EqualTo("my_query_tag"));
        var commitRequest = Fixture.SpannerMock.Requests.OfType<CommitRequest>().Single();
        Assert.That(commitRequest.RequestOptions.TransactionTag ?? "", Is.EqualTo("my_transaction_tag"));
    }

    [Test]
    public async Task TestTransactionSample()
    {
        const string query = "SELECT SingerId FROM Singers WHERE BirthDate IS NULL";
        Fixture.SpannerMock.AddOrUpdateStatementResult(query,
            StatementResult.CreateResultSet([Tuple.Create(TypeCode.Int64, "SingerId")], [[1L]]));
        const string update = "UPDATE Singers SET BirthDate=DATE '1900-01-01' WHERE SingerId=@singerId";
        Fixture.SpannerMock.AddOrUpdateStatementResult(update, StatementResult.CreateUpdateCount(1L));
        
        await TransactionSample.Run(ConnectionString);
        
        Assert.That(Writer.ToString(), Is.EqualTo($"Set a default birthdate for 1 singers{Environment.NewLine}"));
        Assert.That(Fixture.SpannerMock.Requests.OfType<BeginTransactionRequest>(), Is.Empty);
        Assert.That(Fixture.SpannerMock.Requests.OfType<CommitRequest>().ToList(), Has.Count.EqualTo(1));
        var requests = Fixture.SpannerMock.Requests.OfType<ExecuteSqlRequest>().ToList();
        Assert.That(requests, Has.Count.EqualTo(2));
        Assert.That(requests[0].Transaction?.Begin?.ReadWrite, Is.Not.Null);
        Assert.That(requests[1].Transaction?.Id, Is.Not.Null);
    }
}