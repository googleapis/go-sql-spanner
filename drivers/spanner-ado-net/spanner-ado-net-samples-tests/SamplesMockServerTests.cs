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
    public async Task TestHelloWorld()
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
}