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

using System.Diagnostics;
using Google.Cloud.Spanner.V1;
using Google.Cloud.SpannerLib.MockServer;
using Google.Cloud.SpannerLib.Native.Impl;
using Google.Protobuf.WellKnownTypes;
using Google.Rpc;
using Grpc.Core;
using Status = Grpc.Core.Status;
using TypeCode = Google.Cloud.Spanner.V1.TypeCode;

namespace Google.Cloud.SpannerLib.Tests;

public class BasicTests : AbstractMockServerTests
{

    [Test]
    public void TestCreatePool([Values] LibType libType)
    {
        var pool = Pool.Create(SpannerLibDictionary[libType], ConnectionString);
        pool.Close();
    }

    [Test]
    public void TestCreateConnection([Values] LibType libType)
    {
        using var pool = Pool.Create(SpannerLibDictionary[libType], ConnectionString);
        var connection = pool.CreateConnection();
        connection.Close();
    }

    [Test]
    public void TestExecuteQuery([Values] LibType libType)
    {
        using var pool = Pool.Create(SpannerLibDictionary[libType], ConnectionString);
        using var connection = pool.CreateConnection();
        using var rows = connection.Execute(new ExecuteSqlRequest { Sql = "SELECT 1" });
        for (var row = rows.Next(); row != null; row = rows.Next())
        {
            Assert.That(row.Values.Count, Is.EqualTo(1));
            Assert.That(row.Values[0].HasStringValue);
            Assert.That(row.Values[0].StringValue, Is.EqualTo("1"));
        }
    }

    [Test]
    public void TestExecuteQueryError([Values] LibType libType)
    {
        var sql = "select * from non_existing_table";
        Fixture.SpannerMock.AddOrUpdateStatementResult(sql, StatementResult.CreateException(new RpcException(new Status(StatusCode.NotFound, "Table not found"))));
        
        using var pool = Pool.Create(SpannerLibDictionary[libType], ConnectionString);
        using var connection = pool.CreateConnection();
        SpannerException exception = Assert.Throws<SpannerException>(() => connection.Execute(new ExecuteSqlRequest { Sql = sql }));
        Assert.That(exception.Code, Is.EqualTo(Code.NotFound));
        Assert.That(exception.Message, Is.EqualTo("Table not found"));
    }

    [Test]
    public void TestExecuteParameterizedQuery([Values] LibType libType)
    {
        var sql = "select col_varchar from all_types where col_bigint=$1::bigint";
        Fixture.SpannerMock.AddOrUpdateStatementResult(sql,
            StatementResult.CreateSingleColumnResultSet(
                new Spanner.V1.Type { Code = TypeCode.String }, 
                "col_varchar", "some-value"));
        
        using var pool = Pool.Create(SpannerLibDictionary[libType], ConnectionString);
        using var connection = pool.CreateConnection();
        var parameters = new Struct
        {
            Fields =
            {
                ["p1"] = Value.ForString("1")
            }
        };
        using var rows = connection.Execute(new ExecuteSqlRequest
        {
            Sql = sql,
            Params = parameters,
        });
        for (var row = rows.Next(); row != null; row = rows.Next())
        {
            Assert.That(row.Values.Count, Is.EqualTo(1));
            Assert.That(row.Values[0].HasStringValue);
            Assert.That(row.Values[0].StringValue, Is.EqualTo("some-value"));
        }
    }

    [Test]
    public void TestQueryParameterStartingWithUnderscore([Values] LibType libType)
    {
        var sql = "select col_string from all_types where col_int64=@__id";
        Fixture.SpannerMock.AddOrUpdateStatementResult(sql,
            StatementResult.CreateSingleColumnResultSet(
                new Spanner.V1.Type { Code = TypeCode.String }, 
                "col_string", "some-value"));
        
        using var pool = Pool.Create(SpannerLibDictionary[libType], ConnectionString);
        using var connection = pool.CreateConnection();
        var parameters = new Struct
        {
            Fields =
            {
                ["__id"] = Value.ForString("1")
            }
        };
        using var rows = connection.Execute(new ExecuteSqlRequest
        {
            Sql = sql,
            Params = parameters,
        });
        for (var row = rows.Next(); row != null; row = rows.Next())
        {
            Assert.That(row.Values.Count, Is.EqualTo(1));
            Assert.That(row.Values[0].HasStringValue);
            Assert.That(row.Values[0].StringValue, Is.EqualTo("some-value"));
        }
    }

    [Test]
    [Ignore("execute async disabled for now")]
    public async Task TestExecuteQueryAsync([Values] LibType libType)
    {
        using var pool = Pool.Create(SpannerLibDictionary[libType], ConnectionString);
        using var connection = pool.CreateConnection();
        using var rows = await connection.ExecuteAsync(new ExecuteSqlRequest { Sql = "SELECT 1" });
        var metadata = rows.Metadata;
        Assert.That(metadata!.RowType.Fields.Count, Is.EqualTo(1));
        for (var row = await rows.NextAsync(); row != null; row = await rows.NextAsync())
        {
            Assert.That(row.Values.Count, Is.EqualTo(1));
            Assert.That(row.Values[0].HasStringValue);
            Assert.That(row.Values[0].StringValue, Is.EqualTo("1"));
        }
    }

    [Test]
    public void TestReadOnlyTransaction([Values] LibType libType)
    {
        using var pool = Pool.Create(SpannerLibDictionary[libType], ConnectionString);
        using var connection = pool.CreateConnection();
        connection.BeginTransaction(new TransactionOptions { ReadOnly = new TransactionOptions.Types.ReadOnly() });
        using var rows = connection.Execute(new ExecuteSqlRequest { Sql = "SELECT 1" });
        for (var row = rows.Next(); row != null; row = rows.Next())
        {
            Assert.That(row.Values.Count, Is.EqualTo(1));
            Assert.That(row.Values[0].HasStringValue);
            Assert.That(row.Values[0].StringValue, Is.EqualTo("1"));
        }
        var commitResponse = connection.Commit();
        Assert.That(commitResponse, Is.Null);
    }

    [Test]
    public void TestReadWriteTransaction([Values] LibType libType)
    {
        var sql = "update table1 set value='one' where id=1";
        Fixture.SpannerMock.AddOrUpdateStatementResult(sql, StatementResult.CreateUpdateCount(1));
        using var pool = Pool.Create(SpannerLibDictionary[libType], ConnectionString);
        using var connection = pool.CreateConnection();
        connection.BeginTransaction(new TransactionOptions { ReadWrite = new TransactionOptions.Types.ReadWrite() });
        using var rows = connection.Execute(new ExecuteSqlRequest { Sql = sql });
        Assert.That(rows.UpdateCount, Is.EqualTo(1));
        var commitResponse = connection.Commit();
        Assert.That(commitResponse, Is.Not.Null);
    }

    [Test]
    [Ignore("for local testing")]
    public void TestBenchmarkNativeSpannerLib()
    {
        var totalRowCount = 1000000;
        Fixture.SpannerMock.AddOrUpdateStatementResult(
            "select * from all_types",
            StatementResult.CreateResultSet(
                new List<Tuple<TypeCode, string>>
                {
                    Tuple.Create(TypeCode.String, "col1"),
                    Tuple.Create(TypeCode.String, "col2"),
                    Tuple.Create(TypeCode.String, "col3"),
                    Tuple.Create(TypeCode.String, "col4"),
                    Tuple.Create(TypeCode.String, "col5"),
                },
                GenerateRandomValues(totalRowCount)));

        var spanner = new SharedLibSpanner();
        using var pool = Pool.Create(spanner, ConnectionString);
        using var connection = pool.CreateConnection();
        
        var stopwatch = Stopwatch.StartNew();
        using var rows = connection.Execute(new ExecuteSqlRequest { Sql = "select * from all_types" });
        var rowCount = 0;
        for (var row = rows.Next(); row != null; row = rows.Next())
        {
            rowCount++;
        }
        Assert.That(rowCount, Is.EqualTo(totalRowCount));
        stopwatch.Stop();
        Console.WriteLine(stopwatch.Elapsed);
    }
    
    [Test]
    [Ignore("for local testing")]
    public void TestBenchmarkDotnetGrpcClient()
    {
        var totalRowCount = 1000000;
        Fixture.SpannerMock.AddOrUpdateStatementResult(
            "select * from all_types",
            StatementResult.CreateResultSet(
                new List<Tuple<TypeCode, string>>
                {
                    Tuple.Create(TypeCode.String, "col1"),
                    Tuple.Create(TypeCode.String, "col2"),
                    Tuple.Create(TypeCode.String, "col3"),
                    Tuple.Create(TypeCode.String, "col4"),
                    Tuple.Create(TypeCode.String, "col5"),
                },
                GenerateRandomValues(totalRowCount)));
        var totalValueCount = totalRowCount * 5;
        var builder = new SpannerClientBuilder
        {
            Endpoint = $"http://{Fixture.Endpoint}",
            ChannelCredentials = ChannelCredentials.Insecure
        };
        SpannerClient client = builder.Build();
        var request = new CreateSessionRequest
        {
            Database = "projects/p1/instances/i1/databases/d1",
            Session = new Session()
        };
        var session = client.CreateSession(request);
        Assert.That(session, Is.Not.Null);

        var stopwatch = Stopwatch.StartNew();
        var executeRequest = new ExecuteSqlRequest
        {
            Sql = "select * from all_types",
            Session = session.Name,
        };
        var stream = client.ExecuteStreamingSql(executeRequest);
        var valueCount = 0;
        foreach (var result in stream.GetResponseStream().ToBlockingEnumerable())
        {
            Assert.That(result, Is.Not.Null);
            valueCount += result.Values.Count;
            if (result.ChunkedValue)
            {
                valueCount--;
            }
        }
        Assert.That(valueCount, Is.EqualTo(totalValueCount));
        stopwatch.Stop();
        Console.WriteLine(stopwatch.Elapsed);
    }
    
    private List<object[]> GenerateRandomValues(int count)
    {
        var result = new List<object[]>(count);
        for (var i = 0; i < count; i++)
        {
            result.Add([
                GenerateRandomString(),
                GenerateRandomString(),
                GenerateRandomString(),
                GenerateRandomString(),
                GenerateRandomString(),
            ]);
        }
        return result;
    }

    private string GenerateRandomString()
    {
        return Guid.NewGuid().ToString();
    }
}
