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
using Google.Cloud.Spanner.V1;
using Google.Cloud.SpannerLib.MockServer;
using Google.Cloud.SpannerLib.Native.Impl;

namespace Google.Cloud.SpannerLib.Tests;

public class RowsTests
{
    private readonly ISpannerLib _spannerLib = new SharedLibSpanner();
    
    private SpannerMockServerFixture _fixture;
    
    private string ConnectionString =>  $"{_fixture.Host}:{_fixture.Port}/projects/p1/instances/i1/databases/d1;UsePlainText=true";
        
    [SetUp]
    public void Setup()
    {
        _fixture = new SpannerMockServerFixture();
        _fixture.SpannerMock.AddOrUpdateStatementResult("SELECT 1", StatementResult.CreateSelect1ResultSet());
    }
        
    [TearDown]
    public void Teardown()
    {
        _fixture.Dispose();
    }

    [Test]
    public void TestExecuteSelect1()
    {
        using var pool = Pool.Create(_spannerLib, ConnectionString);
        using var connection = pool.CreateConnection();
        using var rows = connection.Execute(new ExecuteSqlRequest { Sql = "SELECT 1" });
        var numRows = 0;
        while (rows.Next() is { } row)
        {
            numRows++;
            Assert.That(row.Values.Count, Is.EqualTo(1));
            Assert.That(row.Values[0].HasStringValue);
            Assert.That(row.Values[0].StringValue, Is.EqualTo("1"));
        }
        Assert.That(numRows, Is.EqualTo(1));
    }

    [Test]
    public void TestRandomResults()
    {
        var numRows = 10;
        var rowType = RandomResultSetGenerator.GenerateAllTypesRowType();
        var results = RandomResultSetGenerator.Generate(rowType, numRows);
        _fixture.SpannerMock.AddOrUpdateStatementResult("select * from random", StatementResult.CreateQuery(results));
        
        using var pool = Pool.Create(_spannerLib, ConnectionString);
        using var connection = pool.CreateConnection();
        using var rows = connection.Execute(new ExecuteSqlRequest { Sql = "select * from random" });

        var rowCount = 0;
        while (rows.Next() is { } row)
        {
            rowCount++;
            Assert.That(row.Values.Count, Is.EqualTo(rowType.Fields.Count));
        }
        Assert.That(rowCount, Is.EqualTo(numRows));

        Assert.That(_fixture.SpannerMock.Requests.OfType<ExecuteSqlRequest>().Count(), Is.EqualTo(1));
        var request = _fixture.SpannerMock.Requests.OfType<ExecuteSqlRequest>().First();
        Assert.That(request.Transaction?.SingleUse?.ReadOnly?.HasStrong ?? false);
    }

    [Test]
    public void TestExecuteDml()
    {
        var sql = "update my_table set value=1 where id=2";
        _fixture.SpannerMock.AddOrUpdateStatementResult(sql, StatementResult.CreateUpdateCount(1L));
        
        using var pool = Pool.Create(_spannerLib, ConnectionString);
        using var connection = pool.CreateConnection();
        using var rows = connection.Execute(new ExecuteSqlRequest { Sql = sql });
        Assert.That(rows.Next(), Is.Null);
        Assert.That(rows.UpdateCount, Is.EqualTo(1L));
        
        Assert.That(_fixture.SpannerMock.Requests.OfType<ExecuteSqlRequest>().Count(), Is.EqualTo(1));
        var request = _fixture.SpannerMock.Requests.OfType<ExecuteSqlRequest>().First();
        Assert.That(request.Transaction?.Begin?.ReadWrite, Is.Not.Null);
        Assert.That(_fixture.SpannerMock.Requests.OfType<CommitRequest>().Count(), Is.EqualTo(1));
    }

    [Test]
    public void TestExecuteDdl()
    {
        // The mock DatabaseAdmin server always responds with a finished operation when
        // UpdateDatabaseDdl is called, so we don't need to set up any results.
        using var pool = Pool.Create(_spannerLib, ConnectionString);
        using var connection = pool.CreateConnection();
        using var rows = connection.Execute(new ExecuteSqlRequest { Sql = "create my_table (id int64 primary key)" });
        Assert.That(rows.Next(), Is.Null);
        Assert.That(rows.UpdateCount, Is.EqualTo(-1L));
        
        Assert.That(_fixture.SpannerMock.Requests.OfType<ExecuteSqlRequest>().Count(), Is.EqualTo(0));
        Assert.That(_fixture.DatabaseAdminMock.Requests.OfType<UpdateDatabaseDdlRequest>().Count(), Is.EqualTo(1));
    }

    [Test]
    public void TestExecuteClientSideStatement()
    {
        using var pool = Pool.Create(_spannerLib, ConnectionString);
        using var connection = pool.CreateConnection();
        using (var rows = connection.Execute(new ExecuteSqlRequest { Sql = "show variable retry_aborts_internally" }))
        {
            var row = rows.Next();
            Assert.That(row, Is.Not.Null);
            Assert.That(row.Values.Count, Is.EqualTo(1));
            Assert.That(row.Values[0].HasBoolValue);
            Assert.That(row.Values[0].BoolValue, Is.True);
            // There should be only one row.
            Assert.That(rows.Next(), Is.Null);
        }
        // Change the value of the variable and re-read it.
        using (var rows = connection.Execute(new ExecuteSqlRequest { Sql = "set retry_aborts_internally = false" }))
        {
            Assert.That(rows.Next(), Is.Null);
        }
        using (var rows = connection.Execute(new ExecuteSqlRequest { Sql = "show variable retry_aborts_internally" }))
        {
            var row = rows.Next();
            Assert.That(row!.Values[0].BoolValue, Is.False);
            Assert.That(rows.Next(), Is.Null);
        }
    }

}