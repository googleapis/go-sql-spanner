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
using Google.Rpc;
using TypeCode = Google.Cloud.Spanner.V1.TypeCode;

namespace Google.Cloud.SpannerLib.Tests;

public class RowsTests : AbstractMockServerTests
{
    [Test]
    public void TestExecuteSelect1([Values] LibType libType)
    {
        using var pool = Pool.Create(SpannerLibDictionary[libType], ConnectionString);
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
    public void TestEmptyResults([Values] LibType libType)
    {
        var sql = "select * from (select 1) where false";
        Fixture.SpannerMock.AddOrUpdateStatementResult(sql, StatementResult.CreateSingleColumnResultSet(new Spanner.V1.Type{Code = TypeCode.Int64}, "c"));
        
        using var pool = Pool.Create(SpannerLibDictionary[libType], ConnectionString);
        using var connection = pool.CreateConnection();
        using var rows = connection.Execute(new ExecuteSqlRequest { Sql = sql });
        Assert.That(rows.Metadata, Is.Not.Null);
        Assert.That(rows.Metadata.RowType.Fields.Count, Is.EqualTo(1));
        Assert.That(rows.Next(), Is.Null);
    }

    [Test]
    public void TestRandomResults([Values] LibType libType)
    {
        var numRows = 10;
        var rowType = RandomResultSetGenerator.GenerateAllTypesRowType();
        var results = RandomResultSetGenerator.Generate(rowType, numRows);
        Fixture.SpannerMock.AddOrUpdateStatementResult("select * from random", StatementResult.CreateQuery(results));
        
        using var pool = Pool.Create(SpannerLibDictionary[libType], ConnectionString);
        using var connection = pool.CreateConnection();
        using var rows = connection.Execute(new ExecuteSqlRequest { Sql = "select * from random" });

        var rowCount = 0;
        while (rows.Next() is { } row)
        {
            rowCount++;
            Assert.That(row.Values.Count, Is.EqualTo(rowType.Fields.Count));
        }
        Assert.That(rowCount, Is.EqualTo(numRows));

        Assert.That(Fixture.SpannerMock.Requests.OfType<ExecuteSqlRequest>().Count(), Is.EqualTo(1));
        var request = Fixture.SpannerMock.Requests.OfType<ExecuteSqlRequest>().First();
        Assert.That(request.Transaction?.SingleUse?.ReadOnly?.HasStrong ?? false);
    }

    [Test]
    public void TestStopHalfway([Values] LibType libType)
    {
        var numRows = 10;
        var rowType = RandomResultSetGenerator.GenerateAllTypesRowType();
        var results = RandomResultSetGenerator.Generate(rowType, numRows);
        Fixture.SpannerMock.AddOrUpdateStatementResult("select * from random", StatementResult.CreateQuery(results));
        var stopAfterRows = Random.Shared.Next(1, numRows - 1);
        
        using var pool = Pool.Create(SpannerLibDictionary[libType], ConnectionString);
        using var connection = pool.CreateConnection();
        using var rows = connection.Execute(new ExecuteSqlRequest { Sql = "select * from random" });
        Assert.That(rows.Metadata, Is.Not.Null);
        Assert.That(rows.Metadata.RowType.Fields.Count, Is.EqualTo(rowType.Fields.Count));

        var rowCount = 0;
        while (rows.Next() is { } row)
        {
            rowCount++;
            Assert.That(row.Values.Count, Is.EqualTo(rowType.Fields.Count));
            if (rowCount == stopAfterRows)
            {
                break;
            }
        }

        Assert.That(Fixture.SpannerMock.Requests.OfType<ExecuteSqlRequest>().Count(), Is.EqualTo(1));
        var request = Fixture.SpannerMock.Requests.OfType<ExecuteSqlRequest>().First();
        Assert.That(request.Transaction?.SingleUse?.ReadOnly?.HasStrong ?? false);
    }

    [Test]
    public void TestCloseConnectionWithOpenRows([Values] LibType libType)
    {
        var numRows = 5000;
        var rowType = RandomResultSetGenerator.GenerateAllTypesRowType();
        var results = RandomResultSetGenerator.Generate(rowType, numRows);
        Fixture.SpannerMock.AddOrUpdateStatementResult("select * from random", StatementResult.CreateQuery(results));
        
        using var pool = Pool.Create(SpannerLibDictionary[libType], ConnectionString);
        using var connection = pool.CreateConnection();
        using var rows = connection.Execute(new ExecuteSqlRequest { Sql = "select * from random" });

        // Verify that we can fetch the first row.
        Assert.That(rows.Next(), Is.Not.Null);
        // Close the connection while the rows object is still open.
        connection.Close();
        // Getting all the rows should not be possible.
        // If the underlying Rows object uses a stream, then it could be that it still receives some rows, but it will
        // eventually fail.
        var exception = Assert.Throws<SpannerException>(() =>
        {
            while (rows.Next() is not null)
            {
            }
        });
        // The error is 'Connection not found' or an internal exception from the underlying driver, depending on exactly
        // when the driver detects that the connection and all related objects have been closed.
        Assert.That(exception.Code is Code.NotFound or Code.Unknown, Is.True);

        if (libType == LibType.Shared)
        {
            // TODO: Remove this once it has been fixed in the shared library.
            //       Closing a Rows object that has already been closed because the connection has been closed, should
            //       be a no-op.
            var closeException = Assert.Throws<SpannerException>(() => rows.Close());
            Assert.That(closeException.Code, Is.EqualTo(Code.NotFound));
        }
    }

    [Test]
    public void TestExecuteDml([Values] LibType libType)
    {
        var sql = "update my_table set value=1 where id=2";
        Fixture.SpannerMock.AddOrUpdateStatementResult(sql, StatementResult.CreateUpdateCount(1L));
        
        using var pool = Pool.Create(SpannerLibDictionary[libType], ConnectionString);
        using var connection = pool.CreateConnection();
        using var rows = connection.Execute(new ExecuteSqlRequest { Sql = sql });
        Assert.That(rows.Next(), Is.Null);
        Assert.That(rows.UpdateCount, Is.EqualTo(1L));
        
        Assert.That(Fixture.SpannerMock.Requests.OfType<ExecuteSqlRequest>().Count(), Is.EqualTo(1));
        var request = Fixture.SpannerMock.Requests.OfType<ExecuteSqlRequest>().First();
        Assert.That(request.Transaction?.Begin?.ReadWrite, Is.Not.Null);
        Assert.That(Fixture.SpannerMock.Requests.OfType<CommitRequest>().Count(), Is.EqualTo(1));
    }

    [Test]
    public void TestExecuteDdl([Values] LibType libType)
    {
        // The mock DatabaseAdmin server always responds with a finished operation when
        // UpdateDatabaseDdl is called, so we don't need to set up any results.
        using var pool = Pool.Create(SpannerLibDictionary[libType], ConnectionString);
        using var connection = pool.CreateConnection();
        using var rows = connection.Execute(new ExecuteSqlRequest { Sql = "create my_table (id int64 primary key)" });
        Assert.That(rows.Next(), Is.Null);
        Assert.That(rows.UpdateCount, Is.EqualTo(-1L));
        
        Assert.That(Fixture.SpannerMock.Requests.OfType<ExecuteSqlRequest>().Count(), Is.EqualTo(0));
        Assert.That(Fixture.DatabaseAdminMock.Requests.OfType<UpdateDatabaseDdlRequest>().Count(), Is.EqualTo(1));
    }

    [Test]
    public void TestExecuteClientSideStatement([Values] LibType libType)
    {
        using var pool = Pool.Create(SpannerLibDictionary[libType], ConnectionString);
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