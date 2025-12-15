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

using System.Text;
using Google.Cloud.Spanner.Admin.Database.V1;
using Google.Cloud.Spanner.V1;
using Google.Cloud.SpannerLib.MockServer;
using Google.Rpc;
using Grpc.Core;
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
        // Verify that calling Next() and NextResultSet() continues to return the correct result.
        Assert.That(rows.Next(), Is.Null);
        Assert.That(rows.NextResultSet(), Is.False);
        Assert.That(rows.Next(), Is.Null);
        Assert.That(rows.Next(), Is.Null);
        Assert.That(rows.NextResultSet(), Is.False);
    }

    [Test]
    public void TestRandomResults([Values] LibType libType, [Values(0, 1, 10)] int numRows, [Values(0, 1, 5, 9, 10, 11)] int prefetchRows)
    {
        var rowType = RandomResultSetGenerator.GenerateAllTypesRowType();
        var results = RandomResultSetGenerator.Generate(rowType, numRows);
        Fixture.SpannerMock.AddOrUpdateStatementResult("select * from random", StatementResult.CreateQuery(results));
        
        using var pool = Pool.Create(SpannerLibDictionary[libType], ConnectionString);
        using var connection = pool.CreateConnection();
        using var rows = connection.Execute(new ExecuteSqlRequest { Sql = "select * from random" }, prefetchRows);

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
    public void TestLargeStringValue([Values] LibType libType)
    {
        const string sql = "select large_value from my_table";
        var value = TestUtils.GenerateRandomString(10_000_000);
        Fixture.SpannerMock.AddOrUpdateStatementResult(sql, StatementResult.CreateSingleColumnResultSet(
            new Spanner.V1.Type{Code = TypeCode.String}, "large_value", value));
        
        using var pool = Pool.Create(SpannerLibDictionary[libType], ConnectionString);
        using var connection = pool.CreateConnection();
        using var rows = connection.Execute(new ExecuteSqlRequest { Sql = sql });
        var numRows = 0;
        while (rows.Next() is { } row)
        {
            numRows++;
            Assert.That(row.Values.Count, Is.EqualTo(1));
            Assert.That(row.Values[0].HasStringValue);
            Assert.That(row.Values[0].StringValue, Is.EqualTo(value));
        }
        Assert.That(numRows, Is.EqualTo(1));
    }

    [Test]
    public void TestStopHalfway([Values] LibType libType, [Values(2, 10)] int numRows, [Values(0, 1, 2, 3, 5, 9, 10, 11)] int prefetchRows)
    {
        var rowType = RandomResultSetGenerator.GenerateAllTypesRowType();
        var results = RandomResultSetGenerator.Generate(rowType, numRows);
        Fixture.SpannerMock.AddOrUpdateStatementResult("select * from random", StatementResult.CreateQuery(results));
        var stopAfterRows = Random.Shared.Next(1, numRows - 1);
        
        using var pool = Pool.Create(SpannerLibDictionary[libType], ConnectionString);
        using var connection = pool.CreateConnection();
        using var rows = connection.Execute(new ExecuteSqlRequest { Sql = "select * from random" }, prefetchRows);
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
    public void TestStopHalfwayTwoQueries([Values] LibType libType, [Values(0, 1, 2, 3)] int prefetchRows)
    {
        const string sql = "select c from my_table";
        Fixture.SpannerMock.AddOrUpdateStatementResult(sql, StatementResult.CreateResultSet(
            [Tuple.Create(TypeCode.Int64, "c")], [[1L], [2L]]));
        
        using var pool = Pool.Create(SpannerLibDictionary[libType], ConnectionString);
        using var connection = pool.CreateConnection();

        for (var i = 0; i < 2; i++)
        {
            using var rows = connection.Execute(new ExecuteSqlRequest { Sql = sql }, prefetchRows);
            Assert.That(rows.Metadata, Is.Not.Null);
            Assert.That(rows.Metadata.RowType.Fields.Count, Is.EqualTo(1));
            var row = rows.Next();
            Assert.That(row, Is.Not.Null);
            Assert.That(row.Values.Count, Is.EqualTo(1));
            Assert.That(row.Values[0].HasStringValue);
            Assert.That(row.Values[0].StringValue, Is.EqualTo("1"));
        }
        Assert.That(Fixture.SpannerMock.Requests.OfType<ExecuteSqlRequest>().Count(), Is.EqualTo(2));
    }

    [Test]
    public void TestStopHalfwayMultipleQueries(
        [Values] LibType libType,
        [Values(2, 10)] int numRows,
        [Values(0, 1, 2, 3, 5, 9, 10, 11)] int prefetchRows,
        [Values(1, 2, 3)] int numQueries)
    {
        const string query = "select * from random";
        
        var rowType = RandomResultSetGenerator.GenerateAllTypesRowType();
        var results = RandomResultSetGenerator.Generate(rowType, numRows);
        Fixture.SpannerMock.AddOrUpdateStatementResult(query, StatementResult.CreateQuery(results));
        
        var stopAfterRows = new int[numQueries];
        var queries = new string[numQueries];
        for (var i = 0; i < numQueries; i++)
        {
            stopAfterRows[i] = Random.Shared.Next(1, numRows - 1);
            queries[i] = query;
        }

        var sql = string.Join(";", queries);
        using var pool = Pool.Create(SpannerLibDictionary[libType], ConnectionString);
        using var connection = pool.CreateConnection();
        using var rows = connection.Execute(new ExecuteSqlRequest { Sql = sql } ,prefetchRows);
        Assert.That(rows.Metadata, Is.Not.Null);
        Assert.That(rows.Metadata.RowType.Fields.Count, Is.EqualTo(rowType.Fields.Count));

        var totalRowCount = 0;
        for (var i = 0; i < numQueries; i++)
        {
            var rowCount = 0;
            while (rows.Next() is { } row)
            {
                rowCount++;
                totalRowCount++;
                Assert.That(row.Values.Count, Is.EqualTo(rowType.Fields.Count));
                if (rowCount == stopAfterRows[i])
                {
                    break;
                }
            }
            Assert.That(rows.NextResultSet(), Is.EqualTo(i < numQueries-1));
        }
        Assert.That(Fixture.SpannerMock.Requests.OfType<ExecuteSqlRequest>().Count(), Is.EqualTo(numQueries));
        Assert.That(totalRowCount, Is.EqualTo(stopAfterRows.Sum()));
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

        var foundRows = 0;
        // Verify that we can fetch the first row.
        Assert.That(rows.Next(), Is.Not.Null);
        foundRows++;
        // Close the connection while the rows object is still open.
        connection.Close();
        // Getting all the rows should not be possible.
        // If the underlying Rows object uses a stream, then it could be that it still receives some rows, but it will
        // eventually fail.
        var exception = Assert.Throws<SpannerException>(() =>
        {
            while (rows.Next() is not null)
            {
                foundRows++;
            }
        });
        // The error is 'Connection not found' or an internal exception from the underlying driver, depending on exactly
        // when the driver detects that the connection and all related objects have been closed.
        Assert.That(exception.Code is Code.NotFound or Code.Unknown, Is.True);
        Assert.That(foundRows, Is.LessThan(numRows));
    }

    [Test]
    public void TestExecuteDml([Values] LibType libType, [Values(0, 1, 2)] int prefetchRows)
    {
        var sql = "update my_table set value=1 where id=2";
        Fixture.SpannerMock.AddOrUpdateStatementResult(sql, StatementResult.CreateUpdateCount(1L));
        
        using var pool = Pool.Create(SpannerLibDictionary[libType], ConnectionString);
        using var connection = pool.CreateConnection();
        using var rows = connection.Execute(new ExecuteSqlRequest { Sql = sql }, prefetchRows);
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
    
    [Test]
    public async Task TestMultipleQueries([Values] LibType libType, [Values] bool async)
    {
        await using var pool = Pool.Create(SpannerLibDictionary[libType], ConnectionString);
        await using var connection = pool.CreateConnection();
        var sql = "SELECT 1;SELECT 1";
        await using var rows = async
            ? await connection.ExecuteAsync(new ExecuteSqlRequest { Sql = sql })
            // ReSharper disable once MethodHasAsyncOverload
            : connection.Execute(new ExecuteSqlRequest { Sql = sql });

        var numResultSets = 0;
        var totalRows = 0;
        do
        {
            var numRows = 0;
            // ReSharper disable once MethodHasAsyncOverload
            while ((async ? await rows.NextAsync() : rows.Next()) is { } row)
            {
                numRows++;
                totalRows++;
                Assert.That(row.Values.Count, Is.EqualTo(1));
                Assert.That(row.Values[0].HasStringValue);
                Assert.That(row.Values[0].StringValue, Is.EqualTo("1"));
            }
            Assert.That(numRows, Is.EqualTo(1));
            numResultSets++;
            // ReSharper disable once MethodHasAsyncOverload
        } while (async ? await rows.NextResultSetAsync() : rows.NextResultSet());
        
        Assert.That(totalRows, Is.EqualTo(2));
        Assert.That(numResultSets, Is.EqualTo(2));
    }
    
    [Test]
    public async Task TestMultipleMixedStatements([Values] LibType libType, [Values(2, 10)] int numRows, [Values(0, 1, 2, 3, 5, 9, 10, 11)] int prefetchRows, [Values] bool async)
    {
        var updateCount = 3L;
        var dml = "update my_table set value=1 where id in (1,2,3)";
        Fixture.SpannerMock.AddOrUpdateStatementResult(dml, StatementResult.CreateUpdateCount(updateCount));
        
        var rowType = RandomResultSetGenerator.GenerateAllTypesRowType();
        var results = RandomResultSetGenerator.Generate(rowType, numRows);
        var query = "select * from random";
        Fixture.SpannerMock.AddOrUpdateStatementResult(query, StatementResult.CreateQuery(results));

        // Create a SQL string containing a mix of DML and queries.
        var sql = $"{dml};{dml};{query};{dml};{query}";

        await using var pool = Pool.Create(SpannerLibDictionary[libType], ConnectionString);
        await using var connection = pool.CreateConnection();
        await using var rows = async
            ? await connection.ExecuteAsync(new ExecuteSqlRequest { Sql = sql }, prefetchRows)
            // ReSharper disable once MethodHasAsyncOverload
            : connection.Execute(new ExecuteSqlRequest { Sql = sql }, prefetchRows);

        var numResultSets = 0;
        var totalRows = 0;
        var totalUpdateCount = 0L;
        do
        {
            // ReSharper disable once MethodHasAsyncOverload
            while ((async ? await rows.NextAsync() : rows.Next()) is not null)
            {
                totalRows++;
            }
            numResultSets++;
            if (rows.UpdateCount > -1)
            {
                totalUpdateCount += rows.UpdateCount;
            }
            // ReSharper disable once MethodHasAsyncOverload
        } while (async ? await rows.NextResultSetAsync() : rows.NextResultSet());
        
        // The query is executed 2 times.
        Assert.That(totalRows, Is.EqualTo(numRows * 2));
        // The DML statement is executed 3 times.
        Assert.That(totalUpdateCount, Is.EqualTo(updateCount * 3));
        // There are 5 statements in the SQL string.
        Assert.That(numResultSets, Is.EqualTo(5));
    }
    
    [Test]
    public async Task TestGetAllUpdateCounts([Values] LibType libType, [Values(0, 1, 2, 5)] int numDmlStatements, [Values(2, 10)] int numRows, [Values(0, 1, 2, 3, 5, 9, 10, 11)] int prefetchRows, [Values] bool async)
    {
        var updateCount = 3L;
        var dml = "update my_table set value=1 where id in (1,2,3)";
        Fixture.SpannerMock.AddOrUpdateStatementResult(dml, StatementResult.CreateUpdateCount(updateCount));
        Fixture.SpannerMock.AddOrUpdateStatementResult(dml + ";", StatementResult.CreateUpdateCount(updateCount));
        
        var rowType = RandomResultSetGenerator.GenerateAllTypesRowType();
        var results = RandomResultSetGenerator.Generate(rowType, numRows);
        var query = "select * from random";
        Fixture.SpannerMock.AddOrUpdateStatementResult(query, StatementResult.CreateQuery(results));

        // Create a SQL string containing a mix of DML and queries.
        var numQueries = 0;
        var builder = new StringBuilder();
        for (var i = 0; i < numDmlStatements; i++)
        {
            while (Random.Shared.Next() % 2 == 0)
            {
                builder.Append(query).Append(';');
                numQueries++;
            }
            builder.Append(dml).Append(';');
            while (Random.Shared.Next() % 5 == 0)
            {
                builder.Append(query).Append(';');
                numQueries++;
            }
        }
        var sql = builder.ToString();
        if ("".Equals(sql))
        {
            sql = query;
            numQueries = 1;
        }

        await using var pool = Pool.Create(SpannerLibDictionary[libType], ConnectionString);
        await using var connection = pool.CreateConnection();
        await using var rows = async
            ? await connection.ExecuteAsync(new ExecuteSqlRequest { Sql = sql }, prefetchRows)
            // ReSharper disable once MethodHasAsyncOverload
            : connection.Execute(new ExecuteSqlRequest { Sql = sql }, prefetchRows);

        // ReSharper disable once MethodHasAsyncOverload
        var totalUpdateCount = async ? await rows.GetTotalUpdateCountAsync() : rows.GetTotalUpdateCount();
        Assert.That(totalUpdateCount, Is.EqualTo(updateCount * numDmlStatements - numQueries));
    }
    
    [Test]
    public async Task TestMultipleMixedStatementsWithErrors(
        [Values] LibType libType,
        [Values(2, 10)] int numRows,
        [Values(0, 1, 2, 3, 5, 9, 10, 11)] int prefetchRows,
        [Values(0, 1, 2, 3, 4, 5)] int errorIndex,
        [Values] bool async)
    {
        const long updateCount = 3L;
        const string dml = "update my_table set value=1 where id in (1,2,3)";
        Fixture.SpannerMock.AddOrUpdateStatementResult(dml, StatementResult.CreateUpdateCount(updateCount));
        
        var rowType = RandomResultSetGenerator.GenerateAllTypesRowType();
        var results = RandomResultSetGenerator.Generate(rowType, numRows);
        const string query = "select * from random";
        Fixture.SpannerMock.AddOrUpdateStatementResult(query, StatementResult.CreateQuery(results));

        const string invalidQuery = "select * from unknown_table";
        Fixture.SpannerMock.AddOrUpdateStatementResult(invalidQuery, StatementResult.CreateException(new RpcException(new global::Grpc.Core.Status(StatusCode.NotFound, "Table not found"))));

        // Create a SQL string containing a mix of DML and queries.
        var numStatements = Random.Shared.Next(errorIndex + 1, errorIndex + 6);
        var statements = new string[numStatements];
        var statementIsDml = new bool[numStatements];
        for (var i = 0; i < statements.Length; i++)
        {
            if (errorIndex == i)
            {
                statements[i] = invalidQuery;
            }
            else
            {
                statementIsDml[i] = Random.Shared.Next(2) == 1;
                statements[i] = statementIsDml[i] ? dml : query;
            }
        }
        var sql = string.Join(";", statements);

        await using var pool = Pool.Create(SpannerLibDictionary[libType], ConnectionString);
        await using var connection = pool.CreateConnection();
        var numResultSets = 0;
        var totalRows = 0;
        var totalUpdateCount = 0L;
        
        if (errorIndex == 0)
        {
            if (async)
            {
                Assert.ThrowsAsync<SpannerException>(() => connection.ExecuteAsync(new ExecuteSqlRequest { Sql = sql }, prefetchRows));
            }
            else
            {
                Assert.Throws<SpannerException>(() => connection.Execute(new ExecuteSqlRequest { Sql = sql }, prefetchRows));
            }
        }
        else
        {
            await using var rows = async
                ? await connection.ExecuteAsync(new ExecuteSqlRequest { Sql = sql }, prefetchRows)
                // ReSharper disable once MethodHasAsyncOverload
                : connection.Execute(new ExecuteSqlRequest { Sql = sql }, prefetchRows);

            var statementIndex = 0;
            while (statementIndex < numStatements)
            {
                // ReSharper disable once MethodHasAsyncOverload
                while ((async ? await rows.NextAsync() : rows.Next()) is not null)
                {
                    totalRows++;
                }
                numResultSets++;
                if (rows.UpdateCount > -1)
                {
                    totalUpdateCount += rows.UpdateCount;
                }
                statementIndex++;
                if (statementIndex == errorIndex)
                {
                    if (async)
                    {
                        Assert.ThrowsAsync<SpannerException>(() => rows.NextResultSetAsync());
                    }
                    else
                    {
                        Assert.Throws<SpannerException>(() => rows.NextResultSet());
                    }
                    break;
                }
                // ReSharper disable once MethodHasAsyncOverload
                Assert.That(async ? await rows.NextResultSetAsync() : rows.NextResultSet());
            }
        }

        var expectedUpdateCount = 0L;
        var expectedRowCount = 0;
        for (var i = 0; i < errorIndex; i++)
        {
            if (statementIsDml[i])
            {
                expectedUpdateCount += updateCount;
            }
            else
            {
                expectedRowCount += numRows;
            }
        }
        
        Assert.That(totalRows, Is.EqualTo(expectedRowCount));
        Assert.That(totalUpdateCount, Is.EqualTo(expectedUpdateCount));
        Assert.That(numResultSets, Is.EqualTo(errorIndex));
    }
}