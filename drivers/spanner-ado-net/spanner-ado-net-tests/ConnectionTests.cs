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

using System.Linq;
using Google.Cloud.Spanner.V1;
using Google.Cloud.SpannerLib;
using Google.Cloud.SpannerLib.MockServer;
using Grpc.Core;
using TypeCode = Google.Cloud.Spanner.V1.TypeCode;

namespace Google.Cloud.Spanner.DataProvider.Tests;

public class ConnectionTests : AbstractMockServerTests
{
    [Test]
    public void TestOpenConnection()
    {
        var connection = new SpannerConnection { ConnectionString = ConnectionString };
        connection.Open();
        connection.Close();
    }

    [Test]
    public void TestExecute()
    {
        var sql = "update all_types set col_float8=1 where col_bigint=1";
        Fixture.SpannerMock.AddOrUpdateStatementResult(sql, StatementResult.CreateUpdateCount(1));
        using var connection = new SpannerConnection();
        connection.ConnectionString = ConnectionString;
        connection.Open();
        
        var command = connection.CreateCommand();
        command.CommandText = sql;
        var updateCount = command.ExecuteNonQuery();
        Assert.That(updateCount, Is.EqualTo(1));
    }

    [Test]
    public void TestQuery()
    {
        var sql = "select col_varchar from all_types where col_varchar is not null limit 10";
        Fixture.SpannerMock.AddOrUpdateStatementResult(sql, StatementResult.CreateSingleColumnResultSet(new V1.Type{Code = TypeCode.String}, "col_varchar", "value1", "value2", "value3"));
        using var connection = new SpannerConnection();
        connection.ConnectionString = ConnectionString;
        connection.Open();
        
        using var command = connection.CreateCommand();
        command.CommandText = sql;
        var rowCount = 0;
        using (var reader = command.ExecuteReader())
        {
            while (reader.Read())
            {
                rowCount++;
                Assert.That(reader.GetString(0), Is.EqualTo($"value{rowCount}"));
            }
        }
        Assert.That(rowCount, Is.EqualTo(3));
    }

    [Test]
    public void TestParameterizedQuery()
    {
        var sql = "select * from all_types where col_varchar=@p1";
        Fixture.SpannerMock.AddOrUpdateStatementResult(sql, StatementResult.CreateSelect1ResultSet());
        using var connection = new SpannerConnection();
        connection.ConnectionString = ConnectionString;
        connection.Open();
        
        using var command = connection.CreateCommand();
        command.CommandText = sql;
        command.Parameters.Add("2de7b24590e00a58fa7358c9531301c5");
        using (var reader = command.ExecuteReader())
        {
            for (int i = 0; i < reader.FieldCount; i++)
            {
                Assert.That(reader.GetFieldType(i), Is.Not.Null);
            }
            while (reader.Read())
            {
                for (int i = 0; i < reader.FieldCount; i++)
                {
                    Assert.That(reader.GetValue(i), Is.Not.Null);
                }
            }
        }
        var requests = Fixture.SpannerMock.Requests.OfType<ExecuteSqlRequest>().ToList();
        Assert.That(requests, Has.Count.EqualTo(1));
        var request = requests.First();
        Assert.That(request.Params.Fields, Has.Count.EqualTo(1));
    }

    [Test]
    public void TestTransaction()
    {
        var sql = "select * from all_types where col_varchar=$1";
        Fixture.SpannerMock.AddOrUpdateStatementResult(sql, StatementResult.CreateSelect1ResultSet());
        using var connection = new SpannerConnection();
        connection.ConnectionString = ConnectionString;
        connection.Open();
        
        using var transaction = connection.BeginTransaction();
        using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = sql;
        command.Parameters.Add("2de7b24590e00a58fa7358c9531301c5");
        using (var reader = command.ExecuteReader())
        {
            for (int i = 0; i < reader.FieldCount; i++)
            {
                Assert.That(reader.GetFieldType(i), Is.Not.Null);
            }
            while (reader.Read())
            {
                for (int i = 0; i < reader.FieldCount; i++)
                {
                    Assert.That(reader.GetValue(i), Is.Not.Null);
                }
            }
        }
        transaction.Commit();
    }
    
    [Test]
    public void TestDisableInternalRetries()
    {
        var sql = "update my_table set value=@p1 where id=@p2 and version=1";
        Fixture.SpannerMock.AddOrUpdateStatementResult(sql, StatementResult.CreateUpdateCount(1));
        using var connection = new SpannerConnection();
        connection.ConnectionString = ConnectionString + ";retryAbortsInternally=false";
        connection.Open();
        
        using var transaction = connection.BeginTransaction();
        using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = sql;
        command.Parameters.Add("2de7b24590e00a58fa7358c9531301c5");
        command.Parameters.Add(1L);
        command.ExecuteNonQuery();
        Fixture.SpannerMock.AddOrUpdateExecutionTime(nameof(Fixture.SpannerMock.Commit), ExecutionTime.CreateException(StatusCode.Aborted, "Transaction was aborted"));
        Assert.Throws<SpannerException>(transaction.Commit);
        
        var requests = Fixture.SpannerMock.Requests.OfType<ExecuteSqlRequest>().ToList();
        Assert.That(requests.Count, Is.EqualTo(1));
    }

    [Test]
    public void TestBatchDml()
    {
        var sql1 = "update all_types set col_float8=1 where col_bigint=1";
        var sql2 = "update all_types set col_float8=2 where col_bigint=2";
        Fixture.SpannerMock.AddOrUpdateStatementResult(sql1, StatementResult.CreateUpdateCount(2));
        Fixture.SpannerMock.AddOrUpdateStatementResult(sql2, StatementResult.CreateUpdateCount(3));
        using var connection = new SpannerConnection();
        connection.ConnectionString = ConnectionString;
        connection.Open();
        
        using var command1 = connection.CreateCommand();
        command1.CommandText = sql1;
        using var command2 = connection.CreateCommand();
        command2.CommandText = sql2;
        var affected = connection.ExecuteBatchDml([command1, command2]);
        Assert.That(affected, Is.EqualTo(new long[] { 2, 3 }));
    }
    
}