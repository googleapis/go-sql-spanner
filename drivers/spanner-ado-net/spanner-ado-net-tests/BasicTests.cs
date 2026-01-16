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
using Google.Cloud.Spanner.Admin.Database.V1;
using Google.Cloud.Spanner.V1;
using Google.Cloud.SpannerLib.MockServer;
using Google.Protobuf.WellKnownTypes;

namespace Google.Cloud.Spanner.DataProvider.Tests;

public class BasicTests : AbstractMockServerTests
{
    [Test]
    public void TestOpenConnection()
    {
        var connection = new SpannerConnection();
        connection.ConnectionString = ConnectionString;
        connection.Open();
        connection.Close();
    }
    
    [Test]
    public void TestExecuteQuery()
    {
        using var connection = new SpannerConnection();
        connection.ConnectionString = ConnectionString;
        connection.Open();
        
        using var cmd = connection.CreateCommand();
        cmd.CommandText = "SELECT 1";
        using var reader = cmd.ExecuteReader();
        while (reader.Read())
        {
            Assert.That(reader.GetInt64(0), Is.EqualTo(1));
        }
    }
    
    [Test]
    public void TestExecuteParameterizedQuery()
    {
        Fixture.SpannerMock.AddOrUpdateStatementResult("SELECT $1", StatementResult.CreateSelect1ResultSet());
        using var connection = new SpannerConnection();
        connection.ConnectionString = ConnectionString;
        connection.Open();
        
        using var cmd = connection.CreateCommand();
        cmd.CommandText = "SELECT $1";
        var param = cmd.CreateParameter();
        param.ParameterName = "p1";
        param.Value = 1;
        cmd.Parameters.Add(param);
        using var reader = cmd.ExecuteReader();
        while (reader.Read())
        {
            Assert.That(reader.GetInt64(0), Is.EqualTo(1));
        }
    }
    
    [Test]
    public void TestExecuteStaleQuery()
    {
        using var connection = new SpannerConnection();
        connection.ConnectionString = ConnectionString;
        connection.Open();
        
        using var cmd = connection.CreateCommand();
        cmd.CommandText = "SELECT 1";
        cmd.SingleUseReadOnlyTransactionOptions = new TransactionOptions.Types.ReadOnly
        {
            ExactStaleness = Duration.FromTimeSpan(TimeSpan.FromSeconds(10)),
        };
        using var reader = cmd.ExecuteReader();
        while (reader.Read())
        {
            Assert.That(reader.GetInt64(0), Is.EqualTo(1));
        }

        var request = Fixture.SpannerMock.Requests.OfType<ExecuteSqlRequest>().First();
        Assert.That(request, Is.Not.Null);
        Assert.That(request.Transaction, Is.Not.Null);
        Assert.That(request.Transaction.SingleUse, Is.Not.Null);
        Assert.That(request.Transaction.SingleUse.ReadOnly, Is.Not.Null);
        Assert.That(request.Transaction.SingleUse.ReadOnly.ExactStaleness, Is.Not.Null);
        Assert.That(request.Transaction.SingleUse.ReadOnly.ExactStaleness.Seconds, Is.EqualTo(10));
    }

    [Test]
    public void TestInsertAllDataTypes()
    {
        var sql = "insert into all_types (col_bool, col_bytes, col_date, col_interval, col_json, col_int64, col_float32, col_float64, col_numeric, col_string, col_timestamp) " +
                  "values (@col_bool, @col_bytes, @col_date, @col_interval, @col_json, @col_int64, @col_float32, @col_float64, @col_numeric, @col_string, @col_timestamp)";
        Fixture.SpannerMock.AddOrUpdateStatementResult(sql, StatementResult.CreateUpdateCount(1));
        
        using var connection = new SpannerConnection();
        connection.ConnectionString = ConnectionString;
        connection.Open();
        
        using var cmd = connection.CreateCommand();
        cmd.CommandText = sql;
        AddParameter(cmd, "col_bool", true);
        AddParameter(cmd, "col_bytes", new byte[] { 1, 2, 3 });
        AddParameter(cmd, "col_date", new DateOnly(2025, 8, 25));
        AddParameter(cmd, "col_interval", TimeSpan.FromHours(1));
        AddParameter(cmd, "col_json", JsonDocument.Parse("{\"key\":\"value\"}"));
        AddParameter(cmd, "col_int64", 10);
        AddParameter(cmd, "col_float32", 3.14f);
        AddParameter(cmd, "col_float64", 3.14d);
        AddParameter(cmd, "col_numeric", 10.1m);
        AddParameter(cmd, "col_string", "hello");
        AddParameter(cmd, "col_timestamp", DateTime.Parse("2025-08-25T16:30:55Z"));
        
        var updateCount = cmd.ExecuteNonQuery();
        Assert.That(updateCount, Is.EqualTo(1));
        
        var requests = Fixture.SpannerMock.Requests.OfType<ExecuteSqlRequest>().ToList();
        Assert.That(requests, Has.Count.EqualTo(1));
        var request = requests.First();
        Assert.That(request.Params.Fields, Has.Count.EqualTo(11));
        Assert.That(request.Params.Fields["col_bool"].BoolValue, Is.EqualTo(true));
        Assert.That(request.Params.Fields["col_bytes"].StringValue, Is.EqualTo(Convert.ToBase64String(new byte[]{1,2,3})));
        Assert.That(request.Params.Fields["col_date"].StringValue, Is.EqualTo("2025-08-25"));
        Assert.That(request.Params.Fields["col_interval"].StringValue, Is.EqualTo("PT1H"));
        Assert.That(request.Params.Fields["col_int64"].StringValue, Is.EqualTo("10"));
        Assert.That(request.Params.Fields["col_float32"].NumberValue, Is.EqualTo(3.14f));
        Assert.That(request.Params.Fields["col_float64"].NumberValue, Is.EqualTo(3.14d));
        Assert.That(request.Params.Fields["col_numeric"].StringValue, Is.EqualTo("10.1"));
        Assert.That(request.Params.Fields["col_string"].StringValue, Is.EqualTo("hello"));
        Assert.That(request.Params.Fields["col_timestamp"].StringValue, Is.EqualTo("2025-08-25T16:30:55.0000000Z"));
        
        Assert.That(request.ParamTypes.Count, Is.EqualTo(0));
    }

    [Test]
    public void TestQueryAllDataTypes([Values(0, 1, 10, 49, 50, 51, 100)] int numRows)
    {
        const string sql = "select * from all_types";
        var result = RandomResultSetGenerator.Generate(numRows, allowNull: true);
        Fixture.SpannerMock.AddOrUpdateStatementResult(sql, StatementResult.CreateQuery(result));
        
        using var connection = new SpannerConnection();
        connection.ConnectionString = ConnectionString;
        connection.Open();
        
        using var cmd = connection.CreateCommand();
        cmd.CommandText = sql;
        using var reader = cmd.ExecuteReader();
        var numRowsFound = 0;
        while (reader.Read())
        {
            var index = 0;
            foreach (var field in result.Metadata.RowType.Fields)
            {
                Assert.That(reader[index], Is.EqualTo(reader[field.Name]));
                index++;
            }
            Assert.That(reader.GetFieldValue<bool?>(reader.GetOrdinal("col_bool")), Is.EqualTo(ValueOrNull(reader["col_bool"])));
            Assert.That(reader.GetFieldValue<byte[]>(reader.GetOrdinal("col_bytes")), Is.EqualTo(ValueOrNull(reader["col_bytes"])));
            Assert.That(reader.GetFieldValue<DateOnly?>(reader.GetOrdinal("col_date")), Is.EqualTo(ValueOrNull(reader["col_date"])));
            Assert.That(reader.GetFieldValue<float?>(reader.GetOrdinal("col_float32")), Is.EqualTo(ValueOrNull(reader["col_float32"])));
            Assert.That(reader.GetFieldValue<double?>(reader.GetOrdinal("col_float64")), Is.EqualTo(ValueOrNull(reader["col_float64"])));
            Assert.That(reader.GetFieldValue<long?>(reader.GetOrdinal("col_int64")), Is.EqualTo(ValueOrNull(reader["col_int64"])));
            Assert.That(reader.GetFieldValue<TimeSpan?>(reader.GetOrdinal("col_interval")), Is.EqualTo(ValueOrNull(reader["col_interval"])));
            Assert.That(reader.GetFieldValue<string?>(reader.GetOrdinal("col_json")), Is.EqualTo(ValueOrNull(reader["col_json"])));
            Assert.That(reader.GetFieldValue<decimal?>(reader.GetOrdinal("col_numeric")), Is.EqualTo(ValueOrNull(reader["col_numeric"])));
            Assert.That(reader.GetFieldValue<string?>(reader.GetOrdinal("col_string")), Is.EqualTo(ValueOrNull(reader["col_string"])));
            Assert.That(reader.GetFieldValue<DateTime?>(reader.GetOrdinal("col_timestamp")), Is.EqualTo(ValueOrNull(reader["col_timestamp"])));
            Assert.That(reader.GetFieldValue<Guid?>(reader.GetOrdinal("col_uuid")), Is.EqualTo(ValueOrNull(reader["col_uuid"])));
            
            Assert.That(reader.GetFieldValue<List<bool?>>(reader.GetOrdinal("col_array_bool")), Is.EqualTo(ValueOrNull(reader["col_array_bool"])));
            Assert.That(reader.GetFieldValue<List<byte[]>>(reader.GetOrdinal("col_array_bytes")), Is.EqualTo(ValueOrNull(reader["col_array_bytes"])));
            Assert.That(reader.GetFieldValue<List<DateOnly?>>(reader.GetOrdinal("col_array_date")), Is.EqualTo(ValueOrNull(reader["col_array_date"])));
            Assert.That(reader.GetFieldValue<List<float?>>(reader.GetOrdinal("col_array_float32")), Is.EqualTo(ValueOrNull(reader["col_array_float32"])));
            Assert.That(reader.GetFieldValue<List<double?>>(reader.GetOrdinal("col_array_float64")), Is.EqualTo(ValueOrNull(reader["col_array_float64"])));
            Assert.That(reader.GetFieldValue<List<long?>>(reader.GetOrdinal("col_array_int64")), Is.EqualTo(ValueOrNull(reader["col_array_int64"])));
            Assert.That(reader.GetFieldValue<List<TimeSpan?>>(reader.GetOrdinal("col_array_interval")), Is.EqualTo(ValueOrNull(reader["col_array_interval"])));
            Assert.That(reader.GetFieldValue<List<string?>>(reader.GetOrdinal("col_array_json")), Is.EqualTo(ValueOrNull(reader["col_array_json"])));
            Assert.That(reader.GetFieldValue<List<decimal?>>(reader.GetOrdinal("col_array_numeric")), Is.EqualTo(ValueOrNull(reader["col_array_numeric"])));
            Assert.That(reader.GetFieldValue<List<string?>>(reader.GetOrdinal("col_array_string")), Is.EqualTo(ValueOrNull(reader["col_array_string"])));
            Assert.That(reader.GetFieldValue<List<DateTime?>>(reader.GetOrdinal("col_array_timestamp")), Is.EqualTo(ValueOrNull(reader["col_array_timestamp"])));
            Assert.That(reader.GetFieldValue<List<Guid?>>(reader.GetOrdinal("col_array_uuid")), Is.EqualTo(ValueOrNull(reader["col_array_uuid"])));
            numRowsFound++;
        }
        Assert.That(numRowsFound, Is.EqualTo(numRows));
    }

    private static object? ValueOrNull(object value)
    {
        return value is DBNull ? null : value;
    }
        
    [Test]
    public void TestExecuteDdl()
    {
        using var connection = new SpannerConnection();
        connection.ConnectionString = ConnectionString;
        connection.Open();
        
        using var cmd = connection.CreateCommand();
        cmd.CommandText = "create table my_table (id int64 primary key, value string(max))";
        Assert.That(cmd.ExecuteNonQuery(), Is.EqualTo(-1));

        var requests = Fixture.DatabaseAdminMock.Requests.OfType<UpdateDatabaseDdlRequest>().ToList();
        Assert.That(requests, Has.Count.EqualTo(1));
        var request = requests.First();
        Assert.That(request.Statements, Has.Count.EqualTo(1));
        var statement = request.Statements.First();
        Assert.That(statement, Is.EqualTo(cmd.CommandText));
    }
    
    [Test]
    public void TestExecuteCreateDatabase()
    {
        using var connection = new SpannerConnection();
        connection.ConnectionString = ConnectionString;
        connection.Open();
        
        using var cmd = connection.CreateCommand();
        cmd.CommandText = "create database my_database";
        Assert.That(cmd.ExecuteNonQuery(), Is.EqualTo(-1));

        var updateDatabaseDdlRequests = Fixture.DatabaseAdminMock.Requests.OfType<UpdateDatabaseDdlRequest>().ToList();
        Assert.That(updateDatabaseDdlRequests, Has.Count.EqualTo(0));
        
        var requests = Fixture.DatabaseAdminMock.Requests.OfType<CreateDatabaseRequest>().ToList();
        Assert.That(requests, Has.Count.EqualTo(1));
        var request = requests.First();
        Assert.That(request.CreateStatement, Is.EqualTo(cmd.CommandText));
        Assert.That(request.Parent, Is.EqualTo("projects/p1/instances/i1"));
    }
    
    [Test]
    public void TestExecuteCreateDatabaseWithExtraStatements()
    {
        using var connection = new SpannerConnection();
        connection.ConnectionString = ConnectionString;
        connection.Open();
        
        using var batch = connection.CreateBatch();
        var cmd = batch.CreateBatchCommand();
        cmd.CommandText = "create database my_database";
        batch.BatchCommands.Add(cmd);
        cmd = batch.CreateBatchCommand();
        cmd.CommandText = "create table my_table (id int64 primary key, value string)";
        batch.BatchCommands.Add(cmd);
        cmd = batch.CreateBatchCommand();
        cmd.CommandText = "create index my_index on my_table (value)";
        batch.BatchCommands.Add(cmd);
        
        // TODO: Check with other drivers what they return when a batch contains multiple statements that return -1.
        Assert.That(batch.ExecuteNonQuery(), Is.EqualTo(-3));

        var updateDatabaseDdlRequests = Fixture.DatabaseAdminMock.Requests.OfType<UpdateDatabaseDdlRequest>().ToList();
        Assert.That(updateDatabaseDdlRequests, Has.Count.EqualTo(0));
        
        var requests = Fixture.DatabaseAdminMock.Requests.OfType<CreateDatabaseRequest>().ToList();
        Assert.That(requests, Has.Count.EqualTo(1));
        var request = requests.First();
        Assert.That(request.CreateStatement, Is.EqualTo("create database my_database"));
        Assert.That(request.Parent, Is.EqualTo("projects/p1/instances/i1"));
    }
    
    private void AddParameter(DbCommand command, string name, object value)
    {
        var param = command.CreateParameter();
        param.ParameterName = name;
        param.Value = value;
        command.Parameters.Add(param);
    }
}
