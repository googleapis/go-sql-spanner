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

    private void AddParameter(DbCommand command, string name, object value)
    {
        var param = command.CreateParameter();
        param.ParameterName = name;
        param.Value = value;
        command.Parameters.Add(param);
    }
}
