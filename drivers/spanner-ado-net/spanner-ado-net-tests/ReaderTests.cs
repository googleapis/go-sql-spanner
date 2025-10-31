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

using System.Buffers.Binary;
using System.Collections;
using System.Data;
using System.Diagnostics.CodeAnalysis;
using System.Runtime.InteropServices;
using System.Text;
using Google.Cloud.Spanner.V1;
using Google.Cloud.SpannerLib.MockServer;
using Grpc.Core;
using TypeCode = Google.Cloud.Spanner.V1.TypeCode;

namespace Google.Cloud.Spanner.DataProvider.Tests;

public class ReaderTests : AbstractMockServerTests
{
    [Test]
    public async Task ResumableNonConsumedToNonResumable()
    {
        var base64Value = Convert.ToBase64String([1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);
        var sql = $"SELECT from_base64('{base64Value}'), 1";
        Fixture.SpannerMock.AddOrUpdateStatementResult(sql, StatementResult.CreateResultSet(
            [Tuple.Create(TypeCode.Bytes, "c"), Tuple.Create(TypeCode.Int64, "c")],
            [[base64Value, 1L]]));
        
        await using var conn = await OpenConnectionAsync();
        await using var cmd = new SpannerCommand(sql, conn);
        await using var reader = await cmd.ExecuteReaderAsync();
        await reader.ReadAsync();

        await reader.IsDBNullAsync(0);
        _ = reader.IsDBNull(0);
        await using var stream = reader.GetStream(0);
        Assert.That(reader.GetString(0), Is.EqualTo(base64Value));
    }

    [Test]
    [SuppressMessage("ReSharper", "MethodHasAsyncOverload")]
    public async Task SeekColumns()
    {
        const string sql = "SELECT 1,2,3";
        Fixture.SpannerMock.AddOrUpdateStatementResult(sql, StatementResult.CreateResultSet(
            [Tuple.Create(TypeCode.Int64, "c"), Tuple.Create(TypeCode.Int64, "c"), Tuple.Create(TypeCode.Int64, "c")],
            [[1,2,3]]));
        
        await using var conn = await OpenConnectionAsync();
        await using var cmd = new SpannerCommand(sql, conn);
        await using var reader = await cmd.ExecuteReaderAsync();
        Assert.That(reader.Read(), Is.True);
        Assert.That(reader.GetInt32(0), Is.EqualTo(1));
        Assert.That(reader.GetInt32(0), Is.EqualTo(1));
        Assert.That(reader.GetInt32(1), Is.EqualTo(2));
        Assert.That(reader.GetInt32(0), Is.EqualTo(1));
    }

    [Ignore("Requires multi-statement support")]
    [Test]
    [SuppressMessage("ReSharper", "MethodHasAsyncOverload")]
    public async Task NoResultSet()
    {
        const string insert = "INSERT INTO my_table VALUES (8)";
        Fixture.SpannerMock.AddOrUpdateStatementResult(insert, StatementResult.CreateUpdateCount(1L));
        
        await using var conn = await OpenConnectionAsync();

        await using (var cmd = new SpannerCommand(insert, conn))
        await using (var reader = await cmd.ExecuteReaderAsync())
        {
            Assert.That(() => reader.GetOrdinal("foo"), Throws.Exception.TypeOf<IndexOutOfRangeException>());
            Assert.That(reader.Read(), Is.False);
            Assert.That(() => reader.GetOrdinal("foo"), Throws.Exception.TypeOf<IndexOutOfRangeException>());
            Assert.That(reader.FieldCount, Is.EqualTo(0));
            Assert.That(reader.NextResult(), Is.False);
            Assert.That(() => reader.GetOrdinal("foo"), Throws.Exception.TypeOf<IndexOutOfRangeException>());
        }

        await using (var cmd = new SpannerCommand($"SELECT 1; {insert}", conn))
        await using (var reader = await cmd.ExecuteReaderAsync())
        {
            await reader.NextResultAsync();
            Assert.That(() => reader.GetOrdinal("foo"), Throws.Exception.TypeOf<InvalidOperationException>());
            Assert.That(reader.Read(), Is.False);
            Assert.That(() => reader.GetOrdinal("foo"), Throws.Exception.TypeOf<InvalidOperationException>());
            Assert.That(reader.FieldCount, Is.EqualTo(0));
        }
    }

    [Test]
    [SuppressMessage("ReSharper", "MethodHasAsyncOverload")]
    public async Task EmptyResultSet()
    {
        const string sql = "SELECT 1 AS foo WHERE FALSE";
        Fixture.SpannerMock.AddOrUpdateStatementResult(sql, StatementResult.CreateResultSet(
            [Tuple.Create(TypeCode.Int64, "foo")], []));
        
        await using var conn = await OpenConnectionAsync();
        await using var cmd = new SpannerCommand(sql, conn);
        await using var reader = await cmd.ExecuteReaderAsync();
        Assert.That(reader.Read(), Is.False);
        Assert.That(reader.FieldCount, Is.EqualTo(1));
        Assert.That(reader.GetOrdinal("foo"), Is.EqualTo(0));
        Assert.That(() => reader[0], Throws.Exception.TypeOf<InvalidOperationException>());
    }

    [Ignore("Requires multi-statement support")]
    [Test]
    [SuppressMessage("ReSharper", "MethodHasAsyncOverload")]
    public async Task FieldCount()
    {
        await using var conn = await OpenConnectionAsync();

        await using var cmd = new SpannerCommand("SELECT 1; SELECT 2,3", conn);
        await using (var reader = await cmd.ExecuteReaderAsync())
        {
            Assert.That(reader.FieldCount, Is.EqualTo(1));
            Assert.That(reader.Read(), Is.True);
            Assert.That(reader.FieldCount, Is.EqualTo(1));
            Assert.That(reader.Read(), Is.False);
            Assert.That(reader.FieldCount, Is.EqualTo(1));
            Assert.That(reader.NextResult(), Is.True);
            Assert.That(reader.FieldCount, Is.EqualTo(2));
            Assert.That(reader.NextResult(), Is.False);
            Assert.That(reader.FieldCount, Is.EqualTo(0));
        }

        cmd.CommandText = $"INSERT INTO my_table (int) VALUES (1)";
        await using (var reader = await cmd.ExecuteReaderAsync())
        {
            Assert.That(() => reader.FieldCount, Is.EqualTo(0));
        }
    }

    [Ignore("Requires multi-statement support")]
    [Test]
    [SuppressMessage("ReSharper", "MethodHasAsyncOverload")]
    public async Task RecordsAffected()
    {
        const int insertCount = 15;
        for (var i = 0; i < insertCount; i++)
        {
            Fixture.SpannerMock.AddOrUpdateStatementResult($"INSERT INTO my_table (int) VALUES ({i});", StatementResult.CreateUpdateCount(1));
        }
        
        await using var conn = await OpenConnectionAsync();

        var sb = new StringBuilder();
        for (var i = 0; i < 10; i++)
        {
            sb.Append($"INSERT INTO my_table (int) VALUES ({i});");
        }
        // Testing, that on close reader consumes all rows (as insert doesn't have a result set, but select does)
        sb.Append("SELECT 1;");
        for (var i = 10; i < 15; i++)
        {
            sb.Append($"INSERT INTO my_table (int) VALUES ({i});");
        }

        var cmd = new SpannerCommand(sb.ToString(), conn);
        var reader = await cmd.ExecuteReaderAsync();
        reader.Close();
        Assert.That(reader.RecordsAffected, Is.EqualTo(insertCount));

        const string select = "SELECT * FROM my_table";
        Fixture.SpannerMock.AddOrUpdateStatementResult(select, StatementResult.CreateResultSet(
            [Tuple.Create(TypeCode.Int64, "int")],
            [[1], [2], [3], [4], [5], [6], [7], [8], [9], [10], [11], [12], [13], [14], [15]]));
        cmd = new SpannerCommand(select, conn);
        reader = await cmd.ExecuteReaderAsync();
        reader.Close();
        Assert.That(reader.RecordsAffected, Is.EqualTo(-1));

        const string update = "UPDATE my_table SET int=int+1 WHERE int > 10";
        Fixture.SpannerMock.AddOrUpdateStatementResult(update, StatementResult.CreateUpdateCount(5));
        cmd = new SpannerCommand(update, conn);
        reader = await cmd.ExecuteReaderAsync();
        reader.Close();
        Assert.That(reader.RecordsAffected, Is.EqualTo(4));

        const string noUpdate = "UPDATE my_table SET int=8 WHERE int=666";
        Fixture.SpannerMock.AddOrUpdateStatementResult(noUpdate, StatementResult.CreateUpdateCount(0));
        cmd = new SpannerCommand(noUpdate, conn);
        reader = await cmd.ExecuteReaderAsync();
        reader.Close();
        Assert.That(reader.RecordsAffected, Is.EqualTo(0));

        const string delete = "DELETE FROM my_table WHERE int > 10";
        Fixture.SpannerMock.AddOrUpdateStatementResult(delete, StatementResult.CreateUpdateCount(4));
        cmd = new SpannerCommand(delete, conn);
        reader = await cmd.ExecuteReaderAsync();
        reader.Close();
        Assert.That(reader.RecordsAffected, Is.EqualTo(4));
    }
    
    [Test]
    [SuppressMessage("ReSharper", "MethodHasAsyncOverload")]
    public async Task GetStringWithParameter()
    {
        await using var conn = await OpenConnectionAsync();
        const string text = "Random text";
        const string sql = "SELECT name FROM my_table WHERE name = @value;";
        Fixture.SpannerMock.AddOrUpdateStatementResult(sql, StatementResult.CreateResultSet(
            [Tuple.Create(TypeCode.String, "name")], [[text]]));

        var command = new SpannerCommand(sql, conn);
        var param = new SpannerParameter
        {
            ParameterName = "value",
            DbType = DbType.String,
            Size = text.Length,
            Value = text
        };
        command.Parameters.Add(param);

        await using var dr = await command.ExecuteReaderAsync();
        dr.Read();
        var result = dr.GetString(0);
        Assert.That(result, Is.EqualTo(text));
    }

    [Test]
    [SuppressMessage("ReSharper", "UseAwaitUsing")]
    [SuppressMessage("ReSharper", "MethodHasAsyncOverload")]
    public async Task GetStringWithQuoteWithParameter()
    {
        const string test = "Text with ' single quote";
        const string sql = "SELECT name FROM my_table WHERE name = @value;";
        Fixture.SpannerMock.AddOrUpdateStatementResult(sql, StatementResult.CreateResultSet(
            [Tuple.Create(TypeCode.String, "name")], [[test]]));
        
        using var conn = await OpenConnectionAsync();
        var command = new SpannerCommand(sql, conn);

        var param = new SpannerParameter
        {
            ParameterName = "value",
            DbType = DbType.String,
            Size = test.Length,
            Value = test
        };
        command.Parameters.Add(param);

        using var dr = await command.ExecuteReaderAsync();
        dr.Read();
        var result = dr.GetString(0);
        Assert.That(result, Is.EqualTo(test));
    }

    [Test]
    [SuppressMessage("ReSharper", "UseAwaitUsing")]
    [SuppressMessage("ReSharper", "MethodHasAsyncOverload")]
    public async Task GetValueByName()
    {
        const string sql = "SELECT 'Random text' AS real_column";
        Fixture.SpannerMock.AddOrUpdateStatementResult(sql, StatementResult.CreateResultSet(
            [Tuple.Create(TypeCode.String, "real_column")], [["Random text"]]));
        
        await using var conn = await OpenConnectionAsync();
        using var command = new SpannerCommand(sql, conn);
        using var dr = await command.ExecuteReaderAsync();
        dr.Read();
        Assert.That(dr["real_column"], Is.EqualTo("Random text"));
        Assert.That(() => dr["non_existing"], Throws.Exception.TypeOf<IndexOutOfRangeException>());
    }
    
    [Test]
    [SuppressMessage("ReSharper", "UseAwaitUsing")]
    [SuppressMessage("ReSharper", "MethodHasAsyncOverload")]
    [SuppressMessage("ReSharper", "ConvertToUsingDeclaration")]
    public async Task GetFieldType()
    {
        const string sql = "SELECT cast(1 as int64) AS some_column";
        Fixture.SpannerMock.AddOrUpdateStatementResult(sql, StatementResult.CreateResultSet(
            [Tuple.Create(TypeCode.Int64, "some_column")], [[1L]]));
        
        using var conn = await OpenConnectionAsync();
        using (var cmd = new SpannerCommand(sql, conn))
        using (var reader = await cmd.ExecuteReaderAsync())
        {
            reader.Read();
            Assert.That(reader.GetFieldType(0), Is.SameAs(typeof(long)));
        }
    }
    
    [Test]
    [SuppressMessage("ReSharper", "MethodHasAsyncOverload")]
    public async Task GetFieldType_SchemaOnly()
    {
        const string sql = "SELECT cast(1 as int64) AS some_column";
        Fixture.SpannerMock.AddOrUpdateStatementResult(sql, StatementResult.CreateResultSet(
            [Tuple.Create(TypeCode.Int64, "some_column")], [[]]));
        
        await using var conn = await OpenConnectionAsync();
        await using var cmd = new SpannerCommand(sql, conn);
        await using var reader = await cmd.ExecuteReaderAsync(CommandBehavior.SchemaOnly);
        Assert.False(reader.Read());
        Assert.That(reader.GetFieldType(0), Is.SameAs(typeof(long)));

        var request = Fixture.SpannerMock.Requests.OfType<ExecuteSqlRequest>().First();
        Assert.That(request, Is.Not.Null);
        Assert.That(request.QueryMode, Is.EqualTo(ExecuteSqlRequest.Types.QueryMode.Plan));
    }
    
    [SuppressMessage("ReSharper", "UseAwaitUsing")]
    [SuppressMessage("ReSharper", "MethodHasAsyncOverload")]
    [Test]
    public async Task GetDataTypeName([Values] TypeCode typeCode)
    {
        if (typeCode == TypeCode.Array || typeCode == TypeCode.Unspecified)
        {
            return;
        }
        
        var sql = $"SELECT cast(NULL as {typeCode.ToString()} AS some_column";
        Fixture.SpannerMock.AddOrUpdateStatementResult(sql, StatementResult.CreateResultSet(
            [Tuple.Create(typeCode, "some_column")], []));
        
        using var conn = await OpenConnectionAsync();
        using var cmd = new SpannerCommand(sql, conn);
        using var reader = await cmd.ExecuteReaderAsync();
        reader.Read();
        Assert.That(reader.GetDataTypeName(0), Is.EqualTo(typeCode.ToString().ToUpper()));
    }
    
    [Test]
    [SuppressMessage("ReSharper", "UseAwaitUsing")]
    public async Task GetName()
    {
        const string sql = "SELECT 1 AS some_column";
        Fixture.SpannerMock.AddOrUpdateStatementResult(sql, StatementResult.CreateResultSet(
            [Tuple.Create(TypeCode.Int64, "some_column")], [[1L]]));
        
        using var conn = await OpenConnectionAsync();
        using var command = new SpannerCommand(sql, conn);
        using var dr = await command.ExecuteReaderAsync();
        await dr.ReadAsync();
        Assert.That(dr.GetName(0), Is.EqualTo("some_column"));
    }

    [Test]
    [SuppressMessage("ReSharper", "UseAwaitUsing")]
    [SuppressMessage("ReSharper", "MethodHasAsyncOverload")]
    public async Task GetFieldValueAsObject()
    {
        const string sql = "SELECT 'foo'";
        Fixture.SpannerMock.AddOrUpdateStatementResult(sql, StatementResult.CreateResultSet(
            [Tuple.Create(TypeCode.String, "c")], [["foo"]]));
        
        using var conn = await OpenConnectionAsync();
        using var cmd = new SpannerCommand(sql, conn);
        using var reader = await cmd.ExecuteReaderAsync();
        reader.Read();
        Assert.That(reader.GetFieldValue<object>(0), Is.EqualTo("foo"));
    }

    [Test]
    public async Task GetValues()
    {
        const string sql = "SELECT 'hello', 1, cast('2014-01-01' as DATE)";
        Fixture.SpannerMock.AddOrUpdateStatementResult(sql, StatementResult.CreateResultSet(
            [
                Tuple.Create(TypeCode.String, "c1"),
                Tuple.Create(TypeCode.Int64, "c2"),
                Tuple.Create(TypeCode.Date, "c3"),
            ], [["hello", 1L, new DateOnly(2014, 1, 1)]]));
        
        await using var conn = await OpenConnectionAsync();
        await using var command = new SpannerCommand(sql, conn);
        await using (var dr = await command.ExecuteReaderAsync())
        {
            await dr.ReadAsync();
            var values = new object[4];
            Assert.That(dr.GetValues(values), Is.EqualTo(3));
            Assert.That(values, Is.EqualTo(new object?[] { "hello", 1, new DateOnly(2014, 1, 1), null }));
        }
        await using (var dr = await command.ExecuteReaderAsync())
        {
            await dr.ReadAsync();
            var values = new object[2];
            Assert.That(dr.GetValues(values), Is.EqualTo(2));
            Assert.That(values, Is.EqualTo(new object[] { "hello", 1 }));
        }
    }

    [Test]
    [SuppressMessage("ReSharper", "UseAwaitUsing")]
    [SuppressMessage("ReSharper", "MethodHasAsyncOverload")]
    public async Task ExecuteReaderGettingEmptyResultSetWithOutputParameter()
    {
        const string sql = "SELECT * FROM my_table WHERE name = NULL;";
        Fixture.SpannerMock.AddOrUpdateStatementResult(sql, StatementResult.CreateResultSet(
            [Tuple.Create(TypeCode.String, "c")], []));
        
        using var conn = await OpenConnectionAsync();
        var command = new SpannerCommand(sql, conn);
        var param = new SpannerParameter("some_param", DbType.String)
        {
            Direction = ParameterDirection.Output
        };
        command.Parameters.Add(param);
        using var dr = await command.ExecuteReaderAsync();
        Assert.That(dr.NextResult(), Is.False);
    }
    
    [Test]
    public async Task GetValueFromEmptyResultSet()
    {
        const string sql = "SELECT * FROM my_table WHERE name = :value;";
        Fixture.SpannerMock.AddOrUpdateStatementResult(sql, StatementResult.CreateResultSet(
            [Tuple.Create(TypeCode.String, "name")], []));
        
        await using var conn = await OpenConnectionAsync();
        await using var command = new SpannerCommand(sql, conn);
        const string test = "Text single quote";
        var param = new SpannerParameter
        {
            ParameterName = "value",
            DbType = DbType.String,
            Size = test.Length,
            Value = test
        };
        command.Parameters.Add(param);

        await using var dr = await command.ExecuteReaderAsync();
        Assert.False(await dr.ReadAsync());
        // This line should throw the invalid operation exception as the data reader will
        // have an empty resultset.
        Assert.That(() => Console.WriteLine(dr.IsDBNull(0)),
            Throws.Exception.TypeOf<InvalidOperationException>());
    }

    [Test]
    [SuppressMessage("ReSharper", "UseAwaitUsing")]
    [SuppressMessage("ReSharper", "MethodHasAsyncOverload")]
    public async Task ReadPastReaderEnd()
    {
        using var conn = await OpenConnectionAsync();
        var command = new SpannerCommand("SELECT 1", conn);
        using var dr = await command.ExecuteReaderAsync();
        while (dr.Read()) {}
        Assert.That(() => dr[0], Throws.Exception.TypeOf<InvalidOperationException>());
    }

    [Ignore("Require multi-statement support")]
    [Test]
    [SuppressMessage("ReSharper", "UseAwaitUsing")]
    [SuppressMessage("ReSharper", "MethodHasAsyncOverload")]
    public async Task SingleResult()
    {
        await using var conn = await OpenConnectionAsync();
        await using var command = new SpannerCommand("SELECT 1; SELECT 2", conn);
        var reader = await command.ExecuteReaderAsync(CommandBehavior.SingleResult);
        Assert.That(reader.Read(), Is.True);
        Assert.That(reader.GetInt32(0), Is.EqualTo(1));
        Assert.That(reader.NextResult(), Is.False);
    }

    [Test]
    [SuppressMessage("ReSharper", "UseAwaitUsing")]
    [SuppressMessage("ReSharper", "MethodHasAsyncOverload")]
    public async Task Exception_thrown_from_ExecuteReaderAsync([Values(PrepareOrNot.Prepared, PrepareOrNot.NotPrepared)] PrepareOrNot prepare)
    {
        const string sql = "SELECT error('test')";
        
        using var conn = await OpenConnectionAsync();

        using var cmd = new SpannerCommand(sql, conn);
        if (prepare == PrepareOrNot.Prepared)
        {
            Fixture.SpannerMock.AddOrUpdateStatementResult(sql, StatementResult.CreateResultSet(
                [Tuple.Create(TypeCode.String, "error")], []));
            cmd.Prepare();
        }
        Fixture.SpannerMock.AddOrUpdateStatementResult(sql, StatementResult.CreateException(new RpcException(new Status(StatusCode.Internal, "test"))));
        Assert.That(async () => await cmd.ExecuteReaderAsync(), Throws.Exception.TypeOf<SpannerDbException>());
    }
    
    [Ignore("Require multi-statement support")]
    [Test]
    public async Task Exception_thrown_from_NextResult([Values(PrepareOrNot.Prepared, PrepareOrNot.NotPrepared)] PrepareOrNot prepare)
    {
        const string select1 = "SELECT 1";
        const string selectError = "SELECT error('test')";
        
        await using var conn = await OpenConnectionAsync();

        await using var cmd = new SpannerCommand($"{select1}; {selectError}", conn);
        if (prepare == PrepareOrNot.Prepared)
        {
            Fixture.SpannerMock.AddOrUpdateStatementResult(selectError, StatementResult.CreateResultSet(
                [Tuple.Create(TypeCode.String, "error")], []));
            await cmd.PrepareAsync();
        }

        Fixture.SpannerMock.AddOrUpdateStatementResult(selectError, StatementResult.CreateException(new RpcException(new Status(StatusCode.Internal, "test"))));
        await using var reader = await cmd.ExecuteReaderAsync();
        Assert.That(() => reader.NextResult(), Throws.Exception.TypeOf<SpannerDbException>());
    }
    
    [Test]
    public async Task SchemaOnlyReturnsNoData()
    {
        const string sql = "SELECT * FROM my_table";
        Fixture.SpannerMock.AddOrUpdateStatementResult(sql, StatementResult.CreateResultSet(
            [Tuple.Create(TypeCode.String, "c")], []));
        
        await using var conn = await OpenConnectionAsync();
        await using var cmd = new SpannerCommand(sql, conn);
        await using var reader = await cmd.ExecuteReaderAsync(CommandBehavior.SchemaOnly);
        Assert.That(await reader.ReadAsync(), Is.False);

        var request = Fixture.SpannerMock.Requests.OfType<ExecuteSqlRequest>().First();
        Assert.That(request, Is.Not.Null);
        Assert.That(request.QueryMode, Is.EqualTo(ExecuteSqlRequest.Types.QueryMode.Plan));
    }

    [Test]
    public async Task SchemaOnlyNextResultBeyondEnd()
    {
        const string sql = "SELECT * FROM my_table";
        Fixture.SpannerMock.AddOrUpdateStatementResult(sql, StatementResult.CreateResultSet(
            [Tuple.Create(TypeCode.Int64, "id")], []));
        
        await using var conn = await OpenConnectionAsync();

        await using var cmd = new SpannerCommand(sql, conn);
        await using var reader = await cmd.ExecuteReaderAsync(CommandBehavior.SchemaOnly);
        Assert.That(await reader.NextResultAsync(), Is.False);
        Assert.That(await reader.NextResultAsync(), Is.False);
    }

    [Test]
    [SuppressMessage("ReSharper", "UseAwaitUsing")]
    public async Task GetOrdinal()
    {
        const string sql = "SELECT 0, 1 AS some_column WHERE 1=0";
        Fixture.SpannerMock.AddOrUpdateStatementResult(sql, StatementResult.CreateResultSet(
            [Tuple.Create(TypeCode.Int64, "c"), Tuple.Create(TypeCode.Int64, "some_column")], []));
        
        using var conn = await OpenConnectionAsync();
        using var command = new SpannerCommand(sql, conn);
        using var reader = await command.ExecuteReaderAsync();
        Assert.That(reader.GetOrdinal("some_column"), Is.EqualTo(1));
        Assert.That(() => reader.GetOrdinal("doesn't_exist"), Throws.Exception.TypeOf<IndexOutOfRangeException>());
    }

    [Test]
    [SuppressMessage("ReSharper", "UseAwaitUsing")]
    public async Task GetOrdinalCaseInsensitive()
    {
        const string sql = "select 123 as FIELD1";
        Fixture.SpannerMock.AddOrUpdateStatementResult(sql, StatementResult.CreateResultSet(
            [Tuple.Create(TypeCode.Int64, "FIELD1")], [[123L]]));
        
        using var conn = await OpenConnectionAsync();
        using var command = new SpannerCommand(sql, conn);
        using var reader = await command.ExecuteReaderAsync();
        await reader.ReadAsync();
        Assert.That(reader.GetOrdinal("fieLd1"), Is.EqualTo(0));
    }
    
    [Test]
    public async Task FieldIndexDoesNotExist()
    {
        await using var conn = await OpenConnectionAsync();
        await using var command = new SpannerCommand("SELECT 1", conn);
        await using var dr = await command.ExecuteReaderAsync();
        await dr.ReadAsync();
        Assert.That(() => dr[5], Throws.Exception.TypeOf<IndexOutOfRangeException>());
    }
    
    [Test]
    [SuppressMessage("ReSharper", "MethodHasAsyncOverload")]
    public async Task ReaderStillOpen_CanExecuteMoreCommands()
    {
        await using var conn = await OpenConnectionAsync();
        await using var cmd1 = new SpannerCommand("SELECT 1", conn);
        await using var reader1 = await cmd1.ExecuteReaderAsync();
        Assert.That(conn.ExecuteNonQuery("SELECT 1"), Is.EqualTo(-1));
        Assert.That(await conn.ExecuteNonQueryAsync("SELECT 1"), Is.EqualTo(-1));
        Assert.That(conn.ExecuteScalar("SELECT 1"), Is.EqualTo(1));
        Assert.That(await conn.ExecuteScalarAsync("SELECT 1"), Is.EqualTo(1));
    }

    [Test]
    [SuppressMessage("ReSharper", "MethodHasAsyncOverload")]
    public async Task CleansUpOkWithDisposeCalls([Values(PrepareOrNot.Prepared, PrepareOrNot.NotPrepared)] PrepareOrNot prepare)
    {
        await using var conn = await OpenConnectionAsync();
        await using var command = new SpannerCommand("SELECT 1", conn);
        await using var dr = await command.ExecuteReaderAsync();
        await dr.ReadAsync();
        dr.Close();

        await using var upd = conn.CreateCommand();
        upd.CommandText = "SELECT 1";
        if (prepare == PrepareOrNot.Prepared)
        {
            upd.Prepare();
        }
    }

    [Test]
    [SuppressMessage("ReSharper", "MethodHasAsyncOverload")]
    [SuppressMessage("ReSharper", "UseAwaitUsing")]
    public async Task Null()
    {
        const string sql = "SELECT @p1, cast(@p2 as string)";
        Fixture.SpannerMock.AddOrUpdateStatementResult(sql, StatementResult.CreateResultSet(
            [Tuple.Create(TypeCode.String, "p1"), Tuple.Create(TypeCode.String, "p2")], [[DBNull.Value, DBNull.Value]]));
        
        using var conn = await OpenConnectionAsync();
        using var cmd = new SpannerCommand(sql, conn);
        cmd.Parameters.Add(new SpannerParameter("p1", DbType.String) { Value = DBNull.Value });
        cmd.Parameters.Add(new SpannerParameter { ParameterName = "p2", Value = DBNull.Value });

        using var reader = await cmd.ExecuteReaderAsync();
        reader.Read();

        for (var i = 0; i < cmd.Parameters.Count; i++)
        {
            Assert.That(reader.IsDBNull(i), Is.True);
            Assert.That(reader.IsDBNullAsync(i).Result, Is.True);
            Assert.That(reader.GetValue(i), Is.EqualTo(DBNull.Value));
            Assert.That(reader.GetFieldValue<object>(i), Is.EqualTo(DBNull.Value));
            Assert.That(reader.GetProviderSpecificValue(i), Is.EqualTo(DBNull.Value));
            Assert.That(() => reader.GetString(i), Throws.Exception.TypeOf<InvalidCastException>());
        }
    }
    
    [Ignore("Requires multi-statement support")]
    [Test]
    [SuppressMessage("ReSharper", "MethodHasAsyncOverload")]
    [SuppressMessage("ReSharper", "UseAwaitUsing")]
    public async Task HasRows([Values(PrepareOrNot.NotPrepared, PrepareOrNot.Prepared)] PrepareOrNot prepare)
    {
        using var conn = await OpenConnectionAsync();

        var command = new SpannerCommand($"SELECT 1; SELECT * FROM my_table WHERE name='does_not_exist'", conn);
        if (prepare == PrepareOrNot.Prepared)
        {
            command.Prepare();
        }
        using (var reader = await command.ExecuteReaderAsync())
        {
            Assert.That(reader.HasRows, Is.True);
            Assert.That(reader.HasRows, Is.True);
            Assert.That(reader.Read(), Is.True);
            Assert.That(reader.HasRows, Is.True);
            Assert.That(reader.Read(), Is.False);
            Assert.That(reader.HasRows, Is.True);
            await reader.NextResultAsync();
            Assert.That(reader.HasRows, Is.False);
        }

        command.CommandText = "SELECT * FROM my_table";
        if (prepare == PrepareOrNot.Prepared)
        {
            command.Prepare();
        }
        using (var reader = await command.ExecuteReaderAsync())
        {
            reader.Read();
            Assert.That(reader.HasRows, Is.False);
        }

        command.CommandText = "SELECT 1";
        if (prepare == PrepareOrNot.Prepared)
        {
            command.Prepare();
        }
        using (var reader = await command.ExecuteReaderAsync())
        {
            reader.Read();
            reader.Close();
            Assert.Throws<InvalidOperationException>(() => _ = reader.HasRows);
        }

        command.CommandText = $"INSERT INTO my_table (name) VALUES ('foo'); SELECT * FROM my_table";
        if (prepare == PrepareOrNot.Prepared)
        {
            command.Prepare();
        }
        using (var reader = await command.ExecuteReaderAsync())
        {
            Assert.That(reader.HasRows, Is.True);
            reader.Read();
            Assert.That(reader.GetString(0), Is.EqualTo("foo"));
        }

        Assert.That(await conn.ExecuteScalarAsync("SELECT 1"), Is.EqualTo(1));
    }
    
    [Test]
    [SuppressMessage("ReSharper", "MethodHasAsyncOverload")]
    [SuppressMessage("ReSharper", "UseAwaitUsing")]
    public async Task HasRowsSingleStatement([Values(PrepareOrNot.NotPrepared, PrepareOrNot.Prepared)] PrepareOrNot prepare)
    {
        const string selectNoRows = "SELECT * FROM my_table WHERE name='does_not_exist'";
        Fixture.SpannerMock.AddOrUpdateStatementResult(selectNoRows, StatementResult.CreateResultSet(
            [Tuple.Create(TypeCode.String, "name")], []));
        
        using var conn = await OpenConnectionAsync();

        var command = new SpannerCommand("SELECT 1", conn);
        if (prepare == PrepareOrNot.Prepared)
        {
            command.Prepare();
        }
        using (var reader = await command.ExecuteReaderAsync())
        {
            Assert.That(reader.HasRows, Is.True);
            Assert.That(reader.HasRows, Is.True);
            Assert.That(reader.Read(), Is.True);
            Assert.That(reader.HasRows, Is.True);
            Assert.That(reader.Read(), Is.False);
            Assert.That(reader.HasRows, Is.True);
            Assert.False(await reader.NextResultAsync());
        }

        command.CommandText = selectNoRows;
        if (prepare == PrepareOrNot.Prepared)
        {
            command.Prepare();
        }
        using (var reader = await command.ExecuteReaderAsync())
        {
            Assert.That(reader.HasRows, Is.False);
            Assert.That(reader.HasRows, Is.False);
            Assert.That(reader.Read(), Is.False);
            Assert.That(reader.HasRows, Is.False);
            Assert.That(reader.Read(), Is.False);
            Assert.That(reader.HasRows, Is.False);
            Assert.False(await reader.NextResultAsync());
        }

        command.CommandText = "SELECT 1";
        if (prepare == PrepareOrNot.Prepared)
        {
            command.Prepare();
        }
        using (var reader = await command.ExecuteReaderAsync())
        {
            reader.Read();
            reader.Close();
            Assert.Throws<InvalidOperationException>(() => _ = reader.HasRows);
        }

        const string insertRow = "INSERT INTO my_table (name) VALUES ('foo')";
        Fixture.SpannerMock.AddOrUpdateStatementResult(insertRow, StatementResult.CreateUpdateCount(1L));
        command.CommandText = insertRow;
        if (prepare == PrepareOrNot.Prepared)
        {
            command.Prepare();
        }
        using (var reader = await command.ExecuteReaderAsync())
        {
            Assert.That(reader.HasRows, Is.False);
            Assert.False(reader.Read());
        }
        
        const string selectRow = "SELECT * FROM my_table";
        Fixture.SpannerMock.AddOrUpdateStatementResult(selectRow, StatementResult.CreateResultSet(
            [Tuple.Create(TypeCode.String, "name")], [["foo"]]));
        command.CommandText = selectRow;
        if (prepare == PrepareOrNot.Prepared)
        {
            command.Prepare();
        }
        using (var reader = await command.ExecuteReaderAsync())
        {
            Assert.That(reader.HasRows, Is.True);
            Assert.True(reader.Read());
            Assert.That(reader.GetString(0), Is.EqualTo("foo"));
        }
        Assert.That(await conn.ExecuteScalarAsync("SELECT 1"), Is.EqualTo(1));
    }

    [Test]
    public async Task HasRowsWithoutResultSet()
    {
        const string sql = "DELETE FROM my_table WHERE name = 'unknown'";
        Fixture.SpannerMock.AddOrUpdateStatementResult(sql, StatementResult.CreateUpdateCount(0L));
        
        await using var conn = await OpenConnectionAsync();
        await using var command = new SpannerCommand(sql, conn);
        await using var reader = await command.ExecuteReaderAsync();
        Assert.That(reader.HasRows, Is.False);
    }

    [Test]
    public async Task IntervalAsTimeSpan()
    {
        const string sql = "SELECT CAST('1 hour' AS interval) AS dauer";
        Fixture.SpannerMock.AddOrUpdateStatementResult(sql, StatementResult.CreateResultSet(
            [Tuple.Create(TypeCode.Interval, "dauer")], [["PT1H"]]));
        
        await using var conn = await OpenConnectionAsync();
        await using var command = new SpannerCommand(sql, conn);
        await using var dr = await command.ExecuteReaderAsync() as SpannerDataReader;
        Assert.That(dr!.HasRows);
        Assert.That(await dr.ReadAsync());
        Assert.That(dr.HasRows);
        var ts = dr.GetTimeSpan(0);
        Assert.That(ts, Is.EqualTo(TimeSpan.FromHours(1)));
    }

    [Test]
    [SuppressMessage("ReSharper", "UseAwaitUsing")]
    [SuppressMessage("ReSharper", "MethodHasAsyncOverload")]
    public async Task CloseConnectionInMiddleOfRow()
    {
        const string sql = "SELECT 1, 2";
        Fixture.SpannerMock.AddOrUpdateStatementResult(sql, StatementResult.CreateResultSet(
            [Tuple.Create(TypeCode.Int64, "c"), Tuple.Create(TypeCode.Int64, "c")], [[1L, 2L]]));
        
        using var conn = await OpenConnectionAsync();
        using var cmd = new SpannerCommand(sql, conn);
        using var reader = await cmd.ExecuteReaderAsync();
        reader.Read();
    }

    [Test]
    [SuppressMessage("ReSharper", "UseAwaitUsing")]
    [SuppressMessage("ReSharper", "MethodHasAsyncOverload")]
    public async Task InvalidCast()
    {
        const string sql = "SELECT 'foo'";
        Fixture.SpannerMock.AddOrUpdateStatementResult(sql, StatementResult.CreateResultSet(
            [Tuple.Create(TypeCode.String, "c")], [["foo"]]));
        
        using var conn = await OpenConnectionAsync();
        using (var cmd = new SpannerCommand(sql, conn))
        using (var reader = await cmd.ExecuteReaderAsync())
        {
            reader.Read();
            Assert.That(() => reader.GetInt32(0), Throws.Exception.TypeOf<InvalidCastException>());
        }
        
        using (var cmd = new SpannerCommand("SELECT 1", conn))
        using (var reader = await cmd.ExecuteReaderAsync())
        {
            reader.Read();
            Assert.That(() => reader.GetDateTime(0), Throws.Exception.TypeOf<InvalidCastException>());
        }
        Assert.That(await conn.ExecuteScalarAsync("SELECT 1"), Is.EqualTo(1));
    }

    [Test]
    public async Task NullableScalar()
    {
        const string sql = "SELECT @p1, @p2";
        Fixture.SpannerMock.AddOrUpdateStatementResult(sql, StatementResult.CreateResultSet(
            [Tuple.Create(TypeCode.Int64, "p1"), Tuple.Create(TypeCode.Int64, "p2")], [[DBNull.Value, 8L]]));
        
        await using var conn = await OpenConnectionAsync();
        await using var cmd = new SpannerCommand(sql, conn);
        var p1 = new SpannerParameter { ParameterName = "p1", Value = DBNull.Value, DbType = DbType.Int16 };
        var p2 = new SpannerParameter { ParameterName = "p2", Value = (short)8 };
        Assert.That(p1.DbType, Is.EqualTo(DbType.Int16));
        Assert.That(p2.DbType, Is.EqualTo(DbType.String)); // This is the ADO.NET default
        cmd.Parameters.Add(p1);
        cmd.Parameters.Add(p2);
        await using var reader = await cmd.ExecuteReaderAsync();
        await reader.ReadAsync();

        for (var i = 0; i < cmd.Parameters.Count; i++)
        {
            Assert.That(reader.GetFieldType(i), Is.EqualTo(typeof(long)));
            Assert.That(reader.GetDataTypeName(i), Is.EqualTo("INT64"));
        }

        Assert.That(() => reader.GetFieldValue<object>(0), Is.EqualTo(DBNull.Value));
        Assert.That(() => reader.GetFieldValue<int>(0), Throws.TypeOf<InvalidCastException>());
        Assert.That(() => reader.GetFieldValue<int?>(0), Throws.Nothing);
        Assert.That(reader.GetFieldValue<int?>(0), Is.Null);

        Assert.That(() => reader.GetFieldValue<object>(1), Throws.Nothing);
        Assert.That(() => reader.GetFieldValue<int>(1), Throws.Nothing);
        Assert.That(() => reader.GetFieldValue<int?>(1), Throws.Nothing);
        Assert.That(reader.GetFieldValue<object>(1), Is.EqualTo(8));
        Assert.That(reader.GetFieldValue<int>(1), Is.EqualTo(8));
        Assert.That(reader.GetFieldValue<int?>(1), Is.EqualTo(8));
    }
    
    [Test]
    public async Task ReaderCloseAndDispose()
    {
        await using var conn = await OpenConnectionAsync();
        await using var cmd1 = conn.CreateCommand();
        cmd1.CommandText = "SELECT 1";

        var reader1 = await cmd1.ExecuteReaderAsync(CommandBehavior.CloseConnection);
        await reader1.CloseAsync();

        await conn.OpenAsync();
        cmd1.Connection = conn;
        var reader2 = await cmd1.ExecuteReaderAsync(CommandBehavior.CloseConnection);
        Assert.That(reader1, Is.Not.SameAs(reader2));
        Assert.Throws<InvalidOperationException>(() => _ = reader2.GetInt64(0));

        await reader1.DisposeAsync();

        Assert.Throws<InvalidOperationException>(() => _ = reader2.GetInt64(0));
    }
    
    [Test]
    public async Task ConnectionCloseAndReaderDispose()
    {
        await using var conn = await OpenConnectionAsync();
        await using var cmd1 = conn.CreateCommand();
        cmd1.CommandText = "SELECT 1";

        var reader1 = await cmd1.ExecuteReaderAsync();
        await conn.CloseAsync();
        await conn.OpenAsync();

        var reader2 = await cmd1.ExecuteReaderAsync();
        Assert.That(reader1, Is.Not.SameAs(reader2));
        Assert.Throws<InvalidOperationException>(() => _ = reader2.GetInt64(0));

        await reader1.DisposeAsync();

        Assert.Throws<InvalidOperationException>(() => _ = reader2.GetInt64(0));
    }
    
    [Test]
    public async Task UnboundReaderReuse()
    {
        Fixture.SpannerMock.AddOrUpdateStatementResult("SELECT 2", StatementResult.CreateResultSet(
            [Tuple.Create(TypeCode.Int64, "c")], [[2L]]));
        Fixture.SpannerMock.AddOrUpdateStatementResult("SELECT 3", StatementResult.CreateResultSet(
            [Tuple.Create(TypeCode.Int64, "c")], [[3L]]));
        
        await using var dataSource = CreateDataSource(csb => { });
        await using var conn1 = await dataSource.OpenConnectionAsync();
        await using var cmd1 = conn1.CreateCommand();
        cmd1.CommandText = "SELECT 1";
        var reader1 = await cmd1.ExecuteReaderAsync();
        await using (var __ = reader1)
        {
            Assert.That(async () => await reader1.ReadAsync(), Is.EqualTo(true));
            Assert.That(() => reader1.GetInt32(0), Is.EqualTo(1));

            await reader1.CloseAsync();
            await conn1.CloseAsync();
        }

        await using var conn2 = await dataSource.OpenConnectionAsync();
        await using var cmd2 = conn2.CreateCommand();
        cmd2.CommandText = "SELECT 2";
        var reader2 = await cmd2.ExecuteReaderAsync();
        await using (var __ = reader2)
        {
            Assert.That(async () => await reader2.ReadAsync(), Is.EqualTo(true));
            Assert.That(() => reader2.GetInt32(0), Is.EqualTo(2));
            Assert.That(reader1, Is.Not.SameAs(reader2));

            await reader2.CloseAsync();
            await conn2.CloseAsync();
        }

        await using var conn3 = await dataSource.OpenConnectionAsync();
        await using var cmd3 = conn3.CreateCommand();
        cmd3.CommandText = "SELECT 3";
        var reader3 = await cmd3.ExecuteReaderAsync();
        await using (var __ = reader3)
        {
            Assert.That(async () => await reader3.ReadAsync(), Is.EqualTo(true));
            Assert.That(() => reader3.GetInt32(0), Is.EqualTo(3));
            Assert.That(reader1, Is.Not.SameAs(reader3));

            await reader3.CloseAsync();
            await conn3.CloseAsync();
        }
    }

    [Test]
    public async Task ReadStringAsChar()
    {
        const string sql = "SELECT 'abcdefgh', 'ijklmnop'";
        Fixture.SpannerMock.AddOrUpdateStatementResult(sql, StatementResult.CreateResultSet(
            [Tuple.Create(TypeCode.String, "c1"), Tuple.Create(TypeCode.String, "c2")], [["abcdefgh", "ijklmnop"]]));
        
        await using var conn = await OpenConnectionAsync();
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;

        await using var reader = await cmd.ExecuteReaderAsync();
        Assert.That(await reader.ReadAsync());
        Assert.That(reader.GetChar(0), Is.EqualTo('a'));
        Assert.That(reader.GetChar(0), Is.EqualTo('a'));
        Assert.That(reader.GetChar(1), Is.EqualTo('i'));
    }
    
    [Test]
    public async Task GetBytes()
    {
        byte[] expected = [1, 2, 3, 4, 5];
        var base64 = Convert.ToBase64String(expected);
        const string query = "SELECT bytes, 'foo', bytes, 'bar', bytes, bytes FROM my_table";
        Fixture.SpannerMock.AddOrUpdateStatementResult(query, StatementResult.CreateResultSet(
            [
                Tuple.Create(TypeCode.Bytes, "bytes"),
                Tuple.Create(TypeCode.String, "foo"),
                Tuple.Create(TypeCode.Bytes, "bytes"),
                Tuple.Create(TypeCode.String, "bar"),
                Tuple.Create(TypeCode.Bytes, "bytes"),
                Tuple.Create(TypeCode.Bytes, "bytes"),
            ], [[
                base64,
                "foo",
                base64,
                "bar",
                base64,
                base64,
            ]]));
        
        await using var conn = await OpenConnectionAsync();
        var actual = new byte[expected.Length];

        await using var cmd = new SpannerCommand(query, conn);
        await using var reader = await cmd.ExecuteReaderAsync();
        await reader.ReadAsync();

        Assert.That(reader.GetBytes(0, 0, actual, 0, 2), Is.EqualTo(2));
        Assert.That(actual[0], Is.EqualTo(expected[0]));
        Assert.That(actual[1], Is.EqualTo(expected[1]));
        Assert.That(reader.GetBytes(0, 0, null, 0, 0), Is.EqualTo(expected.Length), "Bad column length");
        
        Assert.That(reader.GetBytes(0, 0, actual, 4, 1), Is.EqualTo(1));
        Assert.That(actual[4], Is.EqualTo(expected[0]));
        
        Assert.That(reader.GetBytes(0, 2, actual, 2, 3), Is.EqualTo(3));
        Assert.That(actual, Is.EqualTo(expected));
        Assert.That(reader.GetBytes(0, 0, null, 0, 0), Is.EqualTo(expected.Length), "Bad column length");

        Assert.That(reader.GetString(1), Is.EqualTo("foo"));
        reader.GetBytes(2, 0, actual, 0, 2);
        
        // Jump to another column from the middle of the column
        reader.GetBytes(4, 0, actual, 0, 2);
        Assert.That(reader.GetBytes(4, expected.Length - 1, actual, 0, 2), Is.EqualTo(1),
            "Length greater than data length");
        Assert.That(actual[0], Is.EqualTo(expected[^1]), "Length greater than data length");
        Assert.That(() => reader.GetBytes(4, 0, actual, 0, actual.Length + 1),
            Throws.Exception.TypeOf<IndexOutOfRangeException>(), "Length great than output buffer length");
        // Close in the middle of a column
        reader.GetBytes(5, 0, actual, 0, 2);

        var result = (byte[]) cmd.ExecuteScalar()!;
        Assert.That(result.Length, Is.EqualTo(5));
    }
    
    [Test]
    public async Task GetStreamSecondTimeWorks()
    {
        var expected = new byte[] { 1, 2, 3, 4, 5, 6, 7, 8 };
        var base64 = Convert.ToBase64String(expected);
        var sql = $"SELECT from_base64({base64})";
        Fixture.SpannerMock.AddOrUpdateStatementResult(sql, StatementResult.CreateResultSet(
            [Tuple.Create(TypeCode.Bytes, "from_base64")], [[base64]]));

        await using var conn = await OpenConnectionAsync();
        await using var cmd = new SpannerCommand(sql, conn);
        await using var reader = await cmd.ExecuteReaderAsync();

        await reader.ReadAsync();
        Assert.That(reader.GetStream(0), Is.Not.Null);
        Assert.That(reader.GetStream(0), Is.Not.Null);
    }

    public static IEnumerable GetStreamCases()
    {
        var binary = MemoryMarshal
            .AsBytes<int>(Enumerable.Range(0, 1024).ToArray())
            .ToArray();
        yield return (binary, binary);

        var bigBinary = MemoryMarshal
            .AsBytes<int>(Enumerable.Range(0, 8193).ToArray())
            .ToArray();
        yield return (bigBinary, bigBinary);

        var bigint = 0xDEADBEEFL;
        var bigintBinary = BitConverter.GetBytes(
            BitConverter.IsLittleEndian
                ? BinaryPrimitives.ReverseEndianness(bigint)
                : bigint);
        yield return (bigint, bigintBinary);
    }

    [Test]
    [SuppressMessage("ReSharper", "MethodHasAsyncOverload")]
    [SuppressMessage("ReSharper", "UseAwaitUsing")]
    public async Task GetStream<T>(
        [Values] bool isAsync,
        [ValueSource(nameof(GetStreamCases))] (T Generic, byte[] Binary) value)
    {
        const string sql = "SELECT @p, @p";
        var base64 = Convert.ToBase64String(value.Binary);
        Fixture.SpannerMock.AddOrUpdateStatementResult(sql, StatementResult.CreateResultSet(
            [Tuple.Create(TypeCode.Bytes, "p"), Tuple.Create(TypeCode.Bytes, "p")], [[base64, base64]]));
        
        var expected = value.Binary;
        var actual = new byte[expected.Length];

        using var conn = await OpenConnectionAsync();
        using var cmd = new SpannerCommand(sql, conn);
        cmd.Parameters.Add(new SpannerParameter("p", value.Generic));
        using var reader = await cmd.ExecuteReaderAsync();

        await reader.ReadAsync();

        using var stream = reader.GetStream(0);
        Assert.That(stream.Length, Is.EqualTo(expected.Length));

        var position = 0;
        while (position < actual.Length)
        {
            if (isAsync)
            {
                position += await stream.ReadAsync(actual, position, actual.Length - position);
            }
            else
            {
                position += stream.Read(actual, position, actual.Length - position);
            }
        }
        Assert.That(actual, Is.EqualTo(expected));
    }

    [Test]
    [SuppressMessage("ReSharper", "MethodHasAsyncOverload")]
    [SuppressMessage("ReSharper", "UseAwaitUsing")]
    public async Task OpenStreamWhenChangingColumns([Values(true, false)] bool isAsync)
    {
        var data = new byte[] { 1, 2, 3 };
        var base64 = Convert.ToBase64String(data);
        const string sql = "SELECT @p, @p";
        Fixture.SpannerMock.AddOrUpdateStatementResult(sql, StatementResult.CreateResultSet(
            [Tuple.Create(TypeCode.Bytes, "p"), Tuple.Create(TypeCode.Bytes, "p")], [[base64, base64]]));
        
        using var conn = await OpenConnectionAsync();
        using var cmd = new SpannerCommand(sql, conn);
        cmd.Parameters.Add(new SpannerParameter("p", data));
        using var reader = await cmd.ExecuteReaderAsync();
        reader.Read();
        var stream = reader.GetStream(0);
        _ = reader.GetValue(1);
        Assert.That(() => stream.ReadByte(), Throws.Nothing);
    }

    [Test]
    [SuppressMessage("ReSharper", "MethodHasAsyncOverload")]
    [SuppressMessage("ReSharper", "UseAwaitUsing")]
    public async Task OpenStreamWhenChangingRows([Values(true, false)] bool isAsync)
    {
        var data = new byte[] { 1, 2, 3 };
        var base64 = Convert.ToBase64String(data);
        const string sql = "SELECT @p";
        Fixture.SpannerMock.AddOrUpdateStatementResult(sql, StatementResult.CreateResultSet(
            [Tuple.Create(TypeCode.Bytes, "p")], [[base64]]));
        
        using var conn = await OpenConnectionAsync();
        using var cmd = new SpannerCommand(sql, conn);
        cmd.Parameters.Add(new SpannerParameter("p", data));
        using var reader = await cmd.ExecuteReaderAsync();
        reader.Read();
        var s1 = reader.GetStream(0);
        reader.Read();
        Assert.That(() => s1.ReadByte(), Throws.Nothing);
    }

    [Test]
    [SuppressMessage("ReSharper", "MethodHasAsyncOverload")]
    [SuppressMessage("ReSharper", "UseAwaitUsing")]
    public async Task GetBytesWithNull([Values(true, false)] bool isAsync)
    {
        const string sql = "SELECT bytes FROM my_table";
        Fixture.SpannerMock.AddOrUpdateStatementResult(sql, StatementResult.CreateResultSet(
            [Tuple.Create(TypeCode.Bytes, "p")], [[DBNull.Value]]));
        
        using var conn = await OpenConnectionAsync();
        var buf = new byte[8];
        using var cmd = new SpannerCommand(sql, conn);
        using var reader = await cmd.ExecuteReaderAsync();
        reader.Read();
        Assert.That(reader.IsDBNull(0), Is.True);
        Assert.That(() => reader.GetBytes(0, 0, buf, 0, 1), Throws.Exception.TypeOf<InvalidCastException>(), "GetBytes");
        Assert.That(() => reader.GetStream(0), Throws.Exception.TypeOf<InvalidCastException>(), "GetStream");
        Assert.That(() => reader.GetBytes(0, 0, null, 0, 0), Throws.Exception.TypeOf<InvalidCastException>(), "GetBytes with null buffer");
    }
    
    [Test]
    public async Task GetStreamSeek()
    {
        const string sql = "SELECT 'abcdefgh'";
        Fixture.SpannerMock.AddOrUpdateStatementResult(sql, StatementResult.CreateResultSet(
            [Tuple.Create(TypeCode.String, "c")], [["abcdefgh"]]));
        
        await using var conn = await OpenConnectionAsync();
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        await using var reader = await cmd.ExecuteReaderAsync();
        await reader.ReadAsync();

        var buffer = new byte[4];

        await using var stream = reader.GetStream(0);
        Assert.That(stream.CanSeek);

        var seekPosition = stream.Seek(-1, SeekOrigin.End);
        Assert.That(seekPosition, Is.EqualTo(stream.Length - 1));
        var read = stream.Read(buffer);
        Assert.That(read, Is.EqualTo(1));
        Assert.That(Encoding.ASCII.GetString(buffer, 0, 1), Is.EqualTo("h"));
        read = stream.Read(buffer);
        Assert.That(read, Is.EqualTo(0));

        seekPosition = stream.Seek(2, SeekOrigin.Begin);
        Assert.That(seekPosition, Is.EqualTo(2));
        read = stream.Read(buffer);
        Assert.That(read, Is.EqualTo(buffer.Length));
        Assert.That(Encoding.ASCII.GetString(buffer), Is.EqualTo("cdef"));

        seekPosition = stream.Seek(-3, SeekOrigin.Current);
        Assert.That(seekPosition, Is.EqualTo(3));
        read = stream.Read(buffer);
        Assert.That(read, Is.EqualTo(buffer.Length));
        Assert.That(Encoding.ASCII.GetString(buffer), Is.EqualTo("defg"));

        stream.Position = 1;
        read = stream.Read(buffer);
        Assert.That(read, Is.EqualTo(buffer.Length));
        Assert.That(Encoding.ASCII.GetString(buffer), Is.EqualTo("bcde"));
    }
    
    [Test]
    [SuppressMessage("ReSharper", "MethodHasAsyncOverload")]
    [SuppressMessage("ReSharper", "UseAwaitUsing")]
    public async Task GetChars()
    {
        const string str = "ABCDE";
        var expected = str.ToCharArray();
        var actual = new char[expected.Length];
        var queryText = $"SELECT '{str}', 3, '{str}', 4, '{str}', '{str}', '{str}'";
        Fixture.SpannerMock.AddOrUpdateStatementResult(queryText, StatementResult.CreateResultSet(
            [
                Tuple.Create(TypeCode.String, "str"),
                Tuple.Create(TypeCode.Int64, "c"),
                Tuple.Create(TypeCode.String, "str"),
                Tuple.Create(TypeCode.Int64, "c"),
                Tuple.Create(TypeCode.String, "str"),
                Tuple.Create(TypeCode.String, "str"),
                Tuple.Create(TypeCode.String, "str"),
            ], [[ str, 3L, str, 4L, str, str, str, ]]));
        
        using var conn = await OpenConnectionAsync();
        using var cmd = new SpannerCommand(queryText, conn);
        using var reader = await cmd.ExecuteReaderAsync();
        reader.Read();

        Assert.That(reader.GetChars(0, 0, actual, 0, 2), Is.EqualTo(2));
        Assert.That(actual[0], Is.EqualTo(expected[0]));
        Assert.That(actual[1], Is.EqualTo(expected[1]));
        Assert.That(reader.GetChars(0, 0, null, 0, 0), Is.EqualTo(expected.Length), "Bad column length");
        
        Assert.That(reader.GetChars(2, 0, actual, 0, 2), Is.EqualTo(2));
        Assert.That(reader.GetChars(2, 0, actual, 4, 1), Is.EqualTo(1));
        Assert.That(actual[4], Is.EqualTo(expected[0]));
        Assert.That(reader.GetChars(2, 2, actual, 2, 3), Is.EqualTo(3));
        Assert.That(actual, Is.EqualTo(expected));
        
        //Assert.That(reader.GetChars(2, 0, null, 0, 0), Is.EqualTo(expected.Length), "Bad column length");

        Assert.That(() => reader.GetChars(3, 0, null, 0, 0), Throws.Exception.TypeOf<InvalidCastException>(), "GetChars on non-text");
        Assert.That(() => reader.GetChars(3, 0, actual, 0, 1), Throws.Exception.TypeOf<InvalidCastException>(), "GetChars on non-text");
        Assert.That(reader.GetInt32(3), Is.EqualTo(4));
        reader.GetChars(4, 0, actual, 0, 2);
        // Jump to another column from the middle of the column
        reader.GetChars(5, 0, actual, 0, 2);
        Assert.That(reader.GetChars(5, expected.Length - 1, actual, 0, 2), Is.EqualTo(1), "Length greater than data length");
        Assert.That(actual[0], Is.EqualTo(expected[^1]), "Length greater than data length");
        Assert.That(() => reader.GetChars(5, 0, actual, 0, actual.Length + 1), Throws.Exception.TypeOf<IndexOutOfRangeException>(), "Length great than output buffer length");
        // Close in the middle of a column
        reader.GetChars(6, 0, actual, 0, 2);
    }
    
    [Test]
    public async Task GetCharsAdvanceConsumed()
    {
        const string value = "01234567";
        var sql = $"SELECT '{value}'";
        Fixture.SpannerMock.AddOrUpdateStatementResult(sql, StatementResult.CreateResultSet(
            [Tuple.Create(TypeCode.String, "c")], [[value]]));

        await using var conn = await OpenConnectionAsync();
        await using var cmd = new SpannerCommand(sql, conn);
        await using var reader = await cmd.ExecuteReaderAsync();
        await reader.ReadAsync();

        var buffer = new char[2];
        // Don't start at the beginning of the column.
        reader.GetChars(0, 2, buffer, 0, 2);
        Assert.That(buffer, Is.EqualTo(new []{'2', '3'}));
        reader.GetChars(0, 4, buffer, 0, 2);
        Assert.That(buffer, Is.EqualTo(new []{'4', '5'}));
        reader.GetChars(0, 6, buffer, 0, 2);
        Assert.That(buffer, Is.EqualTo(new []{'6', '7'}));
        reader.GetChars(0, 7, buffer, 0, 2);
        Assert.That(buffer, Is.EqualTo(new []{'7', '7'}));

        reader.GetChars(0, 4, buffer, 0, 2);
        Assert.That(buffer, Is.EqualTo(new []{'4', '5'}));
        reader.GetChars(0, 6, buffer, 0, 2);
        Assert.That(buffer, Is.EqualTo(new []{'6', '7'}));
    }
    
    [Test]
    [SuppressMessage("ReSharper", "MethodHasAsyncOverload")]
    public async Task GetTextReader([Values(true, false)] bool isAsync)
    {
        const string str = "ABCDE";
        var queryText = $@"SELECT '{str}', 'foo'";
        Fixture.SpannerMock.AddOrUpdateStatementResult(queryText, StatementResult.CreateResultSet(
            [Tuple.Create(TypeCode.String, "c"), Tuple.Create(TypeCode.String, "c")], [[str, "foo"]]));
        
        await using var conn = await OpenConnectionAsync();
        var expected = str.ToCharArray();
        var actual = new char[expected.Length];

        await using var cmd = new SpannerCommand(queryText, conn);
        await using var reader = await cmd.ExecuteReaderAsync();
        await reader.ReadAsync();

        var textReader = reader.GetTextReader(0);
        textReader.Read(actual, 0, 2);
        Assert.That(actual[0], Is.EqualTo(expected[0]));
        Assert.That(actual[1], Is.EqualTo(expected[1]));
        Assert.That(() => reader.GetTextReader(0), Throws.Nothing, "Sequential text reader twice on same column");
        textReader.Read(actual, 2, 1);
        Assert.That(actual[2], Is.EqualTo(expected[2]));
        textReader.Dispose();

        Assert.That(reader.GetChars(0, 0, actual, 4, 1), Is.EqualTo(1));
        Assert.That(actual[4], Is.EqualTo(expected[0]));
        Assert.That(reader.GetString(1), Is.EqualTo("foo"));
    }

    [Test]
    [SuppressMessage("ReSharper", "MethodHasAsyncOverload")]
    public async Task TextReaderZeroLengthColumn()
    {
        const string sql = "SELECT ''";
        Fixture.SpannerMock.AddOrUpdateStatementResult(sql, StatementResult.CreateResultSet(
            [Tuple.Create(TypeCode.String, "c")], [[""]]));
        
        await using var conn = await OpenConnectionAsync();
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;

        await using var reader = await cmd.ExecuteReaderAsync();
        Assert.That(await reader.ReadAsync());

        using var textReader = reader.GetTextReader(0);
        Assert.That(textReader.Peek(), Is.EqualTo(-1));
        Assert.That(textReader.ReadToEnd(), Is.EqualTo(string.Empty));
    }

    [Test]
    [SuppressMessage("ReSharper", "MethodHasAsyncOverload")]
    [SuppressMessage("ReSharper", "UseAwaitUsing")]
    public async Task OpenTextReaderWhenChangingColumns()
    {
        const string sql = "SELECT 'some_text', 'some_text'";
        Fixture.SpannerMock.AddOrUpdateStatementResult(sql, StatementResult.CreateResultSet(
            [Tuple.Create(TypeCode.String, "c"), Tuple.Create(TypeCode.String, "c")], [["some_text", "some-text"]]));
        
        using var conn = await OpenConnectionAsync();
        using var cmd = new SpannerCommand(sql, conn);
        using var reader = await cmd.ExecuteReaderAsync();
        reader.Read();
        var textReader = reader.GetTextReader(0);
        _ = reader.GetValue(1);
        Assert.That(() => textReader.Peek(), Throws.Nothing);
    }

    [Test]
    [SuppressMessage("ReSharper", "MethodHasAsyncOverload")]
    [SuppressMessage("ReSharper", "UseAwaitUsing")]
    public async Task OpenTextReaderWhenChangingRows()
    {
        const string sql = "SELECT 'some_text', 'some_text'";
        Fixture.SpannerMock.AddOrUpdateStatementResult(sql, StatementResult.CreateResultSet(
            [Tuple.Create(TypeCode.String, "c"), Tuple.Create(TypeCode.String, "c")], [["some_text", "some-text"]]));
        
        using var conn = await OpenConnectionAsync();
        using var cmd = new SpannerCommand(sql, conn);
        using var reader = await cmd.ExecuteReaderAsync();
        reader.Read();
        var tr1 = reader.GetTextReader(0);
        reader.Read();
        Assert.That(() => tr1.Peek(), Throws.Nothing);
    }

    [Test]
    public async Task GetCharsWhenNull()
    {
        const string sql = "SELECT cast(null as string)";
        Fixture.SpannerMock.AddOrUpdateStatementResult(sql, StatementResult.CreateResultSet(
            [Tuple.Create(TypeCode.String, "c")], [[DBNull.Value]]));
        
        var buf = new char[8];
        await using var conn = await OpenConnectionAsync();
        await using var cmd = new SpannerCommand(sql, conn);
        await using var reader = await cmd.ExecuteReaderAsync();
        await reader.ReadAsync();
        Assert.That(reader.IsDBNull(0), Is.True);
        Assert.That(reader.GetChars(0, 0, buf, 0, 1), Is.EqualTo(0));
        Assert.That(() => reader.GetTextReader(0), Throws.Nothing, "GetTextReader");
        Assert.That(reader.GetChars(0, 0, null, 0, 0), Is.EqualTo(0), "GetChars with null buffer");
    }

    [Test]
    public async Task GetTextReaderAfterConsumingColumnWorks()
    {
        const string sql = "SELECT 'foo'";
        Fixture.SpannerMock.AddOrUpdateStatementResult(sql, StatementResult.CreateResultSet(
            [Tuple.Create(TypeCode.String, "c")], [["foo"]]));
        
        await using var conn = await OpenConnectionAsync();
        await using var cmd = new SpannerCommand(sql, conn);
        await using var reader = await cmd.ExecuteReaderAsync(CommandBehavior.SequentialAccess);
        await reader.ReadAsync();

        _ = reader.GetString(0);
        Assert.That(() => reader.GetTextReader(0), Throws.Nothing);
    }
    
    [Test]
    public async Task GetTextReaderInMiddleOfColumnWorks()
    {
        const string sql = "SELECT 'foo'";
        Fixture.SpannerMock.AddOrUpdateStatementResult(sql, StatementResult.CreateResultSet(
            [Tuple.Create(TypeCode.String, "c")], [["foo"]]));
        
        await using var conn = await OpenConnectionAsync();
        await using var cmd = new SpannerCommand(sql, conn);
        await using var reader = await cmd.ExecuteReaderAsync(CommandBehavior.SequentialAccess);
        await reader.ReadAsync();

        _ = reader.GetChars(0, 0, new char[2], 0, 2);
        Assert.That(() => reader.GetTextReader(0), Throws.Nothing);
    }

}