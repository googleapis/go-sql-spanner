using System.Diagnostics.CodeAnalysis;
using System.Text;
using Google.Cloud.SpannerLib.MockServer;
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
    
}