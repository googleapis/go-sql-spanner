using Google.Cloud.Spanner.V1;
using Google.Cloud.SpannerLib.Tests.MockServer;

namespace Google.Cloud.SpannerLib.Tests;

public class BasicTests
{
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
    public void TestCreatePool()
    {
        var pool = Pool.Create(ConnectionString);
        pool.Close();
    }

    [Test]
    public void TestCreateConnection()
    {
        using var pool = Pool.Create(ConnectionString);
        var connection = pool.CreateConnection();
        connection.Close();
    }

    [Test]
    public void TestExecuteQuery()
    {
        using var pool = Pool.Create(ConnectionString);
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
    public void TestReadOnlyTransaction()
    {
        using var pool = Pool.Create(ConnectionString);
        using var connection = pool.CreateConnection();
        using var transaction = connection.BeginTransaction(new TransactionOptions
            { ReadOnly = new TransactionOptions.Types.ReadOnly() });
        using var rows = transaction.Execute(new ExecuteSqlRequest { Sql = "SELECT 1" });
        for (var row = rows.Next(); row != null; row = rows.Next())
        {
            Assert.That(row.Values.Count, Is.EqualTo(1));
            Assert.That(row.Values[0].HasStringValue);
            Assert.That(row.Values[0].StringValue, Is.EqualTo("1"));
        }
        var commitResponse = transaction.Commit();
        Assert.That(commitResponse, Is.Not.Null);
    }

    [Test]
    public void TestReadWriteTransaction()
    {
        var sql = "update table1 set value='one' where id=1";
        _fixture.SpannerMock.AddOrUpdateStatementResult(sql, StatementResult.CreateUpdateCount(1));
        using var pool = Pool.Create(ConnectionString);
        using var connection = pool.CreateConnection();
        using var transaction = connection.BeginTransaction(new TransactionOptions
            { ReadWrite = new TransactionOptions.Types.ReadWrite() });
        using var rows = transaction.Execute(new ExecuteSqlRequest { Sql = sql });
        Assert.That(rows.UpdateCount, Is.EqualTo(1));
        var commitResponse = transaction.Commit();
        Assert.That(commitResponse, Is.Not.Null);
    }
}
