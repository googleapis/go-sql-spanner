using System.Diagnostics;
using Google.Cloud.Spanner.V1;
using Google.Cloud.SpannerLib.Tests.MockServer;
using Grpc.Core;
using TypeCode = Google.Cloud.Spanner.V1.TypeCode;

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

    [Test]
    public void TestBenchmark()
    {
        var totalRowCount = 1000000;
        _fixture.SpannerMock.AddOrUpdateStatementResult(
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
        
        using var pool = Pool.Create(ConnectionString);
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
    public void TestBenchmarkGrpcClient()
    {
        var totalRowCount = 1000000;
        _fixture.SpannerMock.AddOrUpdateStatementResult(
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
            Endpoint = $"http://{_fixture.Endpoint}",
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
