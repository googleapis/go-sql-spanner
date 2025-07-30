using Google.Cloud.Spanner.V1;
using Google.Cloud.SpannerLib.V1;
using Grpc.Core;

namespace Google.Cloud.SpannerLib.GrpcTests;

public class Tests
{
    [SetUp]
    public void Setup()
    {
    }

    [Test]
    public async Task TestStreaming()
    {
        var lib = new Grpc.SpannerLib();
        var pool = lib.CreatePool(
            "projects/appdev-soda-spanner-staging/instances/knut-test-ycsb/databases/knut-test-db");
        var connection = lib.CreateConnection(pool);
    
        var stream =
            lib.ExecuteStreaming(connection, new ExecuteSqlRequest { Sql = "select * from all_types limit 10" });
        var first = true;
        await foreach (var result in stream.ResponseStream.ReadAllAsync())
        {
            if (first)
            {
                first = false;
                foreach (var field in result.Metadata.RowType.Fields)
                {
                    Console.Write(field.Name);
                    Console.Write("|");
                }
                Console.WriteLine();
            }
            foreach (var row in result.Rows)
            {
                foreach (var value in row.Values)
                {
                    Console.Write(value.ToString());
                    Console.Write("|");
                }
            }
            Console.WriteLine();
        }
        lib.CloseConnection(connection);
        lib.ClosePool(pool);
    }

    [Test]
    [Ignore("Intended for local testing")]
    public async Task Test1()
    {
        var lib = new Grpc.SpannerLib();
        var pool = lib.CreatePool(
            "projects/appdev-soda-spanner-staging/instances/knut-test-ycsb/databases/knut-test-db");
        var connection = lib.CreateConnection(pool);
        
        var rows = lib.Execute(connection, new ExecuteSqlRequest{Sql = "select * from all_types limit 10"});
        var metadata = lib.Metadata(rows);
        foreach (var field in metadata.RowType.Fields)
        {
            Console.Write(field.Name);
            Console.Write("|");
        }
        Console.WriteLine();
        for (var row = lib.Next(rows); row.Values.Count > 0; row = lib.Next(rows))
        {
            foreach (var value in row.Values)
            {
                Console.Write(value.ToString());
                Console.Write("|");
            }
            Console.WriteLine();
        }
        lib.CloseRows(rows);

        await TestStream(lib, pool, connection);
        
        lib.CloseConnection(connection);
        lib.ClosePool(pool);
    }

    private async Task TestStream(Grpc.SpannerLib lib, Pool pool, Connection connection)
    {
        Console.WriteLine();
        Console.WriteLine("Starting stream");
        using var stream = lib.CreateStream();
        await stream.RequestStream.WriteAsync(new ConnectionStreamRequest
        {
            ExecuteRequest = new ExecuteRequest
            {
                Connection = connection,
                ExecuteSqlRequest = new ExecuteSqlRequest{Sql = "select * from all_types limit 10"},
            }
        });
        await stream.ResponseStream.MoveNext(CancellationToken.None);
        var response = stream.ResponseStream.Current;
        if (response.ResponseCase == ConnectionStreamResponse.ResponseOneofCase.Rows)
        {
            var rows = response.Rows;
            while (true)
            {
                await stream.RequestStream.WriteAsync(new ConnectionStreamRequest
                {
                    NextRequest = new NextRequest{Rows = rows}
                });
                await stream.ResponseStream.MoveNext(CancellationToken.None);
                var rowResponse = stream.ResponseStream.Current;
                if (rowResponse.ResponseCase == ConnectionStreamResponse.ResponseOneofCase.Row)
                {
                    var row = rowResponse.Row;
                    if (row.Values.Count > 0)
                    {
                        Console.Write(row.Values.ToString());
                    }
                    else
                    {
                        break;
                    }
                }
                else
                {
                    Console.WriteLine("Unexpected response: " + response.ResponseCase);
                }
            }
        }
    }
}