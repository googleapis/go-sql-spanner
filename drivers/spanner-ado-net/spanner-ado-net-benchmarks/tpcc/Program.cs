using System.Collections.Concurrent;
using System.Data.Common;
using System.Diagnostics;
using Google.Cloud.Spanner.Admin.Database.V1;
using Google.Cloud.Spanner.DataProvider.Benchmarks.tpcc.loader;
using Microsoft.AspNetCore.Builder;

namespace Google.Cloud.Spanner.DataProvider.Benchmarks.tpcc;

public static class Program
{
    enum ClientType
    {
        SpannerLib,
        NativeSpannerLib,
        ClientLib,
    }
    
    public static async Task Main(string[] args)
    {
        var cancellationTokenSource = new CancellationTokenSource();
        var builder = WebApplication.CreateBuilder(args);
        var port = Environment.GetEnvironmentVariable("PORT") ?? "8080";
        var url = $"http://0.0.0.0:{port}";
        var app = builder.Build();
        app.MapGet("/", () => { });
        var webapp = app.RunAsync(url);

        var logWaitTime = int.Parse(Environment.GetEnvironmentVariable("LOG_WAIT_TIME") ?? "10");
        var database = Environment.GetEnvironmentVariable("DATABASE") ?? "projects/appdev-soda-spanner-staging/instances/knut-test-ycsb/databases/dotnet-tpcc";
        var retryAbortsInternally = bool.Parse(Environment.GetEnvironmentVariable("RETRY_ABORTS_INTERNALLY") ?? "true");
        var numWarehouses = int.Parse(Environment.GetEnvironmentVariable("NUM_WAREHOUSES") ?? "10");
        var numClients = int.Parse(Environment.GetEnvironmentVariable("NUM_CLIENTS") ?? "10");
        var targetTps = int.Parse(Environment.GetEnvironmentVariable("TRANSACTIONS_PER_SECOND") ?? "0");
        var clientTypeName = Environment.GetEnvironmentVariable("CLIENT_TYPE") ?? "SpannerLib";
        if (!Enum.TryParse(clientTypeName, out ClientType clientType))
        {
            throw new ArgumentException($"Unknown client type: {clientTypeName}");
        }

        var connectionString = $"Data Source={database}";
        if (!retryAbortsInternally)
        {
            connectionString += ";retryAbortsInternally=false";
        }
        await using (var connection = new SpannerConnection())
        {
            connection.ConnectionString = connectionString;
            await connection.OpenAsync(cancellationTokenSource.Token);

            Console.WriteLine("Creating schema...");
            await SchemaUtil.CreateSchemaAsync(connection, DatabaseDialect.Postgresql, cancellationTokenSource.Token);

            Console.WriteLine("Loading data...");
            var loader = new DataLoader(connection, numWarehouses);
            await loader.LoadAsync(cancellationTokenSource.Token);
        }

        Console.WriteLine("Running benchmark...");
        var stats = new Stats();

        if (targetTps > 0)
        {
            var maxWaitTime = 2 * 1000 / targetTps;
            Console.WriteLine($"Clients: {numClients}");
            Console.WriteLine($"Transactions per second: {targetTps}");
            Console.WriteLine($"Max wait time: {maxWaitTime}");
            var runners = new BlockingCollection<TpccRunner?>();
            for (var client = 0; client < numClients; client++)
            {
                runners.Add(await CreateRunnerAsync(clientType, connectionString, stats, numWarehouses, cancellationTokenSource), cancellationTokenSource.Token);
            }
            var lastLogTime = DateTime.UtcNow;
            while (!cancellationTokenSource.IsCancellationRequested)
            {
                var randomWaitTime = Random.Shared.Next(0, maxWaitTime);
                var stopwatch = Stopwatch.StartNew();
                if (runners.TryTake(out var runner, 20_000, cancellationTokenSource.Token))
                {
                    var source = new CancellationTokenSource();
                    source.CancelAfter(TimeSpan.FromSeconds(10));
                    var token =  source.Token;
                    stats.RegisterTransactionStarted();
                    var task = runner!.RunTransactionAsync(token);
                    _ = task.ContinueWith(_ =>
                    {
                        stats.RegisterTransactionCompleted();
                        runners.Add(runner, cancellationTokenSource.Token);
                        task.Dispose();
                    }, TaskContinuationOptions.ExecuteSynchronously);
                }
                else
                {
                    await Console.Error.WriteLineAsync("No runner available");
                }
                randomWaitTime -= (int) stopwatch.ElapsedMilliseconds;
                if (randomWaitTime > 0)
                {
                    await Task.Delay(TimeSpan.FromMilliseconds(randomWaitTime), cancellationTokenSource.Token);
                }
                if ((DateTime.UtcNow - lastLogTime).TotalSeconds >= logWaitTime)
                {
                    Console.WriteLine($"Num available runners: {runners.Count}");
                    Console.WriteLine($"Thread pool size: {ThreadPool.ThreadCount}");
                    stats.LogStats();
                    lastLogTime = DateTime.UtcNow;
                }
            }
        }
        else
        {
            var tasks = new List<Task>();
            for (var client = 0; client < numClients; client++)
            {
                var runner = await CreateRunnerAsync(clientType, connectionString, stats, numWarehouses, cancellationTokenSource);
                tasks.Add(runner.RunAsync(cancellationTokenSource.Token));
            }
            while (!cancellationTokenSource.Token.IsCancellationRequested)
            {
                await Task.Delay(TimeSpan.FromSeconds(logWaitTime), cancellationTokenSource.Token);
                stats.LogStats();
            }
            await Task.WhenAll(tasks);
        }

        await app.StopAsync(cancellationTokenSource.Token);
        await webapp;
    }

    private static async Task<TpccRunner> CreateRunnerAsync(
        ClientType clientType,
        string connectionString,
        Stats stats,
        int numWarehouses,
        CancellationTokenSource cancellationTokenSource)
    {
        DbConnection connection;
        if (clientType == ClientType.SpannerLib)
        {
            connection = new SpannerConnection();
        }
        else if (clientType == ClientType.NativeSpannerLib)
        {
            connection = new SpannerConnection {UseNativeLibrary = true};
        }
        else if (clientType == ClientType.ClientLib)
        {
            connection = new Google.Cloud.Spanner.Data.SpannerConnection();
        }
        else
        {
            throw new ArgumentException($"Unknown client type: {clientType}");
        }
        connection.ConnectionString = connectionString;
        await connection.OpenAsync(cancellationTokenSource.Token);
        return new TpccRunner(stats, connection, numWarehouses);
    }
}