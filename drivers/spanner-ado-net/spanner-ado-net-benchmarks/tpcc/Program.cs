using System.Collections.Concurrent;
using System.Data.Common;
using System.Diagnostics;
using System.Runtime.InteropServices;
using Google.Api;
using Google.Api.Gax.ResourceNames;
using Google.Cloud.Monitoring.V3;
using Google.Cloud.Spanner.Admin.Database.V1;
using Google.Cloud.Spanner.Common.V1;
using Google.Cloud.Spanner.DataProvider.Benchmarks.tpcc.loader;
using Google.Cloud.SpannerLib.Grpc;
using Microsoft.AspNetCore.Builder;
using Enum = System.Enum;

namespace Google.Cloud.Spanner.DataProvider.Benchmarks.tpcc;

public static class Program
{
    public enum ClientType
    {
        SpannerLib,
        BidiSpannerLib,
        SpannerLibNoRetries,
        NativeSpannerLib,
        ClientLib,
    }

    public enum BenchmarkType
    {
        Tpcc,
        PointQuery,
        LargeQuery,
        Scalar,
        PointDml,
        ReadWriteTx,
    }

    public enum OperationType
    {
        PointQuery,
        LargeQuery,
        Scalar,
        PointDml,
        ReadWriteTx,
        NewOrder,
        Payment,
        OrderStatus,
        Delivery,
        StockLevel,
    }
    
    public static async Task Main(string[] args)
    {
        Console.WriteLine($"Runtime: {RuntimeInformation.RuntimeIdentifier}");

        var createAlivenessServer = bool.Parse(Environment.GetEnvironmentVariable("CREATE_ALIVENESS_SERVER") ?? "true");
        var loadData = bool.Parse(Environment.GetEnvironmentVariable("LOAD_DATA") ?? "true");
        var exportStats = bool.Parse(Environment.GetEnvironmentVariable("EXPORT_STATS") ?? "true");
        var logWaitTime = int.Parse(Environment.GetEnvironmentVariable("LOG_WAIT_TIME") ?? "60");
        var database = Environment.GetEnvironmentVariable("DATABASE") ?? "projects/appdev-soda-spanner-staging/instances/knut-test-ycsb/databases/dotnet-tpcc";
        var retryAbortsInternally = bool.Parse(Environment.GetEnvironmentVariable("RETRY_ABORTS_INTERNALLY") ?? "true");
        var numWarehouses = int.Parse(Environment.GetEnvironmentVariable("NUM_WAREHOUSES") ?? "10");
        var numClients = int.Parse(Environment.GetEnvironmentVariable("NUM_CLIENTS") ?? "10");
        var targetTps = int.Parse(Environment.GetEnvironmentVariable("TRANSACTIONS_PER_SECOND") ?? "0");
        var clientTypeName = Environment.GetEnvironmentVariable("CLIENT_TYPE") ?? "BidiSpannerLib";
        var useSharedLib = bool.Parse(Environment.GetEnvironmentVariable("SPANNER_ADO_USE_NATIVE_LIB") ?? "false");
        var directPath = bool.Parse(Environment.GetEnvironmentVariable("GOOGLE_SPANNER_ENABLE_DIRECT_ACCESS") ?? "false");
        var communicationStyle = Enum.Parse<GrpcLibSpanner.CommunicationStyle>(Environment.GetEnvironmentVariable("SPANNER_ADO_COMMUNICATION_STYLE") ?? nameof(GrpcLibSpanner.CommunicationStyle.BidiStreaming));
        var transactionTypeName = Environment.GetEnvironmentVariable("TRANSACTION_TYPE") ?? "Tpcc";
        if (!Enum.TryParse(clientTypeName, out ClientType clientType))
        {
            throw new ArgumentException($"Unknown client type: {clientTypeName}");
        }
        if (useSharedLib && clientType != ClientType.NativeSpannerLib)
        {
            throw new ArgumentException("Invalid combination of clientType and useSharedLib");
        }
        if (!Enum.TryParse(transactionTypeName, out BenchmarkType transactionType))
        {
            throw new ArgumentException($"Unknown transaction type: {transactionTypeName}");
        }
        
        var cancellationTokenSource = new CancellationTokenSource();
        WebApplication? app = null;
        Task? webapp = null;
        if (createAlivenessServer)
        {
            var builder = WebApplication.CreateBuilder(args);
            var port = Environment.GetEnvironmentVariable("PORT") ?? "8080";
            var url = $"http://0.0.0.0:{port}";
            app = builder.Build();
            app.MapGet("/", () => { });
            webapp = app.RunAsync(url);
        }

        var databaseName = DatabaseName.Parse(database);
        var projectName = ProjectName.FromProject(databaseName.ProjectId);
        var metricsClient = await MetricServiceClient.CreateAsync(cancellationTokenSource.Token);
        var numTransactionsDescriptor = await metricsClient.CreateMetricDescriptorAsync(new CreateMetricDescriptorRequest
        {
            ProjectName = projectName,
            MetricDescriptor = new MetricDescriptor
            {
                Name = "spanner-ado-net-tpcc-tps",
                DisplayName = "Spanner ADO.NET TPCC Number of Transactions",
                Description = "Spanner ADO.NET TPCC Number of Transactions",
                LaunchStage = LaunchStage.Alpha,
                MetricKind = MetricDescriptor.Types.MetricKind.Cumulative,
                ValueType = MetricDescriptor.Types.ValueType.Int64,
                Type = "custom.googleapis.com/spanner-ado-net-tpcc/num_transactions",
                Labels = { 
                    new LabelDescriptor { Key = "num_clients" , Description = "Number of clients", ValueType = LabelDescriptor.Types.ValueType.Int64 },
                    new LabelDescriptor { Key = "client_type" , Description = "Client type", ValueType = LabelDescriptor.Types.ValueType.String },
                }
            },
        });
        // await metricsClient.DeleteMetricDescriptorAsync(new DeleteMetricDescriptorRequest
        // {
        //     MetricDescriptorName = MetricDescriptorName.FromProjectMetricDescriptor("appdev-soda-spanner-staging", "custom.googleapis.com/spanner-ado-net-tpcc/operation_latency"),
        // });
        var operationLatencyDescriptor = await metricsClient.CreateMetricDescriptorAsync(new CreateMetricDescriptorRequest
        {
            ProjectName = projectName,
            MetricDescriptor = new MetricDescriptor
            {
                Name = "spanner-ado-net-operation-latency",
                DisplayName = "Spanner ADO.NET Operation Latency",
                Description = "Spanner ADO.NET Operation Latency",
                LaunchStage = LaunchStage.Alpha,
                MetricKind = MetricDescriptor.Types.MetricKind.Gauge,
                ValueType = MetricDescriptor.Types.ValueType.Distribution,
                Type = "custom.googleapis.com/spanner-ado-net-tpcc/operation_latency",
                Labels = { 
                    new LabelDescriptor { Key = "num_clients" , Description = "Number of clients", ValueType = LabelDescriptor.Types.ValueType.Int64 },
                    new LabelDescriptor { Key = "client_type" , Description = "Client type", ValueType = LabelDescriptor.Types.ValueType.String },
                    new LabelDescriptor { Key = "operation_type" , Description = "Operation type", ValueType = LabelDescriptor.Types.ValueType.String },
                }
            },
        });

        var connectionString = $"Data Source={database}";
        if (!retryAbortsInternally)
        {
            connectionString += ";retryAbortsInternally=false";
        }

        if (loadData)
        {
            await using var connection = new SpannerConnection();
            connection.ConnectionString = connectionString;
            await connection.OpenAsync(cancellationTokenSource.Token);

            Console.WriteLine("Creating schema...");
            await SchemaUtil.CreateSchemaAsync(connection, DatabaseDialect.Postgresql,
                cancellationTokenSource.Token);

            Console.WriteLine("Loading data...");
            var loader = new DataLoader(connection, numWarehouses);
            await loader.LoadAsync(cancellationTokenSource.Token);
        }

        Console.WriteLine($"Running benchmark {transactionType}...");
        Console.WriteLine($"Client type: {clientType}");
        Console.WriteLine($"Num clients: {numClients}");
        Console.WriteLine($"Target TPS: {targetTps}");
        Console.WriteLine($"Exporting stats: {exportStats}");
        var stats = new Stats(exportStats, projectName, metricsClient, numTransactionsDescriptor, operationLatencyDescriptor, numClients, clientType, directPath);

        if (false && targetTps > 0 && transactionType == BenchmarkType.Tpcc)
        {
            var maxWaitTime = 2 * 1000 / targetTps;
            Console.WriteLine($"Clients: {numClients}");
            Console.WriteLine($"Transactions per second: {targetTps}");
            Console.WriteLine($"Max wait time: {maxWaitTime}");
            var runners = new BlockingCollection<AbstractRunner?>();
            for (var client = 0; client < numClients; client++)
            {
                runners.Add(await CreateRunnerAsync(targetTps, clientType, transactionType, connectionString, stats, numWarehouses, cancellationTokenSource), cancellationTokenSource.Token);
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
                    await stats.ExportMetrics();
                    lastLogTime = DateTime.UtcNow;
                }
            }
        }
        else
        {
            var tasks = new List<Task>();
            for (var client = 0; client < numClients; client++)
            {
                var runner = await CreateRunnerAsync(targetTps, clientType, transactionType, connectionString, stats, numWarehouses, cancellationTokenSource);
                tasks.Add(runner.RunAsync(cancellationTokenSource.Token));
            }
            while (!cancellationTokenSource.Token.IsCancellationRequested)
            {
                await Task.Delay(TimeSpan.FromSeconds(logWaitTime), cancellationTokenSource.Token);
                stats.LogStats();
                await stats.ExportMetrics();
            }
            await Task.WhenAll(tasks);
        }

        if (app != null && webapp != null)
        {
            await app.StopAsync(CancellationToken.None);
            await webapp;
        }
    }

    private static async Task<AbstractRunner> CreateRunnerAsync(
        int targetTps,
        ClientType clientType,
        BenchmarkType benchmarkType,
        string connectionString,
        Stats stats,
        int numWarehouses,
        CancellationTokenSource cancellationTokenSource)
    {
        DbConnection connection;
        if (clientType == ClientType.ClientLib)
        {
            connection = new Google.Cloud.Spanner.Data.SpannerConnection();
        }
        else
        {
            connection = new SpannerConnection();
        }
        connection.ConnectionString = connectionString;
        await connection.OpenAsync(cancellationTokenSource.Token);
        if (benchmarkType == BenchmarkType.Tpcc)
        {
            return new TpccRunner(targetTps, stats, connection, numWarehouses);
        }
        return new BasicsRunner(targetTps, stats, connection, benchmarkType, numWarehouses);
    }
}