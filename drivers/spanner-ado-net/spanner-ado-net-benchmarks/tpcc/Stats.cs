using System.Text;
using Google.Api;
using Google.Api.Gax.ResourceNames;
using Google.Cloud.Monitoring.V3;
using Google.Protobuf.WellKnownTypes;
using Enum = System.Enum;

namespace Google.Cloud.Spanner.DataProvider.Benchmarks.tpcc;

internal static class BenchmarkTypeExtensions
{
    internal static string GetLabel(this Program.OperationType type)
    {
        return type.ToString().ToSnakeCase();
    }
    
    private static string ToSnakeCase(this string text)
    {
        if(text == null)
        {
            throw new ArgumentNullException(nameof(text));
        }
        if(text.Length < 2)
        {
            return text.ToLowerInvariant();
        }
        var sb = new StringBuilder();
        sb.Append(char.ToLowerInvariant(text[0]));
        for(int i = 1; i < text.Length; ++i)
        {
            char c = text[i];
            if(char.IsUpper(c))
            {
                sb.Append('_');
                sb.Append(char.ToLowerInvariant(c));
            }
            else
            {
                sb.Append(c);
            }
        }
        return sb.ToString();
    }
}

internal class Stats
{
    private readonly bool _exportStats;
    private readonly ProjectName _projectName;
    private readonly MetricServiceClient _metricsClient;
    private readonly MetricDescriptor _numTransactionsDescriptor;
    private readonly MetricDescriptor _operationLatencyDescriptor;
    private readonly Dictionary<string, string> _labels;
    private readonly Dictionary<Program.OperationType, Dictionary<string, string>> _operationLabels = new();
    
    private readonly DateTime _startTime;
    private ulong _numTransactions;
    private ulong _numTransactionsStarted;
    private ulong _numTransactionsCompleted;
    private ulong _numAbortedTransactions;
    private ulong _numFailedTransactions;
    private ulong _numNewOrderTransactions;
    private ulong _numPaymentTransactions;
    private ulong _numOrderStatusTransactions;
    private ulong _numDeliveryTransactions;
    private ulong _numStockLevelTransactions;
    private Exception? _lastException;

    private ulong _totalMillis;
    private readonly object _lock = new();
    private readonly Dictionary<Program.OperationType, LinkedList<double>> _operationLatencies = new();
    private ulong _numOperations;
    private ulong _numFailedOperations;
    private double _totalOperationMillis;

    internal Stats(
        bool exportStats,
        ProjectName projectName,
        MetricServiceClient metricsClient,
        MetricDescriptor numTransactionsDescriptor,
        MetricDescriptor operationLatencyDescriptor,
        int numClients,
        Program.ClientType clientType,
        bool directPath)
    {
        _exportStats = exportStats;
        _projectName = projectName;
        _metricsClient = metricsClient;
        _numTransactionsDescriptor = numTransactionsDescriptor;
        _operationLatencyDescriptor = operationLatencyDescriptor;
        var clientTypeName = clientType.ToString();
        if (directPath)
        {
            clientTypeName += "DirectPath";
        }
        _labels = new Dictionary<string, string>
        {
            { "num_clients", numClients.ToString() },
            { "client_type", clientTypeName },
        };
        foreach (var type in Enum.GetValues<Program.OperationType>())
        {
            var labels = new Dictionary<string, string>(_labels)
            {
                ["operation_type"] = type.GetLabel(),
            };
            _operationLabels[type] = labels;
            _operationLatencies.Add(type, new LinkedList<double>());
        }
        _startTime = DateTime.UtcNow;
    }

    internal void RegisterTransactionStarted()
    {
        Interlocked.Increment(ref _numTransactionsStarted);
    }

    internal void RegisterTransactionCompleted()
    {
        Interlocked.Increment(ref _numTransactionsCompleted);
    }

    internal void RegisterTransaction(TpccRunner.TransactionType transactionType, TimeSpan duration)
    {
        Interlocked.Increment(ref _numTransactions);
        Interlocked.Add(ref _totalMillis, (ulong) duration.TotalMilliseconds);
        switch (transactionType)
        {
            case TpccRunner.TransactionType.NewOrder:
                Interlocked.Increment(ref _numNewOrderTransactions);
                RegisterOperationLatency(Program.OperationType.NewOrder, duration.TotalMilliseconds);
                break;
            case TpccRunner.TransactionType.Payment:
                Interlocked.Increment(ref _numPaymentTransactions);
                RegisterOperationLatency(Program.OperationType.Payment, duration.TotalMilliseconds);
                break;
            case TpccRunner.TransactionType.OrderStatus:
                Interlocked.Increment(ref _numOrderStatusTransactions);
                RegisterOperationLatency(Program.OperationType.OrderStatus, duration.TotalMilliseconds);
                break;
            case TpccRunner.TransactionType.Delivery:
                Interlocked.Increment(ref _numDeliveryTransactions);
                RegisterOperationLatency(Program.OperationType.Delivery, duration.TotalMilliseconds);
                break;
            case TpccRunner.TransactionType.StockLevel:
                Interlocked.Increment(ref _numStockLevelTransactions);
                RegisterOperationLatency(Program.OperationType.StockLevel, duration.TotalMilliseconds);
                break;
            default:
                throw new ArgumentOutOfRangeException(nameof(transactionType), transactionType, null);
        }
    }
    
    internal void RegisterAbortedTransaction(TpccRunner.TransactionType transactionType, TimeSpan duration, Exception error)
    {
        Interlocked.Increment(ref _numAbortedTransactions);
    }

    internal void RegisterFailedTransaction(TpccRunner.TransactionType transactionType, TimeSpan duration, Exception error)
    {
        Interlocked.Increment(ref _numFailedTransactions);
        lock (this)
        {
            _lastException = error;
        }
    }

    internal void RegisterFailedOperation(Program.OperationType operationType, TimeSpan duration, Exception error)
    {
        Interlocked.Increment(ref _numFailedOperations);
    }

    internal void LogStats()
    {
        lock (this)
        {
            if (_lastException != null)
            {
                Console.Error.WriteLine(_lastException);
                _lastException = null;
            }
        }
        if (Interlocked.Read(ref _numTransactions) > 0)
        {
            Console.Write(ToString());
        }
        if (Interlocked.Read(ref _numOperations) > 0)
        {
            double avg;
            lock (_lock)
            {
                avg = _totalOperationMillis / _numOperations;
            }
            Console.WriteLine($"           Total time: {DateTime.UtcNow - _startTime}");
            Console.WriteLine($"                  Avg: {avg}");
            Console.WriteLine($"       Num operations: {Interlocked.Read(ref _numOperations)}");
            Console.WriteLine($"Num failed operations: {Interlocked.Read(ref _numFailedOperations)}");
        }
        Console.WriteLine($"Total pause duration: {GC.GetTotalPauseDuration()}");
        Console.WriteLine($"Pause time percentage: {GC.GetGCMemoryInfo().PauseTimePercentage}");
    }

    public override string ToString()
    {
        if (Interlocked.Read(ref _numTransactions) == 0)
        {
            return "No TPCC stats";
        }
        return $"  Total duration: {DateTime.UtcNow - _startTime}{Environment.NewLine}" +
               $"Transactions/sec: {Interlocked.Read(ref _numTransactions) / (DateTime.UtcNow - _startTime).TotalSeconds}{Environment.NewLine}" +
               $"           Total: {Interlocked.Read(ref _numTransactions)}{Environment.NewLine}" +
               $"             Avg: {Interlocked.Read(ref _totalMillis) / Interlocked.Read(ref _numTransactions)}{Environment.NewLine}" +
               $"         Started: {Interlocked.Read(ref _numTransactionsStarted)}{Environment.NewLine}" +
               $"       Completed: {Interlocked.Read(ref _numTransactionsCompleted)}{Environment.NewLine}" +
               $"         Aborted: {Interlocked.Read(ref _numAbortedTransactions)}{Environment.NewLine}" +
               $"      Abort rate: {Interlocked.Read(ref _numAbortedTransactions) / Interlocked.Read(ref _numTransactions)}{Environment.NewLine}" +
               $"          Failed: {Interlocked.Read(ref _numFailedTransactions)}{Environment.NewLine}" +
               $"   Num new order: {Interlocked.Read(ref _numNewOrderTransactions)}{Environment.NewLine}" +
               $"     Num payment: {Interlocked.Read(ref _numPaymentTransactions)}{Environment.NewLine}" +
               $"Num order status: {Interlocked.Read(ref _numOrderStatusTransactions)}{Environment.NewLine}" +
               $"    Num delivery: {Interlocked.Read(ref _numDeliveryTransactions)}{Environment.NewLine}" +
               $" Num stock level: {Interlocked.Read(ref _numStockLevelTransactions)}{Environment.NewLine}";
    }

    internal async Task ExportMetrics()
    {
        if (Interlocked.Read(ref _numTransactions) > 0)
        {
            await ExportTpccMetrics();
        }

        var exportOperationLatencies = false;
        lock (_operationLatencies)
        {
            foreach (var latencies in _operationLatencies)
            {
                if (latencies.Value.First != null)
                {
                    exportOperationLatencies = true;
                }
            }
        }

        if (exportOperationLatencies)
        {
            await ExportOperationLatencyMetrics();
        }
    }
    

    private async Task ExportTpccMetrics()
    {
        var request = new CreateTimeSeriesRequest
        {
            ProjectName = _projectName,
            TimeSeries =
            {
                new TimeSeries
                {
                    ValueType = _numTransactionsDescriptor.ValueType,
                    MetricKind = _numTransactionsDescriptor.MetricKind,
                    Metric = new Metric
                    {
                        Type = _numTransactionsDescriptor.Type,
                        Labels = { _labels },
                    },
                    Points =
                    {
                        new Point
                        {
                            Interval = new TimeInterval
                            {
                                StartTime = Timestamp.FromDateTime(_startTime),
                                EndTime = Timestamp.FromDateTime(DateTime.UtcNow),
                            },
                            Value = new TypedValue { Int64Value = (long)_numTransactions }
                        }
                    }
                }
            }
        };
        if (_exportStats)
        {
            await _metricsClient.CreateTimeSeriesAsync(request);
        }
        else
        {
            Console.WriteLine(request);
        }
    }

    internal void RegisterOperationLatency(Program.OperationType benchmarkType, double millis)
    {
        Interlocked.Increment(ref _numOperations);
        lock (_lock)
        {
            _totalOperationMillis += millis;
            _operationLatencies[benchmarkType].AddLast(millis);
        }
    }

    private async Task ExportOperationLatencyMetrics()
    {
        Dictionary<Program.OperationType, List<double>> latencies = new Dictionary<Program.OperationType, List<double>>();
        lock (_lock)
        {
            foreach (var entry in _operationLatencies)
            {
                if (entry.Value.First != null)
                {
                    var points = new List<double>(entry.Value);
                    latencies[entry.Key] = points;
                    entry.Value.Clear();
                }
            }
        }
        var exportTime = DateTime.UtcNow;

        foreach (var entry in latencies)
        {
            var labels = _operationLabels[entry.Key];
            var points = entry.Value;
            var request = new CreateTimeSeriesRequest
            {
                ProjectName = _projectName,
                TimeSeries =
                {
                    new TimeSeries
                    {
                        ValueType = _operationLatencyDescriptor.ValueType,
                        MetricKind = _operationLatencyDescriptor.MetricKind,
                        Metric = new Metric
                        {
                            Type = _operationLatencyDescriptor.Type,
                            Labels = { labels },
                        },
                        Points =
                        {
                            new Point
                            {
                                Interval = new TimeInterval
                                {
                                    EndTime = Timestamp.FromDateTime(exportTime),
                                },
                                Value = new TypedValue
                                {
                                    DistributionValue = CreateDistribution(entry.Key, points),
                                }
                            }
                        },
                    }
                },
            };
            if (_exportStats)
            {
                await _metricsClient.CreateTimeSeriesAsync(request);
            }
            else
            {
                Console.WriteLine(request);
            }
        }
    }

    private Distribution CreateDistribution(Program.OperationType benchmarkType, List<double> points)
    {
        if (points.Count == 0)
        {
            return new Distribution
            {
                Count = 0,
                Mean = 0.0d,
            };
        }
        double[] bounds;

        if (benchmarkType == Program.OperationType.LargeQuery)
        {
            bounds =
            [
                250d, 500d, 750d, 1000d, 1250d, 1500d, 2000d, 2250d, 2500d,
                2750d, 3000d, 3250d, 3500d, 4000d, 5000d, 6000d, 7000d,
                8000d, 9000d, 10000d, 11000d, 12000d, 13000d, 14000d, 15000d,
                16000d, 17000d, 18000d, 19000d, 20000d, 25000d, 30000d, 50000d
            ];
        }
        else
        {
            bounds =
            [
                1.0d, 1.1d, 1.2d, 1.3d, 1.4d, 1.5d, 1.6d, 1.7d, 1.8d, 1.9d,
                2.0d, 2.1d, 2.2d, 2.3d, 2.4d, 2.5d, 2.6d, 2.7d, 2.8d, 2.9d,
                3.0d, 3.1d, 3.2d, 3.3d, 3.4d, 3.5d, 3.6d, 3.7d, 3.8d, 3.9d,
                4.0d, 4.1d, 4.2d, 4.3d, 4.4d, 4.5d, 4.6d, 4.7d, 4.8d, 4.9d,
                5.0d, 5.1d, 5.2d, 5.3d, 5.4d, 5.5d, 5.6d, 5.7d, 5.8d, 5.9d,
                6.0d, 6.1d, 6.2d, 6.3d, 6.4d, 6.5d, 6.6d, 6.7d, 6.8d, 6.9d,
                7.0d, 7.2d, 7.4d, 7.6d, 7.8d, 8.0d, 8.2d, 8.4d, 8.6d, 8.8d,
                9.0d, 9.5d, 10.0d, 10.5d, 11.0d, 11.5d, 12.0d, 12.5d, 13.0d,
                14d, 15d, 17.5d, 20d, 25d, 30d, 35d, 40d, 50d, 100d, 200d,
                300d, 400d, 500d, 600d, 700d, 800d, 900d, 1000d, 2000d
            ];
        }

        var bucketCounts = new long[bounds.Length + 1];
        foreach (var point in points)
        {
            if (point < bounds[0])
            {
                bucketCounts[0]++;
            }
            else if (point >= bounds[^1])
            {
                bucketCounts[^1]++;
            }
            else
            {
                for (var i = 1; i < bounds.Length; i++)
                {
                    if (point >= bounds[i-1] && point < bounds[i])
                    {
                        bucketCounts[i]++;
                        break;
                    }
                }
            }
        }
        
        return new Distribution
        {
            Count = points.Count,
            BucketCounts = { bucketCounts },
            BucketOptions = new Distribution.Types.BucketOptions
            {
                ExplicitBuckets = new Distribution.Types.BucketOptions.Types.Explicit
                {
                    Bounds = { bounds },
                },
            }
        };
    }
}