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

using System.Diagnostics;
using Google.Cloud.Spanner.Data;
using Google.Cloud.Spanner.V1;
using Google.Protobuf.WellKnownTypes;

namespace Google.Cloud.SpannerLib.Tests;

public class BenchmarkTests : AbstractMockServerTests
{
    readonly struct Stats
    {
        public TimeSpan Min { get; init; }
        public TimeSpan P50 { get; init; }
        public TimeSpan P90 { get; init; }
        public TimeSpan P95 { get; init; }
        public TimeSpan P99 { get; init; }
        public TimeSpan Max { get; init; }
        public TimeSpan Avg { get; init; }

        public override string ToString()
        {
            return $"Min: {Min}{Environment.NewLine}" +
                   $"P50: {P50}{Environment.NewLine}" +
                   $"P90: {P90}{Environment.NewLine}" +
                   $"P95: {P95}{Environment.NewLine}" +
                   $"P99: {P99}{Environment.NewLine}" +
                   $"Max: {Max}{Environment.NewLine}" +
                   $"Avg: {Avg}{Environment.NewLine}";
        }
    }
    
    [Test, Sequential]
    [Ignore("for local testing")]
    public async Task TestBenchmarkRealSpanner([Values] LibType? libType, [Values(false, false, false, true)] bool clientLib)
    {
        const string connectionString = "projects/appdev-soda-spanner-staging/instances/knut-test-ycsb/databases/knut-test-db";
        const int numTasks = 2000;
        object connector;
        
        if (clientLib)
        {
            connector = $"Data Source={connectionString}";
        }
        else
        {
            var pool = Pool.Create(SpannerLibDictionary[libType!.Value], connectionString);
            connector = pool;
        }

        // Warmup
        var warmupDuration = await ReadRandomRows(connector, 10);
        Console.WriteLine($"Warmup Duration: {warmupDuration}");

        for (var i = 0; i < 10; i++)
        {
            var numRows = (i + 1) * 10;
            var duration = await ReadRandomRows(connector, numRows);
            Console.WriteLine($"Duration ({numRows}): {duration}");
        }

        var stopwatch = Stopwatch.StartNew();
        var tasks = new Task<TimeSpan>[numTasks];
        for (var i = 0; i < numTasks; i++)
        {
            tasks[i] = ReadRandomRows(connector, 10);
        }

        await Task.WhenAll(tasks);
        var durations = new TimeSpan[numTasks];
        for (var i = 0; i < numTasks; i++)
        {
            durations[i] = tasks[i].Result;
        }
        var totalDuration = stopwatch.Elapsed;

        var stats = CalculateStats(durations);
        Console.WriteLine();
        Console.WriteLine($"Num tasks: {numTasks}");
        Console.WriteLine($"{stats}");
        ThreadPool.GetMaxThreads(out var workerThreads, out _);
        Console.WriteLine($"Max threads: {workerThreads}");
        Console.WriteLine($"Total time: {totalDuration}");

        if (connector is IDisposable disposable)
        {
            disposable.Dispose();
        }
    }

    static Stats CalculateStats(TimeSpan[] durations)
    {
        var ordered = durations.Order().ToArray();
        var stats = new Stats 
        {
            Min = ordered[0],
            P50 = ordered[ordered.Length * 50 / 100],
            P90 = ordered[ordered.Length * 90 / 100],
            P95 = ordered[ordered.Length * 95 / 100],
            P99 = ordered[ordered.Length * 99 / 100],
            Max = ordered[^1],
            Avg = TimeSpan.FromTicks((long) ordered.Average(duration => duration.Ticks))
        };
        return stats;
    }

    static Task<TimeSpan> ReadRandomRows(object connector, int maxRows)
    {
        if (connector is Pool pool)
        {
            return ReadRandomRows(pool, maxRows);
        }
        if (connector is string connString)
        {
            return ReadRandomRows(connString, maxRows);
        }
        throw new NotSupportedException();
    }

    static async Task<TimeSpan> ReadRandomRows(Pool pool, int maxRows)
    {
        // Add the randomly selected identifiers to a set to ensure that we know exactly how many rows will be returned,
        // as the random selection of identifiers could contain duplicates.
        var set = new HashSet<int>();
        var list = Value.ForList();
        list.ListValue.Values.Capacity = maxRows;
        for (var c = 0; c < maxRows; c++)
        {
            var id = Random.Shared.Next(1, 1_000_001);
            set.Add(id);
            list.ListValue.Values.Add(Value.ForString($"{id}"));
        }
        await using var connection = pool.CreateConnection();
        
        var stopwatch = Stopwatch.StartNew();
        var request = new ExecuteSqlRequest
        {
            Sql = "select * from all_types where col_bigint = any($1)",
            Params = new Struct { Fields = { ["p1"] = list } },
        };
        var count = 0;
        await using var rows = await connection.ExecuteAsync(request);
        while (await rows.NextAsync() is { } row)
        {
            Assert.That(row.Values.Count, Is.EqualTo(10));
            count++;
        }
        Assert.That(count, Is.EqualTo(set.Count));
        
        return stopwatch.Elapsed;
    }

    static async Task<TimeSpan> ReadRandomRows(string connectionString, int maxRows)
    {
        var set = new HashSet<int>();
        var list = new List<long>
        {
            Capacity = maxRows
        };
        for (var c = 0; c < maxRows; c++)
        {
            var id = Random.Shared.Next(1, 1_000_001);
            set.Add(id);
            list.Add(id);
        }
        await using var connection = new SpannerConnection(connectionString);
        
        var stopwatch = Stopwatch.StartNew();
        await using var cmd = connection.CreateSelectCommand(
            "select * from all_types where col_bigint = any($1)",
            new SpannerParameterCollection {{"p1", SpannerDbType.ArrayOf(SpannerDbType.Int64), list}}
            );
        var count = 0;
        await using var rows = await cmd.ExecuteReaderAsync();
        while (await rows.ReadAsync())
        {
            Assert.That(rows.FieldCount, Is.EqualTo(10));
            count++;
        }
        Assert.That(count, Is.EqualTo(set.Count));
        
        return stopwatch.Elapsed;
    }
}