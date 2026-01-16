using System.Data;
using System.Data.Common;
using System.Diagnostics;
using Google.Cloud.Spanner.DataProvider.Benchmarks.tpcc.loader;

namespace Google.Cloud.Spanner.DataProvider.Benchmarks.tpcc;

public class BasicsRunner : AbstractRunner
{
    private readonly double _delayBetweenTransactions;
    private readonly Stats _stats;
    private readonly DbConnection _connection;
    private readonly Program.BenchmarkType _benchmarkType;
    private readonly int _numWarehouses;
    private readonly int _numDistrictsPerWarehouse;
    private readonly int _numCustomersPerDistrict;
    private readonly int _numItems;

    internal BasicsRunner(
        long transactionsPerSecond,
        Stats stats,
        DbConnection connection,
        Program.BenchmarkType benchmarkType,
        int numWarehouses,
        int numDistrictsPerWarehouse = 10,
        int numCustomersPerDistrict = 3000,
        int numItems = 100_000)
    {
        if (transactionsPerSecond > 0)
        {
            _delayBetweenTransactions = 1000d / transactionsPerSecond;
        }
        else
        {
            _delayBetweenTransactions = 0;
        }
        _stats = stats;
        _connection = connection;
        _benchmarkType = benchmarkType;
        _numWarehouses = numWarehouses;
        _numDistrictsPerWarehouse = numDistrictsPerWarehouse;
        _numCustomersPerDistrict = numCustomersPerDistrict;
        _numItems = numItems;
    }
    
    public override async Task RunAsync(CancellationToken cancellationToken)
    {
        while (!cancellationToken.IsCancellationRequested)
        {
            var stopwatch = Stopwatch.StartNew();
            await RunTransactionAsync(cancellationToken);
            stopwatch.Stop();
            int delay;
            if (_delayBetweenTransactions > 0)
            {
                delay = (int) (_delayBetweenTransactions - stopwatch.ElapsedMilliseconds);
            }
            else
            {
                delay = Random.Shared.Next(10, 100);
            }
            delay = Math.Max(delay, 0);
            await Task.Delay(delay, cancellationToken);
        }
    }

    public override async Task RunTransactionAsync(CancellationToken cancellationToken)
    {
        switch (_benchmarkType)
        {
            case Program.BenchmarkType.PointQuery:
                await PointQueryAsync(cancellationToken);
                break;
            case Program.BenchmarkType.Scalar:
                await ScalarAsync(cancellationToken);
                break;
            case Program.BenchmarkType.LargeQuery:
                await LargeQueryAsync(cancellationToken);
                break;
            case Program.BenchmarkType.PointDml:
                await PointDmlAsync(cancellationToken);
                break;
            case Program.BenchmarkType.ReadWriteTx:
                await ReadWriteTransactionAsync(cancellationToken);
                break;
            default:
                throw new NotSupportedException($"Transaction type {_benchmarkType} is not supported.");
        }
    }

    private async Task PointQueryAsync(CancellationToken cancellationToken)
    {
        var watch = Stopwatch.StartNew();
        await using var command = CreatePointReadCommand();
        var numRows = 0;
        await using var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
        while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
        {
            for (var col = 0; col < reader.FieldCount; col++)
            {
                _ = reader.GetValue(col);
            }
            numRows++;
        }
        if (numRows != 1)
        {
            throw new InvalidOperationException("Unexpected number of rows returned: " + numRows);
        }
        watch.Stop();
        _stats.RegisterOperationLatency(Program.OperationType.PointQuery, watch.Elapsed.TotalMilliseconds);
    }

    private async Task ScalarAsync(CancellationToken cancellationToken)
    {
        var watch = Stopwatch.StartNew();
        await using var command = CreatePointReadCommand();
        var item = await command.ExecuteScalarAsync(cancellationToken);
        if (item == null)
        {
            throw new InvalidOperationException("No row returned");
        }
        watch.Stop();
        _stats.RegisterOperationLatency(Program.OperationType.Scalar, watch.Elapsed.TotalMilliseconds);
    }

    private async Task PointDmlAsync(CancellationToken cancellationToken)
    {
        var warehouseId = DataLoader.ReverseBitsUnsigned((ulong)Random.Shared.Next(_numWarehouses));
        var districtId = DataLoader.ReverseBitsUnsigned((ulong)Random.Shared.Next(_numDistrictsPerWarehouse));
        var customerId = DataLoader.ReverseBitsUnsigned((ulong)Random.Shared.Next(_numCustomersPerDistrict));
        
        var watch = Stopwatch.StartNew();
        await using var command = _connection.CreateCommand();
        command.CommandText = "update customer set c_data=$1 where w_id=$2 and d_id=$3 and c_id=$4";
        AddParameter(command, "p1", DataLoader.RandomString(500));
        AddParameter(command, "p2", warehouseId);
        AddParameter(command, "p3", districtId);
        AddParameter(command, "p4", customerId);
        var updated = await command.ExecuteNonQueryAsync(cancellationToken);
        if (updated != 1)
        {
            throw new InvalidOperationException("Unexpected affected rows: " + updated);
        }
        watch.Stop();
        _stats.RegisterOperationLatency(Program.OperationType.PointDml, watch.Elapsed.TotalMilliseconds);
    }

    private async Task ReadWriteTransactionAsync(CancellationToken cancellationToken)
    {
        while (true)
        {
            var watch = Stopwatch.StartNew();
            try
            {
                await using var transaction = await _connection.BeginTransactionAsync(IsolationLevel.RepeatableRead, cancellationToken);
                if (transaction is Data.SpannerTransaction spannerTransaction)
                {
                    spannerTransaction.Tag = "client-lib";
                }
                else
                {
                    await using var cmd = _connection.CreateCommand();
                    cmd.CommandText = "set local transaction_tag = 'spanner-lib'";
                    await cmd.ExecuteNonQueryAsync(cancellationToken);
                }

                for (var i = 0; i < 3; i++)
                {
                    var warehouseId = DataLoader.ReverseBitsUnsigned((ulong)Random.Shared.Next(_numWarehouses));
                    var districtId =
                        DataLoader.ReverseBitsUnsigned((ulong)Random.Shared.Next(_numDistrictsPerWarehouse));
                    var customerId =
                        DataLoader.ReverseBitsUnsigned((ulong)Random.Shared.Next(_numCustomersPerDistrict));

                    await using var selectCommand = _connection.CreateCommand();
                    selectCommand.CommandText = "select * from customer where w_id=$1 and d_id=$2 and c_id=$3";
                    selectCommand.Transaction = transaction;
                    AddParameter(selectCommand, "p1", warehouseId);
                    AddParameter(selectCommand, "p2", districtId);
                    AddParameter(selectCommand, "p3", customerId);
                    await using var reader = await selectCommand.ExecuteReaderAsync(cancellationToken);
                    var foundRows = 0;
                    while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
                    {
                        foundRows++;
                    }

                    if (foundRows != 1)
                    {
                        throw new InvalidOperationException("Unexpected found rows: " + foundRows);
                    }

                    await using var updateCommand = _connection.CreateCommand();
                    updateCommand.CommandText = "update customer set c_data=$1 where w_id=$2 and d_id=$3 and c_id=$4";
                    updateCommand.Transaction = transaction;
                    AddParameter(updateCommand, "p1", DataLoader.RandomString(500));
                    AddParameter(updateCommand, "p2", warehouseId);
                    AddParameter(updateCommand, "p3", districtId);
                    AddParameter(updateCommand, "p4", customerId);
                    var updated = await updateCommand.ExecuteNonQueryAsync(cancellationToken);
                    if (updated != 1)
                    {
                        throw new InvalidOperationException("Unexpected affected rows: " + updated);
                    }
                }

                await transaction.CommitAsync(cancellationToken);
                watch.Stop();
                _stats.RegisterOperationLatency(Program.OperationType.ReadWriteTx, watch.Elapsed.TotalMilliseconds);
                break;
            }
            catch (Exception exception)
            {
                _stats.RegisterFailedOperation(Program.OperationType.ReadWriteTx, watch.Elapsed, exception);
            }
        }
    }

    private async Task LargeQueryAsync(CancellationToken cancellationToken)
    {
        Stopwatch? watch = null;
        try
        {
            await using var command = _connection.CreateCommand();
            command.CommandText = "select * from customer limit $1 offset $2";
            AddParameter(command, "p1", 100_000L);
            AddParameter(command, "p2", Random.Shared.Next(1, 1001));
            await using var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
            // Fetch the first row outside the measurement to ensure a fair comparison between clients that delay the
            // query execution to the first read, and those that don't.
            await reader.ReadAsync(cancellationToken).ConfigureAwait(false);
            
            watch = Stopwatch.StartNew();
            while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
            {
                for (var col = 0; col < reader.FieldCount; col++)
                {
                    _ = reader.GetValue(col);
                }
            }
            watch.Stop();
            _stats.RegisterOperationLatency(Program.OperationType.LargeQuery, watch.Elapsed.TotalMilliseconds);
        }
        catch (Exception exception)
        {
            Console.WriteLine(exception.Message);
            _stats.RegisterFailedOperation(Program.OperationType.LargeQuery, watch?.Elapsed ?? TimeSpan.Zero, exception);
        }
    }

    private DbCommand CreatePointReadCommand()
    {
        var warehouseId = DataLoader.ReverseBitsUnsigned((ulong)Random.Shared.Next(_numWarehouses));
        var itemId = DataLoader.ReverseBitsUnsigned((ulong)Random.Shared.Next(_numItems));
        
        var command = _connection.CreateCommand();
        command.CommandText = "select * from stock where s_i_id=$1 and w_id=$2";
        AddParameter(command, "p1", itemId);
        AddParameter(command, "p2", warehouseId);
        
        return command;
    }

    private void AddParameter(DbCommand command, string name, object value)
    {
        var itemParameter = command.CreateParameter();
        itemParameter.ParameterName = name;
        itemParameter.Value = value;
        command.Parameters.Add(itemParameter);
    }
    
}