namespace Google.Cloud.Spanner.DataProvider.Benchmarks.tpcc;

internal class Stats
{
    private readonly DateTime _startTime;
    private ulong _numTransactions;
    private ulong _numTransactionsStarted;
    private ulong _numTransactionsCompleted;
    private ulong _numFailedTransactions;
    private ulong _numNewOrderTransactions;
    private ulong _numPaymentTransactions;
    private ulong _numOrderStatusTransactions;
    private ulong _numDeliveryTransactions;
    private ulong _numStockLevelTransactions;
    private Exception? _lastException;

    private ulong _totalMillis;

    internal Stats()
    {
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
                break;
            case TpccRunner.TransactionType.Payment:
                Interlocked.Increment(ref _numPaymentTransactions);
                break;
            case TpccRunner.TransactionType.OrderStatus:
                Interlocked.Increment(ref _numOrderStatusTransactions);
                break;
            case TpccRunner.TransactionType.Delivery:
                Interlocked.Increment(ref _numDeliveryTransactions);
                break;
            case TpccRunner.TransactionType.StockLevel:
                Interlocked.Increment(ref _numStockLevelTransactions);
                break;
            default:
                throw new ArgumentOutOfRangeException(nameof(transactionType), transactionType, null);
        }
    }

    internal void RegisterFailedTransaction(TpccRunner.TransactionType transactionType, TimeSpan duration, Exception error)
    {
        Interlocked.Increment(ref _numFailedTransactions);
        lock (this)
        {
            _lastException = error;
        }
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
        Console.Write(ToString());
    }

    public override string ToString()
    {
        return $"  Total duration: {DateTime.UtcNow - _startTime}{Environment.NewLine}" +
               $"Transactions/sec: {Interlocked.Read(ref _numTransactions) / (DateTime.UtcNow - _startTime).TotalSeconds}{Environment.NewLine}" +
               $"           Total: {Interlocked.Read(ref _numTransactions)}{Environment.NewLine}" +
               $"             Avg: {Interlocked.Read(ref _totalMillis) / Interlocked.Read(ref _numTransactions)}{Environment.NewLine}" +
               $"         Started: {Interlocked.Read(ref _numTransactionsStarted)}{Environment.NewLine}" +
               $"       Completed: {Interlocked.Read(ref _numTransactionsCompleted)}{Environment.NewLine}" +
               $"          Failed: {Interlocked.Read(ref _numFailedTransactions)}{Environment.NewLine}" +
               $"   Num new order: {Interlocked.Read(ref _numNewOrderTransactions)}{Environment.NewLine}" +
               $"     Num payment: {Interlocked.Read(ref _numPaymentTransactions)}{Environment.NewLine}" +
               $"Num order status: {Interlocked.Read(ref _numOrderStatusTransactions)}{Environment.NewLine}" +
               $"    Num delivery: {Interlocked.Read(ref _numDeliveryTransactions)}{Environment.NewLine}" +
               $" Num stock level: {Interlocked.Read(ref _numStockLevelTransactions)}{Environment.NewLine}";
    }
}