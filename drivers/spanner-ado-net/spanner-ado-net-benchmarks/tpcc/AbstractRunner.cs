namespace Google.Cloud.Spanner.DataProvider.Benchmarks.tpcc;

public abstract class AbstractRunner
{
    public abstract Task RunAsync(CancellationToken cancellationToken);

    public abstract Task RunTransactionAsync(CancellationToken cancellationToken);
}