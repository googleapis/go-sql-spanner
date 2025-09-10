using System.Collections.Generic;
using System.Threading.Tasks;
using Google.Cloud.Spanner.V1;

namespace Google.Cloud.SpannerLib;

public class Connection : AbstractLibObject
{
    internal Pool Pool { get; }

    internal Connection(Pool pool, long id) : base(pool.Spanner, id)
    {
        Pool = pool;
    }

    public Transaction BeginTransaction(TransactionOptions transactionOptions)
    {
        return Spanner.BeginTransaction(this, transactionOptions);
    }

    public CommitResponse WriteMutations(BatchWriteRequest.Types.MutationGroup mutations)
    {
        return Spanner.WriteMutations(this, mutations);
    }

    public Rows Execute(ExecuteSqlRequest statement)
    {
        return Spanner.Execute(this, statement);
    }

    public Task<Rows> ExecuteAsync(ExecuteSqlRequest statement)
    {
        return Spanner.ExecuteAsync(this, statement);
    }

    public long[] ExecuteBatch(List<ExecuteBatchDmlRequest.Types.Statement> statements)
    {
        var request = new ExecuteBatchDmlRequest
        {
            Statements = { statements }
        };
        return Spanner.ExecuteBatch(this, request);
    }

    public Task<long[]> ExecuteBatchAsync(List<ExecuteBatchDmlRequest.Types.Statement> statements)
    {
        var request = new ExecuteBatchDmlRequest
        {
            Statements = { statements }
        };
        return Spanner.ExecuteBatchAsync(this, request);
    }

    protected override void CloseLibObject()
    {
        Spanner.CloseConnection(this);
    }
}