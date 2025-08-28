using System.Threading.Tasks;
using Google.Cloud.Spanner.V1;
using Google.Protobuf.WellKnownTypes;

namespace Google.Cloud.SpannerLib;

public interface ISpanner
{
    public Pool CreatePool(string dsn);

    public void ClosePool(Pool pool);

    public Connection CreateConnection(Pool pool);

    public void CloseConnection(Connection connection);

    public CommitResponse Apply(Connection connection, BatchWriteRequest.Types.MutationGroup mutations);

    public void BufferWrite(Transaction transaction, BatchWriteRequest.Types.MutationGroup mutations);

    public Rows Execute(Connection connection, ExecuteSqlRequest statement);
    
    public Task<Rows> ExecuteAsync(Connection connection, ExecuteSqlRequest statement);

    public Rows ExecuteTransaction(Transaction transaction, ExecuteSqlRequest statement);

    public long[] ExecuteBatch(Connection connection, ExecuteBatchDmlRequest statements);
    
    public Task<long[]> ExecuteBatchAsync(Connection connection, ExecuteBatchDmlRequest statements);

    public ResultSetMetadata? Metadata(Rows rows);
    
    public Task<ResultSetMetadata?> MetadataAsync(Rows rows);

    public ResultSetStats? Stats(Rows rows);

    public ListValue? Next(Rows rows);
    
    public Task<ListValue?> NextAsync(Rows rows);

    public void CloseRows(Rows rows);

    public Transaction BeginTransaction(Connection connection, TransactionOptions transactionOptions);

    public CommitResponse Commit(Transaction transaction);

    public void Rollback(Transaction transaction);
}