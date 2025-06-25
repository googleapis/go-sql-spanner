using Google.Cloud.Spanner.V1;

namespace Google.Cloud.SpannerLib
{

    public class LibTransaction
    {
        internal LibConnection LibConnection { get; private set; }
        internal long Id { get; }

        internal LibTransaction(LibConnection libConnection, long id)
        {
            LibConnection = libConnection;
            Id = id;
        }

        internal void BufferWrite(BatchWriteRequest.Types.MutationGroup mutations)
        {
            Spanner.BufferWrite(this, mutations);
        }

        internal LibRows Execute(ExecuteSqlRequest statement)
        {
            return Spanner.ExecuteTransaction(this, statement);
        }

        internal void Commit()
        {
            Spanner.Commit(this);
        }

        internal void Rollback()
        {
            Spanner.Rollback(this);
        }

        internal void Close()
        {
            Spanner.Rollback(this);
        }

    }
}