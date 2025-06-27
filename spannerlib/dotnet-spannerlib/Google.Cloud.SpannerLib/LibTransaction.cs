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

        public void BufferWrite(BatchWriteRequest.Types.MutationGroup mutations)
        {
            Spanner.BufferWrite(this, mutations);
        }

        public LibRows Execute(ExecuteSqlRequest statement)
        {
            return Spanner.ExecuteTransaction(this, statement);
        }

        public void Commit()
        {
            Spanner.Commit(this);
        }

        public void Rollback()
        {
            Spanner.Rollback(this);
        }

        public void Close()
        {
            Spanner.Rollback(this);
        }

    }
}