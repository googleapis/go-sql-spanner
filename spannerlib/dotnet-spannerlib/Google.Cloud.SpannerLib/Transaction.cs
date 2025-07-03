using System;
using Google.Cloud.Spanner.V1;

namespace Google.Cloud.SpannerLib
{

    public class Transaction : AbstractLibObject
    {
        internal Connection Connection { get; private set; }
        internal long Id { get; }

        internal Transaction(Connection connection, long id)
        {
            Connection = connection;
            Id = id;
        }

        public void BufferWrite(BatchWriteRequest.Types.MutationGroup mutations)
        {
            Spanner.BufferWrite(this, mutations);
        }

        public Rows Execute(ExecuteSqlRequest statement)
        {
            return Spanner.ExecuteTransaction(this, statement);
        }

        public CommitResponse Commit()
        {
            MarkDisposed();
            return Spanner.Commit(this);
        }

        public void Rollback()
        {
            MarkDisposed();
            Spanner.Rollback(this);
        }

        protected override void CloseLibObject()
        {
            Spanner.Rollback(this);
        }

    }
}