using System;
using System.Collections.Generic;
using Google.Cloud.Spanner.V1;

namespace Google.Cloud.SpannerLib
{

    public class Connection : AbstractLibObject
    {
        internal Pool Pool { get; }
        internal long Id { get; }

        internal Connection(Pool pool, long id)
        {
            Id = id;
            Pool = pool;
        }

        public Transaction BeginTransaction(TransactionOptions transactionOptions)
        {
            return Spanner.BeginTransaction(this, transactionOptions);
        }

        public CommitResponse Apply(BatchWriteRequest.Types.MutationGroup mutations)
        {
            return Spanner.Apply(this, mutations);
        }

        public Rows Execute(ExecuteSqlRequest statement)
        {
            return Spanner.Execute(this, statement);
        }

        public long[] ExecuteBatchDml(List<ExecuteBatchDmlRequest.Types.Statement> statements)
        {
            var request = new ExecuteBatchDmlRequest
            {
                Statements = { statements }
            };
            return Spanner.ExecuteBatchDml(this, request);
        }

        protected override void CloseLibObject()
        {
            Spanner.CloseConnection(this);
        }
    }
}