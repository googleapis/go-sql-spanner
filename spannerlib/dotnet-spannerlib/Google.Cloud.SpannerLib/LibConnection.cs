using System.Collections.Generic;
using Google.Cloud.Spanner.V1;

namespace Google.Cloud.SpannerLib
{

    internal class LibConnection
    {
        internal LibPool LibPool { get; private set; }
        internal long Id { get; private set; }

        internal LibConnection(LibPool libPool, long id)
        {
            Id = id;
            LibPool = libPool;
        }

        internal CommitResponse Apply(BatchWriteRequest.Types.MutationGroup mutations)
        {
            return Spanner.Apply(this, mutations);
        }

        internal LibRows Execute(ExecuteSqlRequest statement)
        {
            return Spanner.Execute(this, statement);
        }

        internal long[] ExecuteBatchDml(List<ExecuteBatchDmlRequest.Types.Statement> statements)
        {
            var request = new ExecuteBatchDmlRequest
            {
                Statements = { statements }
            };
            return Spanner.ExecuteBatchDml(this, request);
        }

        internal void Close()
        {
            Spanner.CloseConnection(this);
        }
    }
}