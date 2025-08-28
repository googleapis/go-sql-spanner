using System;
using Google.Cloud.Spanner.V1;

namespace Google.Cloud.SpannerLib;

public class Transaction : AbstractLibObject
{
    internal Connection Connection { get; private set; }

    internal Transaction(Connection connection, long id) : base(connection.Spanner, id)
    {
        Connection = connection;
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
        try
        {
            Spanner.Rollback(this);
        }
        catch (Exception)
        {
            // ignore any exceptions
        }
    }

}