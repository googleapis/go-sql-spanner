using System;
using Google.Cloud.Spanner.V1;

namespace Google.Cloud.SpannerLib;

public class Transaction : AbstractLibObject
{
    internal Connection SpannerConnection { get; private set; }

    internal Transaction(Connection connection, long id) : base(connection.Spanner, id)
    {
        SpannerConnection = connection;
    }

    public Rows Execute(ExecuteSqlRequest statement)
    {
        return Spanner.Execute(SpannerConnection, statement);
    }

    public CommitResponse Commit()
    {
        MarkDisposed();
        return Spanner.Commit(SpannerConnection);
    }

    public void Rollback()
    {
        MarkDisposed();
        Spanner.Rollback(SpannerConnection);
    }

    protected override void CloseLibObject()
    {
        try
        {
            Spanner.Rollback(SpannerConnection);
        }
        catch (Exception)
        {
            // ignore any exceptions
        }
    }

}