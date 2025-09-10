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

    public Rows Execute(ExecuteSqlRequest statement)
    {
        return Spanner.Execute(Connection, statement);
    }

    public CommitResponse Commit()
    {
        MarkDisposed();
        return Spanner.Commit(Connection);
    }

    public void Rollback()
    {
        MarkDisposed();
        Spanner.Rollback(Connection);
    }

    protected override void CloseLibObject()
    {
        try
        {
            Spanner.Rollback(Connection);
        }
        catch (Exception)
        {
            // ignore any exceptions
        }
    }

}