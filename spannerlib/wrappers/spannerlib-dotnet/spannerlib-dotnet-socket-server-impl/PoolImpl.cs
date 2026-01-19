using System;

namespace Google.Cloud.SpannerLib.SocketServer;

internal class PoolImpl : Google.Cloud.SpannerLib.Pool
{
    internal SocketLibSpanner Spanner { get; }
    
    internal string GeneratedId { get; } = Guid.NewGuid().ToString();
    
    internal string ConnectionString { get; }

    internal PoolImpl(SocketLibSpanner spanner, long id, string connectionString) : base(spanner, id)
    {
        Spanner = spanner;
        ConnectionString = connectionString;
    }
}