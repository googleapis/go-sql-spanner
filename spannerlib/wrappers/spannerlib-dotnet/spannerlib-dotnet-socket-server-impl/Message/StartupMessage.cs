using System;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;

namespace Google.Cloud.SpannerLib.SocketServer.Message;

internal class StartupMessage : Message
{
    private static readonly byte MessageId = (byte)'S';
    
    private PoolImpl Pool { get; }

    internal StartupMessage(PoolImpl pool)
    {
        Pool = pool;
    }

    internal void Write(NetworkStream stream)
    {
        stream.WriteByte(MessageId);
        WriteString(stream, Pool.GeneratedId);
        WriteString(stream, Pool.ConnectionString);
    }

    internal async Task WriteAsync(NetworkStream stream, CancellationToken cancellationToken)
    {
        stream.WriteByte(MessageId);
        await WriteStringAsync(stream, Pool.GeneratedId, cancellationToken).ConfigureAwait(false);
        await WriteStringAsync(stream, Pool.ConnectionString,  cancellationToken).ConfigureAwait(false);
    }
}
