using System;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;
using Google.Cloud.SpannerLib.SocketServer.Message;

namespace Google.Cloud.SpannerLib.SocketServer;

internal class ConnectionImpl : Connection
{
    private readonly PoolImpl _pool;
    
    internal NetworkStream Stream { get; }

    internal static ConnectionImpl Create(PoolImpl pool)
    {
        NetworkStream? stream = null;
        try
        {
            var socket = pool.Spanner.CreateSocket();
            stream = new NetworkStream(socket);
            ExecuteStartup(stream, pool);
            return new ConnectionImpl(pool, stream);
        }
        catch (Exception)
        {
            stream?.Close();
            throw;
        }
    }

    internal static async Task<ConnectionImpl> CreateAsync(PoolImpl pool, CancellationToken cancellationToken)
    {
        NetworkStream? stream = null;
        try
        {
            Socket socket = await pool.Spanner.CreateSocketAsync().ConfigureAwait(false);
            stream = new NetworkStream(socket);
            await ExecuteStartupAsync(stream, pool,  cancellationToken).ConfigureAwait(false);
            return new ConnectionImpl(pool, stream);
        }
        catch (Exception)
        {
            stream?.Close();
            throw;
        }
    }
    
    private ConnectionImpl(PoolImpl pool, NetworkStream stream) : base(pool, 0)
    {
        _pool = pool;
        Stream = stream;
    }

    private static void ExecuteStartup(NetworkStream stream, PoolImpl pool)
    {
        var startup = new StartupMessage(pool);
        startup.Write(stream);
        stream.Flush();
        Message.Message.ReadStatusMessage(stream);
    }

    private static Task ExecuteStartupAsync(NetworkStream stream, PoolImpl pool, CancellationToken cancellationToken)
    {
        var startup = new StartupMessage(pool);
        return startup.WriteAsync(stream, cancellationToken);
    }
    
}