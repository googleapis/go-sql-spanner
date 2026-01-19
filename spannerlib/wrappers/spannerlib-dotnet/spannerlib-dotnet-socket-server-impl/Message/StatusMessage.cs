using System.Net.Sockets;
using Google.Rpc;

namespace Google.Cloud.SpannerLib.SocketServer.Message;

internal class StatusMessage : Message
{
    internal Status Status { get; }
    
    internal static StatusMessage Read(NetworkStream stream)
    {
        var bytes = ReadBytes(stream);
        var status = Status.Parser.ParseFrom(bytes);
        return new StatusMessage(status);
    }

    internal StatusMessage(Status status)
    {
        Status = status;
    }
}