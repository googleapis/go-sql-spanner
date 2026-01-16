using System;
using System.Buffers.Binary;
using System.IO;
using System.Net.Sockets;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Google.Rpc;

namespace Google.Cloud.SpannerLib.SocketServer.Message;

internal abstract class Message
{
    internal const byte StatusMessageId = (byte) '0';
    internal const byte StartupMessageId = (byte) 'S';

    internal static Message ReadMessage(NetworkStream stream)
    {
        var id = stream.ReadByte();
        if (id == -1)
        {
            throw new EndOfStreamException();
        }

        var messageId = (byte)id;
        return messageId switch
        {
            StatusMessageId => StatusMessage.Read(stream),
            _ => throw new InvalidOperationException("Unknown message id: " + messageId)
        };
    }

    internal static void ReadStatusMessage(NetworkStream stream)
    {
        var message = ReadMessage(stream);
        if (message is StatusMessage statusMessage)
        {
            if (statusMessage.Status.Code == (int) Code.Ok)
            {
                return;
            }
            throw new SpannerException(statusMessage.Status);
        }
        throw new InvalidOperationException("Unexpected message type: " + message.GetType().Name);
    }
    
}