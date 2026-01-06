using System;
using System.Buffers.Binary;
using System.IO;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;

namespace Google.Cloud.SpannerLib.SocketServer.Protocol;

internal static class Encoding
{
    
    private static void ReadExactly(NetworkStream stream, byte[] buffer)
    {
        ReadExactly(stream, buffer, 0, buffer.Length);
    }
    
    private static void ReadExactly(NetworkStream stream, byte[] buffer, int offset, int count)
    {
        var bytesRead = 0;
        while (bytesRead < count)
        {
            var read = stream.Read(buffer, offset + bytesRead, count - bytesRead);
            if (read == 0)
            {
                // The remote host has closed the connection prematurely
                throw new EndOfStreamException("The remote host closed the connection before all data was received.");
            }
            bytesRead += read;
        }
    }
    
    internal static byte[] ReadBytes(NetworkStream stream)
    {
        var length = ReadInt(stream);
        var buffer = new byte[length];
        ReadExactly(stream, buffer);
        return buffer;
    }

    internal static void WriteBytes(NetworkStream stream, byte[] bytes)
    {
        WriteInt(stream, bytes.Length);
        stream.Write(bytes, 0, bytes.Length);
    }

    internal static Task WriteBytesAsync(NetworkStream stream, byte[] bytes, CancellationToken cancellationToken)
    {
        WriteInt(stream, bytes.Length);
        return stream.WriteAsync(bytes, 0, bytes.Length, cancellationToken);
    }
    
    internal static void WriteString(NetworkStream stream, string str)
    {
        var bytes = System.Text.Encoding.UTF8.GetBytes(str);
        WriteInt(stream, bytes.Length);
        stream.Write(bytes, 0, bytes.Length);
    }

    internal static Task WriteStringAsync(NetworkStream stream, string str, CancellationToken cancellationToken)
    {
        var bytes = System.Text.Encoding.UTF8.GetBytes(str);
        WriteInt(stream, bytes.Length);
        return stream.WriteAsync(bytes, 0, bytes.Length, cancellationToken);
    }

    internal static int ReadInt(NetworkStream stream)
    {
        var buffer = new byte[4];
        ReadExactly(stream, buffer);
        return BinaryPrimitives.ReadInt32BigEndian(buffer);
    }

    internal static void WriteInt(NetworkStream stream, int value)
    {
        Span<byte> buffer = stackalloc byte[4];
        BinaryPrimitives.WriteInt32BigEndian(buffer, value);
        stream.Write(buffer);
    }

    internal static long ReadLong(NetworkStream stream)
    {
        var buffer = new byte[8];
        ReadExactly(stream, buffer);
        return BinaryPrimitives.ReadInt64BigEndian(buffer);
    }

    internal static void WriteLong(NetworkStream stream, long value)
    {
        Span<byte> buffer = stackalloc byte[8];
        BinaryPrimitives.WriteInt64BigEndian(buffer, value);
        stream.Write(buffer);
    }

    internal static bool ReadBool(NetworkStream stream)
    {
        var b = stream.ReadByte();
        if (b == -1)
        {
            throw new EndOfStreamException();
        }
        return b != 0;
    }

    internal static void WriteBool(NetworkStream stream, bool value)
    {
        stream.WriteByte((byte)(value ? 1 : 0));
    }

}