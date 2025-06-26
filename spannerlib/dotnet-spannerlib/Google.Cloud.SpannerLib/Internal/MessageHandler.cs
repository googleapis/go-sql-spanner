using System;
using System.Data;
using System.Text;

namespace Google.Cloud.SpannerLib.Internal
{

    internal class MessageHandler : IDisposable
    {
        private SpannerLib SpannerLib { get; }
        
        private Message Message { get; }

        internal long Length => Message.Length;

        internal MessageHandler(SpannerLib spannerLib, Message message)
        {
            SpannerLib = spannerLib;
            Message = message;
        }

        internal int Code()
        {
            return Message.Code;
        }

        internal long ObjectId()
        {
            return Message.ObjectId;
        }

        internal bool HasError()
        {
            return Message.Code != 0;
        }

        internal string? Error()
        {
            if (!HasError())
            {
                return null;
            }

            return ValueAsString();
        }

        internal string ValueAsString()
        {
            unsafe
            {
                Span<byte> tmp = new(Message.Pointer, Message.Length);
                return Encoding.UTF8.GetString(tmp);
            }
        }

        internal unsafe ReadOnlySpan<byte> Value()
        {
            return new(Message.Pointer, Message.Length);
        }

        public void Dispose()
        {
            var code = SpannerLib.Release(Message.Pinner);
            if (code != 0)
            {
                throw new DataException("Failed to release message");
            }
        }
    }
}