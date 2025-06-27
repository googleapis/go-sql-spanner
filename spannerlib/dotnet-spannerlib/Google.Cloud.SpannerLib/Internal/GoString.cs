using System;
using System.Runtime.InteropServices;
using System.Text;

namespace Google.Cloud.SpannerLib.Internal
{

    internal unsafe struct GoString : IDisposable
    {
        IntPtr Pointer { get; }
        internal long Length;

        internal GoString(string value)
        {
            (Pointer, Length) = StringToHGlobalUtf8(value);
        }

        private static (IntPtr, int) StringToHGlobalUtf8(string? s)
        {
            if (s is null)
            {
                return (IntPtr.Zero, 0);
            }

            var nb = Encoding.UTF8.GetMaxByteCount(s.Length);
            var ptr = Marshal.AllocHGlobal(nb);

            var pbMem = (byte*)ptr;
            var nbWritten = Encoding.UTF8.GetBytes(s, new Span<byte>(pbMem, nb));

            return (ptr, nbWritten);
        }

        public void Dispose()
        {
            Marshal.FreeHGlobal(Pointer);
        }
    }
}