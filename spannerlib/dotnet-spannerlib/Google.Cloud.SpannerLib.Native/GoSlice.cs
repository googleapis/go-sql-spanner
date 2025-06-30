using System;
using System.Runtime.InteropServices;

namespace Google.Cloud.SpannerLib.Native
{

    internal class DisposableGoSlice : IDisposable
    {
        private readonly GCHandle? _handle;
        internal GoSlice GoSlice { get; }

        internal static DisposableGoSlice Create(byte[]? value)
        {
            var (handle, length, capacity) = Pin(value);
            return new DisposableGoSlice(handle,
                new GoSlice(handle?.AddrOfPinnedObject() ?? IntPtr.Zero, length, capacity));
        }

        private DisposableGoSlice(GCHandle? handle, GoSlice goSlice)
        {
            _handle = handle;
            GoSlice = goSlice;
        }

        private static (GCHandle?, int, int) Pin(byte[]? value)
        {
            if (value is null)
            {
                return (null, 0, 0);
            }

            var length = value.Length;
            var handle = GCHandle.Alloc(value, GCHandleType.Pinned);
            return (handle, length, length);
        }

        public void Dispose()
        {
            _handle?.Free();
        }

    }

    internal struct GoSlice
    {
        public IntPtr Pointer;
        public long Length;
        public long Capacity;

        internal GoSlice(IntPtr pointer, long length, long capacity)
        {
            Pointer = pointer;
            Length = length;
            Capacity = capacity;
        }

    }
}