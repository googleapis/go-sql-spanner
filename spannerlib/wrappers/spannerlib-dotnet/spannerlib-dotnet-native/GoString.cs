// Copyright 2025 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

using System;
using System.Runtime.InteropServices;
using System.Text;

namespace Google.Cloud.SpannerLib.Native;

/// <summary>
/// GoString is the .NET equivalent of a Go string.
/// </summary>
public unsafe struct GoString : IDisposable
{
    IntPtr Pointer { get; }
    internal long Length;

    public GoString(string value)
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