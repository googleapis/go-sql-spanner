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

namespace Google.Cloud.SpannerLib.Native;

/// <summary>
/// Wraps a GoSlice and implements the IDisposable interface.
/// </summary>
public class DisposableGoSlice : IDisposable
{
    private readonly GCHandle? _handle;
    public GoSlice GoSlice { get; }

    public static DisposableGoSlice Create(byte[]? value)
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

/// <summary>
/// GoSlice is the .NET equivalent of a Go []byte slice.
/// </summary>
public struct GoSlice
{
    public IntPtr Pointer;
    public long Length;
    public long Capacity;

    public GoSlice(IntPtr pointer, long length, long capacity)
    {
        Pointer = pointer;
        Length = length;
        Capacity = capacity;
    }

}