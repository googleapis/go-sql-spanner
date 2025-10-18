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
using System.Data;
using System.Text;

namespace Google.Cloud.SpannerLib.Native;

/// <summary>
/// MessageHandler is a wrapper class around Message that implements the IDisposable interface.
/// </summary>
/// <param name="message">The message that should be wrapped</param>
public class MessageHandler(Message message) : IDisposable
{
    private Message Message { get; } = message;

    public long Length => Message.Length;

    /// <returns>The result code of the function call</returns>
    public int Code()
    {
        return Message.Code;
    }

    /// <returns>The ID of the object that was created, or zero if no object was created.</returns>
    public long ObjectId()
    {
        return Message.ObjectId;
    }

    /// <returns>True if the result code is non-zero, and otherwise false.</returns>
    public bool HasError()
    {
        return Message.Code != 0;
    }

    /// <returns>The data in this message as a .NET string</returns>
    internal string ValueAsString()
    {
        unsafe
        {
            Span<byte> tmp = new(Message.Pointer, Message.Length);
            return Encoding.UTF8.GetString(tmp);
        }
    }

    /// <returns>A view of the underlying data</returns>
    public unsafe ReadOnlySpan<byte> Value()
    {
        return new(Message.Pointer, Message.Length);
    }

    public void Dispose()
    {
        var code = SpannerLib.Release(Message.Pinner);
        if (code != 0)
        {
            throw new Exception("Failed to release message");
        }
    }
}