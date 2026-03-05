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
using System.Threading.Tasks;

namespace Google.Cloud.SpannerLib;

/// <summary>
/// This is the base class for all objects that are created by SpannerLib. It implements IDisposable and automatically
/// closes the object in SpannerLib when the object is either being closed, disposed, or finalized.
/// </summary>
public abstract class AbstractLibObject : IDisposable, IAsyncDisposable
{
    internal ISpannerLib Spanner { get; }
    public long Id { get; }

    private bool _disposed;

    protected void CheckDisposed()
    {
        if (_disposed)
        {
            throw new ObjectDisposedException(GetType().Name);
        }
    }

    internal AbstractLibObject(ISpannerLib spanner, long id)
    {
        Spanner = spanner;
        Id = id;
    } 

    ~AbstractLibObject()
    {
        Dispose(false);
    }

    protected void MarkDisposed()
    {
        _disposed = true;
    }

    public void Dispose()
    {
        Dispose(true);
        GC.SuppressFinalize(this);
    }
    
    public async ValueTask DisposeAsync()
    {
        await DisposeAsyncCore().ConfigureAwait(false);
        GC.SuppressFinalize(this);
    }
    
    public void Close()
    {
        Dispose(true);
        GC.SuppressFinalize(this);
    }

    protected virtual void Dispose(bool disposing)
    {
        if (_disposed)
        {
            return;
        }
        try
        {
            if (Id > 0)
            {
                CloseLibObject();
            }
        }
        finally
        {
            _disposed = true;
        }
    }
    
    protected virtual async ValueTask DisposeAsyncCore()
    {
        if (_disposed)
        {
            return;
        }
        try
        {
            if (Id > 0)
            {
                await CloseLibObjectAsync().ConfigureAwait(false);
            }
        }
        finally
        {
            _disposed = true;
        }
    }

    /// <summary>
    /// CloseLibObject should be implemented by concrete subclasses and call the corresponding Close function in
    /// SpannerLib.
    /// </summary>
    protected abstract void CloseLibObject();

    protected virtual ValueTask CloseLibObjectAsync()
    {
        CloseLibObject();
        return default;
    }
}