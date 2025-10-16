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

using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using Google.Cloud.SpannerLib;
using Google.Cloud.SpannerLib.Grpc;
using Google.Cloud.SpannerLib.Native.Impl;

namespace Google.Cloud.Spanner.DataProvider;

internal class SpannerPool
{
    private static ISpannerLib? _gRpcSpannerLib;

    private static ISpannerLib GrpcSpannerLib
    {
        get
        {
            _gRpcSpannerLib ??= new GrpcLibSpanner();
            return _gRpcSpannerLib;
        }
    }
    
    private static ISpannerLib? _nativeSpannerLib;

    private static ISpannerLib NativeSpannerLib
    {
        get
        {
            _nativeSpannerLib ??= new SharedLibSpanner();
            return _nativeSpannerLib;
        }
    }
        
    private static readonly ConcurrentDictionary<string, SpannerPool> Pools = new();

    [MethodImpl(MethodImplOptions.Synchronized)]
    internal static SpannerPool GetOrCreate(string dsn, bool useNativeLibrary = false)
    {
        if (Pools.TryGetValue(dsn, out var value))
        {
            return value;
        }
        var pool = Pool.Create(useNativeLibrary ? NativeSpannerLib : GrpcSpannerLib, dsn);
        var spannerPool = new SpannerPool(dsn, pool);
        Pools[dsn] = spannerPool;
        return spannerPool;
    }

    [MethodImpl(MethodImplOptions.Synchronized)]
    internal static void CloseSpannerLib()
    {
        foreach (var pool in Pools.Values)
        {
            pool.Close();
        }
        Pools.Clear();
        GrpcSpannerLib.Dispose();
        _gRpcSpannerLib = null;
        NativeSpannerLib.Dispose();
        _nativeSpannerLib = null;
    }

    private readonly string _dsn;
    
    private readonly Pool _libPool;

    private SpannerPool(string dsn, Pool libPool)
    {
        _dsn = dsn;
        _libPool = libPool;
    }

    internal void Close()
    {
        _libPool.Close();
        Pools.Remove(_dsn, out _);
    }

    internal Connection CreateConnection()
    {
        return _libPool.CreateConnection();
    }
}