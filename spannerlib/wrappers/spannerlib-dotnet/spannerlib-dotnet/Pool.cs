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

namespace Google.Cloud.SpannerLib;

/// <summary>
/// A pool of connections to Spanner. All connections that are created by a pool share the same underlying Spanner
/// client and gRPC channel pool.
/// </summary>
/// <param name="spanner">The SpannerLib instance that was used to create this pool.</param>
/// <param name="id">The id of the pool. This value is generated and returned by SpannerLib.</param>
public class Pool(ISpannerLib spanner, long id) : AbstractLibObject(spanner, id)
{
    
    /// <summary>
    /// Create a new Pool using the given SpannerLib instance. This is a relatively expensive operation.
    /// </summary>
    /// <param name="spanner">
    /// The SpannerLib instance to use to create the pool, and that will be used for all subsequent function calls for
    /// objects that have been created from this pool.
    /// </param>
    /// <param name="connectionString">The connectionString to use to connect to Spanner</param>
    /// <returns>A new pool</returns>
    public static Pool Create(ISpannerLib spanner, string connectionString)
    {
        return spanner.CreatePool(connectionString);
    }

    /// <summary>
    /// Create a new connection in this pool. This is a relatively cheap operation.
    /// </summary>
    /// <returns>A new connection in this pool</returns>
    public Connection CreateConnection()
    {
        CheckDisposed();
        return Spanner.CreateConnection(this);
    }

    /// <summary>
    /// Closes this pool and any open connections in the pool.
    /// </summary>
    protected override void CloseLibObject()
    {
        Spanner.ClosePool(this);
    }
}
