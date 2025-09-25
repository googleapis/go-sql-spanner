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

namespace Google.Cloud.SpannerLib.Native;

/// <summary>
/// Message is the .NET implementation of the generic Message structure that is returned by all functions in SpannerLib.
/// </summary>
public unsafe struct Message
{
#pragma warning disable CS0649 // Field is never assigned to, and will always have its default value
    /// <summary>
    /// The memory pinner identifier. If non-zero, the caller must call Release(Pinner) to release the memory that was
    /// held by this message.
    /// </summary>
    public long Pinner;
    /// <summary>
    /// The result code of the function call. A non-zero value indicates an error.
    /// </summary>
    public int Code;
    /// <summary>
    /// The ID of the object that was created. Zero for functions that do not create an object.
    /// </summary>
    public long ObjectId;
    /// <summary>
    /// The length of the returned data. Zero for functions that do not return data.
    /// </summary>
    public int Length;
    /// <summary>
    /// A pointer to the returned data. Null for functions that do not return data.
    /// </summary>
    public void* Pointer;
#pragma warning restore CS0649 // Field is never assigned to, and will always have its default value
}