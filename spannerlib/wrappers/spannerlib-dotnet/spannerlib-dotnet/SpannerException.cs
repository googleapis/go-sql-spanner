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
using Google.Rpc;
using Grpc.Core;
using Status = Google.Rpc.Status;

namespace Google.Cloud.SpannerLib;

/// <summary>
/// SpannerLib returns errors as protobuf Status instances. These are translated to this SpannerException class if a
/// function in SpannerLib returns a non-zero status code.
/// </summary>
/// <param name="status">The status that was returned by SpannerLib</param>
public class SpannerException(Status status) : Exception(CreateMessage(status))
{
    private static string CreateMessage(Status status)
    {
        if (Enum.IsDefined(typeof(StatusCode), status.Code))
        {
            return $"{((StatusCode)status.Code).ToString()}: {status.Message}";
        }
        return status.Message;
    }
    
    public static SpannerException ToSpannerException(RpcException exception)
    {
        return new SpannerException(new Status { Code = (int) exception.Status.StatusCode, Message = exception.Message });
    }
    
    public Status Status { get; } = status;

    public Code Code => (Code)Status.Code;
}