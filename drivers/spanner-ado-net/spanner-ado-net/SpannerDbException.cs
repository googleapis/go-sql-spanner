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
using System.Data.Common;
using Google.Cloud.SpannerLib;

namespace Google.Cloud.Spanner.DataProvider;

public class SpannerDbException : DbException
{
    internal static T TranslateException<T>(Func<T> func)
    {
        try
        {
            return func();
        }
        catch (SpannerException exception)
        {
            throw new SpannerDbException(exception);
        }
    }
    
    private SpannerException SpannerException { get; }

    internal SpannerDbException(SpannerException spannerException) : base(spannerException.Message, spannerException.Status.Code)
    {
        SpannerException = spannerException;
    }
    
}