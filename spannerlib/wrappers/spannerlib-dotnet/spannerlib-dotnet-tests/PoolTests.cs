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

using Google.Cloud.Spanner.V1;
using Google.Cloud.SpannerLib.MockServer;
using Google.Rpc;
using Grpc.Core;

namespace Google.Cloud.SpannerLib.Tests;

public class PoolTests : AbstractMockServerTests
{
    [Test]
    public void TestCreatePool([Values] LibType libType)
    {
        using var pool = Pool.Create(SpannerLibDictionary[libType], ConnectionString);
        Assert.That(pool, Is.Not.Null);
        Assert.That(pool.Id, Is.GreaterThan(0));
        // Creating a pool should create the underlying client and a multiplexed session.
        Assert.That(Fixture.SpannerMock.Requests.OfType<CreateSessionRequest>().Count(), Is.EqualTo(1));
    }

    [Test]
    public void TestCreatePoolFails([Values] LibType libType)
    {
        Fixture.SpannerMock.AddOrUpdateExecutionTime(nameof(Fixture.SpannerMock.CreateSession), ExecutionTime.CreateException(StatusCode.PermissionDenied, "Not allowed"));

        SpannerException exception = Assert.Throws<SpannerException>(() => Pool.Create(SpannerLibDictionary[libType], ConnectionString));
        Assert.That(exception.Code, Is.EqualTo(Code.PermissionDenied));
    }

}