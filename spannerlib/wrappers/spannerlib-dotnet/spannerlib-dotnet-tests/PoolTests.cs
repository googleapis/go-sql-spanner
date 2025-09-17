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
using Google.Cloud.SpannerLib.Native.Impl;
using Google.Rpc;
using Grpc.Core;

namespace Google.Cloud.SpannerLib.Tests;

public class PoolTests
{
    private readonly ISpannerLib _spannerLib = new SharedLibSpanner();
    
    private SpannerMockServerFixture _fixture;
        
    private string ConnectionString =>  $"{_fixture.Host}:{_fixture.Port}/projects/p1/instances/i1/databases/d1;UsePlainText=true";
        
    [SetUp]
    public void Setup()
    {
        _fixture = new SpannerMockServerFixture();
    }
        
    [TearDown]
    public void Teardown()
    {
        _fixture.Dispose();
    }

    [Test]
    public void TestCreatePool()
    {
        using var pool = Pool.Create(_spannerLib, ConnectionString);
        Assert.That(pool, Is.Not.Null);
        Assert.That(pool.Id, Is.GreaterThan(0));
        // Creating a pool should create the underlying client and a multiplexed session.
        Assert.That(_fixture.SpannerMock.Requests.OfType<CreateSessionRequest>().Count(), Is.EqualTo(1));
    }

    [Test]
    public void TestCreatePoolFails()
    {
        _fixture.SpannerMock.AddOrUpdateExecutionTime(nameof(_fixture.SpannerMock.CreateSession), ExecutionTime.CreateException(StatusCode.PermissionDenied, "Not allowed"));

        SpannerException exception = Assert.Throws<SpannerException>(() => Pool.Create(_spannerLib, ConnectionString));
        Assert.That(exception.Code, Is.EqualTo(Code.PermissionDenied));
    }

}