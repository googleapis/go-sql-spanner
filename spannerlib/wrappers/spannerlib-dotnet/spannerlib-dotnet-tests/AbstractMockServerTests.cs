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

using Google.Cloud.SpannerLib.Grpc;
using Google.Cloud.SpannerLib.MockServer;
using Google.Cloud.SpannerLib.Native.Impl;
using NUnit.Framework.Internal;

namespace Google.Cloud.SpannerLib.Tests;

public abstract class AbstractMockServerTests
{
    public enum LibType
    {
        Shared,
        Grpc,
        GrpcTcp,
    }
    
    protected readonly Dictionary<LibType, ISpannerLib> SpannerLibDictionary = new([
        new KeyValuePair<LibType, ISpannerLib>(LibType.Shared, new SharedLibSpanner()),
        new KeyValuePair<LibType, ISpannerLib>(LibType.Grpc, new GrpcLibSpanner()),
        new KeyValuePair<LibType, ISpannerLib>(LibType.GrpcTcp, new GrpcLibSpanner(addressType: Server.AddressType.Tcp)),
    ]);
    
    protected SpannerMockServerFixture Fixture;
        
    protected string ConnectionString =>  $"{Fixture.Host}:{Fixture.Port}/projects/p1/instances/i1/databases/d1;UsePlainText=true";
        
    [OneTimeSetUp]
    public void Setup()
    {
        Fixture = new SpannerMockServerFixture();
        Fixture.SpannerMock.AddOrUpdateStatementResult("SELECT 1", StatementResult.CreateSelect1ResultSet());
    }
        
    [OneTimeTearDown]
    public void Teardown()
    {
        foreach (var spannerLib in SpannerLibDictionary.Values)
        {
            spannerLib.Dispose();
        }
        Fixture.Dispose();
    }

    [TearDown]
    public void Reset()
    {
        Fixture.SpannerMock.Reset();
        Fixture.DatabaseAdminMock.Reset();
        Fixture.SpannerMock.AddOrUpdateStatementResult("SELECT 1", StatementResult.CreateSelect1ResultSet());
    }
    
}