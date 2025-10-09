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

using AdoNet.Specification.Tests;
using Google.Cloud.Spanner.V1;
using Google.Cloud.SpannerLib.MockServer;
using Xunit;
using TypeCode = Google.Cloud.Spanner.V1.TypeCode;

namespace Google.Cloud.Spanner.DataProvider.SpecificationTests;

public class ParameterTests(DbFactoryFixture fixture) : ParameterTestBase<DbFactoryFixture>(fixture)
{
    protected override Task OnInitializeAsync()
    {
        Fixture.Reset();
        return base.OnInitializeAsync();
    }

    [Fact(Skip = "Spanner assumes that it is a positional parameter if it has no name")]
    public override void Bind_requires_set_name()
    {
    }

    [Fact(Skip = "Unknown parameters are converted to strings")]
    public override void Bind_throws_when_unknown()
    {
    }

    [Fact]
    public override void Bind_works_with_byte_array()
    {
        Fixture.MockServerFixture.SpannerMock.AddOrUpdateStatementResult("SELECT @Parameter;",
            StatementResult.CreateSingleColumnResultSet(new V1.Type{Code = TypeCode.Bytes}, "Parameter", new byte[]{1,2,3,4}));
        base.Bind_works_with_byte_array();
        
        var requests = Fixture.MockServerFixture.SpannerMock.Requests.OfType<ExecuteSqlRequest>();
        var request = Assert.Single(requests);
        Assert.Equal(Convert.ToBase64String(new byte[]{1,2,3,4}), request.Params.Fields["Parameter"].StringValue);
        // The parameter value should be sent as an untyped string.
        Assert.False(request.ParamTypes.ContainsKey("Parameter"));
    }

    [Fact]
    public override void Bind_works_with_stream()
    {
        Fixture.MockServerFixture.SpannerMock.AddOrUpdateStatementResult("SELECT @Parameter;",
            StatementResult.CreateSingleColumnResultSet(new V1.Type{Code = TypeCode.Bytes}, "Parameter", new byte[]{1,2,3,4}));
        base.Bind_works_with_stream();
        
        var requests = Fixture.MockServerFixture.SpannerMock.Requests.OfType<ExecuteSqlRequest>();
        var request = Assert.Single(requests);
        Assert.Equal(Convert.ToBase64String(new byte[]{1,2,3,4}), request.Params.Fields["Parameter"].StringValue);
        // The parameter value should be sent as an untyped string.
        Assert.False(request.ParamTypes.ContainsKey("Parameter"));
    }

    [Fact]
    public override void Bind_works_with_string()
    {
        Fixture.MockServerFixture.SpannerMock.AddOrUpdateStatementResult("SELECT @Parameter;",
            StatementResult.CreateSingleColumnResultSet(new V1.Type{Code = TypeCode.String}, "Parameter", "test"));
        base.Bind_works_with_string();
        
        var requests = Fixture.MockServerFixture.SpannerMock.Requests.OfType<ExecuteSqlRequest>();
        var request = Assert.Single(requests);
        Assert.Equal("test", request.Params.Fields["Parameter"].StringValue);
        // The parameter value should be sent as an untyped string.
        Assert.False(request.ParamTypes.ContainsKey("Parameter"));
    }

}