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
using Google.Cloud.SpannerLib.MockServer;
using Xunit;
using TypeCode = Google.Cloud.Spanner.V1.TypeCode;

namespace Google.Cloud.Spanner.DataProvider.SpecificationTests;

public class CommandTests(DbFactoryFixture fixture) : CommandTestBase<DbFactoryFixture>(fixture)
{
    [Fact]
    public override void Execute_throws_for_unknown_ParameterValue_type()
    {
        Fixture.MockServerFixture.SpannerMock.AddOrUpdateStatementResult("SELECT @Parameter;",
            StatementResult.CreateSingleColumnResultSet(new V1.Type{Code = TypeCode.String}, "Parameter", new CustomClass().ToString()));
        base.Execute_throws_for_unknown_ParameterValue_type();
    }

    [Fact]
    public override void ExecuteReader_binds_parameters()
    {
        Fixture.MockServerFixture.SpannerMock.AddOrUpdateStatementResult("SELECT @Parameter;",
            StatementResult.CreateSingleColumnResultSet(new V1.Type{Code = TypeCode.Int64}, "Parameter", 1L));
        base.ExecuteReader_binds_parameters();
    }

    [Fact]
    public override void ExecuteReader_supports_CloseConnection()
    {
        Fixture.MockServerFixture.SpannerMock.AddOrUpdateStatementResult("SELECT 0;",
            StatementResult.CreateSingleColumnResultSet(new V1.Type{Code = TypeCode.Int64}, "c", 0L));
        base.ExecuteReader_supports_CloseConnection();
    }

    [Fact]
    public override void ExecuteReader_works_when_trailing_comments()
    {
        Fixture.MockServerFixture.SpannerMock.AddOrUpdateStatementResult("SELECT 0; -- My favorite number",
            StatementResult.CreateSingleColumnResultSet(new V1.Type{Code = TypeCode.Int64}, "c", 0L));
        base.ExecuteReader_works_when_trailing_comments();
    }

    [Fact]
    public override void ExecuteScalar_returns_DBNull_when_null()
    {
        Fixture.MockServerFixture.SpannerMock.AddOrUpdateStatementResult("SELECT NULL;",
            StatementResult.CreateSingleColumnResultSet(new V1.Type{Code = TypeCode.Int64}, "c", DBNull.Value));
        base.ExecuteScalar_returns_DBNull_when_null();
    }

    [Fact]
    public override void ExecuteScalar_returns_first_when_multiple_columns()
    {
        Fixture.MockServerFixture.SpannerMock.AddOrUpdateStatementResult("SELECT 42, 43;",
            StatementResult.CreateResultSet([Tuple.Create(TypeCode.Int64, "c1"), Tuple.Create(TypeCode.Int64, "c1")], [[42L, 43L]]));
        base.ExecuteScalar_returns_first_when_multiple_columns();
    }

    [Fact]
    public override void ExecuteScalar_returns_real()
    {
        Fixture.MockServerFixture.SpannerMock.AddOrUpdateStatementResult("SELECT 3.14;",
            StatementResult.CreateSingleColumnResultSet(new V1.Type{Code = TypeCode.Float64}, "c", 3.14d));
        base.ExecuteScalar_returns_real();
    }

    [Fact]
    public override void ExecuteScalar_returns_string_when_text()
    {
        Fixture.MockServerFixture.SpannerMock.AddOrUpdateStatementResult("SELECT 'test';",
            StatementResult.CreateSingleColumnResultSet(new V1.Type{Code = TypeCode.String}, "c", "test"));
        base.ExecuteScalar_returns_string_when_text();
    }

    [Fact(Skip = "Spanner does not support empty statements")]
    public override void ExecuteReader_HasRows_is_false_for_comment()
    {
    }
    
    [Fact(Skip = "Spanner does not use the command text once the reader has been opened")]
    public override void CommandText_throws_when_set_when_open_reader()
    {
    }

    [Fact(Skip = "Spanner does not need the connection after the reader has been opened")]
    public override void Connection_throws_when_set_when_open_reader()
    {
    }

    [Fact(Skip = "Spanner does not need the connection after the reader has been opened")]
    public override void Connection_throws_when_set_to_null_when_open_reader()
    {
    }

    [Fact(Skip = "Spanner supports multiple open readers for one command")]
    public override void ExecuteReader_throws_when_reader_open()
    {
    }

    [Fact(Skip = "Spanner only supports one transaction per connection and therefore ignores the transaction property")]
    public override void ExecuteReader_throws_when_transaction_required()
    {
    }

}
