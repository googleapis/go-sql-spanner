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

using System.Data;
using AdoNet.Specification.Tests;
using Google.Cloud.SpannerLib.MockServer;
using Xunit;
using TypeCode = Google.Cloud.Spanner.V1.TypeCode;

namespace Google.Cloud.Spanner.DataProvider.SpecificationTests;

public class DataReaderTests(DbFactoryFixture fixture) : DataReaderTestBase<DbFactoryFixture>(fixture)
{

    [Fact(Skip = "Getting stats after closing a DataReader is not supported")]
    public override void RecordsAffected_returns_negative_1_after_close_when_no_rows()
    {
    }

    [Fact(Skip = "Getting stats after closing a DataReader is not supported")]
    public override void RecordsAffected_returns_negative_1_after_dispose_when_no_rows()
    {
    }
    
    public override void GetFieldValue_works_utf8_four_bytes()
    {
        Fixture.MockServerFixture.SpannerMock.AddOrUpdateStatementResult("SELECT '😀';",
            StatementResult.CreateSingleColumnResultSet(new V1.Type{Code = TypeCode.String}, "c", "😀"));
        base.GetFieldValue_works_utf8_four_bytes();
    }

    public override void GetString_works_utf8_four_bytes()
    {
        Fixture.MockServerFixture.SpannerMock.AddOrUpdateStatementResult("SELECT '😀';",
            StatementResult.CreateSingleColumnResultSet(new V1.Type{Code = TypeCode.String}, "c", "😀"));
        base.GetString_works_utf8_four_bytes();
    }

    public override void GetValue_to_string_works_utf8_four_bytes()
    {
        Fixture.MockServerFixture.SpannerMock.AddOrUpdateStatementResult("SELECT '😀';",
            StatementResult.CreateSingleColumnResultSet(new V1.Type{Code = TypeCode.String}, "c", "😀"));
        base.GetValue_to_string_works_utf8_four_bytes();
    }

    public override void GetFieldValue_works_utf8_three_bytes()
    {
        Fixture.MockServerFixture.SpannerMock.AddOrUpdateStatementResult("SELECT 'Ḁ';",
            StatementResult.CreateSingleColumnResultSet(new V1.Type{Code = TypeCode.String}, "c", "Ḁ"));
        base.GetFieldValue_works_utf8_three_bytes();
    }

    public override void GetFieldValue_works_utf8_two_bytes()
    {
        Fixture.MockServerFixture.SpannerMock.AddOrUpdateStatementResult("SELECT 'Ä';",
            StatementResult.CreateSingleColumnResultSet(new V1.Type{Code = TypeCode.String}, "c", "Ä"));
        base.GetFieldValue_works_utf8_two_bytes();
    }

    public override void GetValues_works()
    {
        Fixture.MockServerFixture.SpannerMock.AddOrUpdateStatementResult("SELECT 'a', NULL;",
            StatementResult.CreateResultSet([Tuple.Create(TypeCode.String, "c1"), Tuple.Create(TypeCode.Int64, "c2")], [["a", DBNull.Value]]));
        base.GetValues_works();
    }

    public override void Item_by_name_works()
    {
        Fixture.MockServerFixture.SpannerMock.AddOrUpdateStatementResult("SELECT 'test' AS Id;",
            StatementResult.CreateSingleColumnResultSet(new V1.Type{Code = TypeCode.String}, "Id", "test"));
        base.Item_by_name_works();
    }

    [Fact(Skip = "The default implementation of GetTextReader returns an empty reader for null values")]
    public override void GetTextReader_throws_for_null_String()
    {
    }
}