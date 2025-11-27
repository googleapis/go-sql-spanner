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

using Google.Cloud.Spanner.Admin.Database.V1;
using Google.Cloud.Spanner.V1;
using Google.Cloud.SpannerLib;
using Google.Cloud.SpannerLib.MockServer;
using TypeCode = Google.Cloud.Spanner.V1.TypeCode;

namespace Google.Cloud.Spanner.DataProvider.Tests;

public class SqlParserTests : AbstractMockServerTests
{
    [Test]
    public void ParameterSimple()
    {
        const string sql = "SELECT @p1, @p2";
        Fixture.SpannerMock.AddOrUpdateStatementResult(sql, StatementResult.CreateResultSet(
            [Tuple.Create(TypeCode.String, "p1"), Tuple.Create(TypeCode.String, "p2")], [["foo", "foo"]]));
        
        using var conn = OpenConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        cmd.AddParameter("p1", "foo");
        cmd.AddParameter("p2", "foo");
        
        using var reader = cmd.ExecuteReader();

        var request = Fixture.SpannerMock.Requests.OfType<ExecuteSqlRequest>().First();
        Assert.That(request, Is.Not.Null);
        Assert.That(request.Sql, Is.EqualTo("SELECT @p1, @p2"));
        Assert.That(request.Params.Fields["p1"].StringValue, Is.EqualTo("foo"));
        Assert.That(request.Params.Fields["p2"].StringValue, Is.EqualTo("foo"));
    }

    [Test]
    public void ParameterNameWithDot()
    {
        const string sql = "SELECT @a.parameter";
        Fixture.SpannerMock.AddOrUpdateStatementResult(sql, StatementResult.CreateResultSet(
            [Tuple.Create(TypeCode.String, "p1"), Tuple.Create(TypeCode.String, "p2")], [["foo", "foo"]]));
        
        using var conn = OpenConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        cmd.AddParameter("a", "foo");
        
        using var reader = cmd.ExecuteReader();

        var request = Fixture.SpannerMock.Requests.OfType<ExecuteSqlRequest>().First();
        Assert.That(request, Is.Not.Null);
        Assert.That(request.Sql, Is.EqualTo("SELECT @a.parameter"));
        Assert.That(request.Params.Fields.Count, Is.EqualTo(1));
        Assert.That(request.Params.Fields["a"].StringValue, Is.EqualTo("foo"));
    }

    [Test, Description("Checks several scenarios in which the SQL is supposed to pass untouched")]
    [TestCase(@"SELECT to_tsvector('fat cats ate rats') @@ to_tsquery('cat & rat')", TestName="AtAt")]
    [TestCase(@"SELECT 'cat'::tsquery @> 'cat & rat'::tsquery", TestName = "AtGt")]
    [TestCase(@"SELECT 'cat'::tsquery <@ 'cat & rat'::tsquery", TestName = "AtLt")]
    [TestCase(@"SELECT 'b''la'", TestName = "DoubleTicks")]
    [TestCase(@"SELECT 'type(''m.response'')#''O''%'", TestName = "DoubleTicks2")]
    [TestCase(@"SELECT 'abc'':str''a:str'", TestName = "DoubleTicks3")]
    [TestCase(@"SELECT 1 FROM "":str""", TestName = "DoubleQuoted")]
    [TestCase(@"SELECT 1 FROM 'yo'::str", TestName = "DoubleColons")]
    [TestCase("SELECT $\u00ffabc0$literal string :str :int$\u00ffabc0 $\u00ffabc0$", TestName = "DollarQuotes")]
    [TestCase("SELECT $$:str$$", TestName = "DollarQuotesNoTag")]
    public void UntouchedPostgresql(string sql)
    {
        Fixture.SpannerMock.AddDialectResult(DatabaseDialect.Postgresql);
        Fixture.SpannerMock.AddOrUpdateStatementResult(sql, StatementResult.CreateResultSet(
            [Tuple.Create(TypeCode.String, "c")], [[1L]]));
        
        using var conn = OpenConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        
        using var reader = cmd.ExecuteReader();

        var request = Fixture.SpannerMock.Requests.OfType<ExecuteSqlRequest>().First();
        Assert.That(request, Is.Not.Null);
        Assert.That(request.Sql, Is.EqualTo(sql));
    }

    [Test]
    [TestCase(@"SELECT 1<@param", TestName = "LessThan")]
    [TestCase(@"SELECT 1>@param", TestName = "GreaterThan")]
    [TestCase(@"SELECT 1<>@param", TestName = "NotEqual")]
    [TestCase("SELECT--comment\r@param", TestName="LineComment")]
    public void ParameterGetsBound(string sql)
    {
        Fixture.SpannerMock.AddOrUpdateStatementResult(sql, StatementResult.CreateResultSet(
            [Tuple.Create(TypeCode.Int64, "c")], [[1L]]));
        
        using var conn = OpenConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        cmd.AddParameter("param", 1L);
        
        using var reader = cmd.ExecuteReader();

        var request = Fixture.SpannerMock.Requests.OfType<ExecuteSqlRequest>().First();
        Assert.That(request, Is.Not.Null);
        Assert.That(request.Sql, Is.EqualTo(sql));
        Assert.That(request.Params.Fields.Count, Is.EqualTo(1));
        Assert.That(request.Params.Fields["param"].StringValue, Is.EqualTo("1"));
    }
    
    [Test]
    [TestCase(@"SELECT e'ab\'c @param'", TestName = "Estring")]
    [TestCase(@"SELECT/*/* -- nested comment @int /*/* *//*/ **/*/*/*/1")]
    [TestCase(@"SELECT 1,
-- Comment, @param and also :param
2", TestName = "LineComment")]
    public void ParameterDoesNotGetBoundPostgresql(string sql)
    {
        Fixture.SpannerMock.AddDialectResult(DatabaseDialect.Postgresql);
        Fixture.SpannerMock.AddOrUpdateStatementResult(sql, StatementResult.CreateResultSet(
            [Tuple.Create(TypeCode.Int64, "c")], [[1L]]));
        
        using var conn = OpenConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        
        using var reader = cmd.ExecuteReader();

        var request = Fixture.SpannerMock.Requests.OfType<ExecuteSqlRequest>().First();
        Assert.That(request, Is.Not.Null);
        Assert.That(request.Sql, Is.EqualTo(sql));
        Assert.That(request.Params.Fields.Count, Is.EqualTo(0));
    }
    
    [Test]
    public void NonConformingString()
    {
        const string sql = @"SELECT e'abc\'?''a ?'";
        Fixture.SpannerMock.AddDialectResult(DatabaseDialect.Postgresql);
        Fixture.SpannerMock.AddOrUpdateStatementResult(sql, StatementResult.CreateResultSet(
            [Tuple.Create(TypeCode.Int64, "c")], [[1L]]));
        
        using var conn = OpenConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        
        using var reader = cmd.ExecuteReader();

        var request = Fixture.SpannerMock.Requests.OfType<ExecuteSqlRequest>().First();
        Assert.That(request, Is.Not.Null);
        Assert.That(request.Sql, Is.EqualTo(sql));
        Assert.That(request.Params.Fields.Count, Is.EqualTo(0));
    }

    [Ignore("Requires multi-statement support")]
    [Test]
    public void MultiqueryWithParameters()
    {
        const string sql1 = "select @p3, @p1";
        const string sql2 = "select @p2, @p3";
        Fixture.SpannerMock.AddOrUpdateStatementResult(sql1, StatementResult.CreateResultSet(
            [Tuple.Create(TypeCode.Int64, "p3"), Tuple.Create(TypeCode.Int64, "p1")], [[3L, 1L]]));
        Fixture.SpannerMock.AddOrUpdateStatementResult(sql2, StatementResult.CreateResultSet(
            [Tuple.Create(TypeCode.Int64, "p2"), Tuple.Create(TypeCode.Int64, "p3")], [[2L, 3L]]));
        
        using var conn = OpenConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = $"{sql1}; {sql2}";
        cmd.AddParameter("p1", 1L);
        cmd.AddParameter("p2", 2L);
        cmd.AddParameter("p3", 3L);
        
        using var reader = cmd.ExecuteReader();

        var requests = Fixture.SpannerMock.Requests.OfType<ExecuteSqlRequest>().ToList();
        Assert.That(requests.Count, Is.EqualTo(2));
        Assert.That(requests[0].Sql, Is.EqualTo(sql1));
        Assert.That(requests[1].Sql, Is.EqualTo(sql2));
        Assert.That(requests[0].Params.Fields.Count, Is.EqualTo(2));
        Assert.That(requests[1].Params.Fields.Count, Is.EqualTo(2));
        
        Assert.That(requests[0].Params.Fields["p1"].StringValue, Is.EqualTo("1"));
        Assert.That(requests[0].Params.Fields["p3"].StringValue, Is.EqualTo("3"));
        
        Assert.That(requests[1].Params.Fields["p2"].StringValue, Is.EqualTo("2"));
        Assert.That(requests[1].Params.Fields["p3"].StringValue, Is.EqualTo("3"));
    }

    [Test]
    public void MissingParameterIsIgnored()
    {
        const string sql = "SELECT @p";
        Fixture.SpannerMock.AddOrUpdateStatementResult(sql, StatementResult.CreateResultSet(
            [Tuple.Create(TypeCode.Int64, "c")], [[1L]]));
        
        using var conn = OpenConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        
        using var reader = cmd.ExecuteReader();

        var request = Fixture.SpannerMock.Requests.OfType<ExecuteSqlRequest>().First();
        Assert.That(request, Is.Not.Null);
        Assert.That(request.Sql, Is.EqualTo(sql));
        Assert.That(request.Params.Fields.Count, Is.EqualTo(1));
        Assert.That(request.Params.Fields["p"].HasNullValue);
    }

    [Ignore("Requires multi-statement support")]
    [Test]
    public void ConsecutiveSemicolons()
    {
        const string sql = "SELECT 1;;";
        
        using var conn = OpenConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        
        using var reader = cmd.ExecuteReader();

        Assert.That(Fixture.SpannerMock.Requests.OfType<ExecuteSqlRequest>().Count(), Is.EqualTo(1));
    }

    [Ignore("Requires multi-statement support")]
    [Test]
    public void TrailingSemicolon()
    {
        const string sql = "SELECT 1;";
        
        using var conn = OpenConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        
        using var reader = cmd.ExecuteReader();

        Assert.That(Fixture.SpannerMock.Requests.OfType<ExecuteSqlRequest>().Count(), Is.EqualTo(1));
    }

    [Ignore("Requires empty command support")]
    [Test]
    public void Empty()
    {
        const string sql = "";
        
        using var conn = OpenConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        
        using var reader = cmd.ExecuteReader();

        Assert.That(Fixture.SpannerMock.Requests.OfType<ExecuteSqlRequest>().Count(), Is.EqualTo(0));
    }

    [Ignore("Requires multi-statement support")]
    [Test]
    public void SemicolonInParentheses()
    {
        const string sql = "CREATE OR REPLACE RULE test AS ON UPDATE TO test DO (SELECT 1; SELECT 1)";
        
        using var conn = OpenConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        
        using var reader = cmd.ExecuteReader();

        Assert.That(Fixture.SpannerMock.Requests.OfType<ExecuteSqlRequest>().Count(), Is.EqualTo(1));
    }

    [Ignore("Requires multi-statement support")]
    [Test]
    public void SemicolonAfterParentheses()
    {
        const string sql = "CREATE OR REPLACE RULE test AS ON UPDATE TO test DO (SELECT 1); SELECT 1";
        
        using var conn = OpenConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        
        using var reader = cmd.ExecuteReader();

        Assert.That(Fixture.SpannerMock.Requests.OfType<ExecuteSqlRequest>().Count(), Is.EqualTo(2));
    }

}