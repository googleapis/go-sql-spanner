using System.Data;
using System.Data.Common;
using Google.Cloud.Spanner.V1;
using Google.Cloud.SpannerLib.MockServer;
using TypeCode = Google.Cloud.Spanner.V1.TypeCode;

namespace Google.Cloud.Spanner.DataProvider.Tests;

public class SpannerParameterTests : AbstractMockServerTests
{
    [Test]
    public void SettingValueDoesNotChangeDbType()
    {
        // ReSharper disable once UseObjectOrCollectionInitializer
        var p = new SpannerParameter { DbType = DbType.String };
        p.Value = 8;
        Assert.That(p.DbType, Is.EqualTo(DbType.String));
    }

    [Test]
    public void DefaultConstructor()
    {
        var p = new SpannerParameter();
        Assert.That(p.DbType, Is.EqualTo(DbType.String), "DbType");
        Assert.That(p.Direction, Is.EqualTo(ParameterDirection.Input), "Direction");
        Assert.That(p.IsNullable, Is.False, "IsNullable");
        Assert.That(p.ParameterName, Is.Empty, "ParameterName");
        Assert.That(p.Precision, Is.EqualTo(0), "Precision");
        Assert.That(p.Scale, Is.EqualTo(0), "Scale");
        Assert.That(p.Size, Is.EqualTo(0), "Size");
        Assert.That(p.SourceColumn, Is.Empty, "SourceColumn");
        Assert.That(p.SourceVersion, Is.EqualTo(DataRowVersion.Current), "SourceVersion");
        Assert.That(p.Value, Is.Null, "Value");
    }

    [Test]
    public void ConstructorValueDateTime()
    {
        var value = new DateTime(2004, 8, 24);

        var p = new SpannerParameter("address", value);
        // Setting a parameter value does not change the type.
        Assert.That(p.DbType, Is.EqualTo(DbType.String), "B:DbType");
        Assert.That(p.Direction, Is.EqualTo(ParameterDirection.Input), "B:Direction");
        Assert.That(p.IsNullable, Is.False, "B:IsNullable");
        Assert.That(p.ParameterName, Is.EqualTo("address"), "B:ParameterName");
        Assert.That(p.Precision, Is.EqualTo(0), "B:Precision");
        Assert.That(p.Scale, Is.EqualTo(0), "B:Scale");
        Assert.That(p.Size, Is.EqualTo(0), "B:Size");
        Assert.That(p.SourceColumn, Is.Empty, "B:SourceColumn");
        Assert.That(p.SourceVersion, Is.EqualTo(DataRowVersion.Current), "B:SourceVersion");
        Assert.That(p.Value, Is.EqualTo(value), "B:Value");
    }

    [Test]
    public void ConstructorValueDbNull()
    {
        var p = new SpannerParameter("address", DBNull.Value);
        Assert.That(p.DbType, Is.EqualTo(DbType.String), "B:DbType");
        Assert.That(p.Direction, Is.EqualTo(ParameterDirection.Input), "B:Direction");
        Assert.That(p.IsNullable, Is.False, "B:IsNullable");
        Assert.That(p.ParameterName, Is.EqualTo("address"), "B:ParameterName");
        Assert.That(p.Precision, Is.EqualTo(0), "B:Precision");
        Assert.That(p.Scale, Is.EqualTo(0), "B:Scale");
        Assert.That(p.Size, Is.EqualTo(0), "B:Size");
        Assert.That(p.SourceColumn, Is.Empty, "B:SourceColumn");
        Assert.That(p.SourceVersion, Is.EqualTo(DataRowVersion.Current), "B:SourceVersion");
        Assert.That(p.Value, Is.EqualTo(DBNull.Value), "B:Value");
    }

    [Test]
    public void ConstructorValueNull()
    {
        var p = new SpannerParameter("address", null);
        Assert.That(p.DbType, Is.EqualTo(DbType.String), "A:DbType");
        Assert.That(p.Direction, Is.EqualTo(ParameterDirection.Input), "A:Direction");
        Assert.That(p.IsNullable, Is.False, "A:IsNullable");
        Assert.That(p.ParameterName, Is.EqualTo("address"), "A:ParameterName");
        Assert.That(p.Precision, Is.EqualTo(0), "A:Precision");
        Assert.That(p.Scale, Is.EqualTo(0), "A:Scale");
        Assert.That(p.Size, Is.EqualTo(0), "A:Size");
        Assert.That(p.SourceColumn, Is.Empty, "A:SourceColumn");
        Assert.That(p.SourceVersion, Is.EqualTo(DataRowVersion.Current), "A:SourceVersion");
        Assert.That(p.Value, Is.Null, "A:Value");
    }

    [Test]
    public void Clone()
    {
        var expected = new SpannerParameter
        {
            Value = 42,
            ParameterName = "TheAnswer",

            DbType = DbType.Int32,

            Direction = ParameterDirection.InputOutput,
            IsNullable = true,
            Precision = 1,
            Scale = 2,
            Size = 4,

            SourceVersion = DataRowVersion.Proposed,
            SourceColumn = "source",
            SourceColumnNullMapping = true,
        };
        var actual = expected.Clone();

        Assert.That(actual.Value, Is.EqualTo(expected.Value));
        Assert.That(actual.ParameterName, Is.EqualTo(expected.ParameterName));

        Assert.That(actual.DbType, Is.EqualTo(expected.DbType));

        Assert.That(actual.Direction, Is.EqualTo(expected.Direction));
        Assert.That(actual.IsNullable, Is.EqualTo(expected.IsNullable));
        Assert.That(actual.Precision, Is.EqualTo(expected.Precision));
        Assert.That(actual.Scale, Is.EqualTo(expected.Scale));
        Assert.That(actual.Size, Is.EqualTo(expected.Size));

        Assert.That(actual.SourceVersion, Is.EqualTo(expected.SourceVersion));
        Assert.That(actual.SourceColumn, Is.EqualTo(expected.SourceColumn));
        Assert.That(actual.SourceColumnNullMapping, Is.EqualTo(expected.SourceColumnNullMapping));
    }
    
    [Test]
    public void ParameterNull()
    {
        var param = new SpannerParameter{ParameterName = "param", DbType = DbType.Decimal};
        Assert.That(param.Scale, Is.EqualTo(0), "#A1");
        param.Value = DBNull.Value;
        Assert.That(param.Scale, Is.EqualTo(0), "#A2");

        param = new SpannerParameter{ParameterName = "param", DbType = DbType.Int32};
        Assert.That(param.Scale, Is.EqualTo(0), "#B1");
        param.Value = DBNull.Value;
        Assert.That(param.Scale, Is.EqualTo(0), "#B2");
    }
    
    [Test]
    public async Task MatchParamIndexCaseInsensitively()
    {
        const string sql = "SELECT @p,@P";
        Fixture.SpannerMock.AddOrUpdateStatementResult(sql, StatementResult.CreateResultSet(
            new List<Tuple<TypeCode, string>>([Tuple.Create(TypeCode.String, "p"), Tuple.Create(TypeCode.String, "p")]),
            new List<object[]>([["Hello World", "Hello World"]])));
        
        await using var conn = await OpenConnectionAsync();
        await using var cmd = new SpannerCommand(sql, conn);
        cmd.AddParameter("p", "Hello World");
        await cmd.ExecuteNonQueryAsync();
        
        var request = Fixture.SpannerMock.Requests.OfType<ExecuteSqlRequest>().Single(r => r.Sql == sql);
        Assert.That(request, Is.Not.Null);
        // TODO: Revisit once https://github.com/googleapis/go-sql-spanner/issues/594 has been decided.
        Assert.That(request.Params.Fields.Count, Is.EqualTo(2));
        Assert.That(request.Params.Fields["p"].StringValue, Is.EqualTo("Hello World"));
        Assert.That(request.Params.Fields["P"].HasNullValue);
    }

    [Test]
    public void PrecisionViaInterface()
    {
        var parameter = new SpannerParameter();
        var paramIface = (IDbDataParameter)parameter;

        paramIface.Precision = 42;

        Assert.That(paramIface.Precision, Is.EqualTo((byte)42));
    }

    [Test]
    public void PrecisionViaBaseClass()
    {
        var parameter = new SpannerParameter();
        var paramBase = (DbParameter)parameter;

        paramBase.Precision = 42;

        Assert.That(paramBase.Precision, Is.EqualTo((byte)42));
    }

    [Test]
    public void ScaleViaInterface()
    {
        var parameter = new SpannerParameter();
        var paramIface = (IDbDataParameter)parameter;

        paramIface.Scale = 42;

        Assert.That(paramIface.Scale, Is.EqualTo((byte)42));
    }

    [Test]
    public void ScaleViaBaseClass()
    {
        var parameter = new SpannerParameter();
        var paramBase = (DbParameter)parameter;

        paramBase.Scale = 42;

        Assert.That(paramBase.Scale, Is.EqualTo((byte)42));
    }

    [Test]
    public void NullValueThrows()
    {
        using var connection = OpenConnection();
        using var command = new SpannerCommand("SELECT @p", connection);
        command.Parameters.Add(new SpannerParameter("p", null));

        Assert.That(() => command.ExecuteReader(), Throws.InvalidOperationException);
    }
    
}