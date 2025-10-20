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
using Google.Cloud.SpannerLib;
using Google.Cloud.SpannerLib.MockServer;
using Google.Rpc;
using Grpc.Core;
using Status = Grpc.Core.Status;

namespace Google.Cloud.Spanner.DataProvider.Tests;

public class ConnectionStringBuilderTests : AbstractMockServerTests
{
    [Test]
    public void Basic()
    {
        var builder = new SpannerConnectionStringBuilder();
        Assert.That(builder.Keys, Is.Empty);
        Assert.That(builder.Count, Is.EqualTo(0));
        Assert.False(builder.ContainsKey("host"));
        builder.Host = "myhost";
        Assert.That(builder["host"], Is.EqualTo("myhost"));
        Assert.That(builder.Count, Is.EqualTo(1));
        Assert.That(builder.ConnectionString, Is.EqualTo("Host=myhost"));
        builder.Remove("HOST");
        Assert.That(builder["host"], Is.EqualTo(""));
        Assert.That(builder.Count, Is.EqualTo(0));
    }

    [Test]
    public void TryGetValue()
    {
        var builder = new SpannerConnectionStringBuilder
        {
            ConnectionString = "Host=myhost"
        };
        Assert.That(builder.TryGetValue("Host", out var value), Is.True);
        Assert.That(value, Is.EqualTo("myhost"));
        Assert.That(builder.TryGetValue("SomethingUnknown", out value), Is.False);
    }

    [Test]
    public void Remove()
    {
        var builder = new SpannerConnectionStringBuilder
        {
            UsePlainText = true
        };
        Assert.That(builder["Use plain text"], Is.True);
        builder.Remove("UsePlainText");
        Assert.That(builder.ConnectionString, Is.EqualTo(""));
    }

    [Test]
    public void Clear()
    {
        var builder = new SpannerConnectionStringBuilder { Host = "myhost" };
        builder.Clear();
        Assert.That(builder.Count, Is.EqualTo(0));
        Assert.That(builder["host"], Is.EqualTo(""));
        Assert.That(builder.Host, Is.Empty);
    }

    [Test]
    public void RemovingResetsToDefault()
    {
        var builder = new SpannerConnectionStringBuilder();
        Assert.That(builder.Port, Is.EqualTo(SpannerConnectionStringOption.Port.DefaultValue));
        builder.Port = 8;
        builder.Remove("Port");
        Assert.That(builder.Port, Is.EqualTo(SpannerConnectionStringOption.Port.DefaultValue));
    }

    [Test]
    public void SettingToNullResetsToDefault()
    {
        var builder = new SpannerConnectionStringBuilder();
        Assert.That(builder.Port, Is.EqualTo(SpannerConnectionStringOption.Port.DefaultValue));
        builder.Port = 8;
        builder["Port"] = null;
        Assert.That(builder.Port, Is.EqualTo(SpannerConnectionStringOption.Port.DefaultValue));
    }

    [Test]
    public void Enum()
    {
        var builder = new SpannerConnectionStringBuilder
        {
            ConnectionString = "DefaultIsolationLevel=Serializable"
        };
        Assert.That(builder.DefaultIsolationLevel, Is.EqualTo(IsolationLevel.Serializable));
        Assert.That(builder.Count, Is.EqualTo(1));
    }

    [Test]
    public void EnumCaseInsensitive()
    {
        var builder = new SpannerConnectionStringBuilder
        {
            ConnectionString = "defaultisolationlevel=repeatable read"
        };
        Assert.That(builder.DefaultIsolationLevel, Is.EqualTo(IsolationLevel.RepeatableRead));
        Assert.That(builder.Count, Is.EqualTo(1));
    }

    [Test]
    public void Clone()
    {
        var builder = new SpannerConnectionStringBuilder
        {
            Host = "myhost"
        };
        var builder2 = builder.Clone();
        Assert.That(builder2.Host, Is.EqualTo("myhost"));
        Assert.That(builder2["Host"], Is.EqualTo("myhost"));
        Assert.That(builder.Port, Is.EqualTo(SpannerConnectionStringOption.Port.DefaultValue));
    }

    [Test]
    public void ConversionErrorThrows()
    {
        // ReSharper disable once CollectionNeverQueried.Local
        var builder = new SpannerConnectionStringBuilder();
        Assert.That(() => builder["Port"] = "hello",
            Throws.Exception.TypeOf<ArgumentException>().With.Message.Contains("Port"));
    }

    [Test]
    public void InvalidConnectionStringThrows()
    {
        var builder = new SpannerConnectionStringBuilder();
        Assert.That(() => builder.ConnectionString = "Server=127.0.0.1;User Id=npgsql_tests;Pooling:false",
            Throws.Exception.TypeOf<ArgumentException>());
    }

    [Test]
    public void ConnectionStringToProperties()
    {
        var builder = new SpannerConnectionStringBuilder
        {
            ConnectionString = "Host=localhost;Port=80;UsePlainText=true;DefaultIsolationLevel=Repeatable read",
        };
        Assert.That(builder.Host, Is.EqualTo("localhost"));
        Assert.That(builder.Port, Is.EqualTo(80));
        Assert.That(builder.UsePlainText, Is.True);
        Assert.That(builder.DefaultIsolationLevel, Is.EqualTo(IsolationLevel.RepeatableRead));
    }

    [Test]
    public void PropertiesToConnectionString()
    {
        var builder = new SpannerConnectionStringBuilder
        {
            Host = "localhost",
            Port = 80,
            UsePlainText = true,
            DefaultIsolationLevel = IsolationLevel.RepeatableRead,
            DataSource = "projects/project1/instances/instance1/databases/database1"
        };
        Assert.That(builder.ConnectionString, Is.EqualTo("Data Source=projects/project1/instances/instance1/databases/database1;Host=localhost;Port=80;UsePlainText=True;DefaultIsolationLevel=RepeatableRead"));
        Assert.That(builder.SpannerLibConnectionString, Is.EqualTo("localhost:80/projects/project1/instances/instance1/databases/database1;UsePlainText=True;DefaultIsolationLevel=RepeatableRead"));
    }

    [Test]
    public void RequiredConnectionStringProperties()
    {
        using var connection = new SpannerConnection();
        Assert.Throws<ArgumentException>(() => connection.ConnectionString = "Host=localhost;Port=80");
    }

    [Test]
    public void FailedConnectThenSucceed()
    {
        // Close all current pools to ensure that we get a fresh pool.
        SpannerPool.CloseSpannerLib();
        // TODO: Make this a public property in the mock server.
        const string detectDialectQuery =
            "select option_value from information_schema.database_options where option_name='database_dialect'";
        Fixture.SpannerMock.AddOrUpdateStatementResult(detectDialectQuery, StatementResult.CreateException(new RpcException(new Status(StatusCode.NotFound, "Database not found"))));
        using var conn = new SpannerConnection();
        conn.ConnectionString = ConnectionString;
        var exception = Assert.Throws<SpannerException>(() => conn.Open());
        Assert.That(exception.Code, Is.EqualTo(Code.NotFound));
        Assert.That(conn.State, Is.EqualTo(ConnectionState.Closed));
        
        // Remove the error and retry.
        Fixture.SpannerMock.AddOrUpdateStatementResult(detectDialectQuery, StatementResult.CreateResultSet(new List<Tuple<Google.Cloud.Spanner.V1.TypeCode, string>>
        {
            Tuple.Create<Google.Cloud.Spanner.V1.TypeCode, string>(V1.TypeCode.String, "option_value")
        }, new List<object[]>
        {
            new object[] { "GOOGLE_STANDARD_SQL" }
        }));
        conn.Open();
        Assert.That(conn.State, Is.EqualTo(ConnectionState.Open));
    }

    [Test]
    [Ignore("Needs connect_timeout property")]
    public void OpenTimeout()
    {
        // TODO: Add connect_timeout property.
        var builder = new SpannerConnectionStringBuilder
        {
            Host = Fixture.Host,
            Port = (uint) Fixture.Port,
            UsePlainText = true,
            DataSource = "projects/project1/instances/instance1/databases/database1",
            //ConnectTimeout = TimeSpan.FromMicroseconds(1),
        };
        using var connection = new SpannerConnection();
        connection.ConnectionString = builder.ConnectionString;
        var exception = Assert.Throws<SpannerDbException>(() => connection.Open());
        Assert.That(exception.ErrorCode, Is.EqualTo((int) Code.DeadlineExceeded));
    }

    [Test]
    [Ignore("OpenAsync must be implemented")]
    public async Task OpenCancel()
    {
        // Close all current pools to ensure that we get a fresh pool.
        SpannerPool.CloseSpannerLib();
        Fixture.SpannerMock.AddOrUpdateExecutionTime(nameof(Fixture.SpannerMock.CreateSession), ExecutionTime.FromMillis(20, 0));
        var builder = new SpannerConnectionStringBuilder
        {
            Host = Fixture.Host,
            Port = (uint) Fixture.Port,
            UsePlainText = true,
            DataSource = "projects/project1/instances/instance1/databases/database1",
        };
        await using var connection = new SpannerConnection();
        connection.ConnectionString = builder.ConnectionString;
        var tokenSource = new CancellationTokenSource(5);
        // TODO: Implement actual async opening of connections
        Assert.ThrowsAsync<OperationCanceledException>(async () => await connection.OpenAsync(tokenSource.Token));
        Assert.That(connection.State, Is.EqualTo(ConnectionState.Closed));
    }
    
    [Test]
    public void DataSourceProperty()
    {
        using var conn = new SpannerConnection();
        Assert.That(conn.DataSource, Is.EqualTo(string.Empty));

        var builder = new SpannerConnectionStringBuilder(ConnectionString);

        conn.ConnectionString = builder.ConnectionString;
        Assert.That(conn.DataSource, Is.EqualTo("projects/p1/instances/i1/databases/d1"));
    }
    
    [Test]
    public void SettingConnectionStringWhileOpenThrows()
    {
        using var conn = new SpannerConnection();
        conn.ConnectionString = ConnectionString;
        conn.Open();
        Assert.That(() => conn.ConnectionString = "", Throws.Exception.TypeOf<InvalidOperationException>());
    }

    [Test]
    public void EmptyConstructor()
    {
        var conn = new SpannerConnection();
        Assert.That(conn.ConnectionTimeout, Is.EqualTo(15));
        Assert.That(conn.ConnectionString, Is.SameAs(string.Empty));
        Assert.That(() => conn.Open(), Throws.Exception.TypeOf<InvalidOperationException>());
    }

    [Test]
    public void Constructor_with_null_connection_string()
    {
        var conn = new SpannerConnection(null);
        Assert.That(conn.ConnectionString, Is.SameAs(string.Empty));
        Assert.That(() => conn.Open(), Throws.Exception.TypeOf<InvalidOperationException>());
    }

    [Test]
    public void Constructor_with_empty_connection_string()
    {
        var conn = new NpgsqlConnection("");
        Assert.That(conn.ConnectionString, Is.SameAs(string.Empty));
        Assert.That(() => conn.Open(), Throws.Exception.TypeOf<InvalidOperationException>());
    }
    
}