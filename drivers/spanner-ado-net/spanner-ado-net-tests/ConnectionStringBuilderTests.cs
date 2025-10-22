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

namespace Google.Cloud.Spanner.DataProvider.Tests;

public class ConnectionStringBuilderTests
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
    
}