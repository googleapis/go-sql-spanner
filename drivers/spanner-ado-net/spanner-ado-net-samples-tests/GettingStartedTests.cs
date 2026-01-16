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

using Docker.DotNet;
using Docker.DotNet.Models;
using Google.Cloud.Spanner.Admin.Database.V1;
using Google.Cloud.Spanner.DataProvider.GettingStartedGuide;

namespace Google.Cloud.Spanner.DataProvider.Samples.Tests;

[TestFixture(DatabaseDialect.GoogleStandardSql)]
[TestFixture(DatabaseDialect.Postgresql)]
public class GettingStartedTests
{
    private static EmulatorRunner? _emulatorRunner;
    private static PortBinding? _portBinding;

    [OneTimeSetUp]
    public static async Task CheckEmulatorAvailability()
    {
        if (EmulatorRunner.IsEmulatorRunning())
        {
            return;
        }
        using var client = EmulatorRunner.CreateDockerClient();
        try
        {
            client.System.GetVersionAsync().ConfigureAwait(false).GetAwaiter().GetResult();
        }
        catch (Exception)
        {
            Assert.Ignore("Docker and Emulator not available");
        }
        _emulatorRunner = new EmulatorRunner();
        try
        {
            _portBinding = await _emulatorRunner.StartEmulator();
        }
        catch (DockerImageNotFoundException)
        {
            _emulatorRunner = null;
            Assert.Ignore("Docker and Emulator not available");
        }
    }

    [OneTimeTearDown]
    public async Task StopEmulator()
    {
        if (_emulatorRunner != null)
        {
            await _emulatorRunner.StopEmulator();
        }
    }

    private readonly DatabaseDialect _dialect;
    private readonly string _database;
    private SpannerConnectionStringBuilder? _builder;

    private SpannerConnectionStringBuilder Builder
    {
        get
        {
            _builder ??= new()
            {
                Project = "emulator-project",
                Instance = "sample-instance",
                Database = _database,
                AutoConfigEmulator = true,
                Options = _dialect == DatabaseDialect.Postgresql ? "dialect=postgresql" : "",
                Host = _portBinding?.HostIP ?? "",
                Port = uint.Parse(_portBinding?.HostPort ?? "443"),
            };
            return _builder;
        }
    }

    private StringWriter? _writer;
    private TextWriter _output;
    private StringWriter Writer => _writer ??= new StringWriter();

    public GettingStartedTests(DatabaseDialect dialect)
    {
        _dialect = dialect;
        _database = _dialect switch
        {
            DatabaseDialect.GoogleStandardSql => "sample-database",
            DatabaseDialect.Postgresql => "sample-database-pg",
            _ => throw new InvalidOperationException("Unknown dialect " + _dialect)
        };
        if (EmulatorRunner.IsEmulatorRunning())
        {
            DropSampleTables().GetAwaiter().GetResult();
        }
    }

    private async Task DropSampleTables()
    {
        await using var connection = new SpannerConnection(Builder.ConnectionString);
        await connection.OpenAsync();
        var batch = connection.CreateBatch();
        batch.BatchCommands.Add("drop table if exists Concerts");
        batch.BatchCommands.Add("drop table if exists Venues");
        batch.BatchCommands.Add("drop table if exists Albums");
        batch.BatchCommands.Add("drop table if exists Singers");
        await batch.ExecuteNonQueryAsync();
    }

    [SetUp]
    public void SetupOutput()
    {
        _output = Console.Out;
        _writer = new StringWriter();
        Console.SetOut(_writer);
    }
    
    [TearDown]
    public void CleanupOutput()
    {
        _writer?.Dispose();
        Console.SetOut(_output);
    }

    // These tests must be ordered, so they match the order of the Getting Started Guide.
    [Test, Order(1)]
    public async Task TestCreateTables()
    {
        if (_dialect == DatabaseDialect.GoogleStandardSql)
        {
            await CreateTablesSample.CreateTables(Builder.ConnectionString);
        }
        else if (_dialect == DatabaseDialect.Postgresql)
        {
            await CreateTablesSamplePostgreSql.CreateTables(Builder.ConnectionString);
        }

        var output = Writer.ToString();
        Assert.That(output, Does.Contain("Created Singers & Albums tables"));
    }

    [Test, Order(2)]
    public async Task TestCreateConnection()
    {
        if (_dialect == DatabaseDialect.GoogleStandardSql)
        {
            await CreateConnectionSample.CreateConnection(Builder.ConnectionString);
        }
        else if (_dialect == DatabaseDialect.Postgresql)
        {
            await CreateConnectionSamplePostgreSql.CreateConnection(Builder.ConnectionString);
        }

        var output = Writer.ToString();
        Assert.That(output, Does.Contain("Greeting from Spanner: Hello World"));
    }

    [Test, Order(3)]
    public async Task TestWriteDataWithDml()
    {
        if (_dialect == DatabaseDialect.GoogleStandardSql)
        {
            await WriteDataWithDmlSample.WriteDataWithDml(Builder.ConnectionString);
        }
        else if (_dialect == DatabaseDialect.Postgresql)
        {
            await WriteDataWithDmlSamplePostgreSql.WriteDataWithDml(Builder.ConnectionString);
        }

        var output = Writer.ToString();
        Assert.That(output, Does.Contain("4 record(s) inserted."));
    }

    [Test, Order(4)]
    public async Task TestWriteDataWithMutations()
    {
        if (_dialect == DatabaseDialect.GoogleStandardSql)
        {
            await WriteDataWithMutationsSample.WriteDataWithMutations(Builder.ConnectionString);
        }
        else if (_dialect == DatabaseDialect.Postgresql)
        {
            await WriteDataWithMutationsSamplePostgreSql.WriteDataWithMutations(Builder.ConnectionString);
        }

        var output = Writer.ToString();
        Assert.That(output, Does.Contain("Inserted 10 rows."));
    }

    [Test, Order(5)]
    public async Task TestQueryData()
    {
        if (_dialect == DatabaseDialect.GoogleStandardSql)
        {
            await QueryDataSample.QueryData(Builder.ConnectionString);
        }
        else if (_dialect == DatabaseDialect.Postgresql)
        {
            await QueryDataSamplePostgreSql.QueryData(Builder.ConnectionString);
        }

        var output = Writer.ToString();
        Assert.That(output, Is.EqualTo(
            $"1 1 Total Junk{Environment.NewLine}" +
            $"1 2 Go, Go, Go{Environment.NewLine}" +
            $"2 1 Green{Environment.NewLine}" +
            $"2 2 Forever Hold Your Peace{Environment.NewLine}" +
            $"2 3 Terrified{Environment.NewLine}"));
    }

    [Test, Order(6)]
    public async Task TestQueryDataWithParameter()
    {
        if (_dialect == DatabaseDialect.GoogleStandardSql)
        {
            await QueryDataWithParameterSample.QueryDataWithParameter(Builder.ConnectionString);
        }
        else if (_dialect == DatabaseDialect.Postgresql)
        {
            await QueryDataWithParameterSamplePostgreSql.QueryDataWithParameter(Builder.ConnectionString);
        }

        var output = Writer.ToString();
        Assert.That(output, Is.EqualTo($"12 Melissa Garcia{Environment.NewLine}"));
    }

    [Test, Order(7)]
    public async Task TestAddColumn()
    {
        if (_dialect == DatabaseDialect.GoogleStandardSql)
        {
            await AddColumnSample.AddColumn(Builder.ConnectionString);
        }
        else if (_dialect == DatabaseDialect.Postgresql)
        {
            await AddColumnSamplePostgreSql.AddColumn(Builder.ConnectionString);
        }

        var output = Writer.ToString();
        if (_dialect == DatabaseDialect.GoogleStandardSql)
        {
            Assert.That(output, Is.EqualTo($"Added MarketingBudget column{Environment.NewLine}"));
        }
        else if (_dialect == DatabaseDialect.Postgresql)
        {
            Assert.That(output, Is.EqualTo($"Added marketing_budget column{Environment.NewLine}"));
        }
    }

    [Test, Order(8)]
    public async Task TestDdlBatch()
    {
        if (_dialect == DatabaseDialect.GoogleStandardSql)
        {
            await DdlBatchSample.DdlBatch(Builder.ConnectionString);
        }
        else if (_dialect == DatabaseDialect.Postgresql)
        {
            await DdlBatchSamplePostgreSql.DdlBatch(Builder.ConnectionString);
        }

        var output = Writer.ToString();
        Assert.That(output, Is.EqualTo($"Added Venues and Concerts tables{Environment.NewLine}"));
    }

    [Test, Order(9)]
    public async Task TestUpdateDataWithMutations()
    {
        if (_dialect == DatabaseDialect.GoogleStandardSql)
        {
            await UpdateDataWithMutationsSample.UpdateDataWithMutations(Builder.ConnectionString);
        }
        else if (_dialect == DatabaseDialect.Postgresql)
        {
            await UpdateDataWithMutationsSamplePostgreSql.UpdateDataWithMutations(Builder.ConnectionString);
        }

        var output = Writer.ToString();
        Assert.That(output, Is.EqualTo($"Updated 2 albums.{Environment.NewLine}"));
    }
    
    [Test, Order(10)]
    public async Task TestQueryNewColumn()
    {
        if (_dialect == DatabaseDialect.GoogleStandardSql)
        {
            await QueryNewColumnSample.QueryNewColumn(Builder.ConnectionString);
        }
        else if (_dialect == DatabaseDialect.Postgresql)
        {
            await QueryNewColumnSamplePostgreSql.QueryNewColumn(Builder.ConnectionString);
        }

        var output = Writer.ToString();
        Assert.That(output, Is.EqualTo(
            $"1 1 100000{Environment.NewLine}" +
            $"1 2 {Environment.NewLine}" +
            $"2 1 {Environment.NewLine}" +
            $"2 2 500000{Environment.NewLine}" +
            $"2 3 {Environment.NewLine}"));
    }

    [Test, Order(11)]
    public async Task TestWriteDataWithTransaction()
    {
        if (_dialect == DatabaseDialect.GoogleStandardSql)
        {
            await WriteDataWithTransactionSample.WriteDataWithTransaction(Builder.ConnectionString);
        }
        else if (_dialect == DatabaseDialect.Postgresql)
        {
            await WriteDataWithTransactionSamplePostgreSql.WriteDataWithTransaction(Builder.ConnectionString);
        }

        var output = Writer.ToString();
        Assert.That(output, Is.EqualTo($"Transferred marketing budget from Album 2 to Album 1{Environment.NewLine}"));
    }

    [Test, Order(12)]
    public async Task TestTags()
    {
        if (_dialect == DatabaseDialect.GoogleStandardSql)
        {
            await TagsSample.Tags(Builder.ConnectionString);
        }
        else if (_dialect == DatabaseDialect.Postgresql)
        {
            await TagsSamplePostgreSql.Tags(Builder.ConnectionString);
        }

        var output = Writer.ToString();
        Assert.That(output, Is.EqualTo($"Reduced marketing budget{Environment.NewLine}"));
    }

    [Test, Order(13)]
    public async Task TestReadOnlyTransaction()
    {
        if (_dialect == DatabaseDialect.GoogleStandardSql)
        {
            await ReadOnlyTransactionSample.ReadOnlyTransaction(Builder.ConnectionString);
        }
        else if (_dialect == DatabaseDialect.Postgresql)
        {
            await ReadOnlyTransactionSamplePostgreSql.ReadOnlyTransaction(Builder.ConnectionString);
        }

        var output = Writer.ToString();
        Assert.That(output, Is.EqualTo(
            $"1 1 Total Junk{Environment.NewLine}" +
            $"1 2 Go, Go, Go{Environment.NewLine}" +
            $"2 1 Green{Environment.NewLine}" +
            $"2 2 Forever Hold Your Peace{Environment.NewLine}" +
            $"2 3 Terrified{Environment.NewLine}" +
            $"2 2 Forever Hold Your Peace{Environment.NewLine}" +
            $"1 2 Go, Go, Go{Environment.NewLine}" +
            $"2 1 Green{Environment.NewLine}" +
            $"2 3 Terrified{Environment.NewLine}" +
            $"1 1 Total Junk{Environment.NewLine}"));
    }

    [Test, Order(14)]
    public async Task TestDataBoost()
    {
        if (_dialect == DatabaseDialect.GoogleStandardSql)
        {
            await DataBoostSample.DataBoost(Builder.ConnectionString);
        }
        else if (_dialect == DatabaseDialect.Postgresql)
        {
            await DataBoostSamplePostgreSql.DataBoost(Builder.ConnectionString);
        }

        // TODO: Implement
        var output = Writer.ToString();
        Assert.That(output, Is.EqualTo(""));
    }

    [Test, Order(15)]
    public async Task TestPartitionedDml()
    {
        if (_dialect == DatabaseDialect.GoogleStandardSql)
        {
            await PartitionedDmlSample.PartitionedDml(Builder.ConnectionString);
        }
        else if (_dialect == DatabaseDialect.Postgresql)
        {
            await PartitionedDmlSamplePostgreSql.PartitionedDml(Builder.ConnectionString);
        }

        var output = Writer.ToString();
        Assert.That(output, Is.EqualTo($"Updated at least 3 albums{Environment.NewLine}"));
    }

}