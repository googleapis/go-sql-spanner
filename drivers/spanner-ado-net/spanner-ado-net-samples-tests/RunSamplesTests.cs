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

namespace Google.Cloud.Spanner.DataProvider.Samples.Tests;

public class RunSamplesTests
{
    private StringWriter? _writer;
    private TextWriter _output;
    
    private StringWriter Writer => _writer ??= new StringWriter();

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
        var emulatorRunner = new EmulatorRunner();
        try
        {
            await emulatorRunner.StartEmulator();
            await emulatorRunner.StopEmulator();
        }
        catch (DockerImageNotFoundException)
        {
            Assert.Ignore("Docker and Emulator not available");
        }
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

    [Test]
    public void TestHelloWorldSample()
    {
        SampleRunner.RunSample("HelloWorld", false);
        
        var output = Writer.ToString();
        Assert.That(output, Does.Contain("Running sample HelloWorld"));
        Assert.That(output, Does.Contain("Greeting from Spanner: Hello World"));
    }

    [Test]
    public void TestEmulatorSample()
    {
        SampleRunner.RunSample("Emulator", false);
        
        var output = Writer.ToString();
        Assert.That(output, Does.Contain("Running sample Emulator"));
        Assert.That(output, Does.Contain("Greeting from Spanner Emulator: Hello World"));
    }
    
    [Test]
    public void TestCommitTimestampSample()
    {
        SampleRunner.RunSample("CommitTimestamp", false);
        
        var output = Writer.ToString();
        Assert.That(output, Does.Contain("Running sample CommitTimestamp"));
        Assert.That(output, Does.Contain("Inserted 1 singer(s)"));
        Assert.That(output, Does.Contain("Transaction committed at "));
    }

    [Test]
    public void TestCustomConfigurationSample()
    {
        SampleRunner.RunSample("CustomConfiguration", false);
        
        var output = Writer.ToString();
        Assert.That(output, Does.Contain("Running sample CustomConfiguration"));
        Assert.That(output, Does.Contain("Greeting: Hello from Spanner"));
    }
    
    [Test]
    public void TestDataTypesSample()
    {
        SampleRunner.RunSample("DataTypes", false);
        
        var output = Writer.ToString();
        Assert.That(output, Does.Contain("Running sample DataTypes"));
        Assert.That(output, Does.Contain("Inserted: 1"));
        Assert.That(output, Does.Contain("id: 1"));
        Assert.That(output, Does.Contain("col_bool: True"));
        Assert.That(output, Does.Contain("col_bytes: System.Byte[]"));
        Assert.That(output, Does.Contain($"col_date: {DateOnly.FromDateTime(DateTime.Now)}"));
        Assert.That(output, Does.Contain($"col_float32: {3.14f}"));
        Assert.That(output, Does.Contain($"col_float64: {3.14d}"));
        Assert.That(output, Does.Contain("col_int64: 100"));
        Assert.That(output, Does.Contain("col_json: {\"key\":\"value\"}"));
        Assert.That(output, Does.Contain($"col_numeric: {3.14m}"));
        Assert.That(output, Does.Contain("col_string: test-string"));
        Assert.That(output, Does.Contain("col_timestamp: "));
    }
    
    [Test]
    public void TestDdlBatchSample()
    {
        SampleRunner.RunSample("DdlBatch", false);
        
        var output = Writer.ToString();
        Assert.That(output, Does.Contain("Running sample DdlBatch"));
        Assert.That(output, Does.Contain("Executed a single SQL string with multiple DDL statements as one batch."));
        Assert.That(output, Does.Contain("Executed ADO.NET batch"));
        Assert.That(output, Does.Contain("Executed DDL batch"));
    }

    [Test]
    public void TestDmlBatchSample()
    {
        SampleRunner.RunSample("DmlBatch", false);
        
        var output = Writer.ToString();
        Assert.That(output, Does.Contain("Running sample DmlBatch"));
        Assert.That(output, Does.Contain($"Executed ADO.NET batch{Environment.NewLine}Affected: 3{Environment.NewLine}"));
        Assert.That(output, Does.Contain($"Executed DML batch{Environment.NewLine}Affected: -1{Environment.NewLine}"));
    }

    [Test]
    public void TestMutationsSample()
    {
        SampleRunner.RunSample("Mutations", false);
        
        var output = Writer.ToString();
        Assert.That(output, Does.Contain("Running sample Mutations"));
        Assert.That(output, Does.Contain("Inserted data using mutations. Affected: 1"));
    }
    
    [Test]
    public void TestPartitionedDmlSample()
    {
        SampleRunner.RunSample("PartitionedDml", false);
        
        var output = Writer.ToString();
        Assert.That(output, Does.Contain("Running sample PartitionedDml"));
        Assert.That(output, Does.Contain("Executed a Partitioned DML statement. Affected: "));
    }

    [Test]
    public void TestQueryParametersSample()
    {
        SampleRunner.RunSample("QueryParameters", false);
        
        var output = Writer.ToString();
        Assert.That(output, Does.Contain("Running sample QueryParameters"));
        Assert.That(output, Does.Contain("Found singer with named parameters: Mark Richards"));
        Assert.That(output, Does.Contain("Found singer with named parameters: Alice Trentor"));
        Assert.That(output, Does.Contain("Found singer with positional parameters: Mark Richards"));
        Assert.That(output, Does.Contain("Found singer with positional parameters: Alice Trentor"));
    }

    [Test]
    public void TestReadOnlyTransactionSample()
    {
        SampleRunner.RunSample("ReadOnlyTransaction", false);
        
        var output = Writer.ToString();
        Assert.That(output, Does.Contain("Running sample ReadOnlyTransaction"));
        Assert.That(output, Does.Contain("Found singer: Mark Richards"));
        Assert.That(output, Does.Contain("Found singer: Alice Trentor"));
    }

    [Test]
    public void TestStaleReadSample()
    {
        SampleRunner.RunSample("StaleRead", false);
        
        var output = Writer.ToString();
        Assert.That(output, Does.Contain("Running sample StaleRead"));
        Assert.That(output, Does.Contain("Found singer using a single stale query: Mark Richards"));
        Assert.That(output, Does.Contain("Found singer using a single stale query: Alice Trentor"));
        Assert.That(output, Does.Contain("Found singer using a stale read-only transaction: Mark Richards"));
        Assert.That(output, Does.Contain("Found singer using a stale read-only transaction: Alice Trentor"));
    }

    [Test]
    public void TestTagsSample()
    {
        SampleRunner.RunSample("Tags", false);
        
        var output = Writer.ToString();
        Assert.That(output, Does.Contain("Running sample Tags"));
        Assert.That(output, Does.Contain("Greeting from Spanner: Hello World"));
    }

    [Test]
    public void TestTransactionSample()
    {
        SampleRunner.RunSample("Transaction", false);
        
        var output = Writer.ToString();
        Assert.That(output, Does.Contain("Running sample Transaction"));
        Assert.That(output, Does.Contain("Set a default birthdate for 1 singers"));
    }
}