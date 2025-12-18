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

using System.Reflection;
using Docker.DotNet.Models;
using Google.Api.Gax;
using Google.Cloud.Spanner.Common.V1;
using Google.Cloud.Spanner.DataProvider.Samples.Snippets;

namespace Google.Cloud.Spanner.DataProvider.Samples;

/// <summary>
/// Main class for running a sample from the Snippets directory.
/// Usage: `dotnet run SampleName`
/// Example: `dotnet run HelloWorld`
/// 
/// The SampleRunner will automatically start a docker container with a Spanner emulator and execute
/// the sample on that emulator instance. No further setup or configuration is required.
/// </summary>
public static class SampleRunner
{
    private static readonly string SnippetsNamespace = typeof(HelloWorldSample).Namespace!;
    
    static void Main(string[] args)
    {
        if (args.Length < 1)
        {
            Console.Error.WriteLine("Not enough arguments.\r\nUsage: dotnet run <SampleName>\r\nExample: dotnet run HelloWorld");
            PrintValidSampleNames();
            return;
        }
        var sampleName = args[0];
        if (sampleName.Equals("All"))
        {
            // Run all samples. This is used to test that all samples are runnable.
            RunAllSamples();
        }
        else
        {
            RunSample(sampleName, false);
        }
    }
    

    private static void RunAllSamples()
    {
        var sampleClasses = GetSampleClasses();
        foreach (var sample in sampleClasses)
        {
            RunSample(sample.Name, true);
        }
    }

    internal static void RunSample(string sampleName, bool failOnException)
    {
        if (sampleName.EndsWith("Sample"))
        {
            sampleName = sampleName.Substring(0, sampleName.Length - "Sample".Length);
        }
        try
        {
            var sampleMethod = GetSampleMethod(sampleName);
            if (sampleMethod != null)
            {
                Console.WriteLine($"Running sample {sampleName}");
                RunSampleAsync((connectionString) => (Task)sampleMethod.Invoke(null, [connectionString])!).WaitWithUnwrappedExceptions();
            }
        }
        catch (Exception e)
        {
            Console.WriteLine($"Running sample failed: {e.Message}\n{e.StackTrace}");
            if (failOnException)
            {
                throw;
            }
        }
    }

    private static async Task RunSampleAsync(Func<string, Task> sampleMethod)
    {
        var emulatorRunner = new EmulatorRunner();
        var startedEmulator = false;
        try
        {
            Console.WriteLine("");
            PortBinding? portBinding = null;
            if (EmulatorRunner.IsEmulatorRunning())
            {
                Console.WriteLine("Emulator is already running. Re-using existing Emulator instance...");
                Console.WriteLine("");
            }
            else
            {
                Console.WriteLine("Starting emulator...");
                portBinding = await emulatorRunner.StartEmulator();
                Console.WriteLine($"Emulator started on port {portBinding.HostPort}");
                Console.WriteLine("");
                startedEmulator = true;
            }

            var projectId = "sample-project";
            var instanceId = "sample-instance";
            var databaseId = "sample-database";
            DatabaseName databaseName = DatabaseName.FromProjectInstanceDatabase(projectId, instanceId, databaseId);
            var connectionStringBuilder = new SpannerConnectionStringBuilder
            {
                DataSource = databaseName.ToString(),
                AutoConfigEmulator = true,
            };
            if (portBinding != null)
            {
                connectionStringBuilder.Host = portBinding.HostIP;
                connectionStringBuilder.Port = uint.Parse(portBinding.HostPort);
            }
            
            await ExecuteScript(connectionStringBuilder.ConnectionString, "create_sample_tables.sql");
            await ExecuteScript(connectionStringBuilder.ConnectionString, "insert_sample_data.sql");
            await sampleMethod.Invoke(connectionStringBuilder.ConnectionString);
        }
        catch (Exception e)
        {
            Console.WriteLine($"Running sample failed: {e.Message}");
            throw;
        }
        finally
        {
            if (startedEmulator)
            {
                Console.WriteLine("");
                Console.WriteLine("Stopping emulator...");
                emulatorRunner.StopEmulator().WaitWithUnwrappedExceptions();
                Console.WriteLine("");
            }
        }
    }

    private static MethodInfo? GetSampleMethod(string sampleName)
    {
        try
        {
            var sampleClass = System.Type.GetType($"{SnippetsNamespace}.{sampleName}Sample");
            if (sampleClass == null)
            {
                Console.Error.WriteLine($"Unknown sample name: {sampleName}");
                PrintValidSampleNames();
                return null;
            }
            var sampleMethod = sampleClass.GetMethod("Run");
            if (sampleMethod == null)
            {
                Console.Error.WriteLine($"{sampleName} is not a valid sample as it does not contain a Run method");
                PrintValidSampleNames();
                return null;
            }
            return sampleMethod;
        }
        catch (Exception e)
        {
            Console.Error.WriteLine($"Could not load sample {sampleName}. Please check that the sample name is a valid sample name.\r\nException: {e.Message}");
            PrintValidSampleNames();
            return null;
        }
    }

    private static async Task ExecuteScript(string connectionString, string file)
    {
        var codeBaseUrl = new Uri(Assembly.GetExecutingAssembly().Location);
        var codeBasePath = Uri.UnescapeDataString(codeBaseUrl.AbsolutePath);
        var dirPath = Path.GetDirectoryName(codeBasePath);
        if (dirPath == null)
        {
            throw new DirectoryNotFoundException("Could not find the sample directory");
        }
        var filePath = Path.Combine(dirPath, file);
        var script = await File.ReadAllTextAsync(filePath);
        var statements = script.Split(";").Where(statement => !string.IsNullOrWhiteSpace(statement));
        await ExecuteBatchAsync(connectionString, statements);
    }

    private static async Task ExecuteBatchAsync(string connectionString, IEnumerable<string> statements)
    {
        await using var connection = new SpannerConnection(connectionString);
        await connection.OpenAsync();
        var batch = connection.CreateBatch();
        foreach (var statement in statements)
        {
            var cmd = batch.CreateBatchCommand();
            cmd.CommandText = statement;
            batch.BatchCommands.Add(cmd);
        }
        await batch.ExecuteNonQueryAsync();
    }

    private static void PrintValidSampleNames()
    {
        var sampleClasses = GetSampleClasses();
        Console.Error.WriteLine("");
        Console.Error.WriteLine("Supported samples:");
        sampleClasses.ToList().ForEach(t => Console.Error.WriteLine($"  * {t.Name}"));
    }

    private static IEnumerable<System.Type> GetSampleClasses()
        => from t in Assembly.GetExecutingAssembly().GetTypes()
            where t.IsClass && t.Name.EndsWith("Sample")
            select t;
}
