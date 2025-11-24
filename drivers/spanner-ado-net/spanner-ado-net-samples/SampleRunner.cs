using System.Reflection;
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

    private static void RunSample(string sampleName, bool failOnException)
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
            if (emulatorRunner.IsEmulatorRunning())
            {
                Console.WriteLine("Emulator is already running. Re-using existing Emulator instance...");
                Console.WriteLine("");
            }
            else
            {
                Console.WriteLine("Starting emulator...");
                var portBinding = await emulatorRunner.StartEmulator();
                Console.WriteLine($"Emulator started on port {portBinding.HostPort}");
                Console.WriteLine("");
                startedEmulator = true;
            }

            var projectId = "sample-project";
            var instanceId = "sample-instance";
            var databaseId = "sample-database";
            DatabaseName databaseName = DatabaseName.FromProjectInstanceDatabase(projectId, instanceId, databaseId);
            var connectionStringBuilder = new SpannerConnectionStringBuilder()
            {
                DataSource = databaseName.ToString(),
                AutoConfigEmulator = true,
            };

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

    private static async Task CreateSampleDataModel(string connectionString)
    {
        var codeBaseUrl = new Uri(Assembly.GetExecutingAssembly().Location);
        var codeBasePath = Uri.UnescapeDataString(codeBaseUrl.AbsolutePath);
        var dirPath = Path.GetDirectoryName(codeBasePath);
        var fileName = Path.Combine(dirPath, "SampleModel/SampleDataModel.sql");
        var script = await File.ReadAllTextAsync(fileName);
        var statements = script.Split(";");
        for (var i = 0; i < statements.Length; i++)
        {
            // Remove license header from script
            if (statements[i].IndexOf("/*", StringComparison.Ordinal) >= 0 && statements[i].IndexOf("*/", StringComparison.Ordinal) >= 0)
            {
                int startIndex = statements[i].IndexOf("/*", StringComparison.Ordinal);
                int endIndex = statements[i].IndexOf("*/", startIndex, StringComparison.Ordinal) + "*/".Length;
                statements[i] = statements[i].Remove(startIndex, endIndex - startIndex);
            }
            statements[i] = statements[i].Trim(new char[] { '\r', '\n' });
        }
        int length = statements.Length;
        if (statements[length - 1] == "")
        {
            length--;
        }
        await ExecuteDdlAsync(connectionString, statements, length);
    }

    private static async Task ExecuteDdlAsync(string connectionString, string[] ddl, int length)
    {
        await using var connection = new SpannerConnection(connectionString);
        await connection.OpenAsync();
        var batch = connection.CreateBatch();
        foreach (var statement in ddl)
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
