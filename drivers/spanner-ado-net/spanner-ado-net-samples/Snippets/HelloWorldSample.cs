namespace Google.Cloud.Spanner.DataProvider.Samples.Snippets;

public class HelloWorldSample
{
    public static async Task Run(string connectionString)
    {
        await using var connection = new SpannerConnection(connectionString);
        await connection.OpenAsync();
        await using var command = connection.CreateCommand();
        command.CommandText = "SELECT 'Hello World' as Message";
        await using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            Console.WriteLine($"Greeting from Spanner: {reader.GetString(0)}");
        }
    }
}
