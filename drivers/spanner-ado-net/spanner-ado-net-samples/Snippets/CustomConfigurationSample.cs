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

namespace Google.Cloud.Spanner.DataProvider.Samples.Snippets;

// Sample that shows how to supply a custom configuration for the Spanner client
// that is used by the driver.
public static class CustomConfigurationSample
{
    public static async Task Run(string connectionString)
    {
        // Use a SpannerConnectionBuilder to programmatically set the various options for a SpannerConnection.
        var builder = new SpannerConnectionStringBuilder(connectionString)
        {
            // Use the various properties to set commonly used configuration options, like the default isolation level.
            DefaultIsolationLevel = IsolationLevel.RepeatableRead,
            
            // The Options property can be used to set any valid connection property that is not included as a
            // programmatic option on the SpannerConnectionStringBuilder class.
            Options = "disable_route_to_leader=true;statement_cache_size=100",
        };
        
        // Create a connection using the generated connection string from the builder.
        await using var connection = new SpannerConnection(builder.ConnectionString);
        await connection.OpenAsync();
        
        // Execute a command on Spanner using the connection.
        await using var command = connection.CreateCommand();
        command.CommandText = "SELECT @greeting as Message";
        command.Parameters.AddWithValue("greeting", "Hello from Spanner", DbType.String);
        await using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            Console.WriteLine($"Greeting: {reader.GetString(0)}");
        }
    }
}
