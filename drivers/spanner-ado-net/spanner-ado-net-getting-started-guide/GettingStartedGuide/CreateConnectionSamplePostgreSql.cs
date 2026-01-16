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

namespace Google.Cloud.Spanner.DataProvider.GettingStartedGuide;

public static class CreateConnectionSamplePostgreSql
{
    // [START spanner_create_connection]
    public static async Task CreateConnection(string connectionString)
    {
        // Use a SpannerConnectionStringBuilder to construct a connection string.
        // The SpannerConnectionStringBuilder contains properties for the most
        // used connection string variables.
        var builder = new SpannerConnectionStringBuilder(connectionString)
        {
            // Sets the default isolation level that should be used for all
            // read/write transactions on this connection.
            DefaultIsolationLevel = IsolationLevel.RepeatableRead,
            
            // The Options property can be used to set any connection property
            // as a key-value pair.
            Options = "statement_cache_size=2000"
        };
        
        await using var connection = new SpannerConnection(builder.ConnectionString);
        await connection.OpenAsync();
        
        await using var command = connection.CreateCommand();
        command.CommandText = "SELECT 'Hello World' as Message";
        await using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            Console.WriteLine($"Greeting from Spanner: {reader.GetString(0)}");
        }
    }
    // [END spanner_create_connection]
}
