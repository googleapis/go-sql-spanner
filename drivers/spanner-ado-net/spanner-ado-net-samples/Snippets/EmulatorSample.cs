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

namespace Google.Cloud.Spanner.DataProvider.Samples.Snippets;

/// <summary>
/// This sample shows how to connect to the Spanner Emulator with the Spanner ADO.NET driver.
/// </summary>
public class EmulatorSample
{
    public static async Task Run(string connectionString)
    {
        // Create a SpannerConnectionStringBuilder and set the AutoConfigEmulator property to true.
        // This instructs the driver to do the following:
        // 1. Connect to the default Emulator endpoint 'localhost:9010' using plain text and no credentials.
        // 2. Automatically create the Spanner instance and database in the connection string.
        // The latter means that you do not need to first create the instance and database using some other tool
        // before connecting to the Emulator.
        // You can override the default endpoint by setting the Host and Port properties in the ConnectionStringBuilder
        // if the Emulator is running on a non-default host/port.
        var builder = new SpannerConnectionStringBuilder(connectionString)
        {
            AutoConfigEmulator = true
        };

        // Create a new connection using the ConnectionString from the SpannerConnectionStringBuilder.
        await using var connection = new SpannerConnection(builder.ConnectionString);
        await connection.OpenAsync();
        await using var command = connection.CreateCommand();
        command.CommandText = "SELECT 'Hello World' as Message";
        await using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            Console.WriteLine($"Greeting from Spanner Emulator: {reader.GetString(0)}");
        }
    }
}
