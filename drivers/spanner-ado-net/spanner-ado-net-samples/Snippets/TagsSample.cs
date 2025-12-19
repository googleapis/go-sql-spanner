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
/// This sample shows how to use request and transaction tags with the Spanner ADO.NET data provider.
/// </summary>
public static class TagsSample
{
    public static async Task Run(string connectionString)
    {
        await using var connection = new SpannerConnection(connectionString);
        await connection.OpenAsync();
        
        // Set a transaction tag on a read/write transaction.
        await using var transaction = await connection.BeginTransactionAsync();
        transaction.Tag = "my_transaction_tag";
        
        // Set a request tag on a command.
        // Assign the transaction to a command to instruct the command to use the transaction and transaction tag.
        await using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.Tag = "my_query_tag";
        command.CommandText = "SELECT 'Hello World' as Message";
        await using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            Console.WriteLine($"Greeting from Spanner: {reader.GetString(0)}");
        }
        
        await transaction.CommitAsync();
    }
}
