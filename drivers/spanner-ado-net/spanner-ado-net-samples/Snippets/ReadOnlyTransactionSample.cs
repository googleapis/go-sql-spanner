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
/// This sample shows how to execute a read-only transaction using the Spanner ADO.NET data provider.
/// Read-only transactions do not take locks on Spanner, and should be used when your application needs
/// to execute multiple queries that read from the same snapshot of the database.
/// </summary>
public static class ReadOnlyTransactionSample
{
    public static async Task Run(string connectionString)
    {
        await using var connection = new SpannerConnection(connectionString);
        await connection.OpenAsync();

        // Start a read-only transaction using the BeginReadOnlyTransaction method.
        await using var transaction = connection.BeginReadOnlyTransaction();
        
        // Execute a query that uses this transaction.
        await using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = "SELECT SingerId, FullName " +
                              "FROM Singers " +
                              "WHERE LastName LIKE @lastName " +
                              "   OR FirstName LIKE @firstName " +
                              "ORDER BY LastName, FirstName";
        command.Parameters.AddWithValue("lastName", "R%");
        command.Parameters.AddWithValue("firstName", "A%");
        await using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            Console.WriteLine($"Found singer: {reader.GetString(1)}");
        }
        
        // The read-only transaction must be committed or rolled back to release it from the connection.
        // Committing or rolling back a read-only transaction is a no-op on Spanner.
        await transaction.CommitAsync();
    }
}
