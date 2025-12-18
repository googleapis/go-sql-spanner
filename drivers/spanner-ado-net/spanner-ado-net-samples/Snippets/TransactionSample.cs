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

/// <summary>
/// This sample shows how to execute a read/write transaction using the Spanner ADO.NET data provider.
/// </summary>
public static class TransactionSample
{
    public static async Task Run(string connectionString)
    {
        await using var connection = new SpannerConnection(connectionString);
        await connection.OpenAsync();

        // Start a read/write transaction by calling th standard BeginTransaction method.
        await using var transaction = await connection.BeginTransactionAsync(IsolationLevel.RepeatableRead);
        
        // Execute a query that uses this transaction.
        await using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = "SELECT SingerId " +
                              "FROM Singers " +
                              "WHERE BirthDate IS NULL";
        var updateCount = 0;
        await using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            // Update the birthdate of each Singer without a known birthdate to a default value.
            await using var updateCommand = connection.CreateCommand();
            updateCommand.Transaction = transaction;
            updateCommand.CommandText = "UPDATE Singers SET BirthDate=DATE '1900-01-01' WHERE SingerId=@singerId";
            updateCommand.Parameters.AddWithValue("singerId", reader["SingerId"]);
            await updateCommand.ExecuteNonQueryAsync();
            updateCount++;
        }
        await transaction.CommitAsync();
        Console.WriteLine($"Set a default birthdate for {updateCount} singers");
    }
}
