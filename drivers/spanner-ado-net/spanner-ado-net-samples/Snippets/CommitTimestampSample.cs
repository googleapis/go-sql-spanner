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

// Example for getting the commit timestamp of a read/write transaction.
public static class CommitTimestampSample
{
    public static async Task Run(string connectionString)
    {
        await using var connection = new SpannerConnection(connectionString);
        await connection.OpenAsync();
        
        // Execute a read/write transaction and retrieve the commit timestamp of that transaction.
        await using var transaction = await connection.BeginTransactionAsync();
        
        await using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = "INSERT INTO Singers (SingerId, FirstName, LastName) VALUES (@id, @first, @last)";
        command.Parameters.AddWithValue("id", Random.Shared.NextInt64());
        command.Parameters.AddWithValue("first", "Bruce");
        command.Parameters.AddWithValue("last", "Allison");
        Console.WriteLine($"Inserted {await command.ExecuteNonQueryAsync()} singer(s)");
        
        await transaction.CommitAsync();
        
        // Retrieve the last commit timestamp of this connection through the COMMIT_TIMESTAMP variable.
        await using var showCommand = connection.CreateCommand();
        showCommand.CommandText = "SHOW VARIABLE COMMIT_TIMESTAMP";
        var commitTimestamp = await showCommand.ExecuteScalarAsync();
        
        Console.WriteLine($"Transaction committed at {commitTimestamp}");
    }
}
