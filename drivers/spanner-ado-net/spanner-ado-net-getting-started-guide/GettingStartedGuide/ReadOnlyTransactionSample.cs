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

namespace Google.Cloud.Spanner.DataProvider.GettingStartedGuide;

public static class ReadOnlyTransactionSample
{
    // [START spanner_read_only_transaction]
    public static async Task ReadOnlyTransaction(string connectionString)
    {
        await using var connection = new SpannerConnection(connectionString);
        await connection.OpenAsync();
        
        // Start a read-only transaction on this connection.
        await using var transaction = await connection.BeginReadOnlyTransactionAsync();
        
        await using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = "SELECT SingerId, AlbumId, AlbumTitle " +
                              "FROM Albums " +
                              "ORDER BY SingerId, AlbumId";
        await using (var reader = await command.ExecuteReaderAsync())
        {
            while (await reader.ReadAsync())
            {
                Console.WriteLine(
                    $"{reader["SingerId"]} {reader["AlbumId"]} {reader["AlbumTitle"]}");
            }
        }

        // Execute another query using the same read-only transaction.
        command.CommandText = "SELECT SingerId, AlbumId, AlbumTitle " +
                              "FROM Albums " +
                              "ORDER BY AlbumTitle";
        await using (var reader = await command.ExecuteReaderAsync())
        {
            while (await reader.ReadAsync())
            {
                Console.WriteLine(
                    $"{reader["SingerId"]} {reader["AlbumId"]} {reader["AlbumTitle"]}");
            }
        }

        // End the read-only transaction by calling Commit.
        await transaction.CommitAsync();
    }
    // [END spanner_read_only_transaction]
}
