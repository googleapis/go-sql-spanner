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

using Google.Cloud.Spanner.V1;
using Google.Protobuf.WellKnownTypes;

namespace Google.Cloud.Spanner.DataProvider.Samples.Snippets;

/// <summary>
/// This sample shows how to execute stale reads using both single statements and read-only transaction
/// with the Spanner ADO.NET data provider.
/// </summary>
public static class StaleReadSample
{
    public static async Task Run(string connectionString)
    {
        await using var connection = new SpannerConnection(connectionString);
        await connection.OpenAsync();

        // Execute a single stale query.
        await SingleStaleQuery(connection);
        // Execute a read-only transaction with a staleness setting.
        await StaleReadOnlyTransaction(connection);
    }

    /// <summary>
    /// Executes a single query that uses a max staleness setting.
    /// </summary>
    private static async Task SingleStaleQuery(SpannerConnection connection)
    {
        await using var command = connection.CreateCommand();
        command.SingleUseReadOnlyTransactionOptions = new TransactionOptions.Types.ReadOnly
        {
            MaxStaleness = Duration.FromTimeSpan(TimeSpan.FromSeconds(10)),
        };
        command.CommandText = "SELECT SingerId, FullName " +
                              "FROM Singers " +
                              "ORDER BY LastName, FirstName";
        await using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            Console.WriteLine($"Found singer using a single stale query: {reader.GetString(1)}");
        }
    }

    /// <summary>
    /// Executes a read-only transaction with an exact staleness.
    /// </summary>
    /// <param name="connection"></param>
    private static async Task StaleReadOnlyTransaction(SpannerConnection connection)
    {
        // Get the current time from Spanner so we can use that as the read-timestamp for the transaction.
        await using var currentTimeCommand = connection.CreateCommand();
        currentTimeCommand.CommandText = "SELECT CURRENT_TIMESTAMP";
        var currentTime = (DateTime?) await currentTimeCommand.ExecuteScalarAsync();
        
        // Start a read-only transaction using the BeginReadOnlyTransaction method.
        await using var transaction = connection.BeginReadOnlyTransaction(new TransactionOptions.Types.ReadOnly
        {
            ReadTimestamp = Timestamp.FromDateTime(currentTime!.Value),
        });
        
        // Execute a query that uses this transaction.
        await using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = "SELECT SingerId, FullName " +
                              "FROM Singers " +
                              "ORDER BY LastName, FirstName";
        await using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            Console.WriteLine($"Found singer using a stale read-only transaction: {reader.GetString(1)}");
        }
        
        // The read-only transaction must be committed or rolled back to release it from the connection.
        // Committing or rolling back a read-only transaction is a no-op on Spanner.
        await transaction.CommitAsync();
    }
}
