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

public static class WriteDataWithTransactionSample
{
    // [START spanner_dml_getting_started_update]
    public static async Task WriteDataWithTransaction(string connectionString)
    {
        await using var connection = new SpannerConnection(connectionString);
        await connection.OpenAsync();
        
        // Transfer marketing budget from one album to another. We do it in a
        // transaction to ensure that the transfer is atomic.
        await using var transaction = await connection.BeginTransactionAsync();
        
        // The Spanner ADO.NET driver supports both positional and named
        // query parameters. This query uses named query parameters.
        const string selectSql =
            "SELECT MarketingBudget " +
            "FROM Albums " +
            "WHERE SingerId = @singerId and AlbumId = @albumId";
        // Get the marketing_budget of singer 2 / album 2.
        await using var command = connection.CreateCommand();
        command.CommandText = selectSql;
        command.Transaction = transaction;
        command.Parameters.AddWithValue("singerId", 2);
        command.Parameters.AddWithValue("albumId", 2);
        var budget2 = (long) (await command.ExecuteScalarAsync() ?? 0L);

        const long transfer = 20000L;
        // The transaction will only be committed if this condition still holds
        // at the time of commit. Otherwise, the transaction will be aborted.
        if (budget2 >= transfer)
        {
            // Get the marketing_budget of singer 1 / album 1.
            command.Parameters["singerId"].Value = 1;
            command.Parameters["albumId"].Value = 1;
            var budget1 = (long) (await command.ExecuteScalarAsync() ?? 0L);
            
            // Transfer part of the marketing budget of Album 2 to Album 1.
            budget1 += transfer;
            budget2 -= transfer;
            const string updateSql =
                "UPDATE Albums " +
                "SET MarketingBudget = @budget " +
                "WHERE SingerId = @singerId and AlbumId = @albumId";
            // Create a DML batch and execute it as part of the current transaction.
            var batch = connection.CreateBatch();
            batch.Transaction = transaction;
            
            // Update the marketing budgets of both Album 1 and Album 2 in a batch.
            (long SingerId, long AlbumId, long MarketingBudget)[] budgets = [
                new (1L, 1L, budget1),
                new (2L, 2L, budget2),
            ];
            foreach (var budget in budgets)
            {
                var batchCommand = batch.CreateBatchCommand();
                batchCommand.CommandText = updateSql;
                var singerIdParameter = batchCommand.CreateParameter();
                singerIdParameter.ParameterName = "singerId";
                singerIdParameter.Value = budget.SingerId;
                batchCommand.Parameters.Add(singerIdParameter);
                var albumIdParameter = batchCommand.CreateParameter();
                albumIdParameter.ParameterName = "albumId";
                albumIdParameter.Value = budget.AlbumId;
                batchCommand.Parameters.Add(albumIdParameter);
                var marketingBudgetParameter = batchCommand.CreateParameter();
                marketingBudgetParameter.ParameterName = "budget";
                marketingBudgetParameter.Value = budget.MarketingBudget;
                batchCommand.Parameters.Add(marketingBudgetParameter);
                batch.BatchCommands.Add(batchCommand);
            }
            var affected = await batch.ExecuteNonQueryAsync();
            // The batch should update 2 rows.
            if (affected != 2)
            {
                await transaction.RollbackAsync();
                throw new InvalidOperationException($"Unexpected num affected: {affected}");
            }
        }
        // Commit the transaction.
        await transaction.CommitAsync();
        Console.WriteLine("Transferred marketing budget from Album 2 to Album 1");
    }
    // [END spanner_dml_getting_started_update]
}
