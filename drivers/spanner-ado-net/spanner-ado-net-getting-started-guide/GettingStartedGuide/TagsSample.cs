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

public static class TagsSample
{
    // [START spanner_transaction_and_statement_tag]
    public static async Task Tags(string connectionString)
    {
        await using var connection = new SpannerConnection(connectionString);
        await connection.OpenAsync();

        const long singerId = 1L;
        const long albumId = 1L;
        
        await using var transaction = await connection.BeginTransactionAsync();
        // Set a tag on the transaction before executing any statements.
        transaction.Tag = "example-tx-tag";
        
        await using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.Tag = "query-marketing-budget";
        command.CommandText =
            "SELECT MarketingBudget " +
            "FROM Albums " +
            "WHERE SingerId=? and AlbumId=?";
        command.Parameters.Add(singerId);
        command.Parameters.Add(albumId);
        var budget = (long)(await command.ExecuteScalarAsync() ?? 0L);
        
        // Reduce the marketing budget by 10% if it is more than 1,000.
        if (budget > 1000)
        {
            budget -= budget / 10;
            await using var updateCommand = connection.CreateCommand();
            updateCommand.Transaction = transaction;
            updateCommand.Tag = "reduce-marketing-budget";
            updateCommand.CommandText =
                "UPDATE Albums SET MarketingBudget=@budget WHERE SingerId=@singerId AND AlbumId=@albumId";
            updateCommand.Parameters.AddWithValue("budget", budget);
            updateCommand.Parameters.AddWithValue("singerId", singerId);
            updateCommand.Parameters.AddWithValue("albumId", albumId);
            await updateCommand.ExecuteNonQueryAsync();
        }
        // Commit the transaction.
        await transaction.CommitAsync();
        Console.WriteLine("Reduced marketing budget");
    }
    // [END spanner_transaction_and_statement_tag]
}
