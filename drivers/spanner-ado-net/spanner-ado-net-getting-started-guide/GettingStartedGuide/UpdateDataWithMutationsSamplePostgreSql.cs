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

public static class UpdateDataWithMutationsSamplePostgreSql
{
    // [START spanner_update_data]
    public static async Task UpdateDataWithMutations(string connectionString)
    {
        await using var connection = new SpannerConnection(connectionString);
        await connection.OpenAsync();

        (long SingerId, long AlbumId, long MarketingBudget)[] albums = [
            (1L, 1L, 100000L),
            (2L, 2L, 500000L),
        ];
        // Use a batch to update two rows in one round-trip.
        var batch = connection.CreateBatch();
        foreach (var album in albums)
        {
            // This creates a command that will use a mutation to update the row.
            var command = batch.CreateUpdateCommand("albums");
            command.AddParameter("singer_id", album.SingerId);
            command.AddParameter("album_id", album.AlbumId);
            command.AddParameter("marketing_budget", album.MarketingBudget);
            batch.BatchCommands.Add(command);
        }
        var affected = await batch.ExecuteNonQueryAsync();
        Console.WriteLine($"Updated {affected} albums.");
    }
    // [END spanner_update_data]
}
