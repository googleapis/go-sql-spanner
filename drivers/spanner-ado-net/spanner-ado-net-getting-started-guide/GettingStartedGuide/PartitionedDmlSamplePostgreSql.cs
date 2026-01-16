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

public static class PartitionedDmlSamplePostgreSql
{
    // [START spanner_partitioned_dml]
    public static async Task PartitionedDml(string connectionString)
    {
        await using var connection = new SpannerConnection(connectionString);
        await connection.OpenAsync();

        // Enable Partitioned DML on this connection.
        await using var command = connection.CreateCommand();
        command.CommandText = "set autocommit_dml_mode='partitioned_non_atomic'";
        await command.ExecuteNonQueryAsync();
        
        // Back-fill a default value for the MarketingBudget column.
        command.CommandText = "update albums set marketing_budget=0 where marketing_budget is null";
        var affected = await command.ExecuteNonQueryAsync();
        
        // Partitioned DML returns the minimum number of records that were affected.
        Console.WriteLine($"Updated at least {affected} albums");
        
        // Reset the value for autocommit_dml_mode to its default.
        command.CommandText = "reset autocommit_dml_mode";
        await command.ExecuteNonQueryAsync();
    }
    // [END spanner_partitioned_dml]
}
