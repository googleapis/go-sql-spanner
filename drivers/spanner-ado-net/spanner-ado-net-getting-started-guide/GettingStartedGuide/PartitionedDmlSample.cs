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

public static class PartitionedDmlSample
{
    // [START spanner_partitioned_dml]
    public static async Task PartitionedDml(string connectionString)
    {
        await using var connection = new SpannerConnection(connectionString);
        await connection.OpenAsync();

        // Enable Partitioned DML on this connection.
        await using var command = connection.CreateCommand();
        command.CommandText = "SET AUTOCOMMIT_DML_MODE='PARTITIONED_NON_ATOMIC'";
        await command.ExecuteNonQueryAsync();
        
        // Back-fill a default value for the MarketingBudget column.
        command.CommandText = "UPDATE Albums SET MarketingBudget=0 WHERE MarketingBudget IS NULL";
        var affected = await command.ExecuteNonQueryAsync();
        
        // Partitioned DML returns the minimum number of records that were affected.
        Console.WriteLine($"Updated at least {affected} albums");
        
        // Reset the value for AUTOCOMMIT_DML_MODE to its default.
        command.CommandText = "RESET AUTOCOMMIT_DML_MODE";
        await command.ExecuteNonQueryAsync();
    }
    // [END spanner_partitioned_dml]
}
