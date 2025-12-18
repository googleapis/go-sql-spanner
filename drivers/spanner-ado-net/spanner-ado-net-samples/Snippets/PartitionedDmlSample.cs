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
/// Example for using Partitioned DML with the Spanner ADO.NET data provider.
/// </summary>
public static class PartitionedDmlSample
{
    public static async Task Run(string connectionString)
    {
        await using var connection = new SpannerConnection(connectionString);
        await connection.OpenAsync();
        
        // Partitioned DML can be used for bulk updates and bulk deletes that exceed the mutation limit in Spanner.
        // Partitioned DML statements must be executed outside transactions, also known as in auto-commit mode.
        // Use the AUTOCOMMIT_DML_MODE variable to set the type of transaction that the ADO.NET driver should use for
        // DML statements in auto-commit mode.
        
        // AUTOCOMMIT_DML_MODE supports two values:
        // 1. 'TRANSACTIONAL': Use a normal, atomic read/write transaction for the statement.
        // 2. 'PARTITIONED_NON_ATOMIC': Use a Partitioned DML transaction for the statement.
        await using var command = connection.CreateCommand();
        command.CommandText = "set autocommit_dml_mode = 'partitioned_non_atomic'";
        await command.ExecuteNonQueryAsync();

        // Do a bulk update to set a default value for all unknown birthdates.
        command.CommandText = "update Singers set BirthDate=date '1900-01-01' where BirthDate is null";
        long affected = await command.ExecuteNonQueryAsync();
        
        // Reset the mode to the default.
        command.CommandText = "reset autocommit_dml_mode";
        await command.ExecuteNonQueryAsync();
        
        Console.WriteLine($"Executed a Partitioned DML statement. Affected: {affected}");
        Console.WriteLine();
    }
}
