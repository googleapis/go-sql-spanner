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
/// Example for using Mutations with the Spanner ADO.NET data provider.
/// </summary>
public static class MutationsSample
{
    public static async Task Run(string connectionString)
    {
        await using var connection = new SpannerConnection(connectionString);
        await connection.OpenAsync();

        // The following methods can be used to create commands that use Mutations instead of DML:
        // - CreateInsertCommand
        // - CreateInsertOrUpdateCommand
        // - CreateUpdateCommand
        // - CreateReplaceCommand
        // - CreateDeleteCommand
        // See https://docs.cloud.google.com/spanner/docs/dml-versus-mutations for more information on mutations.
        await using var command = connection.CreateInsertCommand("Singers");
        // The parameter names must correspond to column names in the table.
        command.Parameters.AddWithValue("SingerId", Random.Shared.NextInt64());
        command.Parameters.AddWithValue("FirstName", "Bruce");
        command.Parameters.AddWithValue("LastName", "Allison");
        long affected = await command.ExecuteNonQueryAsync();
        
        Console.WriteLine($"Inserted data using mutations. Affected: {affected}");
        Console.WriteLine();
    }
}
