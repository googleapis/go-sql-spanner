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
/// This sample shows how to execute DDL batches using the Spanner ADO.NET data provider.
/// Executing multiple DDL statements as a single batch is much more efficient than
/// executing them as separate statements.
/// </summary>
public static class DdlBatchSample
{
    public static async Task Run(string connectionString)
    {
        await using var connection = new SpannerConnection(connectionString);
        await connection.OpenAsync();

        string[] statements = [
            "create table if not exists table1 (id int64 primary key, value string(max))",
            "create table if not exists table2 (id int64 primary key, value string(max))"
        ];
        
        // Execute a batch of DDL statements using the generic ADO.NET DbBatch API.
        await ExecuteWithAdoBatch(connection, statements);
        
        // Execute a batch of DDL statements using START BATCH DDL / RUN BATCH commands.
        await ExecuteWithStartBatchDdl(connection, statements);
        
        // Execute a batch of DDL statements as a single command text.
        await ExecuteAsSingleCommand(connection, statements);
    }

    private static async Task ExecuteAsSingleCommand(SpannerConnection connection, string[] statements)
    {
        // A batch of DDL statements can also be executed as a single SQL command text.
        // Each statement should be separated by a semicolon.
        await using var command = connection.CreateCommand();
        command.CommandText = string.Join(";", statements);
        await command.ExecuteNonQueryAsync();
        
        Console.WriteLine("Executed a single SQL string with multiple DDL statements as one batch.");
    }

    /// <summary>
    /// This method shows how to use the generic ADO.NET batch API to execute a batch of DDL statements.
    /// </summary>
    private static async Task ExecuteWithAdoBatch(SpannerConnection connection, string[] statements)
    {
        var batch = connection.CreateBatch();
        foreach (var statement in statements)
        {
            var command = batch.CreateBatchCommand();
            command.CommandText = statement;
            batch.BatchCommands.Add(command);
        }
        // Execute the batch of DDL statements.
        await batch.ExecuteNonQueryAsync();
        
        Console.WriteLine("Executed ADO.NET batch");
    }
    
    /// <summary>
    /// This method shows how to use the custom SQL statements START BATCH DDL / RUN BATCH to execute a batch of DDL
    /// statements.
    /// </summary>
    private static async Task ExecuteWithStartBatchDdl(SpannerConnection connection, string[] statements)
    {
        var command = connection.CreateCommand();
        command.CommandText = "START BATCH DDL";
        await command.ExecuteNonQueryAsync();
        
        // All following DDL statements will just be buffered in memory until we execute RUN BATCH.
        foreach (var statement in statements)
        {
            command.CommandText = statement;
            // This does not really execute anything on Spanner, as the DDL statement is just buffered in memory.
            await command.ExecuteNonQueryAsync();
        }
        
        // Execute the batch of DDL statements.
        command.CommandText = "RUN BATCH";
        await command.ExecuteNonQueryAsync();
        
        Console.WriteLine("Executed DDL batch");
    }
    
}
