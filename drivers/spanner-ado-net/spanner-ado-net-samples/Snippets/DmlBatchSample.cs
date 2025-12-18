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

using System.Data.Common;

namespace Google.Cloud.Spanner.DataProvider.Samples.Snippets;

/// <summary>
/// This sample shows how to execute DML batches using the Spanner ADO.NET data provider.
/// Executing multiple DML statements as a single batch is more efficient than
/// executing them as separate statements, as it reduces the number of round-trips to Spanner.
/// </summary>
public static class DmlBatchSample
{
    private struct Singer
    {
        internal long Id;
        internal string FirstName;
        internal string LastName;
    }
    
    public static async Task Run(string connectionString)
    {
        await using var connection = new SpannerConnection(connectionString);
        await connection.OpenAsync();

        Singer[] singers = [
            new() {Id = Random.Shared.NextInt64(), FirstName = "Lea", LastName = "Martin"},
            new() {Id = Random.Shared.NextInt64(), FirstName = "David", LastName = "Lomond"},
            new() {Id = Random.Shared.NextInt64(), FirstName = "Elena", LastName = "Campbell"},
        ];
        const string insertStatement = "insert or update into Singers (SingerId, FirstName, LastName) values (@Id, @FirstName, @LastName)";
        const string updateStatement = "update Singers set BirthDate = NULL where BirthDate < DATE '1900-01-01'";
        
        // Execute a batch of DDL statements using the generic ADO.NET DbBatch API.
        await ExecuteWithAdoBatch(connection, insertStatement, updateStatement, singers);
        
        // Execute a batch of DDL statements using START BATCH DDL / RUN BATCH commands.
        await ExecuteWithStartBatchDml(connection, insertStatement, updateStatement, singers);
    }

    /// <summary>
    /// This method shows how to use the generic ADO.NET batch API to execute a batch of DML statements.
    /// </summary>
    private static async Task ExecuteWithAdoBatch(SpannerConnection connection, string insertStatement, string updateStatement, Singer[] singers)
    {
        var batch = connection.CreateBatch();
        // Add some INSERT statements to the batch.
        foreach (var singer in singers)
        {
            var command = batch.CreateBatchCommand();
            command.CommandText = insertStatement;
            AddBatchParameter(command, "Id", singer.Id);
            AddBatchParameter(command, "FirstName", singer.FirstName);
            AddBatchParameter(command, "LastName", singer.LastName);
            batch.BatchCommands.Add(command);
        }
        // Add an UPDATE statement to the batch.
        var updateCommand = batch.CreateBatchCommand();
        updateCommand.CommandText = updateStatement;
        batch.BatchCommands.Add(updateCommand);
        
        // Execute the batch of DML statements.
        long affected = await batch.ExecuteNonQueryAsync();
        
        Console.WriteLine($"Executed ADO.NET batch");
        Console.WriteLine("Affected: " + affected);
        Console.WriteLine();
    }

    private static void AddBatchParameter(DbBatchCommand command, string name, object value)
    {
        var parameter = command.CreateParameter();
        parameter.ParameterName = name;
        parameter.Value = value;
        command.Parameters.Add(parameter);
    }

    /// <summary>
    /// This method shows how to use the custom SQL statements START BATCH DML / RUN BATCH to execute a batch of DML
    /// statements.
    /// </summary>
    private static async Task ExecuteWithStartBatchDml(SpannerConnection connection, string insertStatement, string updateStatement, Singer[] singers)
    {
        await using var command = connection.CreateCommand();
        command.CommandText = "START BATCH DML";
        await command.ExecuteNonQueryAsync();
        
        // All following DML statements will just be buffered in memory until we execute RUN BATCH.
        foreach (var singer in singers)
        {
            await using var insertCommand = connection.CreateCommand();
            insertCommand.CommandText = insertStatement;
            AddParameter(insertCommand, "Id", singer.Id);
            AddParameter(insertCommand, "FirstName", singer.FirstName);
            AddParameter(insertCommand, "LastName", singer.LastName);
            // This does not really execute anything on Spanner, as the DML statement is just buffered in memory.
            await insertCommand.ExecuteNonQueryAsync();
        }
        await using var updateCommand = connection.CreateCommand();
        updateCommand.CommandText = updateStatement;
        await updateCommand.ExecuteNonQueryAsync();
        
        // Execute the batch of DML statements.
        command.CommandText = "RUN BATCH";
        var affected = await command.ExecuteNonQueryAsync();
        
        Console.WriteLine("Executed DML batch");
        Console.WriteLine("Affected: " + affected);
        Console.WriteLine();
    }

    private static void AddParameter(DbCommand command, string name, object value)
    {
        var parameter = command.CreateParameter();
        parameter.ParameterName = name;
        parameter.Value = value;
        command.Parameters.Add(parameter);
    }
    
}
