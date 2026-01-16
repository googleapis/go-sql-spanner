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

public static class WriteDataWithDmlSample
{
    // [START spanner_dml_getting_started_insert]
    public static async Task WriteDataWithDml(string connectionString)
    {
        await using var connection = new SpannerConnection(connectionString);
        await connection.OpenAsync();
        
        // Add 4 rows in one statement.
        // The ADO.NET driver supports positional query parameters.
        await using var command = connection.CreateCommand();
        command.CommandText = "INSERT INTO Singers (SingerId, FirstName, LastName) " +
                              "VALUES (?, ?, ?), (?, ?, ?), " +
                              "       (?, ?, ?), (?, ?, ?)";
        command.Parameters.Add(12);
        command.Parameters.Add("Melissa");
        command.Parameters.Add("Garcia");

        command.Parameters.Add(13);
        command.Parameters.Add("Russel");
        command.Parameters.Add("Morales");

        command.Parameters.Add(14);
        command.Parameters.Add("Jacqueline");
        command.Parameters.Add("Long");

        command.Parameters.Add(15);
        command.Parameters.Add("Dylan");
        command.Parameters.Add("Shaw");

        var affected = await command.ExecuteNonQueryAsync();
        Console.WriteLine($"{affected} record(s) inserted.");
    }
    // [END spanner_dml_getting_started_insert]
}
