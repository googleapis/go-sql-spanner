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
/// This sample shows how to use query parameters with the Spanner ADO.NET data provider.
/// Using query parameters is recommended, as it allows Spanner to cache and reuse the query plan,
/// and it helps to prevent SQL injection attacks in your application.
/// </summary>
public static class QueryParametersSample
{
    public static async Task Run(string connectionString)
    {
        await using var connection = new SpannerConnection(connectionString);
        await connection.OpenAsync();
        
        // The Spanner ADO.NET driver supports named query parameters.
        await NamedQueryParameters(connection);

        // The Spanner ADO.NET driver also supports positional query parameters.
        // Use a question mark (?) for positional query parameters in the SQL string.
        await PositionalQueryParameters(connection);
    }

    private static async Task NamedQueryParameters(SpannerConnection connection)
    {
        await using var command = connection.CreateCommand();
        command.CommandText = "SELECT SingerId, FullName " +
                              "FROM Singers " +
                              "WHERE LastName LIKE @lastName " +
                              "   OR FirstName LIKE @firstName " +
                              "ORDER BY LastName, FirstName";
        command.Parameters.AddWithValue("lastName", "R%");
        command.Parameters.AddWithValue("firstName", "A%");
        await using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            Console.WriteLine($"Found singer with named parameters: {reader.GetString(1)}");
        }
    }

    private static async Task PositionalQueryParameters(SpannerConnection connection)
    {
        await using var command = connection.CreateCommand();
        command.CommandText = "SELECT SingerId, FullName " +
                              "FROM Singers " +
                              "WHERE LastName LIKE ? " +
                              "   OR FirstName LIKE ? " +
                              "ORDER BY LastName, FirstName";
        // The driver allows you to just add the parameter values that you want to use when using positional parameters.
        // The values must be added in the order of the positional parameters in the SQL string.
        command.Parameters.Add("R%");
        command.Parameters.Add("A%");
        await using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            Console.WriteLine($"Found singer with positional parameters: {reader.GetString(1)}");
        }
    }
}
