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

public static class WriteDataWithMutationsSamplePostgreSql
{
    // [START spanner_insert_data]
    struct Singer
    {
        internal long SingerId;
        internal string FirstName;
        internal string LastName;
    }

    struct Album
    {
        internal long SingerId;
        internal long AlbumId;
        internal string Title;
    }
    
    public static async Task WriteDataWithMutations(string connectionString)
    {
        await using var connection = new SpannerConnection(connectionString);
        await connection.OpenAsync();

        Singer[] singers =
        [
            new() {SingerId=1, FirstName = "Marc", LastName = "Richards"},
            new() {SingerId=2, FirstName = "Catalina", LastName = "Smith"},
            new() {SingerId=3, FirstName = "Alice", LastName = "Trentor"},
            new() {SingerId=4, FirstName = "Lea", LastName = "Martin"},
            new() {SingerId=5, FirstName = "David", LastName = "Lomond"},
        ];
        Album[] albums =
        [
            new() {SingerId = 1, AlbumId = 1, Title = "Total Junk"},
            new() {SingerId = 1, AlbumId = 2, Title = "Go, Go, Go"},
            new() {SingerId = 2, AlbumId = 1, Title = "Green"},
            new() {SingerId = 2, AlbumId = 2, Title = "Forever Hold Your Peace"},
            new() {SingerId = 2, AlbumId = 3, Title = "Terrified"},
        ];
        var batch = connection.CreateBatch();
        foreach (var singer in singers)
        {
            // The name of a parameter must correspond with a column name.
            var command = batch.CreateInsertCommand("singers");
            command.AddParameter("singer_id", singer.SingerId);
            command.AddParameter("first_name", singer.FirstName);
            command.AddParameter("last_name", singer.LastName);
            batch.BatchCommands.Add(command);
        }
        foreach (var album in albums)
        {
            // The name of a parameter must correspond with a column name.
            var command = batch.CreateInsertCommand("albums");
            command.AddParameter("singer_id", album.SingerId);
            command.AddParameter("album_id", album.AlbumId);
            command.AddParameter("album_title", album.Title);
            batch.BatchCommands.Add(command);
        }
        var affected = await batch.ExecuteNonQueryAsync();
        Console.WriteLine($"Inserted {affected} rows.");
    }
    // [END spanner_insert_data]
}
