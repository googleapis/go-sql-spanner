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

public static class CreateTablesSample
{
    // [START spanner_create_database]
    public static async Task CreateTables(string connectionString)
    {
        await using var connection = new SpannerConnection(connectionString);
        await connection.OpenAsync();
        
        // Create two tables in one batch on Spanner.
        var batch = connection.CreateBatch();
        batch.BatchCommands.Add("CREATE TABLE Singers (" +
                                "  SingerId   INT64 NOT NULL, " +
                                "  FirstName  STRING(1024), " +
                                "  LastName   STRING(1024), " +
                                "  SingerInfo BYTES(MAX) " +
                                ") PRIMARY KEY (SingerId)");
        batch.BatchCommands.Add("CREATE TABLE Albums ( " +
                                "  SingerId     INT64 NOT NULL, " +
                                "  AlbumId      INT64 NOT NULL, " +
                                "  AlbumTitle   STRING(MAX)" +
                                ") PRIMARY KEY (SingerId, AlbumId), " +
                                "INTERLEAVE IN PARENT Singers ON DELETE CASCADE");
        await batch.ExecuteNonQueryAsync();
        Console.WriteLine("Created Singers & Albums tables");
    }
    // [END spanner_create_database]
}
