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

public static class DdlBatchSample
{
    // [START spanner_ddl_batch]
    public static async Task DdlBatch(string connectionString)
    {
        await using var connection = new SpannerConnection(connectionString);
        await connection.OpenAsync();
        
        // Executing multiple DDL statements as one batch is
        // more efficient than executing each statement individually.
        var batch = connection.CreateBatch();
        batch.BatchCommands.Add(
            "CREATE TABLE Venues (" +
            "  VenueId     INT64 NOT NULL, " +
            "  Name        STRING(1024), " +
            "  Description JSON, " +
            ") PRIMARY KEY (VenueId)");
        batch.BatchCommands.Add(
            "CREATE TABLE Concerts (" +
            "  ConcertId INT64 NOT NULL, " +
            "  VenueId   INT64 NOT NULL, " +
            "  SingerId  INT64 NOT NULL, " +
            "  StartTime TIMESTAMP, " +
            "  EndTime   TIMESTAMP, " +
            "  CONSTRAINT Fk_Concerts_Venues " +
            "    FOREIGN KEY (VenueId) REFERENCES Venues (VenueId), " +
            "  CONSTRAINT Fk_Concerts_Singers " +
            "    FOREIGN KEY (SingerId) REFERENCES Singers (SingerId), " +
            ") PRIMARY KEY (ConcertId)");
        await batch.ExecuteNonQueryAsync();
        
        Console.WriteLine("Added Venues and Concerts tables");
    }
    // [END spanner_ddl_batch]
}
