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

public static class DdlBatchSamplePostgreSql
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
            "create table venues (" +
            "  venue_id    bigint not null primary key, " +
            "  name        varchar(1024), " +
            "  description jsonb" +
            ")");
        batch.BatchCommands.Add(
            "create table concerts (" +
            "  concert_id bigint not null primary key, " +
            "  venue_id   bigint not null, " +
            "  singer_id  bigint not null, " +
            "  start_time timestamptz, " +
            "  end_time   timestamptz, " +
            "  constraint fk_concerts_venues foreign key " +
            "    (venue_id) references venues (venue_id), " +
            "  constraint fk_concerts_singers foreign key " +
            "    (singer_id) references singers (singer_id)" +
            ")");
        await batch.ExecuteNonQueryAsync();
        
        Console.WriteLine("Added Venues and Concerts tables");
    }
    // [END spanner_ddl_batch]
}
