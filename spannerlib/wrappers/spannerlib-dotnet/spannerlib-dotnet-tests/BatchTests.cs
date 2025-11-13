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

using Google.Cloud.Spanner.Admin.Database.V1;
using Google.Cloud.Spanner.V1;
using Google.Cloud.SpannerLib.MockServer;
using Google.Protobuf.WellKnownTypes;

namespace Google.Cloud.SpannerLib.Tests;

public class BatchTests : AbstractMockServerTests
{
    [Test]
    public void TestBatchDml([Values] LibType libType)
    {
        var insert = "insert into test (id, value) values (@id, @value)";
        Fixture.SpannerMock.AddOrUpdateStatementResult(insert, StatementResult.CreateUpdateCount(1));
        
        using var pool = Pool.Create(SpannerLibDictionary[libType], ConnectionString);
        using var connection = pool.CreateConnection();
        var updateCounts = connection.ExecuteBatch([
            new ExecuteBatchDmlRequest.Types.Statement {Sql = insert, Params = new Struct
            {
                Fields =
                {
                    ["id"] = Value.ForString("1"),
                    ["value"] = Value.ForString("One"),
                },
            }},
            new ExecuteBatchDmlRequest.Types.Statement {Sql = insert, Params = new Struct
            {
                Fields =
                {
                    ["id"] = Value.ForString("2"),
                    ["value"] = Value.ForString("Two"),
                },
            }},
        ]);
        Assert.That(updateCounts, Is.EqualTo(new long[]{1,1}));
        Assert.That(Fixture.SpannerMock.Requests.OfType<ExecuteBatchDmlRequest>().Count(), Is.EqualTo(1));
    }

    [Test]
    public void TestBatchDdl([Values] LibType libType)
    {
        // We don't need to set up any results for DDL statements on the mock server.
        // It automatically responds with an long-running operation that has finished when it receives a DDL request.
        using var pool = Pool.Create(SpannerLibDictionary[libType], ConnectionString);
        using var connection = pool.CreateConnection();
        // The input argument for ExecuteBatch is always a ExecuteBatchDmlRequest, even for DDL statements.
        var updateCounts = connection.ExecuteBatch([
            new ExecuteBatchDmlRequest.Types.Statement {Sql = "create table my_table (id int64 primary key, value string(max))"},
            new ExecuteBatchDmlRequest.Types.Statement {Sql = "create index my_index on my_table (value)"},
        ]);
        Assert.That(updateCounts, Is.EqualTo(new long[]{-1,-1}));
        Assert.That(Fixture.DatabaseAdminMock.Requests.OfType<UpdateDatabaseDdlRequest>().Count(), Is.EqualTo(1));
    }
    
}