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
using Grpc.Core;

namespace Google.Cloud.SpannerLib.Tests;

public class BatchTests : AbstractMockServerTests
{
    [Test]
    public void TestBatchDml([Values] LibType libType, [Values] bool useTransaction)
    {
        var insert = "insert into test (id, value) values (@id, @value)";
        Fixture.SpannerMock.AddOrUpdateStatementResult(insert, StatementResult.CreateUpdateCount(1));
        
        using var pool = Pool.Create(SpannerLibDictionary[libType], ConnectionString);
        using var connection = pool.CreateConnection();
        if (useTransaction)
        {
            connection.BeginTransaction(new TransactionOptions());
        }
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
        if (useTransaction)
        {
            connection.Commit();
        }
        Assert.That(updateCounts, Is.EqualTo(new long[]{1,1}));
        Assert.That(Fixture.SpannerMock.Requests.OfType<ExecuteBatchDmlRequest>().Count(), Is.EqualTo(1));
    }
    
    [Test]
    public void TestBatchDmlScript([Values] LibType libType, [Values] bool useTransaction)
    {
        var insert = "insert into test (id, value) values (@id, @value)";
        Fixture.SpannerMock.AddOrUpdateStatementResult(insert, StatementResult.CreateUpdateCount(1));
        
        using var pool = Pool.Create(SpannerLibDictionary[libType], ConnectionString);
        using var connection = pool.CreateConnection();
        if (useTransaction)
        {
            connection.BeginTransaction(new TransactionOptions());
        }
        using (connection.Execute(new ExecuteSqlRequest{Sql = "start batch dml"}));
        using (connection.Execute(new ExecuteSqlRequest
               {
                   Sql = insert,
                   Params = new Struct
                   {
                       Fields =
                       {
                           ["id"] = Value.ForString("1"),
                           ["value"] = Value.ForString("One"),
                       },
                   },
               }));
        using (connection.Execute(new ExecuteSqlRequest
               {
                   Sql = insert,
                   Params = new Struct
                   {
                       Fields =
                       {
                           ["id"] = Value.ForString("2"),
                           ["value"] = Value.ForString("Two"),
                       },
                   },
               }));
        using (connection.Execute(new ExecuteSqlRequest{Sql = "run batch"}));
        if (useTransaction)
        {
            connection.Commit();
        }
        
        Assert.That(Fixture.SpannerMock.Requests.OfType<ExecuteSqlRequest>(), Is.Empty);
        Assert.That(Fixture.SpannerMock.Requests.OfType<ExecuteBatchDmlRequest>().ToList(), Has.Count.EqualTo(1));
        Assert.That(Fixture.SpannerMock.Requests.OfType<ExecuteBatchDmlRequest>().First().Statements, Has.Count.EqualTo(2));
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
    
    [Test]
    public void TestBatchDdlScript([Values] LibType libType)
    {
        // We don't need to set up any results for DDL statements on the mock server.
        // It automatically responds with an long-running operation that has finished when it receives a DDL request.
        using var pool = Pool.Create(SpannerLibDictionary[libType], ConnectionString);
        using var connection = pool.CreateConnection();
        using (connection.Execute(new ExecuteSqlRequest{Sql = "start batch ddl"}));
        using (connection.Execute(new ExecuteSqlRequest{Sql = "create table my_table (id int64 primary key, value string(max))"}));
        using (connection.Execute(new ExecuteSqlRequest{Sql = "create index my_index on my_table (value)"}));
        using (connection.Execute(new ExecuteSqlRequest{Sql = "run batch"}));
        
        Assert.That(Fixture.SpannerMock.Requests.OfType<ExecuteSqlRequest>(), Is.Empty);
        Assert.That(Fixture.DatabaseAdminMock.Requests.OfType<UpdateDatabaseDdlRequest>().ToList(), Has.Count.EqualTo(1));
        Assert.That(Fixture.DatabaseAdminMock.Requests.OfType<UpdateDatabaseDdlRequest>().First().Statements, Has.Count.EqualTo(2));
    }

    [Test]
    public void TestBatchDmlWithRequestOptions([Values] LibType libType, [Values] bool useTransaction)
    {
        var insert = "insert into test (id, value) values (1, 'One')";
        Fixture.SpannerMock.AddOrUpdateStatementResult(insert, StatementResult.CreateUpdateCount(1));

        using var pool = Pool.Create(SpannerLibDictionary[libType], ConnectionString);
        using var connection = pool.CreateConnection();
        if (useTransaction)
        {
            connection.BeginTransaction(new TransactionOptions());
        }
        var request = new ExecuteBatchDmlRequest
        {
            Statements =
            {
                new ExecuteBatchDmlRequest.Types.Statement { Sql = insert },
                new ExecuteBatchDmlRequest.Types.Statement { Sql = insert },
            },
            RequestOptions = new RequestOptions
            {
                RequestTag = "dotnet_batch_req';--",
                TransactionTag = "dotnet_batch_tx';--",
                Priority = RequestOptions.Types.Priority.Medium,
            },
        };
        var updateCounts = connection.ExecuteBatch(request);
        if (useTransaction)
        {
            connection.Commit();
        }
        Assert.That(updateCounts, Is.EqualTo(new long[] { 1, 1 }));
        var batchRequests = Fixture.SpannerMock.Requests.OfType<ExecuteBatchDmlRequest>().ToList();
        Assert.That(batchRequests, Has.Count.EqualTo(1));
        Assert.That(batchRequests[0].RequestOptions?.RequestTag, Is.EqualTo("dotnet_batch_req';--"));
        Assert.That(batchRequests[0].RequestOptions?.TransactionTag, Is.EqualTo("dotnet_batch_tx';--"));
        Assert.That(batchRequests[0].RequestOptions?.Priority, Is.EqualTo(RequestOptions.Types.Priority.Medium));

        var commitRequests = Fixture.SpannerMock.Requests.OfType<CommitRequest>().ToList();
        Assert.That(commitRequests, Has.Count.EqualTo(1));
        Assert.That(commitRequests[0].RequestOptions?.TransactionTag, Is.EqualTo("dotnet_batch_tx';--"));

        using var rows = connection.Execute(new ExecuteSqlRequest { Sql = insert });
        var execRequests = Fixture.SpannerMock.Requests.OfType<ExecuteSqlRequest>().ToList();
        Assert.That(execRequests, Has.Count.EqualTo(1));
        Assert.That(execRequests[0].RequestOptions?.RequestTag ?? "", Is.Empty);
        Assert.That(execRequests[0].RequestOptions?.TransactionTag ?? "", Is.Empty);
        Assert.That(execRequests[0].RequestOptions?.Priority ?? RequestOptions.Types.Priority.Unspecified, Is.EqualTo(RequestOptions.Types.Priority.Unspecified));
    }

    [Test]
    public async Task TestBatchDmlWithRequestOptionsAsync([Values] LibType libType, [Values] bool useTransaction)
    {
        var insert = "insert into test (id, value) values (1, 'One')";
        Fixture.SpannerMock.AddOrUpdateStatementResult(insert, StatementResult.CreateUpdateCount(1));

        await using var pool = Pool.Create(SpannerLibDictionary[libType], ConnectionString);
        await using var connection = pool.CreateConnection();
        if (useTransaction)
        {
            await connection.BeginTransactionAsync(new TransactionOptions());
        }
        var request = new ExecuteBatchDmlRequest
        {
            Statements =
            {
                new ExecuteBatchDmlRequest.Types.Statement { Sql = insert },
                new ExecuteBatchDmlRequest.Types.Statement { Sql = insert },
            },
            RequestOptions = new RequestOptions
            {
                RequestTag = "dotnet_async_batch_req",
                TransactionTag = "dotnet_async_batch_tx",
                Priority = RequestOptions.Types.Priority.Low,
            },
        };
        var updateCounts = await connection.ExecuteBatchAsync(request);
        if (useTransaction)
        {
            await connection.CommitAsync();
        }
        Assert.That(updateCounts, Is.EqualTo(new long[] { 1, 1 }));
        var batchRequests = Fixture.SpannerMock.Requests.OfType<ExecuteBatchDmlRequest>().ToList();
        Assert.That(batchRequests, Has.Count.EqualTo(1));
        Assert.That(batchRequests[0].RequestOptions?.RequestTag, Is.EqualTo("dotnet_async_batch_req"));
        Assert.That(batchRequests[0].RequestOptions?.TransactionTag, Is.EqualTo("dotnet_async_batch_tx"));
        Assert.That(batchRequests[0].RequestOptions?.Priority, Is.EqualTo(RequestOptions.Types.Priority.Low));

        var commitRequests = Fixture.SpannerMock.Requests.OfType<CommitRequest>().ToList();
        Assert.That(commitRequests, Has.Count.EqualTo(1));
        Assert.That(commitRequests[0].RequestOptions?.TransactionTag, Is.EqualTo("dotnet_async_batch_tx"));

        await using var rows = await connection.ExecuteAsync(new ExecuteSqlRequest { Sql = insert });
        var execRequests = Fixture.SpannerMock.Requests.OfType<ExecuteSqlRequest>().ToList();
        Assert.That(execRequests, Has.Count.EqualTo(1));
        Assert.That(execRequests[0].RequestOptions?.RequestTag ?? "", Is.Empty);
        Assert.That(execRequests[0].RequestOptions?.TransactionTag ?? "", Is.Empty);
        Assert.That(execRequests[0].RequestOptions?.Priority ?? RequestOptions.Types.Priority.Unspecified, Is.EqualTo(RequestOptions.Types.Priority.Unspecified));
    }

    [Test]
    public void TestBatchDmlWithRequestOptionsErrorNoLeak([Values] LibType libType)
    {
        var invalidInsert = "insert into non_existing_table (id, value) values (1, 'One')";
        var validInsert = "insert into test (id, value) values (1, 'One')";
        Fixture.SpannerMock.AddOrUpdateStatementResult(invalidInsert,
            StatementResult.CreateException(new RpcException(new Status(StatusCode.NotFound, "Table not found"))));
        Fixture.SpannerMock.AddOrUpdateStatementResult(validInsert, StatementResult.CreateUpdateCount(1));

        using var pool = Pool.Create(SpannerLibDictionary[libType], ConnectionString);
        using var connection = pool.CreateConnection();
        Assert.Throws<SpannerException>(() => connection.ExecuteBatch(new ExecuteBatchDmlRequest
        {
            Statements = { new ExecuteBatchDmlRequest.Types.Statement { Sql = invalidInsert } },
            RequestOptions = new RequestOptions
            {
                RequestTag = "dotnet_err_req",
                TransactionTag = "dotnet_err_tx",
                Priority = RequestOptions.Types.Priority.High,
            },
        }));

        using var rows = connection.Execute(new ExecuteSqlRequest { Sql = validInsert });
        var execRequests = Fixture.SpannerMock.Requests.OfType<ExecuteSqlRequest>().ToList();
        Assert.That(execRequests, Has.Count.EqualTo(1));
        Assert.That(execRequests[0].RequestOptions?.RequestTag ?? "", Is.Empty);
        Assert.That(execRequests[0].RequestOptions?.TransactionTag ?? "", Is.Empty);
        Assert.That(execRequests[0].RequestOptions?.Priority ?? RequestOptions.Types.Priority.Unspecified, Is.EqualTo(RequestOptions.Types.Priority.Unspecified));
    }
}