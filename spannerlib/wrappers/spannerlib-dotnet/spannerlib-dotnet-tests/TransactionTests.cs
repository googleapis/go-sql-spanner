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

using Google.Cloud.Spanner.V1;
using Google.Cloud.SpannerLib.MockServer;
using Google.Protobuf.WellKnownTypes;
using Google.Rpc;
using Grpc.Core;

namespace Google.Cloud.SpannerLib.Tests;

public class TransactionTests : AbstractMockServerTests
{
    [Test]
    public void TestBeginAndCommit([Values] LibType libType)
    {
        using var pool = Pool.Create(SpannerLibDictionary[libType], ConnectionString);
        using var connection = pool.CreateConnection();
        connection.BeginTransaction(new TransactionOptions());
        connection.Commit();
        
        Assert.That(Fixture.SpannerMock.Requests.OfType<BeginTransactionRequest>().Count(), Is.EqualTo(0));
        Assert.That(Fixture.SpannerMock.Requests.OfType<CommitRequest>().Count(), Is.EqualTo(0));
    }

    [Test]
    public void TestBeginAndRollback([Values] LibType libType)
    {
        using var pool = Pool.Create(SpannerLibDictionary[libType], ConnectionString);
        using var connection = pool.CreateConnection();
        connection.BeginTransaction(new TransactionOptions());
        connection.Rollback();
        
        // An empty transaction that is rolled back should be a no-op.
        Assert.That(Fixture.SpannerMock.Requests.OfType<BeginTransactionRequest>().Count(), Is.EqualTo(0));
        Assert.That(Fixture.SpannerMock.Requests.OfType<RollbackRequest>().Count(), Is.EqualTo(0));
        Assert.That(Fixture.SpannerMock.Requests.OfType<CommitRequest>().Count(), Is.EqualTo(0));
    }

    [Test]
    public void TestReadWriteTransaction([Values] LibType libType)
    {
        var updateSql = "update my_table set value=@value where id=@id";
        Fixture.SpannerMock.AddOrUpdateStatementResult(updateSql, StatementResult.CreateUpdateCount(1));
        
        using var pool = Pool.Create(SpannerLibDictionary[libType], ConnectionString);
        using var connection = pool.CreateConnection();
        connection.BeginTransaction(new TransactionOptions());

        using var rows = connection.Execute(new ExecuteSqlRequest
        {
            Sql = updateSql,
            Params = new Struct
            {
                Fields =
                {
                    ["value"] = Value.ForString("test-value"),
                    ["id"] = Value.ForString("1")
                },
            }
        });
        Assert.That(rows.Next(), Is.Null);
        Assert.That(rows.UpdateCount, Is.EqualTo(1));
        
        var commitResponse = connection.Commit();
        Assert.That(commitResponse, Is.Not.Null);
        Assert.That(commitResponse.CommitTimestamp, Is.Not.Null);
        
        // There should be no BeginTransaction requests, as the transaction start is inlined with the
        // first statement.
        Assert.That(Fixture.SpannerMock.Requests.OfType<BeginTransactionRequest>().Count(), Is.EqualTo(0));
        Assert.That(Fixture.SpannerMock.Requests.OfType<ExecuteSqlRequest>().Count(), Is.EqualTo(1));
        Assert.That(Fixture.SpannerMock.Requests.OfType<CommitRequest>().Count(), Is.EqualTo(1));
        
        var request = Fixture.SpannerMock.Requests.OfType<ExecuteSqlRequest>().Single();
        Assert.That(request.Transaction?.Begin?.ReadWrite, Is.Not.Null);
    }

    [Test]
    public void TestAbortedReadWriteTransaction([Values] LibType libType)
    {
        var updateSql = "update my_table set value=@value where id=@id";
        Fixture.SpannerMock.AddOrUpdateStatementResult(updateSql, StatementResult.CreateUpdateCount(1));
        
        using var pool = Pool.Create(SpannerLibDictionary[libType], ConnectionString);
        using var connection = pool.CreateConnection();
        connection.BeginTransaction(new TransactionOptions
        {
            IsolationLevel = TransactionOptions.Types.IsolationLevel.RepeatableRead,
        });
        using(connection.Execute(new ExecuteSqlRequest
        {
            Sql = "set local transaction_tag = 'my_tx_tag'",
        }));

        using var rows = connection.Execute(new ExecuteSqlRequest
        {
            Sql = updateSql,
            Params = new Struct
            {
                Fields =
                {
                    ["value"] = Value.ForString("test-value"),
                    ["id"] = Value.ForString("1")
                },
            }
        });
        Assert.That(rows.Next(), Is.Null);
        Assert.That(rows.UpdateCount, Is.EqualTo(1));
        
        Fixture.SpannerMock.AbortNextStatement();
        var commitResponse = connection.Commit();
        Assert.That(commitResponse, Is.Not.Null);
        Assert.That(commitResponse.CommitTimestamp, Is.Not.Null);
        
        Assert.That(Fixture.SpannerMock.Requests.OfType<BeginTransactionRequest>().Count(), Is.EqualTo(1));
        Assert.That(Fixture.SpannerMock.Requests.OfType<ExecuteSqlRequest>().Count(), Is.EqualTo(2));
        Assert.That(Fixture.SpannerMock.Requests.OfType<CommitRequest>().Count(), Is.EqualTo(2));
        
        var requests = Fixture.SpannerMock.Requests.OfType<ExecuteSqlRequest>().ToList();
        Assert.That(requests[0].Transaction?.Begin?.ReadWrite, Is.Not.Null);
        Assert.That(requests[1].Transaction?.HasId ?? false, Is.True);
        foreach (var request in requests)
        {
            Assert.That(request.RequestOptions.TransactionTag, Is.EqualTo("my_tx_tag"));
        }
        foreach (var request in Fixture.SpannerMock.Requests.OfType<CommitRequest>())
        {
            Assert.That(request.RequestOptions.TransactionTag, Is.EqualTo("my_tx_tag"));
        }
        // TODO: Enable once this has been fixed in the client library.
        // foreach (var request in Fixture.SpannerMock.Requests.OfType<BeginTransactionRequest>())
        // {
        //     Assert.That(request.RequestOptions.TransactionTag, Is.EqualTo("my_tx_tag"));
        // }
    }

    [Test]
    public void TestReadOnlyTransaction([Values] LibType libType)
    {
        var numRows = 5;
        var sql = "select * from random";
        Fixture.SpannerMock.AddOrUpdateStatementResult(sql, StatementResult.CreateQuery(RandomResultSetGenerator.Generate(numRows)));
        
        using var pool = Pool.Create(SpannerLibDictionary[libType], ConnectionString);
        using var connection = pool.CreateConnection();
        connection.BeginTransaction(new TransactionOptions
        {
            ReadOnly = new TransactionOptions.Types.ReadOnly(),
        });

        using var rows = connection.Execute(new ExecuteSqlRequest { Sql = sql });
        var rowCount = 0;
        while (rows.Next() != null)
        {
            rowCount++;
        }
        Assert.That(rowCount, Is.EqualTo(numRows));
        
        // Read-only transactions must also be committed or rolled back, although this is a no-op on Spanner.
        // There is no CommitResponse for read-only transactions.
        var commitResponse = connection.Commit();
        Assert.That(commitResponse, Is.Null);
        
        // There should be no BeginTransaction requests, as the transaction start is inlined with the
        // first statement.
        Assert.That(Fixture.SpannerMock.Requests.OfType<BeginTransactionRequest>().Count(), Is.EqualTo(0));
        Assert.That(Fixture.SpannerMock.Requests.OfType<ExecuteSqlRequest>().Count(), Is.EqualTo(1));
        Assert.That(Fixture.SpannerMock.Requests.OfType<CommitRequest>().Count(), Is.EqualTo(0));
        
        var request = Fixture.SpannerMock.Requests.OfType<ExecuteSqlRequest>().Single();
        Assert.That(request.Transaction?.Begin?.ReadOnly, Is.Not.Null);
    }

    [Test]
    public void TestBeginTwice([Values] LibType libType)
    {
        using var pool = Pool.Create(SpannerLibDictionary[libType], ConnectionString);
        using var connection = pool.CreateConnection();
        // Try to start two transactions on a connection.
        connection.BeginTransaction(new TransactionOptions());
        var exception = Assert.Throws<SpannerException>(() => connection.BeginTransaction(new TransactionOptions()));
        Assert.That(exception.Code, Is.EqualTo(Code.FailedPrecondition));
    }

    [Test]
    public void TestCommitFailed([Values] LibType libType)
    {
        Fixture.SpannerMock.AddOrUpdateExecutionTime(nameof(Fixture.SpannerMock.Commit), ExecutionTime.CreateException(StatusCode.FailedPrecondition, "Invalid mutations"));
        
        using var pool = Pool.Create(SpannerLibDictionary[libType], ConnectionString);
        using var connection = pool.CreateConnection();
        var exception = Assert.Throws<SpannerException>(() => connection.WriteMutations(new BatchWriteRequest.Types.MutationGroup()));
        Assert.That(exception.Code, Is.EqualTo(Code.FailedPrecondition));
    }
    
}