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
using Google.Cloud.SpannerLib.Native.Impl;
using Google.Protobuf.WellKnownTypes;
using Google.Rpc;

namespace Google.Cloud.SpannerLib.Tests;

public class TransactionTests
{
    private readonly ISpannerLib _spannerLib = new SharedLibSpanner();
    
    private SpannerMockServerFixture _fixture;
    
    private string ConnectionString =>  $"{_fixture.Host}:{_fixture.Port}/projects/p1/instances/i1/databases/d1;UsePlainText=true";
        
    [SetUp]
    public void Setup()
    {
        _fixture = new SpannerMockServerFixture();
    }
        
    [TearDown]
    public void Teardown()
    {
        _fixture.Dispose();
    }

    [Test]
    public void TestBeginAndCommit()
    {
        using var pool = Pool.Create(_spannerLib, ConnectionString);
        using var connection = pool.CreateConnection();
        connection.BeginTransaction(new TransactionOptions());
        connection.Commit();
        
        // TODO: The library should take a shortcut and just skip committing empty transactions.
        Assert.That(_fixture.SpannerMock.Requests.OfType<BeginTransactionRequest>().Count(), Is.EqualTo(1));
        Assert.That(_fixture.SpannerMock.Requests.OfType<CommitRequest>().Count(), Is.EqualTo(1));
    }

    [Test]
    public void TestBeginAndRollback()
    {
        using var pool = Pool.Create(_spannerLib, ConnectionString);
        using var connection = pool.CreateConnection();
        connection.BeginTransaction(new TransactionOptions());
        connection.Rollback();
        
        // An empty transaction that is rolled back should be a no-op.
        Assert.That(_fixture.SpannerMock.Requests.OfType<BeginTransactionRequest>().Count(), Is.EqualTo(0));
        Assert.That(_fixture.SpannerMock.Requests.OfType<RollbackRequest>().Count(), Is.EqualTo(0));
        Assert.That(_fixture.SpannerMock.Requests.OfType<CommitRequest>().Count(), Is.EqualTo(0));
    }

    [Test]
    public void TestReadWriteTransaction()
    {
        var updateSql = "update my_table set value=@value where id=@id";
        _fixture.SpannerMock.AddOrUpdateStatementResult(updateSql, StatementResult.CreateUpdateCount(1));
        
        using var pool = Pool.Create(_spannerLib, ConnectionString);
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
        Assert.That(_fixture.SpannerMock.Requests.OfType<BeginTransactionRequest>().Count(), Is.EqualTo(0));
        Assert.That(_fixture.SpannerMock.Requests.OfType<ExecuteSqlRequest>().Count(), Is.EqualTo(1));
        Assert.That(_fixture.SpannerMock.Requests.OfType<CommitRequest>().Count(), Is.EqualTo(1));
        
        var request = _fixture.SpannerMock.Requests.OfType<ExecuteSqlRequest>().Single();
        Assert.That(request.Transaction?.Begin?.ReadWrite, Is.Not.Null);
    }

    [Test]
    public void TestReadOnlyTransaction()
    {
        var numRows = 5;
        var sql = "select * from random";
        _fixture.SpannerMock.AddOrUpdateStatementResult(sql, StatementResult.CreateQuery(RandomResultSetGenerator.Generate(numRows)));
        
        using var pool = Pool.Create(_spannerLib, ConnectionString);
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
        Assert.That(_fixture.SpannerMock.Requests.OfType<BeginTransactionRequest>().Count(), Is.EqualTo(0));
        Assert.That(_fixture.SpannerMock.Requests.OfType<ExecuteSqlRequest>().Count(), Is.EqualTo(1));
        Assert.That(_fixture.SpannerMock.Requests.OfType<CommitRequest>().Count(), Is.EqualTo(0));
        
        var request = _fixture.SpannerMock.Requests.OfType<ExecuteSqlRequest>().Single();
        Assert.That(request.Transaction?.Begin?.ReadOnly, Is.Not.Null);
    }

    [Test]
    public void TestBeginTwice()
    {
        using var pool = Pool.Create(_spannerLib, ConnectionString);
        using var connection = pool.CreateConnection();
        // Try to start two transactions on a connection.
        connection.BeginTransaction(new TransactionOptions());
        var exception = Assert.Throws<SpannerException>(() => connection.BeginTransaction(new TransactionOptions()));
        Assert.That(exception.Code, Is.EqualTo(Code.FailedPrecondition));
    }
}