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
using Google.Protobuf.WellKnownTypes;
using Google.Rpc;

namespace Google.Cloud.SpannerLib.Tests;

public class ConnectionTests : AbstractMockServerTests
{
    [Test]
    public void TestCreateConnection([Values] LibType libType)
    {
        using var pool = Pool.Create(SpannerLibDictionary[libType], ConnectionString);
        using var connection = pool.CreateConnection();
        Assert.That(connection, Is.Not.Null);
        Assert.That(connection.Id, Is.GreaterThan(0));
        Assert.That(Fixture.SpannerMock.Requests.OfType<CreateSessionRequest>().Count(), Is.EqualTo(1));
    }

    [Test]
    public void TestCreateTwoConnections([Values] LibType libType)
    {
        using var pool = Pool.Create(SpannerLibDictionary[libType], ConnectionString);
        using var connection1 = pool.CreateConnection();
        using var connection2 = pool.CreateConnection();
        Assert.That(connection1, Is.Not.Null);
        Assert.That(connection2, Is.Not.Null);
        Assert.That(connection1.Id, Is.GreaterThan(0));
        Assert.That(connection2.Id, Is.GreaterThan(0));
        Assert.That(connection1.Id, Is.Not.EqualTo(connection2.Id));
        Assert.That(Fixture.SpannerMock.Requests.OfType<CreateSessionRequest>().Count(), Is.EqualTo(1));
    }

    [Test]
    public void TestWriteMutations([Values] LibType libType)
    {
        using var pool = Pool.Create(SpannerLibDictionary[libType], ConnectionString);
        using var connection = pool.CreateConnection();
        var insertMutation = new Mutation
        {
            Insert = new Mutation.Types.Write
            {
                Table = "my_table",
                Columns = { new[] { "id", "value" } },
            }
        };
        insertMutation.Insert.Values.AddRange([
            new ListValue{Values = { Value.ForString("1"), Value.ForString("One") }},
            new ListValue{Values = { Value.ForString("2"), Value.ForString("Two") }}
        ]);
        var insertOrUpdateMutation = new Mutation
        {
            InsertOrUpdate = new Mutation.Types.Write
            {
                Table = "my_table",
                Columns = { new[] { "id", "value" } },
            }
        };
        insertOrUpdateMutation.InsertOrUpdate.Values.AddRange([
            new ListValue{Values = { Value.ForString("0"), Value.ForString("Zero") }}
        ]);
        
        var response = connection.WriteMutations(new BatchWriteRequest.Types.MutationGroup
        {
            Mutations = { new []{insertMutation, insertOrUpdateMutation}}
        });
        Assert.That(response, Is.Not.Null);
        Assert.That(response.CommitTimestamp, Is.Not.Null);
        Assert.That(Fixture.SpannerMock.Requests.OfType<BeginTransactionRequest>().Count(), Is.EqualTo(1));
        Assert.That(Fixture.SpannerMock.Requests.OfType<CommitRequest>().Count(), Is.EqualTo(1));
        var commit = Fixture.SpannerMock.Requests.OfType<CommitRequest>().Single();
        Assert.That(commit, Is.Not.Null);
        Assert.That(commit.Mutations.Count, Is.EqualTo(2));
        Assert.That(commit.Mutations[0].Insert.Values.Count, Is.EqualTo(2));
        Assert.That(commit.Mutations[1].InsertOrUpdate.Values.Count, Is.EqualTo(1));
    }

    [Test]
    public async Task TestWriteMutationsAsync([Values] LibType libType)
    {
        await using var pool = Pool.Create(SpannerLibDictionary[libType], ConnectionString);
        await using var connection = pool.CreateConnection();
        var insertMutation = new Mutation
        {
            Insert = new Mutation.Types.Write
            {
                Table = "my_table",
                Columns = { new[] { "id", "value" } },
            }
        };
        insertMutation.Insert.Values.AddRange([
            new ListValue{Values = { Value.ForString("1"), Value.ForString("One") }},
            new ListValue{Values = { Value.ForString("2"), Value.ForString("Two") }}
        ]);
        var insertOrUpdateMutation = new Mutation
        {
            InsertOrUpdate = new Mutation.Types.Write
            {
                Table = "my_table",
                Columns = { new[] { "id", "value" } },
            }
        };
        insertOrUpdateMutation.InsertOrUpdate.Values.AddRange([
            new ListValue{Values = { Value.ForString("0"), Value.ForString("Zero") }}
        ]);
        
        var response = await connection.WriteMutationsAsync(new BatchWriteRequest.Types.MutationGroup
        {
            Mutations = { new []{insertMutation, insertOrUpdateMutation}}
        });
        Assert.That(response, Is.Not.Null);
        Assert.That(response.CommitTimestamp, Is.Not.Null);
        Assert.That(Fixture.SpannerMock.Requests.OfType<BeginTransactionRequest>().Count(), Is.EqualTo(1));
        Assert.That(Fixture.SpannerMock.Requests.OfType<CommitRequest>().Count(), Is.EqualTo(1));
        var commit = Fixture.SpannerMock.Requests.OfType<CommitRequest>().Single();
        Assert.That(commit, Is.Not.Null);
        Assert.That(commit.Mutations.Count, Is.EqualTo(2));
        Assert.That(commit.Mutations[0].Insert.Values.Count, Is.EqualTo(2));
        Assert.That(commit.Mutations[1].InsertOrUpdate.Values.Count, Is.EqualTo(1));
    }

    [Test]
    public void TestWriteMutationsInTransaction([Values] LibType libType)
    {
        using var pool = Pool.Create(SpannerLibDictionary[libType], ConnectionString);
        using var connection = pool.CreateConnection();
        connection.BeginTransaction(new TransactionOptions());
        
        var insertMutation = new Mutation
        {
            Insert = new Mutation.Types.Write
            {
                Table = "my_table",
                Columns = { new[] { "id", "value" } },
            }
        };
        insertMutation.Insert.Values.AddRange([
            new ListValue{Values = { Value.ForString("1"), Value.ForString("One") }}
        ]);
        var response = connection.WriteMutations(new BatchWriteRequest.Types.MutationGroup
        {
            Mutations = { new []{insertMutation}}
        });
        // The response should be null, as the mutations are only buffered in the current transaction.
        Assert.That(response, Is.Null);
        // Committing the transaction should send the mutations to Spanner.
        response = connection.Commit();
        Assert.That(response, Is.Not.Null);
        
        Assert.That(Fixture.SpannerMock.Requests.OfType<BeginTransactionRequest>().Count(), Is.EqualTo(1));
        Assert.That(Fixture.SpannerMock.Requests.OfType<CommitRequest>().Count(), Is.EqualTo(1));
        var commit = Fixture.SpannerMock.Requests.OfType<CommitRequest>().Single();
        Assert.That(commit, Is.Not.Null);
        Assert.That(commit.Mutations.Count, Is.EqualTo(1));
        Assert.That(commit.Mutations[0].Insert.Values.Count, Is.EqualTo(1));
    }

    [Test]
    public void TestWriteMutationsInReadOnlyTransaction([Values] LibType libType)
    {
        using var pool = Pool.Create(SpannerLibDictionary[libType], ConnectionString);
        using var connection = pool.CreateConnection();
        connection.BeginTransaction(new TransactionOptions{ReadOnly = new TransactionOptions.Types.ReadOnly()});
        
        var insertMutation = new Mutation
        {
            Insert = new Mutation.Types.Write
            {
                Table = "my_table",
                Columns = { new[] { "id", "value" } },
            }
        };
        insertMutation.Insert.Values.AddRange([
            new ListValue{Values = { Value.ForString("1"), Value.ForString("One") }}
        ]);
        var exception = Assert.Throws<SpannerException>((Action)(() => connection.WriteMutations(
            new BatchWriteRequest.Types.MutationGroup
            {
                Mutations = { new[] { insertMutation } }
            })));
        Assert.That(exception.Code, Is.EqualTo(Code.FailedPrecondition));
    }

    private class TestLibObject : Google.Cloud.SpannerLib.AbstractLibObject
    {
        public bool CloseLibObjectCalled { get; private set; }

        public TestLibObject(long id) : base(null!, id)
        {
        }

        public void CallDispose(bool disposing)
        {
            Dispose(disposing);
        }

        protected override void CloseLibObject()
        {
            CloseLibObjectCalled = true;
        }
    }

    [Test]
    public void Finalizer_DoesNotCallCloseLibObject()
    {
        var testObj = new TestLibObject(id: 42);

        testObj.CallDispose(disposing: false);

        Assert.That(testObj.CloseLibObjectCalled, Is.False);
    }

    [Test]
    public void Dispose_CallsCloseLibObject()
    {
        var testObj = new TestLibObject(id: 42);

        testObj.CallDispose(disposing: true);

        Assert.That(testObj.CloseLibObjectCalled, Is.True);
    }
}