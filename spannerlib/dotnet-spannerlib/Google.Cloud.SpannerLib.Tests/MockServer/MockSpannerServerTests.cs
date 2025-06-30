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

using System.Globalization;
using Google.Cloud.Spanner.V1;
using Google.Protobuf;
using Google.Protobuf.WellKnownTypes;
using Grpc.Core;
using V1 = Google.Cloud.Spanner.V1;

namespace Google.Cloud.SpannerLib.Tests.MockServer
{
    public class MockSpannerServerTests
    {
        private SpannerMockServerFixture _fixture;
        
        private string ConnectionString =>  $"{_fixture.Host}:{_fixture.Port}/projects/p1/instances/i1/databases/d1;UsePlainText=true";
        
        [SetUp]
        public void Setup()
        {
            _fixture = new SpannerMockServerFixture();
            _fixture.SpannerMock.AddOrUpdateStatementResult("SELECT 1", StatementResult.CreateSelect1ResultSet());
        }
        
        [TearDown]
        public void Teardown()
        {
            _fixture.Dispose();
        }
        
        [Test]
        public void BatchCreateSessions()
        {
            var builder = new SpannerClientBuilder
            {
                Endpoint = $"http://{_fixture.Endpoint}",
                ChannelCredentials = ChannelCredentials.Insecure
            };
            SpannerClient client = builder.Build();
            BatchCreateSessionsRequest request = new BatchCreateSessionsRequest
            {
                Database = "projects/p1/instances/i1/databases/d1",
                SessionCount = 25,
                SessionTemplate = new Session()
            };
            BatchCreateSessionsResponse response = client.BatchCreateSessions(request);
            Assert.That(response.Session.Count, Is.EqualTo(25));
        }
        
        [Test]
        public void SingleUseSelect()
        {
            // Create connection to Cloud Spanner.
            using var pool = Pool.Create(ConnectionString);
            using var connection = pool.CreateConnection();
            var statement = new ExecuteSqlRequest
            {
                Sql = "SELECT 1"
            };
            using var rows = connection.Execute(statement);
            for (var row = rows.Next(); row != null; row = rows.Next())
            {
                Assert.That(row.Values.Count, Is.EqualTo(1));
                Assert.That(row.Values[0].HasStringValue);
                Assert.That(row.Values[0].StringValue, Is.EqualTo("1"));
            }
        }
        
        [Test]
        public void ReadOnlyTxSelect()
        {
            using var pool = Pool.Create(ConnectionString);
            using var connection = pool.CreateConnection();
            var options = new TransactionOptions
            {
                ReadOnly = new TransactionOptions.Types.ReadOnly()
            };
            using var transaction = connection.BeginTransaction(options);
            var statement = new ExecuteSqlRequest
            {
                Sql = "SELECT 1"
            };
            using var rows = transaction.Execute(statement);
            for (var row = rows.Next(); row != null; row = rows.Next())
            {
                Assert.That(row.Values.Count, Is.EqualTo(1));
                Assert.That(row.Values[0].HasStringValue);
                Assert.That(row.Values[0].StringValue, Is.EqualTo("1"));
            }
        }
        
        [Test]
        public void WriteMutations()
        {
            using var pool = Pool.Create(ConnectionString);
            using (var connection = pool.CreateConnection())
            {
                var commitResponse = connection.Apply(new BatchWriteRequest.Types.MutationGroup
                {
                    Mutations = { new Mutation
                    {
                        InsertOrUpdate = new Mutation.Types.Write
                        {
                            Table = "Singers",
                            Columns = { "SingerId", "FirstName", "LastName" },
                            Values = { new ListValue
                            {
                                Values = { new Value {StringValue = "1"}, new Value {StringValue = "FirstName1"}, new Value {StringValue = "LastName1"} }
                            } }
                        }
                    } }
                });
                Assert.That(commitResponse, Is.Not.Null);
                Assert.That(commitResponse.CommitTimestamp, Is.Not.Null);
            }
            var requests = _fixture.SpannerMock.Requests;
            var commit = (CommitRequest)requests.Last();
            Assert.That(commit.Mutations.First().OperationCase, Is.EqualTo(Mutation.OperationOneofCase.InsertOrUpdate));
            Assert.That(commit.Mutations.First().InsertOrUpdate.Table, Is.EqualTo("Singers"));
        }
        
    [Test]
    public void ReadWriteTransaction()
    {
        decimal initialBudget1 = 1225250.00m;
        decimal initialBudget2 = 2250198.28m;
        _fixture.SpannerMock.AddOrUpdateStatementResult(
            "SELECT MarketingBudget FROM Albums WHERE SingerId = 1 AND AlbumId = 1",
            StatementResult.CreateSingleColumnResultSet(new V1.Type { Code = V1.TypeCode.Numeric }, "MarketingBudget", initialBudget1));
        _fixture.SpannerMock.AddOrUpdateStatementResult(
            "SELECT MarketingBudget FROM Albums WHERE SingerId = 2 AND AlbumId = 2",
            StatementResult.CreateSingleColumnResultSet(new V1.Type { Code = V1.TypeCode.Numeric }, "MarketingBudget", initialBudget2));
    
        decimal transferAmount = 200000;
        decimal secondBudget = 0;
        decimal firstBudget = 0;
    
        using var pool = Pool.Create(ConnectionString);
        using var connection = pool.CreateConnection();
        using (var transaction = connection.BeginTransaction(new TransactionOptions{ReadWrite = new TransactionOptions.Types.ReadWrite()}))
        {
            // Create statement to select the second album's data.
            var cmdLookup = new ExecuteSqlRequest
            {
                Sql = "SELECT MarketingBudget FROM Albums WHERE SingerId = 2 AND AlbumId = 2",
            };
            // Excecute the select query.
            using (var rows = transaction.Execute(cmdLookup))
            {
                for (var row = rows.Next(); row != null; row = rows.Next())
                {
                    secondBudget = decimal.Parse(row.Values[0].StringValue);
                }
            }
            // Read the first album's budget.
            cmdLookup = new ExecuteSqlRequest
            {
                Sql = "SELECT MarketingBudget FROM Albums WHERE SingerId = 1 AND AlbumId = 1",
            };
            using (var rows = transaction.Execute(cmdLookup))
            {
                for (var row = rows.Next(); row != null; row = rows.Next())
                {
                    firstBudget = decimal.Parse(row.Values[0].StringValue);
                }
            }
            // Update second album to remove the transfer amount.
            secondBudget -= transferAmount;
            // Specify update command parameters.
            var updateBudget2 = new Mutation
            {
                Update = new Mutation.Types.Write
                {
                    Table = "Albums",
                    Columns = { "SingerId", "AlbumId", "MarketingBudget" },
                    Values = { new ListValue
                    {
                        Values = { new Value { StringValue = "2" }, new Value { StringValue = "2" }, new Value { StringValue = secondBudget.ToString(CultureInfo.InvariantCulture) } } 
                    } }
                }
            };
            firstBudget += transferAmount;
            var updateBudget1 = new Mutation
            {
                Update = new Mutation.Types.Write
                {
                    Table = "Albums",
                    Columns = { "SingerId", "AlbumId", "MarketingBudget" },
                    Values = { new ListValue
                    {
                        Values = { new Value { StringValue = "1" }, new Value { StringValue = "1" }, new Value { StringValue = firstBudget.ToString(CultureInfo.InvariantCulture) } }
                    } }
                }
            };
            transaction.BufferWrite(new BatchWriteRequest.Types.MutationGroup {Mutations = { updateBudget2, updateBudget1 }});
            transaction.Commit();
        }
        // Assert that the correct updates were sent.
        // Ignore Rollback requests, as these are sent async and can come from other test cases.
        Stack<IMessage> requests = new Stack<IMessage>(_fixture.SpannerMock.Requests.Where(request => request.GetType() != typeof(RollbackRequest)));
        Assert.That(requests.Peek().GetType(), Is.EqualTo(typeof(CommitRequest)));
        CommitRequest commit = (CommitRequest)requests.Pop();
        Assert.That(commit.Mutations.Count, Is.EqualTo(2));
    
        Mutation update1 = commit.Mutations.Last();
        Assert.That(update1.OperationCase, Is.EqualTo(Mutation.OperationOneofCase.Update));
        Assert.That(update1.Update.Table, Is.EqualTo("Albums"));
        Assert.That(update1.Update.Values[0].Values[0].StringValue, Is.EqualTo("1"));
        Assert.That(decimal.Parse(update1.Update.Values[0].Values[2].StringValue), Is.EqualTo(firstBudget));
    
        Mutation update2 = commit.Mutations.First();
        Assert.That(update2.OperationCase, Is.EqualTo(Mutation.OperationOneofCase.Update));
        Assert.That(update2.Update.Table, Is.EqualTo("Albums"));
        Assert.That(update2.Update.Values[0].Values[0].StringValue, Is.EqualTo("2"));
        Assert.That(decimal.Parse(update2.Update.Values[0].Values[2].StringValue), Is.EqualTo(secondBudget));
    }
        
    }
}