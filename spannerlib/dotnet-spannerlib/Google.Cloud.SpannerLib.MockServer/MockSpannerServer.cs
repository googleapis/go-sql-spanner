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

using System;
using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Google.Cloud.Spanner.Admin.Database.V1;
using Google.Cloud.Spanner.Common.V1;
using Google.Cloud.Spanner.V1;
using Google.Cloud.SpannerLib.Tests.MockServer;
using Google.LongRunning;
using Google.Protobuf;
using Google.Protobuf.WellKnownTypes;
using Google.Rpc;
using Grpc.Core;
using Status = Google.Rpc.Status;
using GrpcCore = Grpc.Core;

namespace Google.Cloud.SpannerLib.MockServer
{
    public class StatementResult
    {
        public enum StatementResultType
        {
            ResultSet,
            UpdateCount,
            Exception
        }

        public StatementResultType Type { get; }
        public ResultSet ResultSet { get; }
        public long UpdateCount { get; }
        public Exception Exception { get; }

        public static StatementResult CreateQuery(ResultSet resultSet)
        {
            return new StatementResult(resultSet);
        }

        public static StatementResult CreateUpdateCount(long count)
        {
            return new StatementResult(count);
        }

        internal static StatementResult CreateException(Exception exception)
        {
            return new StatementResult(exception);
        }

        public static StatementResult CreateSelect1ResultSet()
        {
            return CreateSingleColumnResultSet(new Spanner.V1.Type { Code = Spanner.V1.TypeCode.Int64 }, "COL1", 1);
        }

        public static StatementResult CreateSingleColumnResultSet(Spanner.V1.Type type, string col, params object[] values)
            => CreateSingleColumnResultSet(null, type, col, values);

        public static StatementResult CreateSingleColumnResultSet(long? updateCount, Spanner.V1.Type type, string col, params object[] values)
        {
            ResultSet rs = new ResultSet
            {
                Metadata = new ResultSetMetadata
                {
                    RowType = new StructType()
                },
            };
            rs.Metadata.RowType.Fields.Add(new StructType.Types.Field
            {
                Name = col,
                Type = type,
            });
            foreach (object val in values)
            {
                ListValue row = new ListValue();
                row.Values.Add(SpannerConverter.ToProtobufValue(type, val));
                rs.Rows.Add(row);
            }
            if (updateCount != null)
            {
                rs.Stats = new ResultSetStats { RowCountExact = updateCount.Value };
            }
            return CreateQuery(rs);
        }

        public static StatementResult CreateResultSet(IEnumerable<Tuple<Spanner.V1.TypeCode, string>> columns, IEnumerable<object[]> rows) =>
            CreateResultSet(columns.Select(x => Tuple.Create(new Spanner.V1.Type { Code = x.Item1, ArrayElementType = x.Item1 == Spanner.V1.TypeCode.Array ? new Spanner.V1.Type {Code=Spanner.V1.TypeCode.String} : null}, x.Item2)).ToList(), rows);

        public static StatementResult CreateResultSet(IEnumerable<Tuple<Spanner.V1.Type, string>> columns, IEnumerable<object[]> rows)
        {
            var rs = new ResultSet
            {
                Metadata = new ResultSetMetadata
                {
                    RowType = new StructType()
                },
            };
            foreach (var col in columns)
            {
                rs.Metadata.RowType.Fields.Add(new StructType.Types.Field
                {
                    Type = col.Item1,
                    Name = col.Item2,
                });
            }
            foreach (var rowValue in rows)
            {
                    var row = new ListValue();
                var colIndex = 0;
                foreach (var value in rowValue)
                {
                    row.Values.Add(SpannerConverter.ToProtobufValue(rs.Metadata.RowType.Fields[colIndex].Type, value));
                    colIndex++;
                }
                rs.Rows.Add(row);
            }
            return CreateQuery(rs);
        }

        private StatementResult(ResultSet resultSet)
        {
            Type = StatementResultType.ResultSet;
            ResultSet = resultSet;
        }

        private StatementResult(long updateCount)
        {
            Type = StatementResultType.UpdateCount;
            UpdateCount = updateCount;
        }

        private StatementResult(Exception exception)
        {
            Type = StatementResultType.Exception;
            Exception = exception;
        }
    }

    public class ExecutionTime
    {
        private readonly int _minimumExecutionTime;
        private readonly int _randomExecutionTime;
        private readonly Random _random;

        // TODO: Support multiple exceptions
        private Exception _exception;
        private readonly int _exceptionStreamIndex;
        private readonly BlockingCollection<int> _streamWritePermissions;
        private bool _alwaysAllowWrite;

        internal bool HasExceptionAtIndex(int index)
        {
            return _exception != null && _exceptionStreamIndex == index;
        }

        internal Exception PopExceptionAtIndex(int index)
        {
            Exception res = _exceptionStreamIndex == index ? _exception : null;
            if (res != null)
            {
                _exception = null;
            }
            return res;
        }

        public void AlwaysAllowWrite()
        {
            _alwaysAllowWrite = true;
            _streamWritePermissions.Add(int.MaxValue);
        }

        internal int TakeWritePermission()
        {
            if (_alwaysAllowWrite || _streamWritePermissions == null)
            {
                return int.MaxValue;
            }
            return _streamWritePermissions.Take();
        }

        internal void SimulateExecutionTime()
        {
            if (_minimumExecutionTime > 0 || _randomExecutionTime > 0)
            {
                int totalWaitTime = _minimumExecutionTime;
                if (_randomExecutionTime > 0)
                {
                    totalWaitTime += _random.Next(_randomExecutionTime);
                }
                Thread.Sleep(totalWaitTime);
            }
        }

        public static ExecutionTime FromMillis(int minimumExecutionTime, int randomExecutionTime)
        {
            return new ExecutionTime(minimumExecutionTime, randomExecutionTime, null, -1, null);
        }

        internal static ExecutionTime StreamException(Exception exception, int streamIndex, BlockingCollection<int> streamWritePermissions)
        {
            return new ExecutionTime(0, 0, exception, streamIndex, streamWritePermissions);
        }

        private ExecutionTime(int minimumExecutionTime, int randomExecutionTime, Exception exception, int exceptionStreamIndex, BlockingCollection<int> streamWritePermissions)
        {
            _minimumExecutionTime = minimumExecutionTime;
            _randomExecutionTime = randomExecutionTime;
            _random = _randomExecutionTime > 0 ? new Random() : null;
            _exception = exception;
            _exceptionStreamIndex = exceptionStreamIndex;
            _streamWritePermissions = streamWritePermissions;
        }
    }

    public class MockSpannerService : Spanner.V1.Spanner.SpannerBase
    {
        private class PartialResultSetsEnumerable : IEnumerable<PartialResultSet>
        {
            private readonly Spanner.V1.Transaction _transaction;
            private readonly ResultSet _resultSet;
            public PartialResultSetsEnumerable(Spanner.V1.Transaction transaction, ResultSet resultSet)
            {
                _transaction = transaction;
                _resultSet = resultSet;
            }

            public IEnumerator<PartialResultSet> GetEnumerator()
            {
                return new PartialResultSetsEnumerator(_transaction, _resultSet);
            }

            IEnumerator IEnumerable.GetEnumerator()
            {
                return new PartialResultSetsEnumerator(_transaction, _resultSet);
            }
        }

        private class PartialResultSetsEnumerator : IEnumerator<PartialResultSet>
        {
            private static readonly int s_maxRowsInChunk = 1;

            private readonly Spanner.V1.Transaction _transaction;
            private readonly ResultSet _resultSet;
            private bool _first = true;
            private int _currentRow;
            private PartialResultSet _current;

            public PartialResultSetsEnumerator(Spanner.V1.Transaction transaction, ResultSet resultSet)
            {
                _transaction = transaction;
                _resultSet = resultSet;
            }

            PartialResultSet IEnumerator<PartialResultSet>.Current => _current;

            object IEnumerator.Current => _current;

            public bool MoveNext()
            {
                _current = new PartialResultSet
                {
                    ResumeToken = ByteString.CopyFromUtf8($"{_currentRow}")
                };
                if (_first)
                {
                    _current.Metadata = _resultSet.Metadata.Clone();
                    if (_transaction != null)
                    {
                        _current.Metadata.Transaction = _transaction;
                    }
                    _first = false;
                }
                else if (_currentRow == _resultSet.Rows.Count)
                {
                    return false;
                }
                int recordCount = 0;
                while (recordCount < s_maxRowsInChunk && _currentRow < _resultSet.Rows.Count)
                {
                    _current.Values.Add(_resultSet.Rows.ElementAt(_currentRow).Values);
                    recordCount++;
                    _currentRow++;
                }
                return true;
            }

            public void Reset()
            {
                throw new NotImplementedException();
            }

            public void Dispose()
            {
            }
        }

        private static readonly string s_dialect_query =
            "select option_value from information_schema.database_options where option_name='database_dialect'";
        private static readonly Empty s_empty = new ();
        private static readonly TransactionOptions s_singleUse = new() { ReadOnly = new TransactionOptions.Types.ReadOnly { Strong = true, ReturnReadTimestamp = false } };

        private readonly object _lock = new();
        private readonly ConcurrentDictionary<string, StatementResult> _results = new();
        private ConcurrentQueue<IMessage> _requests = new();
        private ConcurrentQueue<ServerCallContext> _contexts = new();
        private ConcurrentQueue<Metadata> _headers = new ();
        private int _sessionCounter;
        private int _transactionCounter;
        private readonly ConcurrentDictionary<SessionName, Session> _sessions = new();
        private readonly ConcurrentDictionary<ByteString, Spanner.V1.Transaction> _transactions = new();
        private readonly ConcurrentDictionary<ByteString, TransactionOptions> _transactionOptions = new();
        private readonly ConcurrentDictionary<ByteString, bool> _abortedTransactions = new();
        private bool _abortNextStatement;
        private readonly ConcurrentDictionary<string, ExecutionTime> _executionTimes = new();

        public MockSpannerService()
        {
            AddDialectResult();
        }

        public void AddOrUpdateStatementResult(string sql, StatementResult result)
        {
            _results.AddOrUpdate(sql.Trim(),
                result,
                (_, _) => result
            );
        }

        public void AddOrUpdateExecutionTime(string method, ExecutionTime executionTime)
        {
            _executionTimes.AddOrUpdate(method,
                executionTime,
                (_, _) => executionTime
            );
        }

        private void AddDialectResult()
        {
            AddOrUpdateStatementResult(s_dialect_query, 
                StatementResult.CreateResultSet(
                    new List<Tuple<Spanner.V1.TypeCode, string>>
                    {
                        Tuple.Create(Spanner.V1.TypeCode.String, "option_value"),
                    },
                    new List<object[]>
                    {
                        new object[] { "GOOGLE_STANDARD_SQL" },
                    }));
        }

        internal void AbortTransaction(string transactionId)
        {
            _abortedTransactions.TryAdd(ByteString.FromBase64(transactionId), true);
        }

        internal void AbortNextStatement()
        {
            lock (_lock)
            {
                _abortNextStatement = true;
            }
        }

        public IEnumerable<IMessage> Requests => new List<IMessage>(_requests).AsReadOnly();

        public IEnumerable<ServerCallContext> Contexts => new List<ServerCallContext>(_contexts).AsReadOnly();

        public IEnumerable<Metadata> Headers => new List<Metadata>(_headers).AsReadOnly();

        public void Reset()
        {
            _requests = new ConcurrentQueue<IMessage>();
            _contexts = new ConcurrentQueue<ServerCallContext>();
            _headers = new ConcurrentQueue<Metadata>();
            _executionTimes.Clear();
            _results.Clear();
            _abortedTransactions.Clear();
            _abortNextStatement = false;
            AddDialectResult();
        }

        public override Task<Spanner.V1.Transaction> BeginTransaction(BeginTransactionRequest request, ServerCallContext context)
        {
            _requests.Enqueue(request);
            _contexts.Enqueue(context);
            _headers.Enqueue(context.RequestHeaders);
            TryFindSession(request.SessionAsSessionName);
            Spanner.V1.Transaction tx = new Spanner.V1.Transaction();
            var id = Interlocked.Increment(ref _transactionCounter);
            tx.Id = ByteString.CopyFromUtf8($"{request.SessionAsSessionName}/transactions/{id}");
            _transactions.TryAdd(tx.Id, tx);
            return Task.FromResult(tx);
        }

        public override Task<CommitResponse> Commit(CommitRequest request, ServerCallContext context)
        {
            _requests.Enqueue(request);
            _contexts.Enqueue(context);
            _headers.Enqueue(context.RequestHeaders);
            TryFindSession(request.SessionAsSessionName);
            if (request.TransactionCase == CommitRequest.TransactionOneofCase.TransactionId)
            {
                TryFindTransaction(request.TransactionId, true);
            }
            CommitResponse response = new CommitResponse();
            Timestamp ts = Timestamp.FromDateTime(DateTime.UtcNow);
            response.CommitTimestamp = ts;
            return Task.FromResult(response);
        }

        public override Task<Empty> Rollback(RollbackRequest request, ServerCallContext context)
        {
            _requests.Enqueue(request);
            _contexts.Enqueue(context);
            _headers.Enqueue(context.RequestHeaders);
            TryFindSession(request.SessionAsSessionName);
            _transactions.TryRemove(request.TransactionId, out _);
            return Task.FromResult(s_empty);
        }

        private Session CreateSession(DatabaseName database, bool multiplexed)
        {
            var id = Interlocked.Increment(ref _sessionCounter);
            Session session = new Session
            {
                SessionName = new SessionName(database.ProjectId, database.InstanceId, database.DatabaseId, $"session-{id}"),
                Multiplexed = multiplexed,
            };
            if (!_sessions.TryAdd(session.SessionName, session))
            {
                throw new RpcException(new GrpcCore.Status(StatusCode.AlreadyExists, $"Session with id session-{id} already exists"));
            }
            return session;
        }

        internal static RpcException CreateAbortedException(string message)
        {
            // Add a 100 nanosecond retry delay to the error to ensure that the delay is used, but does not slow
            // down the tests unnecessary (100ns == 1 Tick is the smallest possible measurable timespan in .NET).
            var key = RetryInfo.Descriptor.FullName + "-bin";
            var entry = new Metadata.Entry(key, new RetryInfo { RetryDelay = new Duration { Nanos = 100 } }.ToByteArray());
            var trailers = new Metadata { entry };

            var status = new GrpcCore.Status(StatusCode.Aborted, $"Transaction aborted: {message}");
            var rpc = new RpcException(status, trailers);

            return rpc;
        }

        internal static RpcException CreateDatabaseNotFoundException(string name)
        {
            var key = ResourceInfo.Descriptor.FullName + "-bin";
            var entry = new Metadata.Entry(key, new ResourceInfo { ResourceName = name, ResourceType = "type.googleapis.com/google.spanner.admin.database.v1.Database"}.ToByteArray());
            var trailers = new Metadata { entry };

            var status = new GrpcCore.Status(StatusCode.NotFound, $"Database not found: Database with id {name} not found");
            var rpc = new RpcException(status, trailers);

            return rpc;
        }

        private Spanner.V1.Transaction TryFindTransaction(ByteString id, Boolean remove = false)
        {
            if (_abortedTransactions.TryGetValue(id, out bool aborted) && aborted)
            {
                throw CreateAbortedException("Transaction marked as aborted");
            }
            lock (_lock)
            {
                if (_abortNextStatement)
                {
                    _abortNextStatement = false;
                    throw CreateAbortedException("Next statement was aborted");
                }
            }
            if (remove ? _transactions.TryRemove(id, out Spanner.V1.Transaction tx) : _transactions.TryGetValue(id, out tx))
            {
                return tx;
            }
            throw new RpcException(new GrpcCore.Status(StatusCode.NotFound, $"Transaction not found: {id.ToBase64()}"));
        }

        private Spanner.V1.Transaction FindOrBeginTransaction(SessionName session, TransactionSelector selector)
        {
            if (selector == null)
            {
                return BeginTransaction(session, s_singleUse, true);
            }
            // TODO: Check that the selected transaction actually belongs to the given session.
            return selector.SelectorCase switch
            {
                TransactionSelector.SelectorOneofCase.SingleUse => BeginTransaction(session, selector.SingleUse, true),
                TransactionSelector.SelectorOneofCase.Begin => BeginTransaction(session, selector.Begin, false),
                TransactionSelector.SelectorOneofCase.Id => TryFindTransaction(selector.Id),
                _ => null,
            };
        }

        private Spanner.V1.Transaction BeginTransaction(SessionName session, TransactionOptions options, bool singleUse)
        {
            Spanner.V1.Transaction tx = new Spanner.V1.Transaction();
            var id = Interlocked.Increment(ref _transactionCounter);
            tx.Id = ByteString.CopyFromUtf8($"{session}/transactions/{id}");
            if (options.ModeCase == TransactionOptions.ModeOneofCase.ReadOnly && options.ReadOnly.ReturnReadTimestamp)
            {
                tx.ReadTimestamp = Timestamp.FromDateTime(DateTime.UtcNow);
            }
            if (!singleUse)
            {
                _transactions.TryAdd(tx.Id, tx);
                _transactionOptions.TryAdd(tx.Id, options);
            }
            return tx;
        }

        public override Task<BatchCreateSessionsResponse> BatchCreateSessions(BatchCreateSessionsRequest request, ServerCallContext context)
        {
            _requests.Enqueue(request);
            _contexts.Enqueue(context);
            _headers.Enqueue(context.RequestHeaders);
            _executionTimes.TryGetValue(nameof(ExecuteStreamingSql), out ExecutionTime executionTime);
            executionTime?.SimulateExecutionTime();
            var database = request.DatabaseAsDatabaseName;
            BatchCreateSessionsResponse response = new BatchCreateSessionsResponse();
            for (int i = 0; i < request.SessionCount; i++)
            {
                response.Session.Add(CreateSession(database, false));
            }
            return Task.FromResult(response);
        }

        public override Task<Session> CreateSession(CreateSessionRequest request, ServerCallContext context)
        {
            _requests.Enqueue(request);
            _contexts.Enqueue(context);
            _headers.Enqueue(context.RequestHeaders);
            var database = request.DatabaseAsDatabaseName;
            return Task.FromResult(CreateSession(database, request.Session?.Multiplexed ?? false));
        }

        public override Task<Session> GetSession(GetSessionRequest request, ServerCallContext context)
        {
            _requests.Enqueue(request);
            _contexts.Enqueue(context);
            _headers.Enqueue(context.RequestHeaders);
            return Task.FromResult(TryFindSession(request.SessionName));
        }

        public override Task<ListSessionsResponse> ListSessions(ListSessionsRequest request, ServerCallContext context)
        {
            _requests.Enqueue(request);
            _contexts.Enqueue(context);
            _headers.Enqueue(context.RequestHeaders);
            ListSessionsResponse response = new ListSessionsResponse();
            foreach (Session session in _sessions.Values)
            {
                response.Sessions.Add(session);
            }
            return Task.FromResult(response);
        }

        public override Task<Empty> DeleteSession(DeleteSessionRequest request, ServerCallContext context)
        {
            _requests.Enqueue(request);
            _contexts.Enqueue(context);
            _headers.Enqueue(context.RequestHeaders);
            _sessions.TryRemove(request.SessionName, out _);
            return Task.FromResult(s_empty);
        }

        private Session TryFindSession(SessionName name)
        {
            if (_sessions.TryGetValue(name, out Session session))
            {
                return session;
            }
            throw new RpcException(new GrpcCore.Status(StatusCode.NotFound, $"Session not found: {name}"));
        }

        public override Task<ExecuteBatchDmlResponse> ExecuteBatchDml(ExecuteBatchDmlRequest request, ServerCallContext context)
        {
            _requests.Enqueue(request);
            _contexts.Enqueue(context);
            _headers.Enqueue(context.RequestHeaders);
            _executionTimes.TryGetValue(nameof(ExecuteBatchDml), out ExecutionTime executionTime);
            executionTime?.SimulateExecutionTime();
            _ = TryFindSession(request.SessionAsSessionName);
            var tx = FindOrBeginTransaction(request.SessionAsSessionName, request.Transaction);
            var response = new ExecuteBatchDmlResponse
            {
                // TODO: Return other statuses based on the mocked results.
                Status = new Status
                {
                    Code = (int)StatusCode.OK
                }
            };
            var index = 0;
            foreach (var statement in request.Statements)
            {
                if (response.Status.Code != (int)StatusCode.OK)
                {
                    break;
                }
                if (_results.TryGetValue(statement.Sql.Trim(), out StatementResult result))
                {
                    switch (result.Type)
                    {
                        case StatementResult.StatementResultType.ResultSet:
                            throw new RpcException(new GrpcCore.Status(StatusCode.InvalidArgument, $"ResultSet is not a valid result type for BatchDml"));
                        case StatementResult.StatementResultType.UpdateCount:
                            if (_executionTimes.TryGetValue(nameof(ExecuteBatchDml) + statement.Sql, out executionTime))
                            {
                                executionTime.SimulateExecutionTime();
                            }
                            var resultSet = CreateUpdateCountResultSet(result.UpdateCount);
                            if (index == 0 && request.Transaction?.Begin != null && tx != null)
                            {
                                resultSet.Metadata.Transaction = tx;
                            }
                            response.ResultSets.Add(resultSet);
                            break;
                        case StatementResult.StatementResultType.Exception:
                            if (index == 0)
                            {
                                throw result.Exception;
                            }
                            response.Status = StatusFromException(result.Exception);
                            break;
                        default:
                            throw new RpcException(new GrpcCore.Status(StatusCode.InvalidArgument, $"Invalid result type {result.Type} for {statement.Sql}"));
                    }
                }
                else
                {
                    throw new RpcException(new GrpcCore.Status(StatusCode.InvalidArgument, $"No result found for {statement.Sql}"));
                }
                index++;
            }
            return Task.FromResult(response);
        }

        private Status StatusFromException(Exception e)
        {
            if (e is RpcException rpc)
            {
                return new Status { Code = (int)rpc.StatusCode, Message = e.Message };
            }
            return new Status { Code = (int)StatusCode.Unknown, Message = e.Message };
        }

        public override async Task ExecuteStreamingSql(ExecuteSqlRequest request, IServerStreamWriter<PartialResultSet> responseStream, ServerCallContext context)
        {
            if (!request.Sql.Equals(s_dialect_query))
            {
                _requests.Enqueue(request);
            }
            _contexts.Enqueue(context);
            _headers.Enqueue(context.RequestHeaders);
            _executionTimes.TryGetValue(nameof(ExecuteStreamingSql) + request.Sql, out ExecutionTime executionTime);
            if (executionTime == null)
            {
                _executionTimes.TryGetValue(nameof(ExecuteStreamingSql), out executionTime);
            }
            executionTime?.SimulateExecutionTime();
            TryFindSession(request.SessionAsSessionName);
            Spanner.V1.Transaction returnTransaction = null;
            var transaction = FindOrBeginTransaction(request.SessionAsSessionName, request.Transaction);
            if (request.Transaction != null && (request.Transaction.SelectorCase == TransactionSelector.SelectorOneofCase.Begin || request.Transaction.SelectorCase == TransactionSelector.SelectorOneofCase.SingleUse))
            {
                returnTransaction = transaction;
            }
            if (_results.TryGetValue(request.Sql.Trim(), out StatementResult result))
            {
                switch (result.Type)
                {
                    case StatementResult.StatementResultType.ResultSet:
                        await WriteResultSet(returnTransaction, result.ResultSet, responseStream, executionTime);
                        break;
                    case StatementResult.StatementResultType.UpdateCount:
                        await WriteUpdateCount(returnTransaction, result.UpdateCount, responseStream);
                        break;
                    case StatementResult.StatementResultType.Exception:
                        throw result.Exception;
                    default:
                        throw new RpcException(new GrpcCore.Status(StatusCode.InvalidArgument, $"Invalid result type {result.Type} for {request.Sql}"));
                }
            }
            else
            {
                throw new RpcException(new GrpcCore.Status(StatusCode.InvalidArgument, $"No result found for {request.Sql}"));
            }
        }

        private async Task WriteResultSet(Spanner.V1.Transaction transaction, ResultSet resultSet, IServerStreamWriter<PartialResultSet> responseStream, ExecutionTime executionTime)
        {
            int index = 0;
            PartialResultSetsEnumerable enumerator = new PartialResultSetsEnumerable(transaction, resultSet);
            int writePermissions = executionTime?.TakeWritePermission() ?? int.MaxValue;
            foreach (PartialResultSet prs in enumerator)
            {
                Exception e = executionTime?.PopExceptionAtIndex(index);
                if (e != null)
                {
                    throw e;
                }
                await responseStream.WriteAsync(prs);
                index++;
                writePermissions--;
                if (writePermissions == 0)
                {
                    writePermissions = executionTime?.TakeWritePermission() ?? int.MaxValue;
                }
            }
        }

        private async Task WriteUpdateCount(Spanner.V1.Transaction transaction, long updateCount, IServerStreamWriter<PartialResultSet> responseStream)
        {
            PartialResultSet prs = new PartialResultSet
            {
                Metadata = new ResultSetMetadata { Transaction = transaction, RowType = new StructType()},
                Stats = new ResultSetStats { RowCountExact = updateCount }
            };
            await responseStream.WriteAsync(prs);
        }

        private ResultSet CreateUpdateCountResultSet(long updateCount)
        {
            ResultSet rs = new ResultSet
            {
                Metadata = new ResultSetMetadata { RowType = new StructType()},
                Stats = new ResultSetStats { RowCountExact = updateCount }
            };
            return rs;
        }
    }

    public class MockDatabaseAdminService : DatabaseAdmin.DatabaseAdminBase
    {
        private readonly ConcurrentQueue<IMessage> _requests = new ConcurrentQueue<IMessage>();

        public IEnumerable<IMessage> Requests => new List<IMessage>(_requests).AsReadOnly();

        public override Task<Operation> CreateDatabase(CreateDatabaseRequest request, ServerCallContext context)
        {
            _requests.Enqueue(request);
            var op = new Operation
            {
                Name = "projects/p1/instances/i1/operations/o1",
                Response = Any.Pack(new Database()),
                Metadata = Any.Pack(new CreateDatabaseMetadata()),
                Done = true,
            };
            return Task.FromResult(op);
        }

        public override Task<Operation> UpdateDatabaseDdl(UpdateDatabaseDdlRequest request, ServerCallContext context)
        {
            _requests.Enqueue(request);
            var op = new Operation
            {
                Name = "projects/p1/instances/i1/operations/o1",
                Response = Any.Pack(new Empty()),
                Metadata = Any.Pack(new UpdateDatabaseDdlMetadata()),
                Done = true,
            };
            return Task.FromResult(op);
        }

        public override Task<Empty> DropDatabase(DropDatabaseRequest request, ServerCallContext context)
        {
            _requests.Enqueue(request);
            return Task.FromResult(new Empty());
        }
    }
}