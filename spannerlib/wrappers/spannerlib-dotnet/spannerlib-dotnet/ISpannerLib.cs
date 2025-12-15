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
using System.Threading;
using System.Threading.Tasks;
using Google.Cloud.Spanner.V1;
using Google.Protobuf.WellKnownTypes;

namespace Google.Cloud.SpannerLib;

/// <summary>
/// This is the generic interface of SpannerLib. It could have multiple different implementations that use different
/// methods for communicating with the library, for example by loading the library as a native library, or by starting
/// it as a child process and communicating with it through a gRPC API. The classes in this assembly use this generic
/// interface to abstract away the underlying communication method.
/// </summary>
public interface ISpannerLib : IDisposable
{
    /// <summary>
    /// RowEncoding is used to specify the format that SpannerLib should use to return row data. 
    /// </summary>
    public enum RowEncoding
    {
        /// <summary>
        /// Return rows as encoded protobuf ListValue objects.
        /// </summary>
        Proto,
    }

    /// <summary>
    /// Creates a pool of connections using the given connection string. Creating a pool is a relatively expensive
    /// operation. All connections in a pool share the same underlying Spanner client and gRPC channel pool.
    /// </summary>
    /// <param name="connectionString">The connection string for this pool of connections</param>
    /// <returns>A Pool object that can be used to create connections</returns>
    public Pool CreatePool(string connectionString);

    /// <summary>
    /// Closes a Pool object. This also closes any open connections in the pool.
    /// </summary>
    /// <param name="pool">The pool that should be closed</param>
    public void ClosePool(Pool pool);

    /// <summary>
    /// Creates a new connection in the given pool. This is a relatively cheap operation. Connections do not have their
    /// own physical connection with Spanner. Instead, all connections in a pool share the same underlying Spanner
    /// client and gRPC channel pool.
    /// </summary>
    /// <param name="pool">The pool that should be used to create the connection</param>
    /// <returns>A new connection</returns>
    public Connection CreateConnection(Pool pool);

    /// <summary>
    /// Closes the given connection. This also closes any open Rows objects of this connection. Any active transaction
    /// on the connection is rolled back.
    /// </summary>
    /// <param name="connection">The connection to close</param>
    public void CloseConnection(Connection connection);
    
    /// <summary>
    /// Closes the given connection. This also closes any open Rows objects of this connection. Any active transaction
    /// on the connection is rolled back.
    /// </summary>
    /// <param name="connection">The connection to close</param>
    /// <param name="cancellationToken">The cancellation token</param>
    public Task CloseConnectionAsync(Connection connection, CancellationToken cancellationToken = default) => Task.Run(() => CloseConnection(connection), cancellationToken);

    /// <summary>
    /// Writes an array of mutations to Spanner. The mutations are buffered in the current transaction of the given
    /// connection (if any). Otherwise, the mutations are written directly to Spanner using a new read/write
    /// transaction and the CommitResponse of that transaction is returned. The returned value is null if the mutations
    /// were only buffered in an active transaction.
    /// </summary>
    /// <param name="connection">The connection to use to write the mutations</param>
    /// <param name="mutations">The mutations to write</param>
    /// <returns>
    /// The CommitResponse of the read/write transaction that was created to write the mutations, or null if the
    /// mutations were buffered in an active transaction on the connection.
    /// </returns>
    public CommitResponse? WriteMutations(Connection connection, BatchWriteRequest.Types.MutationGroup mutations);

    /// <summary>
    /// Writes an array of mutations to Spanner. The mutations are buffered in the current transaction of the given
    /// connection (if any). Otherwise, the mutations are written directly to Spanner using a new read/write
    /// transaction and the CommitResponse of that transaction is returned. The returned value is null if the mutations
    /// were only buffered in an active transaction.
    /// </summary>
    /// <param name="connection">The connection to use to write the mutations</param>
    /// <param name="mutations">The mutations to write</param>
    /// <param name="cancellationToken">The cancellation token</param>
    /// <returns>
    /// The CommitResponse of the read/write transaction that was created to write the mutations, or null if the
    /// mutations were buffered in an active transaction on the connection.
    /// </returns>
    public Task<CommitResponse?> WriteMutationsAsync(Connection connection, BatchWriteRequest.Types.MutationGroup mutations, CancellationToken cancellationToken = default);
    
    /// <summary>
    /// Executes a SQL statement of any type on the given connection.
    /// </summary>
    /// <param name="connection">The connection to use to execute the SQL statement</param>
    /// <param name="statement">The statement to execute</param>
    /// <param name="prefetchRows">The number of rows to prefetch and include in the initial result</param>
    /// <returns>
    /// A Rows object with the results of the statement. The contents of the Rows object depends on the type of SQL
    /// statement.
    /// </returns>
    public Rows Execute(Connection connection, ExecuteSqlRequest statement, int prefetchRows = 0);

    /// <summary>
    /// Executes a SQL statement of any type on the given connection.
    /// </summary>
    /// <param name="connection">The connection to use to execute the SQL statement</param>
    /// <param name="statement">The statement to execute</param>
    /// <param name="prefetchRows">The number of rows to prefetch and include in the initial result</param>
    /// <param name="cancellationToken">The cancellation token</param>
    /// <returns>
    /// A Rows object with the results of the statement. The contents of the Rows object depends on the type of SQL
    /// statement.
    /// </returns>
    public Task<Rows> ExecuteAsync(Connection connection, ExecuteSqlRequest statement, int prefetchRows = 0, CancellationToken cancellationToken = default);

    /// <summary>
    /// Executes a batch of DML or DDL statements on Spanner. The batch may not contain a mix of DML and DDL statements.
    /// </summary>
    /// <param name="connection">The connection to use to execute the batch</param>
    /// <param name="statements">The DML or DDL statements to execute</param>
    /// <returns>The update count per statement. The update count for a DDL statement is -1.</returns>
    public long[] ExecuteBatch(Connection connection, ExecuteBatchDmlRequest statements);

    /// <summary>
    /// Executes a batch of DML or DDL statements on Spanner. The batch may not contain a mix of DML and DDL statements.
    /// </summary>
    /// <param name="connection">The connection to use to execute the batch</param>
    /// <param name="statements">The DML or DDL statements to execute</param>
    /// <param name="cancellationToken">The cancellation token</param>
    /// <returns>The update count per statement. The update count for a DDL statement is -1.</returns>
    public Task<long[]> ExecuteBatchAsync(Connection connection, ExecuteBatchDmlRequest statements, CancellationToken cancellationToken = default);

    /// <summary>
    /// Converts an ExecuteBatchDmlResponse to an array of update counts.
    /// </summary>
    /// <param name="response">The response to convert</param>
    /// <returns>An array of update counts extracted from the given response</returns>
    public static long[] ToUpdateCounts(ExecuteBatchDmlResponse response)
    {
        var result = new long[response.ResultSets.Count];
        for (var i = 0; i < result.Length; i++)
        {
            if (response.ResultSets[i].Stats.HasRowCountExact)
            {
                result[i] = response.ResultSets[i].Stats.RowCountExact;
            }
            else if (response.ResultSets[i].Stats.HasRowCountLowerBound)
            {
                result[i] = response.ResultSets[i].Stats.RowCountLowerBound;
            }
            else
            {
                result[i] = -1;
            }
        }
        return result;
    }

    /// <summary>
    /// Returns the ResultSetMetadata of a Rows object. This can be used to inspect the type of data that a Rows object
    /// contains.
    /// </summary>
    /// <param name="rows">The Rows object to get the metadata of</param>
    /// <returns>The ResultSetMetadata of the given Rows object</returns>
    public ResultSetMetadata? Metadata(Rows rows);

    /// <summary>
    /// Returns the ResultSetMetadata of a Rows object. This can be used to inspect the type of data that a Rows object
    /// contains.
    /// </summary>
    /// <param name="rows">The Rows object to get the metadata of</param>
    /// <param name="cancellationToken">The cancellation token</param>
    /// <returns>The ResultSetMetadata of the given Rows object</returns>
    public Task<ResultSetMetadata?> MetadataAsync(Rows rows, CancellationToken cancellationToken = default);

    /// <summary>
    /// Moves the cursor in the given Rows object to the next result set and returns the ResultSetMetadata of that
    /// result set, or null if there are no more result sets.
    /// </summary>
    /// <param name="rows">The Rows object to move to the next result set</param>
    /// <returns>The ResultSetMetadata of the next result set, or null if there are no more results</returns>
    public ResultSetMetadata? NextResultSet(Rows rows);

    /// <summary>
    /// Moves the cursor in the given Rows object to the next result set and returns the ResultSetMetadata of that
    /// result set, or null if there are no more result sets.
    /// </summary>
    /// <param name="rows">The Rows object to move to the next result set</param>
    /// <param name="cancellationToken">The cancellation token</param>
    /// <returns>The ResultSetMetadata of the next result set, or null if there are no more results</returns>
    public Task<ResultSetMetadata?> NextResultSetAsync(Rows rows, CancellationToken cancellationToken = default);

    /// <summary>
    /// Returns the ResultSetStats of a Rows object. This object contains the update count of a DML statement that was
    /// executed. This method can only be called once all data rows in the Rows object have been returned. That is; the
    /// Next method must have returned null for this Rows object (or the caller must know beforehand that the statement
    /// did not return any rows).
    /// </summary>
    /// <param name="rows">The Rows object to get the stats for</param>
    /// <returns>The ResultSetStats for the given Rows object</returns>
    public ResultSetStats? Stats(Rows rows);

    /// <summary>
    /// Returns the next numRows data rows from the given Rows object. The data is encoded using the specified
    /// RowEncoding. The default is an encoded protobuf ListValue containing as many values as there are columns in the
    /// query result, multiplied by the number of rows that were returned.
    /// </summary>
    /// <param name="rows">The Rows object to return data rows for</param>
    /// <param name="numRows">The maximum number of rows to return</param>
    /// <param name="encoding">The encoding that should be used for the data rows</param>
    /// <returns>A ListValue with the actual row data, or null if there are no more rows</returns>
    public ListValue? Next(Rows rows, int numRows, RowEncoding encoding);

    /// <summary>
    /// Returns the next numRows data rows from the given Rows object. The data is encoded using the specified
    /// RowEncoding. The default is an encoded protobuf ListValue containing as many values as there are columns in the
    /// query result, multiplied by the number of rows that were returned.
    /// </summary>
    /// <param name="rows">The Rows object to return data rows for</param>
    /// <param name="numRows">The maximum number of rows to return</param>
    /// <param name="encoding">The encoding that should be used for the data rows</param>
    /// <param name="cancellationToken">The cancellation token</param>
    /// <returns>A ListValue with the actual row data, or null if there are no more rows</returns>
    public Task<ListValue?> NextAsync(Rows rows, int numRows, RowEncoding encoding, CancellationToken cancellationToken = default);

    /// <summary>
    /// Closes the given Rows object. This releases all resources associated with this statement result.
    /// </summary>
    /// <param name="rows">The Rows object to close</param>
    public void CloseRows(Rows rows);

    /// <summary>
    /// Closes the given Rows object. This releases all resources associated with this statement result.
    /// </summary>
    /// <param name="rows">The Rows object to close</param>
    /// <param name="cancellationToken">The cancellation token</param>
    public Task CloseRowsAsync(Rows rows, CancellationToken cancellationToken = default) => Task.Run(() => CloseRows(rows), cancellationToken);

    /// <summary>
    /// Starts a new transaction on this connection. A connection can have at most one transaction at any time. All
    /// transactions, including read-only transactions, must be either committed or rolled back.
    /// </summary>
    /// <param name="connection">The connection to use to start the transaction</param>
    /// <param name="transactionOptions">
    /// The options for the new transaction. The default is to create a read/write transaction. Set the ReadOnly option
    /// to create a read-only transaction.
    /// </param>
    public void BeginTransaction(Connection connection, TransactionOptions transactionOptions);
    
    /// <summary>
    /// Starts a new transaction on this connection. A connection can have at most one transaction at any time. All
    /// transactions, including read-only transactions, must be either committed or rolled back.
    /// </summary>
    /// <param name="connection">The connection to use to start the transaction</param>
    /// <param name="transactionOptions">
    /// The options for the new transaction. The default is to create a read/write transaction. Set the ReadOnly option
    /// to create a read-only transaction.
    /// </param>
    /// <param name="cancellationToken">The cancellation token</param>
    public Task BeginTransactionAsync(Connection connection, TransactionOptions transactionOptions, CancellationToken cancellationToken = default);
    
    /// <summary>
    /// Commits the current transaction on this connection.
    /// </summary>
    /// <param name="connection">The connection that has the transaction that should be committed</param>
    /// <returns>The CommitResponse of the transaction, or null if it was a read-only transaction</returns>
    public CommitResponse? Commit(Connection connection);
    
    /// <summary>
    /// Commits the current transaction on this connection.
    /// </summary>
    /// <param name="connection">The connection that has the transaction that should be committed</param>
    /// <returns>The CommitResponse of the transaction, or null if it was a read-only transaction</returns>
    /// <param name="cancellationToken">The cancellation token</param>
    public Task<CommitResponse?> CommitAsync(Connection connection, CancellationToken cancellationToken = default);

    /// <summary>
    /// Rollbacks the current transaction.
    /// </summary>
    /// <param name="connection">The connection that has the transaction that should be rolled back</param>
    public void Rollback(Connection connection);

    /// <summary>
    /// Rollbacks the current transaction.
    /// </summary>
    /// <param name="connection">The connection that has the transaction that should be rolled back</param>
    /// <param name="cancellationToken">The cancellation token</param>
    public Task RollbackAsync(Connection connection, CancellationToken cancellationToken = default);

    void IDisposable.Dispose()
    {
        // no-op
    }
}