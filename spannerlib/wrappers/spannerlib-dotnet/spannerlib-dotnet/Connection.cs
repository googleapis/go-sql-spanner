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

using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Google.Cloud.Spanner.V1;

namespace Google.Cloud.SpannerLib;

/// <summary>
/// A Connection in a Pool that has been created by SpannerLib.
/// </summary>
/// <param name="pool">The pool that created and owns this connection</param>
/// <param name="id">The id of this connection</param>
public class Connection(Pool pool, long id) : AbstractLibObject(pool.Spanner, id)
{
    public Pool Pool { get; } = pool;

    /// <summary>
    /// Begins a new transaction on this connection. A connection can have at most one active transaction at any time.
    /// Calling this method does not immediately start the transaction on Spanner. Instead, the transaction is only
    /// registered on the connection, and the BeginTransaction option will be inlined with the first statement in the
    /// transaction.
    /// </summary>
    /// <param name="transactionOptions">
    /// The transaction options that will be used to create the transaction. The default is a read/write transaction.
    /// Explicitly set the ReadOnly transaction option to start a read-only transaction.
    /// </param>
    public void BeginTransaction(TransactionOptions transactionOptions)
    {
        Spanner.BeginTransaction(this, transactionOptions);
    }

    /// <summary>
    /// Commits the current transaction on this connection and returns the CommitResponse (if any). Both read/write and
    /// read-only transactions must be either committed or rolled back. Committing or rolling back a read-only
    /// transaction is a no-op on Spanner, and this method does not return a CommitResponse when a read-only transaction
    /// is committed.
    /// </summary>
    /// <returns>The CommitResponse for this transaction, or null for read-only transactions</returns>
    public CommitResponse? Commit()
    {
        return Spanner.Commit(this);
    }

    public Task<CommitResponse?> CommitAsync(CancellationToken cancellationToken = default)
    {
        return Spanner.CommitAsync(this, cancellationToken);
    }

    /// <summary>
    /// Rollbacks the current transaction.
    /// </summary>
    public void Rollback()
    {
        Spanner.Rollback(this);
    }

    public Task RollbackAsync(CancellationToken cancellationToken = default)
    {
        return Spanner.RollbackAsync(this, cancellationToken);
    }

    /// <summary>
    /// Writes the given list of mutations to Spanner. If the connection has an active read/write transaction, then the
    /// mutations will be buffered in the current transaction and sent to Spanner when the transaction is committed.
    /// If the connection does not have a transaction, then the mutations are sent to Spanner directly in a new
    /// read/write transaction.
    /// </summary>
    /// <param name="mutations">The mutations to write to Spanner</param>
    /// <returns>
    /// The CommitResponse that is returned by Spanner, or null if the mutations were only buffered in the current
    /// transaction.
    /// </returns>
    public CommitResponse? WriteMutations(BatchWriteRequest.Types.MutationGroup mutations)
    {
        return Spanner.WriteMutations(this, mutations);
    }
    
    /// <summary>
    /// Writes the given list of mutations to Spanner. If the connection has an active read/write transaction, then the
    /// mutations will be buffered in the current transaction and sent to Spanner when the transaction is committed.
    /// If the connection does not have a transaction, then the mutations are sent to Spanner directly in a new
    /// read/write transaction.
    /// </summary>
    /// <param name="mutations">The mutations to write to Spanner</param>
    /// <param name="cancellationToken">The cancellation token</param>
    /// <returns>
    /// The CommitResponse that is returned by Spanner, or null if the mutations were only buffered in the current
    /// transaction.
    /// </returns>
    public Task<CommitResponse?> WriteMutationsAsync(BatchWriteRequest.Types.MutationGroup mutations,
        CancellationToken cancellationToken = default)
    {
        return Spanner.WriteMutationsAsync(this, mutations, cancellationToken);
    }
    
    /// <summary>
    /// Executes any type of SQL statement on this connection. The SQL statement will use the current transaction of the
    /// connection. The contents of the returned Rows object depends on the type of SQL statement.
    /// </summary>
    /// <param name="statement">The SQL statement that should be executed</param>
    /// <returns>A Rows object with the statement result</returns>
    public Rows Execute(ExecuteSqlRequest statement)
    {
        return Spanner.Execute(this, statement);
    }

    /// <summary>
    /// Executes any type of SQL statement on this connection. The SQL statement will use the current transaction of the
    /// connection. The contents of the returned Rows object depends on the type of SQL statement.
    /// </summary>
    /// <param name="statement">The SQL statement that should be executed</param>
    /// <param name="cancellationToken">The cancellation token</param>
    /// <returns>A Rows object with the statement result</returns>
    public Task<Rows> ExecuteAsync(ExecuteSqlRequest statement, CancellationToken cancellationToken = default)
    {
        return Spanner.ExecuteAsync(this, statement, cancellationToken);
    }

    /// <summary>
    /// Executes a batch of DML or DDL statements on Spanner. The batch may not contain a mix of DML and DDL statements.
    /// The batch will use the current transaction of the connection (if any). Executing a batch of DDL statements in a
    /// transaction is not supported.
    /// </summary>
    /// <param name="statements">The DML or DDL statements to execute</param>
    /// <returns>The update count per statement. The update count for a DDL statement is -1.</returns>
    public long[] ExecuteBatch(IEnumerable<ExecuteBatchDmlRequest.Types.Statement> statements)
    {
        var request = new ExecuteBatchDmlRequest
        {
            Statements = { statements }
        };
        return Spanner.ExecuteBatch(this, request);
    }

    /// <summary>
    /// Executes a batch of DML or DDL statements on Spanner. The batch may not contain a mix of DML and DDL statements.
    /// The batch will use the current transaction of the connection (if any). Executing a batch of DDL statements in a
    /// transaction is not supported.
    /// </summary>
    /// <param name="statements">The DML or DDL statements to execute</param>
    /// <param name="cancellationToken">The cancellation token</param>
    /// <returns>The update count per statement. The update count for a DDL statement is -1.</returns>
    public Task<long[]> ExecuteBatchAsync(List<ExecuteBatchDmlRequest.Types.Statement> statements, CancellationToken cancellationToken = default)
    {
        var request = new ExecuteBatchDmlRequest
        {
            Statements = { statements }
        };
        return Spanner.ExecuteBatchAsync(this, request, cancellationToken);
    }

    /// <summary>
    /// Closes this connection.
    /// </summary>
    protected override void CloseLibObject()
    {
        Spanner.CloseConnection(this);
    }

    protected override async ValueTask CloseLibObjectAsync()
    {
        await Spanner.CloseConnectionAsync(this);
    }
}
