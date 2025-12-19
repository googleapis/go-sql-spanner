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
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Diagnostics.CodeAnalysis;
using System.Threading;
using System.Threading.Tasks;
using Google.Api.Gax;
using Google.Cloud.Spanner.Common.V1;
using Google.Cloud.Spanner.V1;
using Google.Cloud.SpannerLib;
using IsolationLevel = System.Data.IsolationLevel;
using TransactionOptions = Google.Cloud.Spanner.V1.TransactionOptions;
using static Google.Cloud.Spanner.DataProvider.SpannerDbException;

namespace Google.Cloud.Spanner.DataProvider;

public class SpannerConnection : DbConnection
{
    private string _connectionString = string.Empty;
    
    private SpannerConnectionStringBuilder? _connectionStringBuilder;
        
    [AllowNull]
    public sealed override string ConnectionString {
        get => _connectionString;
        set
        {
            AssertClosed();
            if (string.IsNullOrWhiteSpace(value))
            {
                _connectionStringBuilder = null;
                _connectionString = string.Empty;
            }
            else
            {
                var builder = new SpannerConnectionStringBuilder(value);
                builder.CheckValid();
                _connectionStringBuilder = builder;
                _connectionString = value;
            }
        }
    }

    public override string Database
    {
        get
        {
            if (string.IsNullOrWhiteSpace(ConnectionString) || _connectionStringBuilder == null)
            {
                return "";
            }
            if (!string.IsNullOrEmpty(_connectionStringBuilder.DataSource))
            {
                return _connectionStringBuilder.DataSource;
            }
            if (!string.IsNullOrEmpty(_connectionStringBuilder.Project) &&
                !string.IsNullOrEmpty(_connectionStringBuilder.Instance) &&
                !string.IsNullOrEmpty(_connectionStringBuilder.Project))
            {
                return $"projects/{_connectionStringBuilder.Project}/instances/{_connectionStringBuilder.Instance}/databases/{_connectionStringBuilder.Database}";
            }
            return "";
        }
    }
        
    private ConnectionState InternalState
    {
        get => _state;
        set
        {
            var originalState = _state;
            _state = value;
            OnStateChange(new StateChangeEventArgs(originalState, _state));
        }
    }

    public override ConnectionState State => InternalState;
    protected override DbProviderFactory DbProviderFactory => SpannerFactory.Instance;
    
    public override string DataSource => _connectionStringBuilder?.DataSource ?? string.Empty;

    public override string ServerVersion
    {
        get
        {
            AssertOpen();
            // TODO: Return an actual version number
            return "1.0.0";
        }
    }

    internal Version ServerVersionNormalized => Version.Parse(ServerVersion);
    
    internal string ServerVersionNormalizedString => FormattableString.Invariant($"{ServerVersionNormalized.Major:000}.{ServerVersionNormalized.Minor:000}.{ServerVersionNormalized.Build:0000}");

    public override bool CanCreateBatch => true;

    private bool _disposed;
    private ConnectionState _state = ConnectionState.Closed;
    private SpannerPool? Pool { get; set; }

    private Connection? _libConnection;
        
    private Connection LibConnection
    {
        get
        {
            AssertOpen();
            return _libConnection!;
        }
    }

    internal uint DefaultCommandTimeout => _connectionStringBuilder?.CommandTimeout ?? 0;
        
    private SpannerTransaction? _transaction;
    
    internal SpannerTransaction? Transaction => _transaction;
    
    private System.Transactions.Transaction? EnlistedTransaction { get; set; }
    
    private SpannerSchemaProvider? _mSchemaProvider;
    
    private SpannerSchemaProvider GetSchemaProvider() => _mSchemaProvider ??= new SpannerSchemaProvider(this);
    
    public SpannerConnection()
    {
    }

    public SpannerConnection(string? connectionString)
    {
        ConnectionString =  connectionString;
    }

    public SpannerConnection(SpannerConnectionStringBuilder connectionStringBuilder)
    {
        GaxPreconditions.CheckNotNull(connectionStringBuilder, nameof(connectionStringBuilder));
        connectionStringBuilder.CheckValid();
        _connectionStringBuilder = connectionStringBuilder;
        _connectionString = connectionStringBuilder.ConnectionString;
    }
    
    public new ValueTask<SpannerTransaction> BeginTransactionAsync(CancellationToken cancellationToken = default)
        => BeginTransactionAsync(IsolationLevel.Unspecified, cancellationToken);

    public new ValueTask<SpannerTransaction> BeginTransactionAsync(IsolationLevel isolationLevel,
        CancellationToken cancellationToken = default)
    {
        if (cancellationToken.IsCancellationRequested)
        {
            return ValueTask.FromCanceled<SpannerTransaction>(cancellationToken);
        }
        return BeginTransactionAsync(new TransactionOptions
        {
            IsolationLevel = SpannerTransaction.TranslateIsolationLevel(isolationLevel),
        }, cancellationToken);
    }
    
    public new SpannerTransaction BeginTransaction() => BeginTransaction(IsolationLevel.Unspecified);

    public new SpannerTransaction BeginTransaction(IsolationLevel isolationLevel)
    {
        return BeginTransaction(new TransactionOptions
        {
            IsolationLevel = SpannerTransaction.TranslateIsolationLevel(isolationLevel),
        });
    }

    protected override DbTransaction BeginDbTransaction(IsolationLevel isolationLevel)
    {
        return BeginTransaction(new TransactionOptions
        {
            IsolationLevel = SpannerTransaction.TranslateIsolationLevel(isolationLevel),
        });
    }

    protected override ValueTask<DbTransaction> BeginDbTransactionAsync(IsolationLevel isolationLevel, CancellationToken cancellationToken)
    {
        return BeginDbTransactionAsync(new TransactionOptions
        {
            IsolationLevel = SpannerTransaction.TranslateIsolationLevel(isolationLevel),
        }, cancellationToken);
    }
    
    /// <summary>
    /// Starts a new read-only transaction with default options.
    /// </summary>
    /// <returns>The new transaction</returns>
    /// <exception cref="InvalidOperationException">If the connection has an active transaction</exception>
    public SpannerTransaction BeginReadOnlyTransaction()
    {
        return BeginTransaction(new TransactionOptions
        {
            ReadOnly = new TransactionOptions.Types.ReadOnly(),
        });
    }

    /// <summary>
    /// Starts a new read-only transaction with default options.
    /// </summary>
    /// <returns>The new transaction</returns>
    /// <exception cref="InvalidOperationException">If the connection has an active transaction</exception>
    public ValueTask<SpannerTransaction> BeginReadOnlyTransactionAsync(CancellationToken cancellationToken = default)
    {
        return BeginTransactionAsync(new TransactionOptions
        {
            ReadOnly = new TransactionOptions.Types.ReadOnly(),
        }, cancellationToken);
    }

    /// <summary>
    /// Starts a new read-only transaction using the given options.
    /// </summary>
    /// <param name="readOnlyOptions">The options to use for the new read-only transaction</param>
    /// <returns>The new transaction</returns>
    /// <exception cref="InvalidOperationException">If the connection has an active transaction</exception>
    public SpannerTransaction BeginReadOnlyTransaction(TransactionOptions.Types.ReadOnly readOnlyOptions)
    {
        return BeginTransaction(new TransactionOptions
        {
            ReadOnly = readOnlyOptions,
        });
    }

    /// <summary>
    /// Starts a new read-only transaction using the given options.
    /// </summary>
    /// <param name="readOnlyOptions">The options to use for the new read-only transaction</param>
    /// <param name="cancellationToken">The cancellation token</param>
    /// <returns>The new transaction</returns>
    /// <exception cref="InvalidOperationException">If the connection has an active transaction</exception>
    public ValueTask<SpannerTransaction> BeginReadOnlyTransactionAsync(
        TransactionOptions.Types.ReadOnly readOnlyOptions, CancellationToken cancellationToken)
    {
        return BeginTransactionAsync(new TransactionOptions
        {
            ReadOnly = readOnlyOptions,
        }, cancellationToken);
    }

    /// <summary>
    /// Start a new transaction using the given TransactionOptions.
    /// </summary>
    /// <param name="transactionOptions">The options to use for the new transaction</param>
    /// <returns>The new transaction</returns>
    /// <exception cref="InvalidOperationException">If the connection has an active transaction</exception>
    private SpannerTransaction BeginTransaction(TransactionOptions transactionOptions)
    {
        EnsureOpen();
        GaxPreconditions.CheckState(!HasTransaction, "This connection has a transaction.");
        _transaction = SpannerTransaction.CreateTransaction(this, LibConnection, transactionOptions);
        return _transaction;
    }

    /// <summary>
    /// Start a new transaction using the given TransactionOptions.
    /// </summary>
    /// <param name="transactionOptions">The options to use for the new transaction</param>
    /// <param name="cancellationToken">The cancellation token</param>
    /// <returns>The new transaction</returns>
    /// <exception cref="InvalidOperationException">If the connection has an active transaction</exception>
    private async ValueTask<SpannerTransaction> BeginTransactionAsync(TransactionOptions transactionOptions, CancellationToken cancellationToken)
    {
        EnsureOpen();
        GaxPreconditions.CheckState(!HasTransaction, "This connection has a transaction.");
        _transaction = await SpannerTransaction.CreateTransactionAsync(this, LibConnection, transactionOptions, cancellationToken).ConfigureAwait(false);
        return _transaction;
    }
    
    private async ValueTask<DbTransaction> BeginDbTransactionAsync(TransactionOptions transactionOptions, CancellationToken cancellationToken)
    {
        EnsureOpen();
        GaxPreconditions.CheckState(!HasTransaction, "This connection has a transaction.");
        _transaction = await SpannerTransaction.CreateTransactionAsync(this, LibConnection, transactionOptions, cancellationToken).ConfigureAwait(false);
        return _transaction;
    }

    internal void ClearTransaction()
    {
        _transaction = null;
    }
    
    internal bool HasTransaction => _transaction != null;

    public override void ChangeDatabase(string databaseName)
    {
        GaxPreconditions.CheckNotNullOrEmpty(databaseName, nameof(databaseName));
        GaxPreconditions.CheckState(!HasTransaction, "Cannot change database when a transaction is open");
        if (_connectionStringBuilder == null)
        {
            ConnectionString = $"Data Source={databaseName}";
            return;
        }
        if (DatabaseName.TryParse(databaseName, allowUnparsed: false, out _))
        {
            _connectionStringBuilder.DataSource = databaseName;
        }
        else
        {
            if (DatabaseName.TryParse(_connectionStringBuilder.DataSource, out var currentDatabase))
            {
                _connectionStringBuilder.DataSource = $"projects/{currentDatabase.ProjectId}/instances/{currentDatabase.InstanceId}/databases/{databaseName}";
            }
            else if (!string.IsNullOrEmpty(_connectionStringBuilder.Project) && !string.IsNullOrEmpty(_connectionStringBuilder.Instance))
            {
                _connectionStringBuilder.Database = databaseName;
            }
            else
            {
                throw new ArgumentException($"Invalid database name: {databaseName}");
            }
        }
        if (_state == ConnectionState.Open)
        {
            Close();
            Open();
        }
    }

    public override void Close()
    {
        if (InternalState == ConnectionState.Closed)
        {
            return;
        }

        InternalState = ConnectionState.Closed;
        _libConnection?.Close();
        _libConnection = null;
    }
        
    protected override void Dispose(bool disposing)
    {
        if (_disposed)
        {
            return;
        }
        if (disposing)
        {
            Close();
        }
        base.Dispose(disposing);
        _disposed = true;
    }

    public override void Open()
    {
        AssertClosed();
        if (ConnectionString == string.Empty || _connectionStringBuilder == null)
        {
            throw new InvalidOperationException("Connection string is empty");
        }

        try
        {
            InternalState = ConnectionState.Connecting;
            Pool = SpannerPool.GetOrCreate(_connectionStringBuilder.SpannerLibConnectionString);
            _libConnection = Pool.CreateConnection();
            InternalState = ConnectionState.Open;
        }
        catch (Exception)
        {
            InternalState = ConnectionState.Closed;
            throw;
        }
    }

    private void EnsureOpen()
    {
        if (InternalState == ConnectionState.Closed)
        {
            Open();
        }
    }

    private void AssertOpen()
    {
        if (InternalState != ConnectionState.Open || _libConnection == null)
        {
            throw new InvalidOperationException("Connection is not open");
        }
    }

    private void AssertClosed()
    {
        if (InternalState != ConnectionState.Closed)
        {
            throw new InvalidOperationException("Connection is not closed");
        }
    }

    public CommitResponse? WriteMutations(BatchWriteRequest.Types.MutationGroup mutations)
    {
        EnsureOpen();
        _transaction?.MarkUsed();
        return LibConnection.WriteMutations(mutations);
    }

    public Task<CommitResponse?> WriteMutationsAsync(BatchWriteRequest.Types.MutationGroup mutations, CancellationToken cancellationToken = default)
    {
        EnsureOpen();
        _transaction?.MarkUsed();
        return LibConnection.WriteMutationsAsync(mutations, cancellationToken);
    }

    /// <summary>
    /// Create a new command for this connection with the given command text.
    /// </summary>
    /// <param name="commandText">The command text to set for the command</param>
    /// <returns>A new command with the given command text</returns>
    public SpannerCommand CreateCommand(string commandText)
    {
        var cmd = CreateCommand();
        cmd.CommandText = commandText;
        return cmd;
    }
    
    public new SpannerCommand CreateCommand() => (SpannerCommand) base.CreateCommand();

    protected override DbCommand CreateDbCommand()
    {
        var cmd = new SpannerCommand(this);
        return cmd;
    }
    
    public Rows Execute(ExecuteSqlRequest statement, int prefetchRows = 0)
    {
        EnsureOpen();
        _transaction?.MarkUsed();
        return TranslateException(() => LibConnection.Execute(statement, prefetchRows));
    }
    
    public Task<Rows> ExecuteAsync(ExecuteSqlRequest statement, int prefetchRows = 0, CancellationToken cancellationToken = default)
    {
        EnsureOpen();
        _transaction?.MarkUsed();
        return TranslateException(LibConnection.ExecuteAsync(statement, prefetchRows, cancellationToken));
    }
    
    public new SpannerBatch CreateBatch() => (SpannerBatch) base.CreateBatch();

    protected override DbBatch CreateDbBatch()
    {
        return new SpannerBatch(this);
    }

    public long[] ExecuteBatch(List<ExecuteBatchDmlRequest.Types.Statement> statements)
    {
        EnsureOpen();
        _transaction?.MarkUsed();
        return TranslateException(() => LibConnection.ExecuteBatch(statements));
    }

    public Task<long[]> ExecuteBatchAsync(List<ExecuteBatchDmlRequest.Types.Statement> statements,
        CancellationToken cancellationToken = default)
    {
        EnsureOpen();
        _transaction?.MarkUsed();
        return TranslateException(LibConnection.ExecuteBatchAsync(statements, cancellationToken));
    }

    public long[] ExecuteBatchDml(List<DbCommand> commands)
    {
        EnsureOpen();
        var statements = new List<ExecuteBatchDmlRequest.Types.Statement>(commands.Count);
        foreach (var command in commands)
        {
            if (command is SpannerCommand spannerCommand)
            {
                var statement = spannerCommand.BuildStatement();
                var batchStatement = new ExecuteBatchDmlRequest.Types.Statement
                {
                    Sql = statement.Sql,
                    Params = statement.Params,
                };
                batchStatement.ParamTypes.Add(statement.ParamTypes);
                statements.Add(batchStatement);
            }
        }
        _transaction?.MarkUsed();
        return TranslateException(() => LibConnection.ExecuteBatch(statements));
    }

    /// <summary>
    /// Creates a command to insert data into Spanner using mutations.
    /// </summary>
    /// <param name="table">The table to insert data into</param>
    public SpannerCommand CreateInsertCommand(string table)
    {
        return new SpannerCommand(this, new Mutation { Insert = new Mutation.Types.Write { Table = table } });
    }

    /// <summary>
    /// Creates a command to insert-or-update data into Spanner using mutations.
    /// </summary>
    /// <param name="table">The table to insert-or-update data into</param>
    public SpannerCommand CreateInsertOrUpdateCommand(string table)
    {
        return new SpannerCommand(this, new Mutation { InsertOrUpdate = new Mutation.Types.Write { Table = table } });
    }

    /// <summary>
    /// Creates a command to update data in Spanner using mutations.
    /// </summary>
    /// <param name="table">The table that contains the data that should be updated</param>
    public SpannerCommand CreateUpdateCommand(string table)
    {
        return new SpannerCommand(this, new Mutation { Update = new Mutation.Types.Write { Table = table } });
    }

    /// <summary>
    /// Creates a command to replace data in Spanner using mutations.
    /// </summary>
    /// <param name="table">The table that contains the data that should be replaced</param>
    public SpannerCommand CreateReplaceCommand(string table)
    {
        return new SpannerCommand(this, new Mutation { Replace = new Mutation.Types.Write { Table = table } });
    }

    /// <summary>
    /// Creates a command to delete data in Spanner using mutations.
    /// </summary>
    /// <param name="table">The table that contains the data that should be deleted</param>
    public SpannerCommand CreateDeleteCommand(string table)
    {
        return new SpannerCommand(this, new Mutation { Delete = new Mutation.Types.Delete { Table = table } });
    }
    
    public override DataTable GetSchema() => GetSchemaProvider().GetSchema();
    
    public override DataTable GetSchema(string collectionName)
        => GetSchema(collectionName, null);

    public override DataTable GetSchema(string collectionName, string?[]? restrictionValues)
        => GetSchemaProvider().GetSchema(collectionName, restrictionValues);
}