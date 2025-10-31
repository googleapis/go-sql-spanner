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
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using Google.Api.Gax;
using Google.Cloud.Spanner.Common.V1;
using Google.Cloud.Spanner.V1;
using Google.Cloud.SpannerLib;

namespace Google.Cloud.Spanner.DataProvider;

public class SpannerConnection : DbConnection
{
    public bool UseSharedLibrary { get; set; }
    
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
            return Assembly.GetAssembly(typeof(Connection))?.GetName().Version?.ToString() ?? "";
        }
    }

    public override bool CanCreateBatch => true;

    private bool _disposed;
    private ConnectionState _state = ConnectionState.Closed;
    private SpannerPool? Pool { get; set; }

    private Connection? _libConnection;
        
    internal Connection? LibConnection
    {
        get
        {
            AssertOpen();
            return _libConnection;
        }
    }

    internal uint DefaultCommandTimeout => _connectionStringBuilder?.CommandTimeout ?? 0;
        
    private SpannerTransaction? _transaction;

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

    protected override DbTransaction BeginDbTransaction(IsolationLevel isolationLevel)
    {
        return BeginTransaction(new TransactionOptions
        {
            IsolationLevel = SpannerTransaction.TranslateIsolationLevel(isolationLevel),
        });
    }

    public DbTransaction BeginReadOnlyTransaction()
    {
        return BeginTransaction(new TransactionOptions
        {
            ReadOnly = new TransactionOptions.Types.ReadOnly(),
        });
    }

    /// <summary>
    /// Start a new transaction using the given TransactionOptions.
    /// </summary>
    /// <param name="transactionOptions">The options to use for the new transaction</param>
    /// <returns>The new transaction</returns>
    /// <exception cref="InvalidOperationException">If the connection has an active transaction</exception>
    public DbTransaction BeginTransaction(TransactionOptions transactionOptions)
    {
        EnsureOpen();
        GaxPreconditions.CheckState(!HasTransaction, "This connection has a transaction.");
        _transaction = new SpannerTransaction(this, transactionOptions);
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
        if (InternalState != ConnectionState.Open)
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
        return LibConnection!.WriteMutations(mutations);
    }

    public Task<CommitResponse?> WriteMutationsAsync(BatchWriteRequest.Types.MutationGroup mutations, CancellationToken cancellationToken = default)
    {
        EnsureOpen();
        return LibConnection!.WriteMutationsAsync(mutations, cancellationToken);
    }

    protected override DbCommand CreateDbCommand()
    {
        var cmd = new SpannerCommand(this);
        return cmd;
    }

    protected override DbBatch CreateDbBatch()
    {
        return new SpannerBatch(this);
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
        return LibConnection!.ExecuteBatch(statements);
    }

    public DbCommand CreateInsertCommand(string table)
    {
        return new SpannerCommand(this, new Mutation { Insert = new Mutation.Types.Write { Table = table } });
    }

    public DbCommand CreateUpdateCommand(string table)
    {
        return new SpannerCommand(this, new Mutation { Update = new Mutation.Types.Write { Table = table } });
    }

    public DbCommand CreateDeleteCommand(string table)
    {
        return new SpannerCommand(this, new Mutation { Delete = new Mutation.Types.Delete { Table = table } });
    }

}