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
using Google.Cloud.Spanner.Common.V1;
using Google.Cloud.Spanner.V1;
using Google.Cloud.SpannerLib;

namespace Google.Cloud.Spanner.DataProvider;

public class SpannerConnection : DbConnection
{
    public bool UseSharedLibrary { get; set; }
    
    private string _connectionString = string.Empty;
        
    [AllowNull]
    public override string ConnectionString {
        get => _connectionString;
        set
        {
            AssertClosed();
            if (!IsValidConnectionString(value))
            {
                throw new ArgumentException($"Invalid connection string: {value}");
            }
            _connectionString = value ?? string.Empty;
        }
    }

    public override string Database
    {
        get
        {
            // TODO: Move this to SpannerLib.
            if (String.IsNullOrWhiteSpace(ConnectionString))
            {
                return "";
            }
            var startIndex = ConnectionString.IndexOf("projects/", StringComparison.Ordinal);
            if (startIndex == -1)
            {
                throw new ArgumentException($"Invalid database name in connection string: {ConnectionString}");
            }
                
            var endIndex = ConnectionString.IndexOf('?');
            if (endIndex == -1)
            {
                endIndex = ConnectionString.IndexOf(';');
            }
            if (endIndex == -1)
            {
                endIndex = ConnectionString.Length;
            }
            var name = ConnectionString.Substring(startIndex, endIndex);
            if (DatabaseName.TryParse(name, false, out var result))
            {
                return result.DatabaseId;
            }
            throw new ArgumentException($"Invalid database name in connection string: {ConnectionString}");
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

    public override string DataSource { get; } = "";
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
        
    private SpannerTransaction? _transaction;

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

    public DbTransaction BeginTransaction(TransactionOptions transactionOptions)
    {
        EnsureOpen();
        if (_transaction != null)
        {
            throw new InvalidOperationException("This connection has a transaction.");
        }
        _transaction = new SpannerTransaction(this, transactionOptions);
        return _transaction;
    }

    internal void ClearTransaction()
    {
        _transaction = null;
    }

    public override void ChangeDatabase(string databaseName)
    {
        throw new NotImplementedException();
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
        if (ConnectionString == string.Empty)
        {
            throw new InvalidOperationException("Connection string is empty");
        }
            
        InternalState = ConnectionState.Connecting;
        Pool = SpannerPool.GetOrCreate(ConnectionString);
        _libConnection = Pool.CreateConnection();
        InternalState = ConnectionState.Open;
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

    private bool IsValidConnectionString(string? connectionString)
    {
        // TODO: Move to Spanner lib.
        return string.IsNullOrEmpty(connectionString) || connectionString.Contains("projects/");
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
        return new SpannerCommand(this);
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