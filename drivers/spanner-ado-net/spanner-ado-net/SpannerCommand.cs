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
using System.Data;
using System.Data.Common;
using System.Diagnostics.CodeAnalysis;
using System.Threading;
using System.Threading.Tasks;
using Google.Api.Gax;
using Google.Cloud.Spanner.V1;
using Google.Cloud.SpannerLib;
using Google.Protobuf.WellKnownTypes;

namespace Google.Cloud.Spanner.DataProvider;

public class SpannerCommand : DbCommand, ICloneable
{
    private SpannerConnection SpannerConnection => (SpannerConnection)Connection!;
        
    private string _commandText = "";
    [AllowNull] public override string CommandText { get => _commandText; set => _commandText = value ?? ""; }

    private int? _timeout;
    
    public override int CommandTimeout
    {
        get => _timeout ?? (int?) (Connection as SpannerConnection)?.DefaultCommandTimeout ?? 0;
        set => _timeout = value;
    }

    public override CommandType CommandType { get; set; } = CommandType.Text;

    public override UpdateRowSource UpdatedRowSource { get; set; } = UpdateRowSource.Both;
    protected override DbConnection? DbConnection { get; set; }
    
    protected override DbParameterCollection DbParameterCollection { get; } = new SpannerParameterCollection();
    public new SpannerParameterCollection Parameters => (SpannerParameterCollection)DbParameterCollection;
    
    SpannerTransaction? _transaction;
    protected override DbTransaction? DbTransaction
    {
        get => _transaction;
        set
        {
            var tx = (SpannerTransaction?)value;
            
            if (tx is { IsCompleted: true })
                throw new InvalidOperationException("Transaction is already completed");
            _transaction = tx;
        }
    }
    
    public override bool DesignTimeVisible { get; set; }

    private bool HasTransaction => DbTransaction is SpannerTransaction;
    private readonly Mutation? _mutation;

    public TransactionOptions.Types.ReadOnly? SingleUseReadOnlyTransactionOptions { get; set; }

    public string Tag
    {
        get => RequestOptions.RequestTag;
        set => RequestOptions.RequestTag = value;
    }

    private RequestOptions RequestOptions { get; } = new ();
    
    private bool _disposed;
        
    public SpannerCommand() {}

    internal SpannerCommand(SpannerConnection connection)
    {
        Connection = GaxPreconditions.CheckNotNull(connection, nameof(connection));
    }

    public SpannerCommand(string commandText, SpannerConnection connection)
    {
        Connection = GaxPreconditions.CheckNotNull(connection, nameof(connection));
        _commandText = GaxPreconditions.CheckNotNull(commandText,  nameof(commandText));
    }
    
    public SpannerCommand(string cmdText, SpannerConnection connection, SpannerTransaction? transaction)
        : this(cmdText, connection)
        => Transaction = transaction;

    internal SpannerCommand(SpannerConnection connection, Mutation mutation)
    {
        Connection = GaxPreconditions.CheckNotNull(connection, nameof(connection));
        _mutation = mutation;
    }

    protected override void Dispose(bool disposing)
    {
        base.Dispose(disposing);
        _disposed = true;
    }

    private void CheckDisposed()
    {
        if (_disposed)
        {
            throw new ObjectDisposedException(nameof(SpannerCommand));
        }
    }

    public override void Cancel()
    {
        // TODO: Implement in Spanner lib
    }

    internal ExecuteSqlRequest BuildStatement(ExecuteSqlRequest.Types.QueryMode mode = ExecuteSqlRequest.Types.QueryMode.Normal)
    {
        GaxPreconditions.CheckState(!(HasTransaction && SingleUseReadOnlyTransactionOptions != null),
            "Cannot set both a transaction and single-use read-only options");
        var spannerParams = Parameters.CreateSpannerParams(prepare: mode == ExecuteSqlRequest.Types.QueryMode.Plan);
        var queryParams = spannerParams.Item1;
        var paramTypes = spannerParams.Item2;
        var sql = CommandText;
        if (CommandType == CommandType.TableDirect)
        {
            // TODO: Quote the table name
            sql = $"select * from {sql}";
        }
        var statement = new ExecuteSqlRequest
        {
            Sql = sql,
            Params = queryParams,
            RequestOptions = RequestOptions,
            QueryMode = mode,
        };
        if (_transaction?.Tag != null)
        {
            RequestOptions.TransactionTag = _transaction?.Tag;
        }

        statement.ParamTypes.Add(paramTypes);
        if (SingleUseReadOnlyTransactionOptions != null)
        {
            statement.Transaction = new TransactionSelector
            {
                SingleUse = new TransactionOptions
                {
                    ReadOnly = SingleUseReadOnlyTransactionOptions,
                },
            };
        }

        return statement;
    }

    private Mutation BuildMutation()
    {
        GaxPreconditions.CheckNotNull(_mutation, nameof(_mutation));
        GaxPreconditions.CheckNotNull(SpannerConnection, nameof(SpannerConnection));
        GaxPreconditions.CheckState(!(HasTransaction && SingleUseReadOnlyTransactionOptions != null),
            "Cannot set both a transaction and single-use read-only options");
        return BuildMutation(_mutation!.Clone(), Parameters);
    }

    internal static Mutation BuildMutation(Mutation mutation, SpannerParameterCollection parameters)
    {
        Mutation.Types.Write? write = null;
        Mutation.Types.Delete? delete = mutation.OperationCase == Mutation.OperationOneofCase.Delete
            ? mutation.Delete
            : null;
        switch (mutation.OperationCase)
        {
            case Mutation.OperationOneofCase.Insert:
                write = mutation.Insert;
                break;
            case Mutation.OperationOneofCase.Update:
                write = mutation.Update;
                break;
            case Mutation.OperationOneofCase.InsertOrUpdate:
                write = mutation.InsertOrUpdate;
                break;
            case Mutation.OperationOneofCase.Replace:
                write = mutation.Replace;
                break;
        }

        var values = new ListValue();
        for (var index = 0; index < parameters.Count; index++)
        {
            var param = parameters[index];
            if (param is SpannerParameter spannerParameter)
            {
                if (write != null)
                {
                    var name = param.ParameterName;
                    if (name.StartsWith("@"))
                    {
                        name = name[1..];
                    }

                    write.Columns.Add(name);
                }

                values.Values.Add(spannerParameter.ConvertToProto(spannerParameter, prepare: false));
            }
            else
            {
                throw new ArgumentException("parameter is not a SpannerParameter: " + param.ParameterName);
            }
        }

        write?.Values.Add(values);
        if (delete != null)
        {
            delete.KeySet = new KeySet();
            delete.KeySet.Keys.Add(values);
        }

        return mutation;
    }

    private BatchWriteRequest.Types.MutationGroup CreateMutationGroup()
    {
        GaxPreconditions.CheckState(_mutation != null, "Cannot execute mutation");
        return new BatchWriteRequest.Types.MutationGroup
        {
            Mutations = { BuildMutation() }
        };
    }

    private void ExecuteMutation()
    {
        SpannerConnection.WriteMutations(CreateMutationGroup());
    }

    private Task ExecuteMutationAsync(CancellationToken cancellationToken)
    {
        return SpannerConnection.WriteMutationsAsync(CreateMutationGroup(), cancellationToken);
    }

    private Rows Execute(ExecuteSqlRequest.Types.QueryMode mode = ExecuteSqlRequest.Types.QueryMode.Normal)
    {
        CheckCommandStateForExecution();
        return SpannerConnection.Execute(BuildStatement(mode));
    }

    private Task<Rows> ExecuteAsync(CancellationToken cancellationToken)
    {
        return ExecuteAsync(ExecuteSqlRequest.Types.QueryMode.Normal, prefetchRows: 0, cancellationToken);
    }

    private Task<Rows> ExecuteAsync(ExecuteSqlRequest.Types.QueryMode mode, int prefetchRows, CancellationToken cancellationToken)
    {
        CheckCommandStateForExecution();
        if (cancellationToken.IsCancellationRequested)
        {
            return Task.FromCanceled<Rows>(cancellationToken);
        }
        return SpannerConnection.ExecuteAsync(BuildStatement(mode), prefetchRows, cancellationToken);
    }

    private void CheckCommandStateForExecution()
    {
        GaxPreconditions.CheckState(!string.IsNullOrEmpty(_commandText), "Cannot execute empty command");
        GaxPreconditions.CheckState(Connection != null, "No connection has been set for the command");
        GaxPreconditions.CheckState(Transaction == null || Transaction.Connection == SpannerConnection,
            "The transaction that has been set for this command is from a different connection");
    }

    public override int ExecuteNonQuery()
    {
        CheckDisposed();
        if (_mutation != null)
        {
            ExecuteMutation();
            return 1;
        }

        using var rows = Execute();
        return (int)rows.GetTotalUpdateCount();
    }

    public override async Task<int> ExecuteNonQueryAsync(CancellationToken cancellationToken)
    {
        CheckDisposed();
        if (_mutation != null)
        {
            await ExecuteMutationAsync(cancellationToken).ConfigureAwait(false);
            return 1;
        }

        var rows = await ExecuteAsync(cancellationToken).ConfigureAwait(false);
        await using (rows.ConfigureAwait(false))
        {
            return (int)await rows.GetTotalUpdateCountAsync(cancellationToken).ConfigureAwait(false);
        }
    }
    
    public override object? ExecuteScalar()
    {
        CheckDisposed();
        GaxPreconditions.CheckState(_mutation == null, "Cannot execute mutations with ExecuteScalar()");
        using var rows = Execute();
        using var reader = new SpannerDataReader(SpannerConnection, rows, CommandBehavior.Default);
        if (reader.Read())
        {
            if (reader.FieldCount > 0)
            {
                return reader.GetValue(0);
            }
        }
        return null;
    }

    public override async Task<object?> ExecuteScalarAsync(CancellationToken cancellationToken)
    {
        CheckDisposed();
        GaxPreconditions.CheckState(_mutation == null, "Cannot execute mutations with ExecuteScalarAsync()");
        var rows = await ExecuteAsync(cancellationToken).ConfigureAwait(false);
        await using (rows.ConfigureAwait(false))
        {
            var reader = new SpannerDataReader(SpannerConnection, rows, CommandBehavior.Default);
            await using (reader.ConfigureAwait(false))
            {
                if (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
                {
                    if (reader.FieldCount > 0)
                    {
                        return reader.GetValue(0);
                    }
                }
            }
            return null;
        }
    }

    public override void Prepare()
    {
        CheckDisposed();
        using var rows = Execute(ExecuteSqlRequest.Types.QueryMode.Plan);
    }

    public override async Task PrepareAsync(CancellationToken cancellationToken = default)
    {
        CheckDisposed();
        var rows = await ExecuteAsync(ExecuteSqlRequest.Types.QueryMode.Plan, prefetchRows: 0, cancellationToken).ConfigureAwait(false);
        await using (rows.ConfigureAwait(false));
    }

    protected override DbParameter CreateDbParameter()
    {
        CheckDisposed();
        return new SpannerParameter();
    }

    protected override DbDataReader ExecuteDbDataReader(CommandBehavior behavior)
    {
        CheckDisposed();
        GaxPreconditions.CheckState(_mutation == null, "Cannot execute mutations with ExecuteDbDataReader()");
        try
        {
            var mode = behavior.HasFlag(CommandBehavior.SchemaOnly)
                ? ExecuteSqlRequest.Types.QueryMode.Plan
                : ExecuteSqlRequest.Types.QueryMode.Normal;
            var rows = Execute(mode);
            return new SpannerDataReader(SpannerConnection, rows, behavior);
        }
        catch (SpannerException exception)
        {
            if (behavior.HasFlag(CommandBehavior.CloseConnection))
            {
                SpannerConnection.Close();
            }
            throw new SpannerDbException(exception);
        }
        catch (Exception)
        {
            if (behavior.HasFlag(CommandBehavior.CloseConnection))
            {
                SpannerConnection.Close();
            }
            throw;
        }
    }

    protected override async Task<DbDataReader> ExecuteDbDataReaderAsync(CommandBehavior behavior,
        CancellationToken cancellationToken)
    {
        CheckDisposed();
        GaxPreconditions.CheckState(_mutation == null, "Cannot execute mutations with ExecuteDbDataReader()");
        try
        {
            var mode = behavior.HasFlag(CommandBehavior.SchemaOnly)
                ? ExecuteSqlRequest.Types.QueryMode.Plan
                : ExecuteSqlRequest.Types.QueryMode.Normal;
            var rows = await ExecuteAsync(mode, prefetchRows: 0, cancellationToken).ConfigureAwait(false);
            return new SpannerDataReader(SpannerConnection, rows, behavior);
        }
        catch (SpannerException exception)
        {
            if (behavior.HasFlag(CommandBehavior.CloseConnection))
            {
                await SpannerConnection.CloseAsync().ConfigureAwait(false);
            }
            throw new SpannerDbException(exception);
        }
        catch (Exception)
        {
            if (behavior.HasFlag(CommandBehavior.CloseConnection))
            {
                await SpannerConnection.CloseAsync().ConfigureAwait(false);
            }
            throw;
        }
    }
    
    object ICloneable.Clone() => Clone();
    
    public virtual SpannerCommand Clone()
    {
        var clone = new SpannerCommand()
        {
            Connection = Connection,
            _commandText = _commandText,
            _transaction = _transaction,
            CommandTimeout = CommandTimeout,
            CommandType = CommandType,
            DesignTimeVisible = DesignTimeVisible,
        };
        (DbParameterCollection as SpannerParameterCollection)?.CloneTo(clone.Parameters);
        return clone;
    }
    
}