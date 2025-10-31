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
using static Google.Cloud.Spanner.DataProvider.SpannerDbException;

namespace Google.Cloud.Spanner.DataProvider;

public class SpannerCommand : DbCommand, ICloneable
{
    private SpannerConnection SpannerConnection => (SpannerConnection)Connection!;
        
    private string _commandText = "";
    [AllowNull] public override string CommandText { get => _commandText; set => _commandText = value ?? ""; }

    private int? _timeout;
    
    public override int CommandTimeout
    {
        get => _timeout ?? (int?) SpannerConnection?.DefaultCommandTimeout ?? 0;
        set => _timeout = value;
    }

    public override CommandType CommandType { get; set; } = CommandType.Text;

    public override UpdateRowSource UpdatedRowSource { get; set; } = UpdateRowSource.Both;
    protected override DbConnection? DbConnection { get; set; }
    protected override DbParameterCollection DbParameterCollection { get; } = new SpannerParameterCollection();
    
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
    public RequestOptions? RequestOptions { get; set; }
    
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
        var spannerParams = ((SpannerParameterCollection)DbParameterCollection).CreateSpannerParams();
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

        var mutation = _mutation!.Clone();
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
        for (var index = 0; index < DbParameterCollection.Count; index++)
        {
            var param = DbParameterCollection[index];
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

                values.Values.Add(spannerParameter.ConvertToProto(spannerParameter));
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

    private void ExecuteMutation()
    {
        GaxPreconditions.CheckState(_mutation != null, "Cannot execute mutation");
        var mutations = new BatchWriteRequest.Types.MutationGroup
        {
            Mutations = { BuildMutation() }
        };
        SpannerConnection.LibConnection!.WriteMutations(mutations);
    }

    private Rows Execute(ExecuteSqlRequest.Types.QueryMode mode = ExecuteSqlRequest.Types.QueryMode.Normal)
    {
        CheckCommandStateForExecution();
        return TranslateException(() => SpannerConnection.LibConnection!.Execute(BuildStatement(mode)));
    }

    private Task<Rows> ExecuteAsync(CancellationToken cancellationToken)
    {
        return ExecuteAsync(ExecuteSqlRequest.Types.QueryMode.Normal, cancellationToken);
    }

    private Task<Rows> ExecuteAsync(ExecuteSqlRequest.Types.QueryMode mode, CancellationToken cancellationToken)
    {
        CheckCommandStateForExecution();
        if (cancellationToken.IsCancellationRequested)
        {
            return Task.FromCanceled<Rows>(cancellationToken);
        }
        return TranslateException(() => SpannerConnection.LibConnection!.ExecuteAsync(BuildStatement(mode)));
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

        var rows = Execute();
        try
        {
            return (int)rows.UpdateCount;
        }
        finally
        {
            rows.Close();
        }
    }

    public override object? ExecuteScalar()
    {
        CheckDisposed();
        GaxPreconditions.CheckState(_mutation == null, "Cannot execute mutations with ExecuteScalar()");
        var rows = Execute();
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

    public override void Prepare()
    {
        CheckDisposed();
        Execute(ExecuteSqlRequest.Types.QueryMode.Plan);
    }

    public override Task PrepareAsync(CancellationToken cancellationToken = default)
    {
        CheckDisposed();
        return ExecuteAsync(ExecuteSqlRequest.Types.QueryMode.Plan, cancellationToken);
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
            var rows = Execute();
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
            var rows = await ExecuteAsync(cancellationToken);
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
                await SpannerConnection.CloseAsync();
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
        (DbParameterCollection as SpannerParameterCollection)?.CloneTo((clone.Parameters as SpannerParameterCollection)!);
        return clone;
    }
    
}