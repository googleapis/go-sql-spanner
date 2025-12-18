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
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Google.Api.Gax;
using Google.Cloud.Spanner.V1;

namespace Google.Cloud.Spanner.DataProvider;

/// <summary>
/// SpannerBatch is the Spanner-specific implementation of DbBatch. SpannerBatch supports batches of DML or DDL
/// statements. Note that all statements in a batch must be of the same type. Batches of queries or DML statements with
/// a THEN RETURN / RETURNING clause are not supported.
/// </summary>
public class SpannerBatch : DbBatch
{
    private SpannerConnection SpannerConnection => (SpannerConnection)Connection!;
    public new SpannerBatchCommandCollection BatchCommands => (SpannerBatchCommandCollection)base.BatchCommands;
    protected override SpannerBatchCommandCollection DbBatchCommands { get; } = new();
    public override int Timeout { get; set; }
    protected override DbConnection? DbConnection { get; set; }
    protected override DbTransaction? DbTransaction { get; set; }
    private SpannerTransaction? SpannerTransaction => DbTransaction as SpannerTransaction;
    public string? Tag { get; set; }

    private bool HasOnlyMutations => BatchCommands.All(cmd => ((SpannerBatchCommand)cmd).HasMutation);
    
    private bool HasOnlyDmlCommands => BatchCommands.All(cmd => !((SpannerBatchCommand)cmd).HasMutation);
    
    public SpannerBatch()
    {}

    internal SpannerBatch(SpannerConnection connection)
    {
        Connection = GaxPreconditions.CheckNotNull(connection, nameof(connection));
    }
    
    protected override DbDataReader ExecuteDbDataReader(CommandBehavior behavior)
    {
        throw new NotImplementedException();
    }

    protected override Task<DbDataReader> ExecuteDbDataReaderAsync(CommandBehavior behavior, CancellationToken cancellationToken)
    {
        throw new NotImplementedException();
    }

    private List<Mutation> CreateMutations()
    {
        var mutations = new List<Mutation>(DbBatchCommands.Count);
        foreach (var command in DbBatchCommands)
        {
            mutations.Add(SpannerCommand.BuildMutation(command.Mutation!, command.SpannerParameterCollection));
        }
        return mutations;
    }
    
    private BatchWriteRequest.Types.MutationGroup CreateMutationGroup()
    {
        return new BatchWriteRequest.Types.MutationGroup
        {
            Mutations = { CreateMutations() },
        };
    }

    private List<ExecuteBatchDmlRequest.Types.Statement> CreateStatements()
    {
        var statements = new List<ExecuteBatchDmlRequest.Types.Statement>(DbBatchCommands.Count);
        foreach (var command in DbBatchCommands)
        {
            var (queryParams, paramTypes) = ((SpannerParameterCollection)command.Parameters).CreateSpannerParams(prepare: false);
            var batchStatement = new ExecuteBatchDmlRequest.Types.Statement
            {
                Sql = command.CommandText,
                Params = queryParams,
            };
            batchStatement.ParamTypes.Add(paramTypes);
            statements.Add(batchStatement);
        }
        return statements;
    }

    private SpannerCommand? CreateSetTagsCommandText()
    {
        if (!string.IsNullOrEmpty(Tag) || !string.IsNullOrEmpty(SpannerConnection.Transaction?.Tag))
        {
            string commandText;
            if (!string.IsNullOrEmpty(SpannerConnection.Transaction?.Tag) && string.IsNullOrEmpty(Tag))
            {
                commandText = $"set local transaction_tag='{SpannerConnection.Transaction.Tag}'";
            }
            else if (!string.IsNullOrEmpty(Tag) && string.IsNullOrEmpty(SpannerConnection.Transaction?.Tag))
            {
                commandText = $"set statement_tag = '{Tag}'";
            }
            else
            {
                commandText = $"set local transaction_tag='{SpannerConnection.Transaction!.Tag}';set statement_tag = '{Tag}'";
            }
            return SpannerConnection.CreateCommand(commandText);
        }
        return null;
    }

    private void SetTags()
    {
        CreateSetTagsCommandText()?.ExecuteNonQuery();
    }

    private Task SetTagsAsync(CancellationToken cancellationToken)
    {
        var command = CreateSetTagsCommandText();
        if (command != null)
        {
            return command.ExecuteNonQueryAsync(cancellationToken);
        }
        return Task.CompletedTask;
    }

    public override int ExecuteNonQuery()
    {
        if (DbBatchCommands.Count == 0)
        {
            return 0;
        }
        long[] results;
        if (HasOnlyMutations)
        {
            var mutationGroup = CreateMutationGroup();
            SetTags();
            SpannerConnection.WriteMutations(mutationGroup);
            results = new long[mutationGroup.Mutations.Count];
            Array.Fill(results, 1L);
        }
        else if (HasOnlyDmlCommands)
        {
            var statements = CreateStatements();
            SetTags();
            results = SpannerConnection.ExecuteBatch(statements);
        }
        else
        {
            throw new InvalidOperationException("Batches must contain either only DML statements or only mutations.");
        }
        DbBatchCommands.SetAffected(results);
        return (int)results.Sum();
    }

    public override async Task<int> ExecuteNonQueryAsync(CancellationToken cancellationToken = default)
    {
        if (DbBatchCommands.Count == 0)
        {
            return 0;
        }
        long[] results;
        if (HasOnlyMutations)
        {
            var mutationGroup = CreateMutationGroup();
            await SetTagsAsync(cancellationToken).ConfigureAwait(false);
            await SpannerConnection.WriteMutationsAsync(mutationGroup, cancellationToken).ConfigureAwait(false);
            results = new long[mutationGroup.Mutations.Count];
            Array.Fill(results, 1L);
        }
        else if (HasOnlyDmlCommands)
        {
            var statements = CreateStatements();
            await SetTagsAsync(cancellationToken).ConfigureAwait(false);
            results = await SpannerConnection.ExecuteBatchAsync(statements, cancellationToken) .ConfigureAwait(false);
        }
        else
        {
            throw new InvalidOperationException("Batches must contain either only DML statements or only mutations.");
        }
        DbBatchCommands.SetAffected(results);
        return (int)results.Sum();
    }

    public override object? ExecuteScalar()
    {
        throw new NotImplementedException();
    }

    public override Task<object?> ExecuteScalarAsync(CancellationToken cancellationToken = default)
    {
        throw new NotImplementedException();
    }

    public override void Prepare()
    {
        throw new NotImplementedException();
    }

    public override Task PrepareAsync(CancellationToken cancellationToken = default)
    {
        throw new NotImplementedException();
    }

    public override void Cancel()
    {
        throw new System.NotImplementedException();
    }

    /// <summary>
    /// Creates a command to insert data into Spanner using mutations.
    /// </summary>
    /// <param name="table">The table to insert data into</param>
    public SpannerBatchCommand CreateInsertCommand(string table)
    {
        return new SpannerBatchCommand(new Mutation { Insert = new Mutation.Types.Write { Table = table } });
    }

    /// <summary>
    /// Creates a command to insert-or-update data into Spanner using mutations.
    /// </summary>
    /// <param name="table">The table to insert-or-update data into</param>
    public SpannerBatchCommand CreateInsertOrUpdateCommand(string table)
    {
        return new SpannerBatchCommand(new Mutation { InsertOrUpdate = new Mutation.Types.Write { Table = table } });
    }

    /// <summary>
    /// Creates a command to update data in Spanner using mutations.
    /// </summary>
    /// <param name="table">The table that contains the data that should be updated</param>
    public SpannerBatchCommand CreateUpdateCommand(string table)
    {
        return new SpannerBatchCommand(new Mutation { Update = new Mutation.Types.Write { Table = table } });
    }

    /// <summary>
    /// Creates a command to replace data in Spanner using mutations.
    /// </summary>
    /// <param name="table">The table that contains the data that should be replaced</param>
    public SpannerBatchCommand CreateReplaceCommand(string table)
    {
        return new SpannerBatchCommand(new Mutation { Replace = new Mutation.Types.Write { Table = table } });
    }

    /// <summary>
    /// Creates a command to delete data in Spanner using mutations.
    /// </summary>
    /// <param name="table">The table that contains the data that should be deleted</param>
    public SpannerBatchCommand CreateDeleteCommand(string table)
    {
        return new SpannerBatchCommand(new Mutation { Delete = new Mutation.Types.Delete { Table = table } });
    }

    protected override DbBatchCommand CreateDbBatchCommand()
    {
        return new SpannerBatchCommand();
    }
}