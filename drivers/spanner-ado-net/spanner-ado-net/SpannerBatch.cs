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
    
    public SpannerBatch()
    {}

    internal SpannerBatch(SpannerConnection connection)
    {
        Connection = GaxPreconditions.CheckNotNull(connection, nameof(connection));
    }
    
    protected override DbDataReader ExecuteDbDataReader(CommandBehavior behavior)
    {
        throw new System.NotImplementedException();
    }

    protected override Task<DbDataReader> ExecuteDbDataReaderAsync(CommandBehavior behavior, CancellationToken cancellationToken)
    {
        throw new System.NotImplementedException();
    }

    private List<ExecuteBatchDmlRequest.Types.Statement> CreateStatements()
    {
        var statements = new List<ExecuteBatchDmlRequest.Types.Statement>(DbBatchCommands.Count);
        foreach (var command in DbBatchCommands)
        {
            var spannerParams = ((SpannerParameterCollection)command.Parameters).CreateSpannerParams(prepare: false);
            var queryParams = spannerParams.Item1;
            var paramTypes = spannerParams.Item2;
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

    private Task SetRequestTagAsync(CancellationToken cancellationToken)
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
        var statements = CreateStatements();
        SetTags();
        var results = SpannerConnection.ExecuteBatch(statements);
        DbBatchCommands.SetAffected(results);
        return (int) results.Sum();
    }

    public override async Task<int> ExecuteNonQueryAsync(CancellationToken cancellationToken = default)
    {
        if (DbBatchCommands.Count == 0)
        {
            return 0;
        }
        var statements = CreateStatements();
        await SetRequestTagAsync(cancellationToken).ConfigureAwait(false);
        var results = await SpannerConnection.ExecuteBatchAsync(statements, cancellationToken).ConfigureAwait(false);
        DbBatchCommands.SetAffected(results);
        return (int) results.Sum();
    }

    public override object? ExecuteScalar()
    {
        throw new System.NotImplementedException();
    }

    public override Task<object?> ExecuteScalarAsync(CancellationToken cancellationToken = default)
    {
        throw new System.NotImplementedException();
    }

    public override void Prepare()
    {
        throw new System.NotImplementedException();
    }

    public override Task PrepareAsync(CancellationToken cancellationToken = default)
    {
        throw new System.NotImplementedException();
    }

    public override void Cancel()
    {
        throw new System.NotImplementedException();
    }

    protected override DbBatchCommand CreateDbBatchCommand()
    {
        return new SpannerBatchCommand();
    }
}