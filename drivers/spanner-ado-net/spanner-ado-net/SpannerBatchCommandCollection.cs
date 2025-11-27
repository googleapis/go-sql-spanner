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
using System.Data.Common;
using Google.Api.Gax;

namespace Google.Cloud.Spanner.DataProvider;

public class SpannerBatchCommandCollection : DbBatchCommandCollection
{
    private readonly List<SpannerBatchCommand> _commands = new ();
    public override int Count => _commands.Count;
    public override bool IsReadOnly => false;

    internal void SetAffected(long[] affected)
    {
        for (var i = 0; i < _commands.Count; i++)
        {
            _commands[i].InternalRecordsAffected = (int) affected[i];
        }
    }

    public override IEnumerator<SpannerBatchCommand> GetEnumerator()
    {
        return _commands.GetEnumerator();
    }

    /// <summary>
    /// Adds a new command to the batch with the given command text.
    /// </summary>
    /// <param name="commandText">The command text for the batch command</param>
    /// <returns>The new batch command</returns>
    public SpannerBatchCommand Add(string commandText)
    {
        var cmd = new SpannerBatchCommand
        {
            CommandText = commandText
        };
        _commands.Add(cmd);
        return cmd;
    }

    public override void Add(DbBatchCommand item)
    {
        GaxPreconditions.CheckNotNull(item, nameof(item));
        GaxPreconditions.CheckArgument(item is SpannerBatchCommand, nameof(item), "Item must be a SpannerBatchCommand");
        _commands.Add((SpannerBatchCommand)item);
    }

    public override void Clear()
    {
        _commands.Clear();
    }

    public override bool Contains(DbBatchCommand item)
    {
        GaxPreconditions.CheckArgument(item is SpannerBatchCommand, nameof(item), "Item must be a SpannerBatchCommand");
        return _commands.Contains((SpannerBatchCommand)item);
    }

    public override void CopyTo(DbBatchCommand[] array, int arrayIndex)
    {
        throw new System.NotImplementedException();
    }

    public override bool Remove(DbBatchCommand item)
    {
        GaxPreconditions.CheckArgument(item is SpannerBatchCommand, nameof(item), "Item must be a SpannerBatchCommand");
        return _commands.Remove((SpannerBatchCommand)item);
    }

    public override int IndexOf(DbBatchCommand item)
    {
        GaxPreconditions.CheckArgument(item is SpannerBatchCommand, nameof(item), "Item must be a SpannerBatchCommand");
        return _commands.IndexOf((SpannerBatchCommand)item);
    }

    public override void Insert(int index, DbBatchCommand item)
    {
        GaxPreconditions.CheckArgument(item is SpannerBatchCommand, nameof(item), "Item must be a SpannerBatchCommand");
        _commands.Insert(index, (SpannerBatchCommand)item);
    }

    public override void RemoveAt(int index)
    {
        _commands.RemoveAt(index);
    }

    protected override SpannerBatchCommand GetBatchCommand(int index)
    {
        return _commands[index];
    }

    protected override void SetBatchCommand(int index, DbBatchCommand batchCommand)
    {
        _commands[index] = (SpannerBatchCommand)batchCommand;
    }
}