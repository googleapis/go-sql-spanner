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

using System.Data;
using System.Data.Common;
using Google.Cloud.Spanner.V1;

namespace Google.Cloud.Spanner.DataProvider;

public class SpannerBatchCommand : DbBatchCommand
{
    public override string CommandText { get; set; } = "";
    public override CommandType CommandType { get; set; }
    
    internal int InternalRecordsAffected;
    public override int RecordsAffected => InternalRecordsAffected;
    protected override DbParameterCollection DbParameterCollection { get; } = new SpannerParameterCollection();
    public override bool CanCreateParameter => true;
    
    internal SpannerParameterCollection SpannerParameterCollection => (SpannerParameterCollection)DbParameterCollection;
    
    internal bool HasMutation => Mutation != null;
    
    internal Mutation? Mutation { get; }

    internal SpannerBatchCommand()
    {
    }

    internal SpannerBatchCommand(Mutation mutation)
    {
        Mutation = mutation;
    }

    public override DbParameter CreateParameter()
    {
        return new SpannerParameter();
    }

    public SpannerParameter AddParameter(string name, object value)
    {
        var parameter = new SpannerParameter
        {
            ParameterName = name,
            Value = value
        };
        SpannerParameterCollection.Add(parameter);
        return parameter;
    }
}