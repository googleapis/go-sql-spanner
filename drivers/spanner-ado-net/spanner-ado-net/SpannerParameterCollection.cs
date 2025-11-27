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
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using Google.Api.Gax;
using Google.Protobuf.Collections;
using Google.Protobuf.WellKnownTypes;

namespace Google.Cloud.Spanner.DataProvider;

public class SpannerParameterCollection : DbParameterCollection
{
    private readonly List<SpannerParameter> _params = new ();
    public override int Count => _params.Count;
    public override object SyncRoot => _params;

    public override int Add(object value)
    {
        GaxPreconditions.CheckNotNull(value, nameof(value));
        var index = _params.Count;
        if (value is SpannerParameter spannerParameter)
        {
            _params.Add(spannerParameter);
        }
        else if (value is DbParameter)
        {
            throw new ArgumentException("value is not a SpannerParameter");
        }
        else
        {
            _params.Add(new SpannerParameter { ParameterName = "p" + (index + 1), Value = value });
        }

        return index;
    }
    
    public SpannerParameter AddWithValue(string parameterName, object? value, DbType? type = null)
    {
        var parameter = new SpannerParameter
        {
            ParameterName = parameterName,
            Value = value,
        };
        if (type != null)
        {
            parameter.DbType = type.Value;
        }
        Add(parameter);
        return parameter;
    }

    public override void Clear()
    {
        _params.Clear();
    }

    public override bool Contains(object value)
    {
        return IndexOf(value) > -1;
    }

    public override int IndexOf(object value)
    {
        if (value is SpannerParameter spannerParameter)
        {
            return _params.IndexOf(spannerParameter);
        }
        return _params.FindIndex(p => Equals(p.Value, value));
    }

    public override void Insert(int index, object value)
    {
        GaxPreconditions.CheckNotNull(value, nameof(value));
        if (value is SpannerParameter spannerParameter)
        {
            _params.Insert(index, spannerParameter);
        }
        else if (value is DbParameter)
        {
            throw new ArgumentException("value is not a SpannerParameter");
        }
        else
        {
            _params.Insert(index, new SpannerParameter { ParameterName = "p" + (index + 1), Value = value });
        }
    }

    public override void Remove(object value)
    {
        GaxPreconditions.CheckNotNull(value, nameof(value));
        var index = IndexOf(value);
        if (index > -1)
        {
            _params.RemoveAt(index);
        }
    }

    public override void RemoveAt(int index)
    {
        _params.RemoveAt(index);
    }

    public override void RemoveAt(string parameterName)
    {
        var index = IndexOf(parameterName);
        if (index > -1)
        {
            _params.RemoveAt(index);
        }
    }

    protected override void SetParameter(int index, DbParameter value)
    {
        GaxPreconditions.CheckNotNull(value, nameof(value));
        if (value is SpannerParameter spannerParameter)
        {
            _params[index] = spannerParameter;
        }
        else
        {
            throw new ArgumentException("value is not a SpannerParameter");
        }
    }

    protected override void SetParameter(string parameterName, DbParameter value)
    {
        GaxPreconditions.CheckNotNull(value, nameof(value));
        if (value is SpannerParameter spannerParameter)
        {
            if (spannerParameter.ParameterName == "")
            {
                spannerParameter.ParameterName = parameterName;
            }
            else if (!spannerParameter.ParameterName.Equals(parameterName, StringComparison.OrdinalIgnoreCase))
            {
                throw new ArgumentException("Parameter names mismatch");
            }
            var index = IndexOf(parameterName);
            if (index > -1)
            {
                _params[index] = spannerParameter;
            }
            else
            {
                spannerParameter.ParameterName = parameterName;
                Add(spannerParameter);
            }
        }
        else
        {
            throw new ArgumentException("value is not a SpannerParameter");
        }
    }

    public override int IndexOf(string parameterName)
    {
        var result = _params.FindIndex(p => Equals(p.ParameterName, parameterName));
        if (result > -1)
        {
            return result;
        }
        return _params.FindIndex(p => p.ParameterName.Equals(parameterName, StringComparison.OrdinalIgnoreCase));
    }

    public override bool Contains(string value)
    {
        return IndexOf(value) > -1;
    }

    public override void CopyTo(Array array, int index)
    {
        if (array == null)
        {
            throw new ArgumentNullException(nameof(array));
        }

        if (array.Length < _params.Count + index)
        {
            throw new ArgumentOutOfRangeException(
                nameof(array), "There is not enough space in the array to copy values.");
        }

        foreach (var item in _params)
        {
            array.SetValue(item, index);
            index++;
        }
    }

    public override IEnumerator GetEnumerator()
    {
        return _params.GetEnumerator();
    }

    protected override DbParameter GetParameter(int index)
    {
        return _params[index];
    }

#pragma warning disable CS8764
    protected override DbParameter? GetParameter(string parameterName)
#pragma warning restore CS8764
    {
        var index = IndexOf(parameterName);
        return index > -1 ? _params[index] : null;
    }

    public override void AddRange(Array values)
    {
        foreach (var value in values)
        {
            Add(value);
        }
    }
        
    internal Tuple<Struct, MapField<string, Google.Cloud.Spanner.V1.Type>> CreateSpannerParams(bool prepare)
    {
        var queryParams = new Struct();
        var paramTypes = new MapField<string, Google.Cloud.Spanner.V1.Type>();
        for (var index = 0; index < Count; index++)
        {
            var param = this[index];
            if (param is SpannerParameter spannerParameter)
            {
                var name = param.ParameterName;
                if (name.StartsWith("@"))
                {
                    name = name[1..];
                }
                else if (name.StartsWith("$"))
                {
                    name = "p" + name[1..];
                }
                else if (string.IsNullOrEmpty(name))
                {
                    name = "p" + (index + 1);
                }
                queryParams.Fields.Add(name, spannerParameter.ConvertToProto(spannerParameter, prepare));
                var paramType = spannerParameter.GetSpannerType();
                if (paramType != null)
                {
                    paramTypes.Add(name, paramType);
                }
            }
            else
            {
                throw new InvalidOperationException("parameter is not a SpannerParameter: " + param.ParameterName);
            }
        }
        return Tuple.Create(queryParams, paramTypes);
    }
    
    internal void CloneTo(SpannerParameterCollection other)
    {
        GaxPreconditions.CheckNotNull(other, nameof(other));
        other._params.Clear();
        foreach (var param in _params)
        {
            var newParam = param.Clone();
            other._params.Add(newParam);
        }
    }
    
}