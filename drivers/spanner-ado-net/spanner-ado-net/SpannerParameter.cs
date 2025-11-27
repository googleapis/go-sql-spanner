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
using System.Data;
using System.Data.Common;
using System.Diagnostics.CodeAnalysis;
using System.Globalization;
using System.IO;
using System.Text.Json;
using System.Text.RegularExpressions;
using System.Xml;
using Google.Api.Gax;
using Google.Cloud.Spanner.V1;
using Google.Protobuf.WellKnownTypes;
using TypeCode = Google.Cloud.Spanner.V1.TypeCode;

namespace Google.Cloud.Spanner.DataProvider;

public class SpannerParameter : DbParameter, IDbDataParameter, ICloneable
{
    private DbType? _dbType;

    public override DbType DbType
    {
        get => _dbType ?? DbType.String;
        set => _dbType = value;
    }
        
    /// <summary>
    /// SpannerParameterType overrides the standard DbType property with a specific Spanner type.
    /// Use this property if you need to set a specific Spanner type that is not supported by DbType, such as
    /// one of the Spanner array types.
    /// </summary>
    public V1.Type? SpannerParameterType { get; set; }

    public override ParameterDirection Direction { get; set; } = ParameterDirection.Input;
    public override bool IsNullable { get; set; }

    public new byte Precision { get; set; }

    public new byte Scale { get; set; }

    private string _name = "";
    [AllowNull] public override string ParameterName
    {
        get => _name;
        set => _name = value ?? "";
    }

    private string _sourceColumn = "";

    [AllowNull]
    public override string SourceColumn
    {
        get => _sourceColumn;
        set => _sourceColumn = value ?? "";
    }
    public sealed override object? Value { get; set; }
    public override bool SourceColumnNullMapping { get; set; }
    
    // TODO: Size should truncate the value to any explicit size that is set.
    public override int Size { get; set; }
    
    public override DataRowVersion SourceVersion
    {
        get => DataRowVersion.Current;
        set { }
    }
    
    public SpannerParameter() { }

    public SpannerParameter(string name, object? value)
    {
        GaxPreconditions.CheckNotNull(name, nameof(name));
        _name = name;
        Value = value;
    }

    public override void ResetDbType()
    {
        _dbType = null;
    }

    internal Value ConvertToProto(DbParameter dbParameter, bool prepare)
    {
        GaxPreconditions.CheckState(prepare || dbParameter.Direction != ParameterDirection.Input || Value != null, $"Parameter {ParameterName} has no value");
        return ConvertToProto(Value);
    }

    internal Google.Cloud.Spanner.V1.Type? GetSpannerType()
    {
        return SpannerParameterType ?? TypeConversion.GetSpannerType(_dbType);
    }

    private Value ConvertToProto(object? value)
    {
        var type = GetSpannerType();
        return ConvertToProto(value, type);
    }

    private static Value ConvertToProto(object? value, Google.Cloud.Spanner.V1.Type? type)
    {
        var proto = new Value();
        switch (value)
        {
            case null:
            case DBNull:
                proto.NullValue = NullValue.NullValue;
                break;
            case bool b:
                proto.BoolValue = b;
                break;
            case double d:
                proto.NumberValue = d;
                break;
            case float f:
                proto.NumberValue = f;
                break;
            case string str:
                proto.StringValue = str;
                break;
            case Regex regex:
                proto.StringValue = regex.ToString();
                break;
            case byte b:
                proto.StringValue = b.ToString();
                break;
            case byte[] bytes:
                proto.StringValue = Convert.ToBase64String(bytes);
                break;
            case MemoryStream memoryStream:
                // TODO: Optimize this
                proto.StringValue = Convert.ToBase64String(memoryStream.ToArray());
                break;
            case short s:
                proto.StringValue = s.ToString();
                break;
            case int i:
                proto.StringValue = i.ToString();
                break;
            case long l:
                proto.StringValue = l.ToString();
                break;
            case decimal d:
                proto.StringValue = d.ToString(CultureInfo.InvariantCulture);
                break;
            case SpannerNumeric num:
                proto.StringValue = num.ToString();
                break;
            case DateOnly d:
                proto.StringValue = d.ToString("O");
                break;
            case SpannerDate d:
                proto.StringValue = d.ToString();
                break;
            case DateTime d:
                // Some framework pass DATE values as DateTime.
                if (type?.Code == TypeCode.Date)
                {
                    proto.StringValue = d.Date.ToString("yyyy-MM-dd");
                }
                else
                {
                    proto.StringValue = d.ToUniversalTime().ToString("O");
                }
                break;
            case TimeSpan t:
                proto.StringValue = XmlConvert.ToString(t);
                break;
            case JsonDocument jd:
                proto.StringValue = jd.RootElement.ToString();
                break;
            case IEnumerable list:
                var elementType = type?.ArrayElementType;
                proto.ListValue = new ListValue();
                foreach (var item in list)
                {
                    proto.ListValue.Values.Add(ConvertToProto(item, elementType));
                }
                break;
            default:
                // Unknown type. Just try to send it as a string.
                proto.StringValue = value.ToString();
                break;
        }
        return proto;
    }
    
    object ICloneable.Clone() => Clone();

    public SpannerParameter Clone()
    {
        var clone = new SpannerParameter(_name, Value)
        {
            _dbType = _dbType,
            Direction = Direction,
            IsNullable = IsNullable,
            Precision = Precision,
            Scale = Scale,
            Size = Size,
            SourceColumn = SourceColumn,
            SourceVersion = SourceVersion,
            SourceColumnNullMapping = SourceColumnNullMapping,
        };
        return clone;
    }

}