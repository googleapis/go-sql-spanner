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

using System.Globalization;
using System.Xml;
using Google.Cloud.Spanner.V1;
using Google.Protobuf.WellKnownTypes;
using TypeCode = Google.Cloud.Spanner.V1.TypeCode;

namespace Google.Cloud.SpannerLib.MockServer;

public static class RandomResultSetGenerator
{
    public static StructType GenerateAllTypesRowType()
    {
        return new StructType
        {
            Fields = { new []
            {
                new StructType.Types.Field {Name = "col_bool", Type = new Spanner.V1.Type{Code = TypeCode.Bool}},
                new StructType.Types.Field {Name = "col_bytes", Type = new Spanner.V1.Type{Code = TypeCode.Bytes}},
                new StructType.Types.Field {Name = "col_date", Type = new Spanner.V1.Type{Code = TypeCode.Date}},
                new StructType.Types.Field {Name = "col_float32", Type = new Spanner.V1.Type{Code = TypeCode.Float32}},
                new StructType.Types.Field {Name = "col_float64", Type = new Spanner.V1.Type{Code = TypeCode.Float64}},
                new StructType.Types.Field {Name = "col_int64", Type = new Spanner.V1.Type{Code = TypeCode.Int64}},
                new StructType.Types.Field {Name = "col_interval", Type = new Spanner.V1.Type{Code = TypeCode.Interval}},
                new StructType.Types.Field {Name = "col_json", Type = new Spanner.V1.Type{Code = TypeCode.Json}},
                new StructType.Types.Field {Name = "col_numeric", Type = new Spanner.V1.Type{Code = TypeCode.Numeric}},
                new StructType.Types.Field {Name = "col_string", Type = new Spanner.V1.Type{Code = TypeCode.String}},
                new StructType.Types.Field {Name = "col_timestamp", Type = new Spanner.V1.Type{Code = TypeCode.Timestamp}},
                new StructType.Types.Field {Name = "col_uuid", Type = new Spanner.V1.Type{Code = TypeCode.Uuid}},
                
                new StructType.Types.Field {Name = "col_array_bool", Type = new Spanner.V1.Type{Code = TypeCode.Array, ArrayElementType = new Spanner.V1.Type{Code = TypeCode.Bool}}},
                new StructType.Types.Field {Name = "col_array_bytes", Type = new Spanner.V1.Type{Code = TypeCode.Array, ArrayElementType = new Spanner.V1.Type{Code = TypeCode.Bytes}}},
                new StructType.Types.Field {Name = "col_array_date", Type = new Spanner.V1.Type{Code = TypeCode.Array, ArrayElementType = new Spanner.V1.Type{Code = TypeCode.Date}}},
                new StructType.Types.Field {Name = "col_array_float32", Type = new Spanner.V1.Type{Code = TypeCode.Array, ArrayElementType = new Spanner.V1.Type{Code = TypeCode.Float32}}},
                new StructType.Types.Field {Name = "col_array_float64", Type = new Spanner.V1.Type{Code = TypeCode.Array, ArrayElementType = new Spanner.V1.Type{Code = TypeCode.Float64}}},
                new StructType.Types.Field {Name = "col_array_int64", Type = new Spanner.V1.Type{Code = TypeCode.Array, ArrayElementType = new Spanner.V1.Type{Code = TypeCode.Int64}}},
                new StructType.Types.Field {Name = "col_array_interval", Type = new Spanner.V1.Type{Code = TypeCode.Array, ArrayElementType = new Spanner.V1.Type{Code = TypeCode.Interval}}},
                new StructType.Types.Field {Name = "col_array_json", Type = new Spanner.V1.Type{Code = TypeCode.Array, ArrayElementType = new Spanner.V1.Type{Code = TypeCode.Json}}},
                new StructType.Types.Field {Name = "col_array_numeric", Type = new Spanner.V1.Type{Code = TypeCode.Array, ArrayElementType = new Spanner.V1.Type{Code = TypeCode.Numeric}}},
                new StructType.Types.Field {Name = "col_array_string", Type = new Spanner.V1.Type{Code = TypeCode.Array, ArrayElementType = new Spanner.V1.Type{Code = TypeCode.String}}},
                new StructType.Types.Field {Name = "col_array_timestamp", Type = new Spanner.V1.Type{Code = TypeCode.Array, ArrayElementType = new Spanner.V1.Type{Code = TypeCode.Timestamp}}},
                new StructType.Types.Field {Name = "col_array_uuid", Type = new Spanner.V1.Type{Code = TypeCode.Array, ArrayElementType = new Spanner.V1.Type{Code = TypeCode.Uuid}}},
            } }
        };
    }

    public static ResultSet Generate(int numRows, bool allowNull = false)
    {
        return Generate(GenerateAllTypesRowType(), numRows, allowNull);
    }
    
    public static ResultSet Generate(StructType rowType, int numRows, bool allowNull = false)
    {
        var result = new ResultSet
        {
            Metadata = new ResultSetMetadata
            {
                RowType = rowType,
            }
        };
        for (var i = 0; i < numRows; i++)
        {
            result.Rows.Add(GenerateRow(rowType, allowNull));
        }
        return result;
    }

    private static ListValue GenerateRow(StructType rowType, bool allowNull)
    {
        var row = new ListValue();
        foreach (var field in rowType.Fields)
        {
            row.Values.Add(GenerateValue(field.Type, allowNull));
        }
        return row;
    }

    private static Value GenerateValue(Spanner.V1.Type type, bool allowNull)
    {
        if (allowNull && Random.Shared.Next(10) == 5)
        {
            return Value.ForNull();
        }
        switch (type.Code)
        {
            case TypeCode.Bool:
                return Value.ForBool(Random.Shared.NextInt64() % 2 == 0);
            case TypeCode.Bytes:
                return Value.ForString(GenerateRandomBase64String());
            case TypeCode.Date:
                var year = Random.Shared.NextInt64(1600, 2100);
                var month = Random.Shared.NextInt64(1, 13);
                var day = Random.Shared.NextInt64(1, 29);
                return Value.ForString($"{year:D4}-{month:D2}-{day:D2}");
            case TypeCode.Float32:
                return Value.ForNumber(Random.Shared.NextSingle() * float.MaxValue);
            case TypeCode.Float64:
                return Value.ForNumber(Random.Shared.NextDouble() * double.MaxValue);
            case TypeCode.Int64:
                return Value.ForString(Random.Shared.NextInt64().ToString());
            case TypeCode.Interval:
                var timespan = TimeSpan.FromTicks(Random.Shared.NextInt64());
                return Value.ForString(XmlConvert.ToString(timespan));
            case TypeCode.Json:
                return Value.ForString("{\"key\":\"" + GenerateRandomBase64String() + "\"}");
            case TypeCode.Numeric:
                var n = Random.Shared.NextInt64(100000000) / 100;
                return Value.ForString(n.ToString());
            case TypeCode.String:
                return Value.ForString(GenerateRandomBase64String());
            case TypeCode.Timestamp:
                var ts = DateTime.Now.AddDays(Random.Shared.NextInt64(2 * 36500) - 36500);
                return Value.ForString(XmlConvert.ToString(Convert.ToDateTime(ts, CultureInfo.InvariantCulture),
                    XmlDateTimeSerializationMode.Utc));
            case TypeCode.Uuid:
                var uuid = Guid.NewGuid();
                return Value.ForString(uuid.ToString());
            case TypeCode.Array:
                var length = Random.Shared.NextInt64(10);
                var values = new Value[length];
                for (var i = 0; i < length; i++)
                {
                    values[i] = GenerateValue(type.ArrayElementType, allowNull);
                }
                return Value.ForList(values);
            default:
                throw new ArgumentOutOfRangeException(nameof(type), type, "unsupported type");
        }
    }

    private static string GenerateRandomBase64String()
    {
        var length = Random.Shared.NextInt64(16, 1024);
        var bytes = new byte[length];
        Random.Shared.NextBytes(bytes);
        return Convert.ToBase64String(bytes);
    }
}
