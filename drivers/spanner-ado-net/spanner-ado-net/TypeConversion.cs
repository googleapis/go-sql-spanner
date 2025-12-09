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
using TypeCode = Google.Cloud.Spanner.V1.TypeCode;

namespace Google.Cloud.Spanner.DataProvider;

internal static class TypeConversion
{
    private static readonly Dictionary<DbType, V1.Type> SDbTypeToSpannerTypeMapping = new ();

    static TypeConversion()
    {
        SDbTypeToSpannerTypeMapping[DbType.Date] = new V1.Type { Code = TypeCode.Date };
        SDbTypeToSpannerTypeMapping[DbType.Binary] = new V1.Type { Code = TypeCode.Bytes };
        SDbTypeToSpannerTypeMapping[DbType.Boolean] = new V1.Type { Code = TypeCode.Bool };
        SDbTypeToSpannerTypeMapping[DbType.Double] = new V1.Type { Code = TypeCode.Float64 };
        SDbTypeToSpannerTypeMapping[DbType.Single] = new V1.Type { Code = TypeCode.Float32 };
        SDbTypeToSpannerTypeMapping[DbType.Guid] = new V1.Type { Code = TypeCode.Uuid };

        var numericType = new V1.Type { Code = TypeCode.Numeric };
        SDbTypeToSpannerTypeMapping[DbType.Decimal] = numericType;
        SDbTypeToSpannerTypeMapping[DbType.VarNumeric] = numericType;

        var timestampType = new V1.Type { Code = TypeCode.Timestamp };
        SDbTypeToSpannerTypeMapping[DbType.DateTime] = timestampType;
        SDbTypeToSpannerTypeMapping[DbType.DateTime2] = timestampType;
        SDbTypeToSpannerTypeMapping[DbType.DateTimeOffset] = timestampType;
        
        var int64Type = new V1.Type { Code = TypeCode.Int64 };
        SDbTypeToSpannerTypeMapping[DbType.Byte] = int64Type;
        SDbTypeToSpannerTypeMapping[DbType.Int16] = int64Type;
        SDbTypeToSpannerTypeMapping[DbType.Int32] = int64Type;
        SDbTypeToSpannerTypeMapping[DbType.Int64] = int64Type;
        SDbTypeToSpannerTypeMapping[DbType.SByte] = int64Type;
        SDbTypeToSpannerTypeMapping[DbType.UInt16] = int64Type;
        SDbTypeToSpannerTypeMapping[DbType.UInt32] = int64Type;
        SDbTypeToSpannerTypeMapping[DbType.UInt64] = int64Type;
        
        var stringType = new V1.Type { Code = TypeCode.String };
        SDbTypeToSpannerTypeMapping[DbType.String] = stringType;
        SDbTypeToSpannerTypeMapping[DbType.StringFixedLength] = stringType;
        SDbTypeToSpannerTypeMapping[DbType.AnsiString] = stringType;
        SDbTypeToSpannerTypeMapping[DbType.AnsiStringFixedLength] = stringType;
    }

    internal static V1.Type? GetSpannerType(DbType? dbType)
    {
        return dbType == null ? null : SDbTypeToSpannerTypeMapping.GetValueOrDefault(dbType.Value);
    }

    internal static System.Type GetSystemType(V1.Type type) => GetSystemType(type.Code);
    
    internal static System.Type GetSystemType(TypeCode code)
    {
        return code switch
        {
            TypeCode.Bool => typeof(bool),
            TypeCode.Bytes => typeof(byte[]),
            TypeCode.Date => typeof(DateOnly),
            TypeCode.Enum => typeof(long),
            TypeCode.Float32 => typeof(float),
            TypeCode.Float64 => typeof(double),
            TypeCode.Int64 => typeof(long),
            TypeCode.Interval => typeof(TimeSpan),
            TypeCode.Json => typeof(string),
            TypeCode.Numeric => typeof(decimal),
            TypeCode.Proto => typeof(byte[]),
            TypeCode.String => typeof(string),
            TypeCode.Timestamp => typeof(DateTime),
            TypeCode.Uuid => typeof(Guid),
            _ => typeof(string)
        };
    }
}