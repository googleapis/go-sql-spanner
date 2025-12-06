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
using System.Globalization;
using AdoNet.Specification.Tests;

namespace Google.Cloud.Spanner.DataProvider.SpecificationTests;

public class GetValueConversionTests(DbFactoryFixture fixture) : GetValueConversionTestBase<DbFactoryFixture>(fixture)
{
    // Spanner uses DateOnly for DATE columns.
    public override void GetFieldType_for_Date() => TestGetFieldType(DbType.Date, ValueKind.One, typeof(DateOnly));
        
    public override void GetValue_for_Date() => TestGetValue(DbType.Date, ValueKind.One, new DateOnly(1111, 11, 11));

    
    // Spanner allows string values to be cast to numerical values.
    public override void GetDecimal_throws_for_zero_String() => TestGetValue(DbType.String, ValueKind.Zero, x => x.GetDecimal(0), 0.0m);

    public override void GetDecimal_throws_for_one_String() => TestGetValue(DbType.String, ValueKind.One, x => x.GetDecimal(0), 1.0m);
    
    public override void GetDouble_throws_for_zero_String() => TestGetValue(DbType.String, ValueKind.Zero, x => x.GetDouble(0), 0.0d);

    public override void GetDouble_throws_for_one_String() => TestGetValue(DbType.String, ValueKind.One, x => x.GetDouble(0), 1.0d);

    public override void GetDouble_throws_for_zero_String_with_GetFieldValue() => TestGetValue(DbType.String, ValueKind.Zero, x => x.GetFieldValue<double>(0), 0.0d);

    public override async Task GetDouble_throws_for_zero_String_with_GetFieldValueAsync() => await TestGetValueAsync(DbType.String, ValueKind.Zero, async x => await x.GetFieldValueAsync<double>(0), 0.0d);

    public override void GetFloat_throws_for_one_String() => TestGetValue(DbType.String, ValueKind.One, x => x.GetFloat(0), 1.0f);

    public override void GetFloat_throws_for_zero_String() => TestGetValue(DbType.String, ValueKind.Zero, x => x.GetFloat(0), 0.0f);
    
    public override void GetFloat_throws_for_zero_String_with_GetFieldValue() => TestGetValue(DbType.String, ValueKind.Zero, x => x.GetFieldValue<float>(0), 0.0f);

    public override async Task GetFloat_throws_for_zero_String_with_GetFieldValueAsync() => await TestGetValueAsync(DbType.String, ValueKind.Zero, async x => await x.GetFieldValueAsync<float>(0), 0.0f);

    public override void GetInt16_throws_for_one_String() => TestGetValue(DbType.String, ValueKind.One, x => x.GetInt16(0), (short) 1);

    public override void GetInt16_throws_for_zero_String() => TestGetValue(DbType.String, ValueKind.Zero, x => x.GetInt16(0), (short) 0);

    public override void GetInt16_throws_for_zero_String_with_GetFieldValue() => TestGetValue(DbType.String, ValueKind.Zero, x => x.GetFieldValue<short>(0), (short) 0);

    public override async Task GetInt16_throws_for_zero_String_with_GetFieldValueAsync() => await TestGetValueAsync(DbType.String, ValueKind.Zero, async x => await x.GetFieldValueAsync<short>(0), (short) 0);

    public override void GetInt32_throws_for_one_String() => TestGetValue(DbType.String, ValueKind.One, x => x.GetInt32(0), 1);

    public override void GetInt32_throws_for_zero_String() => TestGetValue(DbType.String, ValueKind.Zero, x => x.GetInt32(0), 0);

    public override void GetInt32_throws_for_zero_String_with_GetFieldValue() => TestGetValue(DbType.String, ValueKind.Zero, x => x.GetFieldValue<int>(0), 0);

    public override async Task GetInt32_throws_for_zero_String_with_GetFieldValueAsync() => await TestGetValueAsync(DbType.String, ValueKind.Zero, async x => await x.GetFieldValueAsync<int>(0), 0);

    public override void GetInt64_throws_for_one_String() => TestGetValue(DbType.String, ValueKind.One, x => x.GetInt64(0), 1L);

    public override void GetInt64_throws_for_zero_String() => TestGetValue(DbType.String, ValueKind.Zero, x => x.GetInt64(0), 0L);

    public override void GetInt64_throws_for_zero_String_with_GetFieldValue() => TestGetValue(DbType.String, ValueKind.Zero, x => x.GetFieldValue<long>(0), 0L);

    public override async Task GetInt64_throws_for_zero_String_with_GetFieldValueAsync() => await TestGetValueAsync(DbType.String, ValueKind.Zero, async x => await x.GetFieldValueAsync<long>(0), 0L);

    public override void GetString_throws_for_maximum_Boolean() => TestGetValue(DbType.Boolean, ValueKind.Maximum, x => x.GetString(0), "True");

    public override void GetString_throws_for_maximum_Decimal() => TestGetValue(DbType.Decimal, ValueKind.Maximum, x => x.GetString(0), "99999999999999999999.999999999999999");

    public override void GetString_throws_for_maximum_Double() => TestGetValue(DbType.Double, ValueKind.Maximum, x => x.GetString(0), "1.79E+308");

    public override void GetString_throws_for_maximum_Int64() => TestGetValue(DbType.Int64, ValueKind.Maximum, x => x.GetString(0), long.MaxValue.ToString(CultureInfo.InvariantCulture));

    public override void GetString_throws_for_maximum_Single() => TestGetValue(DbType.Single, ValueKind.Maximum, x => x.GetString(0), 3.40e38f.ToString(CultureInfo.InvariantCulture));

    public override void GetString_throws_for_minimum_Boolean() => TestGetValue(DbType.Boolean, ValueKind.Minimum, x => x.GetString(0), "False");
    
    public override void GetString_throws_for_minimum_Decimal() => TestGetValue(DbType.Decimal, ValueKind.Minimum, x => x.GetString(0), "0.000000000000001");

    public override void GetString_throws_for_minimum_Double() => TestGetValue(DbType.Double, ValueKind.Minimum, x => x.GetString(0), "2.23E-308");

    public override void GetString_throws_for_minimum_Int64() => TestGetValue(DbType.Int64, ValueKind.Minimum, x => x.GetString(0), long.MinValue.ToString(CultureInfo.InvariantCulture));

    public override void GetString_throws_for_minimum_Single() => TestGetValue(DbType.Single, ValueKind.Minimum, x => x.GetString(0), "1.18E-38");

    public override void GetString_throws_for_one_Boolean() => TestGetValue(DbType.Boolean, ValueKind.One, x => x.GetString(0), "True");

    public override void GetString_throws_for_one_Decimal() => TestGetValue(DbType.Decimal, ValueKind.One, x => x.GetString(0), "1");

    public override void GetString_throws_for_one_Double() => TestGetValue(DbType.Double, ValueKind.One, x => x.GetString(0), "1");

    public override void GetString_throws_for_one_Guid() => TestGetValue(DbType.Guid, ValueKind.One, x => x.GetString(0), "11111111-1111-1111-1111-111111111111");

    public override void GetString_throws_for_one_Int64() => TestGetValue(DbType.Int64, ValueKind.One, x => x.GetString(0), "1");

    public override void GetString_throws_for_one_Single() => TestGetValue(DbType.Single, ValueKind.One, x => x.GetString(0), "1");

    public override void GetString_throws_for_zero_Boolean() => TestGetValue(DbType.Boolean, ValueKind.Zero, x => x.GetString(0), "False");

    public override void GetString_throws_for_zero_Decimal() => TestGetValue(DbType.Decimal, ValueKind.Zero, x => x.GetString(0), "0");

    public override void GetString_throws_for_zero_Double() => TestGetValue(DbType.Double, ValueKind.Zero, x => x.GetString(0), "0");
    
    public override void GetString_throws_for_zero_Guid() => TestGetValue(DbType.Guid, ValueKind.Zero, x => x.GetString(0), "00000000-0000-0000-0000-000000000000");

    public override void GetString_throws_for_zero_Int64() => TestGetValue(DbType.Int64, ValueKind.Zero, x => x.GetString(0), "0");

    public override void GetString_throws_for_zero_Single() => TestGetValue(DbType.Single, ValueKind.Zero, x => x.GetString(0), "0");

    public override void GetString_throws_for_null_String_with_GetFieldValue() => TestGetValue(DbType.String, ValueKind.Null, x => x.GetFieldValue<string>(0), null);
    
    public override async Task GetString_throws_for_null_String_with_GetFieldValueAsync() => await TestGetValueAsync(DbType.String, ValueKind.Null, async x => await x.GetFieldValueAsync<string>(0), null);

    public override void GetDouble_throws_for_one_String_with_GetFieldValue() => TestGetValue(DbType.String, ValueKind.One, x => x.GetFieldValue<double>(0), 1.0d);

    public override async Task GetDouble_throws_for_one_String_with_GetFieldValueAsync() => await TestGetValueAsync(DbType.String, ValueKind.One, async x => await x.GetFieldValueAsync<double>(0), 1.0d);

    public override void GetFloat_throws_for_one_String_with_GetFieldValue() => TestGetValue(DbType.String, ValueKind.One, x => x.GetFieldValue<float>(0), 1.0f);

    public override async Task GetFloat_throws_for_one_String_with_GetFieldValueAsync() => await TestGetValueAsync(DbType.String, ValueKind.One, async x => await x.GetFieldValueAsync<float>(0), 1.0f);

    public override void GetInt16_throws_for_one_String_with_GetFieldValue() => TestGetValue(DbType.String, ValueKind.One, x => x.GetFieldValue<short>(0), (short) 1);

    public override async Task GetInt16_throws_for_one_String_with_GetFieldValueAsync() => await TestGetValueAsync(DbType.String, ValueKind.One, async x => await x.GetFieldValueAsync<short>(0), (short) 1);

    public override void GetInt32_throws_for_one_String_with_GetFieldValue() => TestGetValue(DbType.String, ValueKind.One, x => x.GetFieldValue<int>(0), 1);

    public override async Task GetInt32_throws_for_one_String_with_GetFieldValueAsync() => await TestGetValueAsync(DbType.String, ValueKind.One, async x => await x.GetFieldValueAsync<int>(0), 1);

    public override void GetInt64_throws_for_one_String_with_GetFieldValue() => TestGetValue(DbType.String, ValueKind.One, x => x.GetFieldValue<long>(0), 1L);

    public override async Task GetInt64_throws_for_one_String_with_GetFieldValueAsync() => await TestGetValueAsync(DbType.String, ValueKind.One, async x => await x.GetFieldValueAsync<long>(0), 1L);

    // GetByte throws OverflowException instead of InvalidCastException if the value is out of range.
    public override void GetByte_throws_for_maximum_Decimal() => TestException(DbType.Decimal, ValueKind.Maximum, x => x.GetByte(0), typeof(OverflowException));

    public override void GetByte_throws_for_maximum_Double() => TestException(DbType.Double, ValueKind.Maximum, x => x.GetByte(0), typeof(OverflowException));
    
    public override void GetByte_throws_for_maximum_Int64() => TestException(DbType.Int64, ValueKind.Maximum, x => x.GetByte(0), typeof(OverflowException));
    
    public override void GetByte_throws_for_maximum_Single() => TestException(DbType.Single, ValueKind.Maximum, x => x.GetByte(0), typeof(OverflowException));
    
    public override void GetByte_throws_for_minimum_Decimal() => TestException(DbType.Decimal, ValueKind.Minimum, x => x.GetByte(0), typeof(OverflowException));
    
    public override void GetByte_throws_for_minimum_Double() => TestGetValue(DbType.Double, ValueKind.Minimum, x => x.GetByte(0), (byte)0);
    
    public override void GetByte_throws_for_minimum_Int64() => TestException(DbType.Int64, ValueKind.Minimum, x => x.GetByte(0), typeof(OverflowException));
    
    public override void GetByte_throws_for_minimum_Single() => TestGetValue(DbType.Single, ValueKind.Minimum, x => x.GetByte(0), (byte)0);
    
    public override void GetByte_throws_for_one_Decimal() => TestGetValue(DbType.Decimal, ValueKind.One, x => x.GetByte(0), (byte)1);
    
    public override void GetByte_throws_for_one_Double() => TestGetValue(DbType.Double, ValueKind.One, x => x.GetByte(0), (byte)1);

    public override void GetByte_throws_for_one_Int64() => TestGetValue(DbType.Int64, ValueKind.One, x => x.GetByte(0), (byte)1);

    public override void GetByte_throws_for_one_Single() => TestGetValue(DbType.Single, ValueKind.One, x => x.GetByte(0), (byte)1);
    
    public override void GetByte_throws_for_one_String() => TestGetValue(DbType.String, ValueKind.One, x => x.GetByte(0), (byte)1);

    public override void GetByte_throws_for_zero_Decimal() => TestGetValue(DbType.Decimal, ValueKind.Zero, x => x.GetByte(0), (byte)0);

    public override void GetByte_throws_for_zero_Double() => TestGetValue(DbType.Double, ValueKind.Zero, x => x.GetByte(0), (byte)0);
    
    public override void GetByte_throws_for_zero_Int64() => TestGetValue(DbType.Int64, ValueKind.Zero, x => x.GetByte(0), (byte)0);
    
    public override void GetByte_throws_for_zero_Single() => TestGetValue(DbType.Single, ValueKind.Zero, x => x.GetByte(0), (byte)0);
    
    public override void GetByte_throws_for_zero_String() => TestGetValue(DbType.String, ValueKind.Zero, x => x.GetByte(0), (byte)0);

    // Spanner allows calling GetFloat for float64 values.
    public override void GetFloat_throws_for_maximum_Double() => TestGetValue(DbType.Double, ValueKind.Maximum, x => x.GetFloat(0), float.PositiveInfinity);
    
    public override void GetFloat_throws_for_maximum_Double_with_GetFieldValue() => TestGetValue(DbType.Double, ValueKind.Maximum, x => x.GetFieldValue<float>(0), float.PositiveInfinity);
    
    public override async Task GetFloat_throws_for_maximum_Double_with_GetFieldValueAsync() => await TestGetValueAsync(DbType.Double, ValueKind.Maximum, async x => await x.GetFieldValueAsync<float>(0), float.PositiveInfinity);
    
    public override void GetFloat_throws_for_minimum_Double() => TestGetValue(DbType.Double, ValueKind.Minimum, x => x.GetFloat(0), 0.0f);
    
    public override void GetFloat_throws_for_minimum_Double_with_GetFieldValue() => TestGetValue(DbType.Double, ValueKind.Minimum, x => x.GetFieldValue<float>(0), 0.0f);
    
    public override async Task GetFloat_throws_for_minimum_Double_with_GetFieldValueAsync() => await TestGetValueAsync(DbType.Double, ValueKind.Minimum, async x => await x.GetFieldValueAsync<float>(0), 0.0f);

    public override void GetFloat_throws_for_one_Double() => TestGetValue(DbType.Double, ValueKind.One, x => x.GetFloat(0), 1.0f);

    public override void GetFloat_throws_for_one_Double_with_GetFieldValue() => TestGetValue(DbType.Double, ValueKind.One, x => x.GetFieldValue<float>(0), 1.0f);

    public override async Task GetFloat_throws_for_one_Double_with_GetFieldValueAsync() => await TestGetValueAsync(DbType.Double, ValueKind.One, async x => await x.GetFieldValueAsync<float>(0), 1.0f);

    public override void GetFloat_throws_for_zero_Double() => TestGetValue(DbType.Double, ValueKind.Zero, x => x.GetFloat(0), 0.0f);

    public override void GetFloat_throws_for_zero_Double_with_GetFieldValue() => TestGetValue(DbType.Double, ValueKind.Zero, x => x.GetFieldValue<float>(0), 0.0f);

    public override async Task GetFloat_throws_for_zero_Double_with_GetFieldValueAsync() => await TestGetValueAsync(DbType.Double, ValueKind.Zero, async x => await x.GetFieldValueAsync<float>(0), 0.0f);

}
