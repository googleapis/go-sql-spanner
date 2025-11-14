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

using System.Collections;
using System.Xml;
using Google.Cloud.Spanner.V1;
using Google.Protobuf.WellKnownTypes;
using static System.Globalization.CultureInfo;
using TypeCode = Google.Cloud.Spanner.V1.TypeCode;

namespace Google.Cloud.SpannerLib.MockServer;

internal static class SpannerConverter
{
    internal static Value ToProtobufValue(Spanner.V1.Type type, object? value)
    {
        if (value == null || value is DBNull)
        {
            return Value.ForNull();
        }

        switch (type.Code)
        {
            case TypeCode.Bytes:
                if (value is string s)
                {
                    return new Value { StringValue = s };
                }
                if (value is byte[] bArray)
                {
                    return new Value { StringValue = Convert.ToBase64String(bArray) };
                }
                throw new ArgumentException("TypeCode.Bytes only supports string and byte[]", nameof(value));
            case TypeCode.Bool:
                return new Value { BoolValue = Convert.ToBoolean(value) };
            case TypeCode.String:
                if (value is DateTime dateTime)
                {
                    // If the value is a DateTime, we always convert using XmlConvert.
                    // This allows us to convert back to a datetime reliably from the
                    // resulting string (so roundtrip works properly if the developer uses
                    // a string as a backing field for a datetime for whatever reason).
                    return new Value { StringValue = XmlConvert.ToString(dateTime, XmlDateTimeSerializationMode.Utc) };
                }
                return new Value { StringValue = Convert.ToString(value, InvariantCulture) };
            case TypeCode.Int64:
                return new Value
                {
                    StringValue = Convert.ToInt64(value, InvariantCulture)
                        .ToString(InvariantCulture)
                };
            case TypeCode.Float32:
                if (value is float float32)
                {
                    return new Value { NumberValue = float32 };
                }
                return new Value { NumberValue = Convert.ToSingle(value, InvariantCulture) };
            case TypeCode.Float64:
                return new Value { NumberValue = Convert.ToDouble(value, InvariantCulture) };
            case TypeCode.Timestamp:
                if (value is string value1)
                {
                    return Value.ForString(value1);
                }
                return new Value
                {
                    StringValue = XmlConvert.ToString(Convert.ToDateTime(value, InvariantCulture), XmlDateTimeSerializationMode.Utc)
                };
            case TypeCode.Date:
                if (value is DateOnly dateOnly)
                {
                    return new Value
                    {
                        StringValue = dateOnly.ToString("yyyy-MM-dd"),
                    };
                }
                return new Value
                {
                    StringValue = StripTimePart(
                        XmlConvert.ToString(Convert.ToDateTime(value, InvariantCulture), XmlDateTimeSerializationMode.Utc))
                };
            case TypeCode.Interval:
                if (value is TimeSpan timeSpan)
                {
                    return Value.ForString(XmlConvert.ToString(timeSpan));
                }
                return Value.ForString(value.ToString());
            case TypeCode.Json:
                if (value is string stringValue)
                {
                    return new Value { StringValue = stringValue };
                }
                throw new ArgumentException("JSON values must be given as string");
            case TypeCode.Array:
                if (value is IEnumerable enumerable)
                {
                    return Value.ForList(
                        enumerable.Cast<object>()
                            .Select(x => ToProtobufValue(type.ArrayElementType, x)).ToArray());
                }
                throw new ArgumentException("The given array instance needs to implement IEnumerable.");

            case TypeCode.Struct:
                throw new ArgumentException("Struct values are not supported");

            case TypeCode.Numeric:
                if (value is SpannerNumeric spannerNumeric)
                {
                    return Value.ForString(spannerNumeric.ToString());
                }
                if (value is string str)
                {
                    return Value.ForString(str);
                }
                if (value is float || value is double || value is decimal)
                {
                    // We throw if there's a loss of precision. We could use
                    // LossOfPrecisionHandling.Truncate but GoogleSQL documentation requests to
                    // use half-away-from-zero rounding but the SpannerNumeric implementation
                    // truncates instead.
                    return Value.ForString(SpannerNumeric.FromDecimal(
                        Convert.ToDecimal(value, InvariantCulture), LossOfPrecisionHandling.Throw).ToString());
                }
                if (value is sbyte || value is short || value is int || value is long)
                {
                    SpannerNumeric numericValue = Convert.ToInt64(value, InvariantCulture);
                    return Value.ForString(numericValue.ToString());
                }
                if (value is byte || value is ushort || value is uint || value is ulong)
                {
                    SpannerNumeric numericValue = Convert.ToUInt64(value, InvariantCulture);
                    return Value.ForString(numericValue.ToString());
                }
                throw new ArgumentException("Numeric parameters must be of type SpannerNumeric or string");
            case TypeCode.Uuid:
                return Value.ForString(value.ToString());

            default:
                throw new ArgumentOutOfRangeException(nameof(type.Code), type.Code, null);
        }
    }

    private static string StripTimePart(string rfc3339String)
    {
        if (!string.IsNullOrEmpty(rfc3339String))
        {
            int timeIndex = rfc3339String.IndexOf('T');
            if (timeIndex != -1)
            {
                return rfc3339String.Substring(0, timeIndex);
            }
        }
        return rfc3339String;
    }
}