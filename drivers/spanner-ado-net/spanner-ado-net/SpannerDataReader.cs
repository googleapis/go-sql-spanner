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
using System.Globalization;
using System.IO;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Xml;
using Google.Api.Gax;
using Google.Cloud.Spanner.V1;
using Google.Cloud.SpannerLib;
using Google.Protobuf.WellKnownTypes;
using TypeCode = Google.Cloud.Spanner.V1.TypeCode;

namespace Google.Cloud.Spanner.DataProvider;

public class SpannerDataReader : DbDataReader
{
    private readonly SpannerConnection _connection;
    private readonly CommandBehavior _commandBehavior;
    private bool IsSingleRow => _commandBehavior.HasFlag(CommandBehavior.SingleRow);
    private bool IsSingleResult => _commandBehavior.HasFlag(CommandBehavior.SingleResult);
    private Rows LibRows { get; }
    private bool _closed;
    private bool _hasReadData;
    private bool _hasData;
    
    private ResultSetMetadata? _metadata;

    private ResultSetMetadata? Metadata
    {
        get
        {
            if (_metadata == null)
            {
                CheckNotClosed();
                _metadata = LibRows.Metadata;
            }
            return _metadata;
        }
    }

    public override int FieldCount
    {
        get
        {
            CheckNotClosed();
            return Metadata?.RowType.Fields.Count ?? 0;
        }
    }

    public override object this[int ordinal] => GetFieldValue<object>(ordinal);
    public override object this[string name] => this[GetOrdinal(name)];

    private long? _stats;

    private long? Stats
    {
        get
        {
            if (_stats == null)
            {
                CheckNotClosed();
                _stats = LibRows.UpdateCount;
            }
            return _stats;
        }
    }

    public override int RecordsAffected => (int) (Stats ?? 0);

    public override bool HasRows
    {
        get
        {
            CheckNotClosed();
            if (Metadata?.RowType.Fields.Count == 0)
            {
                return false;
            }
            if (_hasReadData)
            {
                return _hasData;
            }
            return CheckForRows();
        }
    }
    public override bool IsClosed => _closed;
    public override int Depth => 0;

    private ListValue? _currentRow;
    private ListValue? _tempRow;

    internal SpannerDataReader(SpannerConnection connection, Rows libRows, CommandBehavior behavior)
    {
        _connection = connection;
        LibRows = libRows;
        _commandBehavior = behavior;
    }

    private void CheckNotClosed()
    {
        GaxPreconditions.CheckState(!_closed, "Reader has been closed");
    }

    public override void Close()
    {
        if (_closed)
        {
            return;
        }

        _closed = true;
        LibRows.Close();
        if (_commandBehavior.HasFlag(CommandBehavior.CloseConnection))
        {
            _connection.Close();
        }
    }

    public override bool Read()
    {
        if (!InternalRead())
        {
            _hasReadData = true;
            _currentRow = LibRows.Next();
        }
        _hasData = _hasData || _currentRow != null;
        return _currentRow != null;
    }

    public override async Task<bool> ReadAsync(CancellationToken cancellationToken)
    {
        try
        {
            if (!InternalRead())
            {
                _hasReadData = true;
                _currentRow = await LibRows.NextAsync(cancellationToken).ConfigureAwait(false);
            }
            _hasData = _hasData || _currentRow != null;
            return _currentRow != null;
        }
        catch (SpannerException exception)
        {
            throw SpannerDbException.TranslateException(exception);
        }
    }

    private bool InternalRead()
    {
        CheckNotClosed();
        if (_tempRow != null)
        {
            _currentRow = _tempRow;
            _tempRow = null;
            _hasReadData = true;
            return true;
        }
        if (IsSingleRow && _hasReadData)
        {
            _currentRow = null;
            return true;
        }
        return false;
    }

    private bool CheckForRows()
    {
        _tempRow ??= LibRows.Next();
        return _tempRow != null;
    }

    public override DataTable? GetSchemaTable()
    {
        CheckNotClosed();
        var metadata = Metadata;
        if (metadata?.RowType == null || metadata.RowType.Fields.Count == 0)
        {
            return null;
        }
        var table = new DataTable("SchemaTable");

        table.Columns.Add("ColumnName", typeof(string));
        table.Columns.Add("ColumnOrdinal", typeof(int));
        table.Columns.Add("ColumnSize", typeof(int));
        table.Columns.Add("NumericPrecision", typeof(int));
        table.Columns.Add("NumericScale", typeof(int));
        table.Columns.Add("IsUnique", typeof(bool));
        table.Columns.Add("IsKey", typeof(bool));
        table.Columns.Add("BaseServerName", typeof(string));
        table.Columns.Add("BaseCatalogName", typeof(string));
        table.Columns.Add("BaseColumnName", typeof(string));
        table.Columns.Add("BaseSchemaName", typeof(string));
        table.Columns.Add("BaseTableName", typeof(string));
        table.Columns.Add("DataType", typeof(System.Type));
        table.Columns.Add("AllowDBNull", typeof(bool));
        table.Columns.Add("ProviderType", typeof(int));
        table.Columns.Add("IsAliased", typeof(bool));
        table.Columns.Add("IsExpression", typeof(bool));
        table.Columns.Add("IsIdentity", typeof(bool));
        table.Columns.Add("IsAutoIncrement", typeof(bool));
        table.Columns.Add("IsRowVersion", typeof(bool));
        table.Columns.Add("IsHidden", typeof(bool));
        table.Columns.Add("IsLong", typeof(bool));
        table.Columns.Add("IsReadOnly", typeof(bool));
        table.Columns.Add("ProviderSpecificDataType", typeof(System.Type));
        table.Columns.Add("DataTypeName", typeof(string));

        var ordinal = 0;
        foreach (var column in metadata.RowType.Fields)
        {
            ordinal++;
            var row = table.NewRow();
            row["ColumnName"] = column.Name;
            row["ColumnOrdinal"] = ordinal;
            row["ColumnSize"] = -1;
            row["NumericPrecision"] = 0;
            row["NumericScale"] = 0;
            row["IsUnique"] = false;
            row["IsKey"] = false;
            row["BaseServerName"] = "";
            row["BaseCatalogName"] = "";
            row["BaseColumnName"] = "";
            row["BaseSchemaName"] = "";
            row["BaseTableName"] = "";
            row["DataType"] = TypeConversion.GetSystemType(column.Type);
            row["AllowDBNull"] = true;
            row["ProviderType"] = (int)column.Type.Code;
            row["IsAliased"] = false;
            row["IsExpression"] = false;
            row["IsIdentity"] = false;
            row["IsAutoIncrement"] = false;
            row["IsRowVersion"] = false;
            row["IsHidden"] = false;
            row["IsLong"] = false;
            row["IsReadOnly"] = false;
            row["DataTypeName"] = column.Type.Code.ToString();

            table.Rows.Add(row);
        }
        return table;
    }

    public override string GetString(int ordinal)
    {
        var value = GetProtoValue(ordinal);
        CheckNotNull(ordinal);
        if (value.HasStringValue)
        {
            return value.StringValue;
        }
        if (value.HasNumberValue)
        {
            var type = GetSpannerType(ordinal);
            if (type.Code == TypeCode.Float32)
            {
                return ((float) value.NumberValue).ToString(CultureInfo.InvariantCulture);
            }
            return value.NumberValue.ToString(CultureInfo.InvariantCulture);
        }
        if (value.HasBoolValue)
        {
            return value.BoolValue.ToString();
        }
        throw new InvalidCastException("not a valid string value");
    }

    public override bool GetBoolean(int ordinal)
    {
        var value = GetProtoValue(ordinal);
        CheckNotNull(ordinal);
        if (value.HasStringValue)
        {
            try
            {
                return bool.Parse(value.StringValue);
            }
            catch (Exception exception)
            {
                throw new InvalidCastException(exception.Message, exception);
            }
        }
        if (value.HasBoolValue)
        {
            return value.BoolValue;
        }
        throw new InvalidCastException("not a valid bool value");
    }

    public override byte GetByte(int ordinal)
    {
        var value = GetProtoValue(ordinal);
        CheckNotNull(ordinal);
        if (value.HasStringValue)
        {
            try
            {
                return byte.Parse(value.StringValue,
                    NumberStyles.AllowDecimalPoint | NumberStyles.AllowLeadingSign | NumberStyles.AllowExponent,
                    CultureInfo.InvariantCulture);
            }
            catch (OverflowException)
            {
                throw;
            }
            catch (Exception exception)
            {
                throw new InvalidCastException(exception.Message, exception);
            }
        }
        if (value.HasNumberValue)
        {
            return checked((byte)value.NumberValue);
        }
        throw new InvalidCastException("not a valid byte value");
    }

    public override long GetBytes(int ordinal, long dataOffset, byte[]? buffer, int bufferOffset, int length)
    {
        CheckValidPosition();
        CheckValidOrdinal(ordinal);
        CheckNotNull(ordinal);
        var code = Metadata!.RowType.Fields[ordinal].Type.Code;
        GaxPreconditions.CheckState(Array.Exists([TypeCode.Bytes, TypeCode.Json, TypeCode.String], c => c == code),
            "Spanner only supports conversion to byte arrays for columns of type BYTES or STRING.");
        Preconditions.CheckIndexRange(bufferOffset, nameof(bufferOffset), 0, buffer?.Length ?? 0);
        Preconditions.CheckIndexRange(length, nameof(length), 0, buffer?.Length ?? int.MaxValue);
        if (buffer != null)
        {
            Preconditions.CheckIndexRange(bufferOffset + length, nameof(length), 0, buffer.Length);
        }

        byte[] bytes;
        if (code == TypeCode.Bytes)
        {
            bytes = GetFieldValue<byte[]>(ordinal);
        }
        else
        {
            var s = GetFieldValue<string>(ordinal);
            bytes = Encoding.UTF8.GetBytes(s);
        }
        if (buffer == null)
        {
            // Return the length of the value if `buffer` is null:
            // https://docs.microsoft.com/en-us/dotnet/api/system.data.idatarecord.getbytes?view=netstandard-2.1#remarks
            return bytes.Length;
        }

        var copyLength = Math.Min(length, bytes.Length - (int)dataOffset);
        if (copyLength < 0)
        {
            // Read nothing and just return.
            return 0;
        }
        
        Array.Copy(bytes, (int)dataOffset, buffer, bufferOffset, copyLength);
        return copyLength;
    }

    public override char GetChar(int ordinal)
    {
        var value = GetProtoValue(ordinal);
        CheckNotNull(ordinal);
        var type = GetSpannerType(ordinal);
        if (type.Code != TypeCode.String)
        {
            throw new InvalidCastException("not a valid char value");
        }
        if (value.HasStringValue)
        {
            if (value.StringValue.Length == 0)
            {
                throw new InvalidCastException("not a valid char value");
            }
            return value.StringValue[0];
        }
        throw new InvalidCastException("not a valid char value");
    }

    public override long GetChars(int ordinal, long dataOffset, char[]? buffer, int bufferOffset, int length)
    {
        var value = GetProtoValue(ordinal);
        var code = GetSpannerType(ordinal).Code;
        if (!Array.Exists([TypeCode.Bytes, TypeCode.Json, TypeCode.String], c => c == code))
        {
            throw new InvalidCastException("not a valid type for getting as chars");
        }
        if (value.HasNullValue)
        {
            return 0;
        }
        if (buffer == null)
        {
            // Return the length of the value if `buffer` is null:
            // https://docs.microsoft.com/en-us/dotnet/api/system.data.idatarecord.getbytes?view=netstandard-2.1#remarks
            return value.StringValue.ToCharArray().Length;
        }
        Preconditions.CheckIndexRange(bufferOffset, nameof(bufferOffset), 0, buffer.Length);
        Preconditions.CheckIndexRange(length, nameof(length), 0, buffer.Length - bufferOffset);

        var intDataOffset = (int)dataOffset;
        var sourceLength = Math.Min(length, value.StringValue.Length - intDataOffset);
        var destLength = Math.Min(length, buffer.Length - bufferOffset);
        destLength = Math.Min(destLength, sourceLength);

        if (destLength <= 0)
        {
            return 0;
        }
        if (bufferOffset + destLength > buffer.Length)
        {
            return 0;
        }
            
        var chars = value.StringValue.ToCharArray();
        if (intDataOffset >= chars.Length)
        {
            return 0;
        }
            
        Array.Copy(chars, dataOffset, buffer, bufferOffset, destLength);
            
        return destLength;
    }

    public override string GetDataTypeName(int ordinal)
    {
        CheckValidOrdinal(ordinal);
        return GetTypeName(Metadata!.RowType.Fields[ordinal].Type);
    }

    private static string GetTypeName(Google.Cloud.Spanner.V1.Type type)
    {
        if (type.Code == TypeCode.Array)
        {
            return type.Code.GetOriginalName() + "<" + type.ArrayElementType.Code.GetOriginalName() + ">";
        }
        return type.Code.GetOriginalName();
    }

    public override DateTime GetDateTime(int ordinal)
    {
        var value = GetProtoValue(ordinal);
        CheckNotNull(ordinal);
        var type = GetSpannerType(ordinal);
        if (type.Code == TypeCode.Date)
        {
            var date = DateOnly.Parse(value.StringValue);
            return date.ToDateTime(TimeOnly.MinValue);
        }
        if (value.HasStringValue)
        {
            try
            {
                return XmlConvert.ToDateTime(value.StringValue, XmlDateTimeSerializationMode.Utc);
            }
            catch (Exception exception)
            {
                throw new InvalidCastException(exception.Message, exception);
            }
        }
        throw new InvalidCastException("not a valid DateTime value");
    }

    public override decimal GetDecimal(int ordinal)
    {
        var value = GetProtoValue(ordinal);
        CheckNotNull(ordinal);
        if (value.HasStringValue)
        {
            try
            {
                return decimal.Parse(value.StringValue, NumberStyles.AllowDecimalPoint | NumberStyles.AllowLeadingSign | NumberStyles.AllowExponent, CultureInfo.InvariantCulture);
            }
            catch (Exception exception)
            {
                throw new InvalidCastException(exception.Message, exception);
            }
        }
        throw new InvalidCastException("not a valid decimal value");
    }

    public override double GetDouble(int ordinal)
    {
        var value = GetProtoValue(ordinal);
        CheckNotNull(ordinal);
        if (value.HasStringValue)
        {
            try
            {
                return double.Parse(value.StringValue,
                    NumberStyles.AllowDecimalPoint | NumberStyles.AllowLeadingSign | NumberStyles.AllowExponent,
                    CultureInfo.InvariantCulture);
            }
            catch (Exception exception)
            {
                throw new InvalidCastException(exception.Message, exception);
            }
        }
        if (value.HasNumberValue)
        {
            return value.NumberValue;
        }
        throw new InvalidCastException("not a valid double value");
    }

    public override System.Type GetFieldType(int ordinal)
    {
        CheckValidOrdinal(ordinal);
        return GetClrType(Metadata!.RowType.Fields[ordinal].Type);
    }

    private static System.Type GetNullableClrType(Google.Cloud.Spanner.V1.Type type)
    {
        var clr = GetClrType(type);
        if (clr.IsValueType)
        {
            return typeof(Nullable<>).MakeGenericType(clr);
        }
        return clr;
    }

    private static System.Type GetClrType(Google.Cloud.Spanner.V1.Type type)
    {
        return type.Code switch
        {
            TypeCode.Array => typeof(List<>).MakeGenericType(GetNullableClrType(type.ArrayElementType)),
            TypeCode.Bool => typeof(bool),
            TypeCode.Bytes => typeof(byte[]),
            TypeCode.Date => typeof(DateOnly),
            TypeCode.Enum => typeof(int),
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
            _ => typeof(Value)
        };
    }

    public override float GetFloat(int ordinal)
    {
        var value = GetProtoValue(ordinal);
        CheckNotNull(ordinal);
        if (value.HasStringValue)
        {
            try
            {
                return float.Parse(value.StringValue,
                    NumberStyles.AllowDecimalPoint | NumberStyles.AllowLeadingSign | NumberStyles.AllowExponent,
                    CultureInfo.InvariantCulture);
            }
            catch (Exception exception)
            {
                throw new InvalidCastException(exception.Message, exception);
            }
        }
        if (value.HasNumberValue)
        {
            return (float)value.NumberValue;
        }
        throw new InvalidCastException("not a valid float value");
    }

    public override Guid GetGuid(int ordinal)
    {
        var value = GetProtoValue(ordinal);
        CheckNotNull(ordinal);
        if (value.HasStringValue)
        {
            try
            {
                return Guid.Parse(value.StringValue);
            }
            catch (Exception exception)
            {
                throw new InvalidCastException(exception.Message, exception);
            }
        }
        throw new InvalidCastException("not a valid Guid value");
    }

    public override short GetInt16(int ordinal)
    {
        var value = GetProtoValue(ordinal);
        CheckNotNull(ordinal);
        if (value.HasStringValue)
        {
            try
            {
                return short.Parse(value.StringValue,
                    NumberStyles.AllowDecimalPoint | NumberStyles.AllowLeadingSign | NumberStyles.AllowExponent,
                    CultureInfo.InvariantCulture);
            }
            catch (OverflowException)
            {
                throw;
            }
            catch (Exception exception)
            {
                throw new InvalidCastException(exception.Message, exception);
            }
        }
        if (value.HasNumberValue)
        {
            return checked((short)value.NumberValue);
        }
        throw new InvalidCastException("not a valid Int16 value");
    }

    public override int GetInt32(int ordinal)
    {
        var value = GetProtoValue(ordinal);
        CheckNotNull(ordinal);
        if (value.HasStringValue)
        {
            try
            {
                return int.Parse(value.StringValue,
                    NumberStyles.AllowDecimalPoint | NumberStyles.AllowLeadingSign | NumberStyles.AllowExponent,
                    CultureInfo.InvariantCulture);
            }
            catch (OverflowException)
            {
                throw;
            }
            catch (Exception exception)
            {
                throw new InvalidCastException(exception.Message, exception);
            }
        }
        if (value.HasNumberValue)
        {
            return checked((int)value.NumberValue);
        }
        throw new InvalidCastException("not a valid Int32 value");
    }

    public override long GetInt64(int ordinal)
    {
        var value = GetProtoValue(ordinal);
        CheckNotNull(ordinal);
        if (value.HasStringValue)
        {
            try
            {
                return long.Parse(value.StringValue,
                    NumberStyles.AllowDecimalPoint | NumberStyles.AllowLeadingSign | NumberStyles.AllowExponent,
                    CultureInfo.InvariantCulture);
            }
            catch (Exception exception)
            {
                throw new InvalidCastException(exception.Message, exception);
            }
        }
        if (value.HasNumberValue)
        {
            return checked((long)value.NumberValue);
        }
        throw new InvalidCastException("not a valid Int64 value");
    }
    
    public TimeSpan GetTimeSpan(int ordinal) => GetFieldValue<TimeSpan>(ordinal);

    public override string GetName(int ordinal)
    {
        CheckValidOrdinal(ordinal);
        return Metadata!.RowType.Fields[ordinal].Name;
    }

    public override int GetOrdinal(string name)
    {
        CheckNotClosed();
        // First try with case sensitivity.
        for (var i = 0; i < Metadata?.RowType.Fields.Count; i++)
        {
            if (Equals(Metadata?.RowType.Fields[i].Name, name))
            {
                return i;
            }
        }
        // Nothing found, try with case-insensitive comparison.
        for (var i = 0; i < Metadata?.RowType.Fields.Count; i++)
        {
            if (string.Equals(Metadata?.RowType.Fields[i].Name, name, StringComparison.InvariantCultureIgnoreCase))
            {
                return i;
            }
        }
        throw new IndexOutOfRangeException($"No column with name {name} found");
    }

    public override T GetFieldValue<T>(int ordinal)
    {
        CheckNotClosed();
        CheckValidPosition();
        CheckValidOrdinal(ordinal);
        if (typeof(T) == typeof(object))
        {
            return base.GetFieldValue<T>(ordinal);
        }
        if (typeof(T) == typeof(Stream))
        {
            CheckNotNull(ordinal);
            return (T)(object)GetStream(ordinal);
        }
        if (typeof(T) == typeof(TextReader))
        {
            CheckNotNull(ordinal);
            return (T)(object)GetTextReader(ordinal);
        }
        if (typeof(T) == typeof(char) || typeof(T) == typeof(char?))
        {
            if (IsDBNull(ordinal) && typeof(T) == typeof(char?))
            {
                return (T)(object)null!;
            }
            return (T)(object)GetChar(ordinal);
        }
        if (typeof(T) == typeof(DateTime) || typeof(T) == typeof(DateTime?))
        {
            if (IsDBNull(ordinal) && typeof(T) == typeof(DateTime?))
            {
                return (T)(object)null!;
            }
            return (T)(object)GetDateTime(ordinal);
        }
        if (typeof(T) == typeof(double) || typeof(T) == typeof(double?))
        {
            if (IsDBNull(ordinal) && typeof(T) == typeof(double?))
            {
                return (T)(object)null!;
            }
            return (T)(object)GetDouble(ordinal);
        }
        if (typeof(T) == typeof(float) || typeof(T) == typeof(float?))
        {
            if (IsDBNull(ordinal) && typeof(T) == typeof(float?))
            {
                return (T)(object)null!;
            }
            return (T)(object)GetFloat(ordinal);
        }
        if (typeof(T) == typeof(Int16) || typeof(T) == typeof(Int16?))
        {
            if (IsDBNull(ordinal) && typeof(T) == typeof(Int16?))
            {
                return (T)(object)null!;
            }
            return (T)(object)GetInt16(ordinal);
        }
        if (typeof(T) == typeof(int) || typeof(T) == typeof(int?))
        {
            if (IsDBNull(ordinal) && typeof(T) == typeof(int?))
            {
                return (T)(object)null!;
            }
            return (T)(object)GetInt32(ordinal);
        }
        if (typeof(T) == typeof(long) || typeof(T) == typeof(long?))
        {
            if (IsDBNull(ordinal) && typeof(T) == typeof(long?))
            {
                return (T)(object)null!;
            }
            return (T)(object)GetInt64(ordinal);
        }

        if (IsDBNull(ordinal) && default(T) == null)
        {
            if (typeof(T) == typeof(DBNull))
            {
                return (T)(object)DBNull.Value;
            }
            return (T)(object)null;
        }

        return base.GetFieldValue<T>(ordinal);
    }

    public override object GetValue(int ordinal)
    {
        CheckValidOrdinal(ordinal);
        CheckValidPosition();
        var type = Metadata!.RowType.Fields[ordinal].Type;
        var value = _currentRow!.Values[ordinal];
        return GetUnderlyingValue(type, value);
    }

    private static object GetUnderlyingValue(Google.Cloud.Spanner.V1.Type type, Value value)
    {
        if (value.HasNullValue)
        {
            return DBNull.Value;
        }

        switch (type.Code)
        {
            case TypeCode.Array:
                var listType = GetClrType(type);
                var list = (IList)Activator.CreateInstance(listType);
                if (list == null)
                {
                    throw new InvalidOperationException($"Failed to create instance of type {listType}.");
                }
                foreach (var element in value.ListValue.Values)
                {
                    var underlyingValue = GetUnderlyingValue(type.ArrayElementType, element);
                    if (underlyingValue is DBNull)
                    {
                        underlyingValue = null;
                    }
                    list.Add(underlyingValue);
                }
                return list;
            case TypeCode.Bool:
                return value.BoolValue;
            case TypeCode.Bytes:
                return Convert.FromBase64String(value.StringValue);
            case TypeCode.Date:
                return DateOnly.Parse(value.StringValue);
            case TypeCode.Enum:
                return long.Parse(value.StringValue);
            case TypeCode.Float32:
                return (float)value.NumberValue;
            case TypeCode.Float64:
                return value.NumberValue;
            case TypeCode.Int64:
                return long.Parse(value.StringValue);
            case TypeCode.Interval:
                return XmlConvert.ToTimeSpan(value.StringValue);
            case TypeCode.Json:
                return value.StringValue;
            case TypeCode.Numeric:
                return decimal.Parse(value.StringValue, NumberStyles.AllowDecimalPoint | NumberStyles.AllowExponent | NumberStyles.AllowLeadingSign, CultureInfo.InvariantCulture);
            case TypeCode.Proto:
                return Convert.FromBase64String(value.StringValue);
            case TypeCode.String:
                return value.StringValue;
            case TypeCode.Timestamp:
                return XmlConvert.ToDateTime(value.StringValue, XmlDateTimeSerializationMode.Utc);
            case TypeCode.Uuid:
                return Guid.Parse(value.StringValue);
        }
        if (value.HasBoolValue)
        {
            return value.BoolValue;
        }
        if (value.HasStringValue)
        {
            return value.StringValue;
        }
        if (value.HasNumberValue)
        {
            return value.NumberValue;
        }
        return value;
    }

    private Value GetProtoValue(int ordinal)
    {
        CheckValidOrdinal(ordinal);
        CheckValidPosition();
        return _currentRow!.Values[ordinal];
    }

    private V1.Type GetSpannerType(int ordinal)
    {
        CheckValidOrdinal(ordinal);
        return Metadata?.RowType.Fields[ordinal].Type ?? throw new DataException("metadata not found");
    }

    public override int GetValues(object[] values)
    {
        CheckValidPosition();
        GaxPreconditions.CheckNotNull(values, nameof(values));
            
        var count = Math.Min(FieldCount, values.Length);
        for (var i = 0; i < count; i++)
        {
            values[i] = this[i];
        }

        return count;
    }

    public override bool IsDBNull(int ordinal)
    {
        var value = GetProtoValue(ordinal);
        return value.HasNullValue;
    }

    public override bool NextResult()
    {
        CheckNotClosed();
        if (IsSingleRow || IsSingleResult)
        {
            return false;
        }
        _currentRow = null;
        _tempRow = null;
        _hasData = false;
        _hasReadData = false;
        _metadata = null;
        _stats = null;
        return LibRows.NextResultSet();
    }

    public override IEnumerator GetEnumerator()
    {
        CheckNotClosed();
        return new DbEnumerator(this);
    }

    private void CheckValidPosition()
    {
        CheckNotClosed();
        if (_currentRow == null)
        {
            throw new InvalidOperationException("DataReader is before the first row or after the last row");
        }
    }

    private void CheckValidOrdinal(int ordinal)
    {
        CheckNotClosed();
        var metadata = Metadata;
        GaxPreconditions.CheckState(metadata != null && metadata.RowType.Fields.Count > 0, "This reader does not contain any rows");
            
        // Check that the ordinal is within the range of the columns in the query.            
        if (ordinal < 0 || ordinal >= metadata!.RowType.Fields.Count)
        {
            throw new IndexOutOfRangeException("ordinal is out of range");
        }
    }

    private void CheckNotNull(int ordinal)
    {
        if (_currentRow?.Values[ordinal]?.HasNullValue ?? false)
        {
            throw new InvalidCastException("Value is null");
        }
    }

}