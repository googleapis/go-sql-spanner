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
using AdoNet.Specification.Tests;
using Google.Cloud.SpannerLib.MockServer;
using TypeCode = Google.Cloud.Spanner.V1.TypeCode;

namespace Google.Cloud.Spanner.DataProvider.SpecificationTests;

public class DbFactoryFixture : IDisposable, ISelectValueFixture, IDeleteFixture
{
    static DbFactoryFixture()
    {
        AppDomain.CurrentDomain.ProcessExit += (_, _) =>
        {
            SpannerPool.CloseSpannerLib();
        };
    }
    
    private bool _disposed;
    internal readonly SpannerMockServerFixture MockServerFixture = new ();
    
    public DbProviderFactory Factory => SpannerFactory.Instance;
    public string ConnectionString => $"Host={MockServerFixture.Host};Port={MockServerFixture.Port};Data Source=projects/p1/instances/i1/databases/d1;UsePlainText=true";

    public IReadOnlyCollection<DbType> SupportedDbTypes { get; } = [
        DbType.Binary,
        DbType.Boolean,
        DbType.Date,
        DbType.DateTime,
        DbType.Decimal,
        DbType.Double,
        DbType.Guid,
        DbType.Int64,
        DbType.Single,
        DbType.String,
    ];
    public string SelectNoRows => "select * from (select 1) where false;";
    public System.Type NullValueExceptionType { get; } = typeof(InvalidCastException);
    public string DeleteNoRows => "delete from foo where false;";
    
    public DbFactoryFixture()
    {
        Reset();
    }

    public void Reset()
    {
        MockServerFixture.SpannerMock.Reset();
        MockServerFixture.SpannerMock.AddOrUpdateStatementResult("SELECT 1;", StatementResult.CreateSelect1ResultSet());
        MockServerFixture.SpannerMock.AddOrUpdateStatementResult("SELECT 1", StatementResult.CreateSelect1ResultSet());
        MockServerFixture.SpannerMock.AddOrUpdateStatementResult("SELECT 2", StatementResult.CreateSelect2ResultSet());
        MockServerFixture.SpannerMock.AddOrUpdateStatementResult(" SELECT 2", StatementResult.CreateSelect2ResultSet());
        MockServerFixture.SpannerMock.AddOrUpdateStatementResult("SELECT NULL;",
            StatementResult.CreateSingleColumnResultSet(new V1.Type{Code = TypeCode.Int64}, "c", DBNull.Value));
        MockServerFixture.SpannerMock.AddOrUpdateStatementResult("SELECT 1 AS id;",
            StatementResult.CreateSingleColumnResultSet(new V1.Type{Code = TypeCode.Int64}, "id", 1));
        MockServerFixture.SpannerMock.AddOrUpdateStatementResult("SELECT 1 AS Id;",
            StatementResult.CreateSingleColumnResultSet(new V1.Type{Code = TypeCode.Int64}, "Id", 1));
        MockServerFixture.SpannerMock.AddOrUpdateStatementResult("SELECT 'test';",
            StatementResult.CreateSingleColumnResultSet(new V1.Type{Code = TypeCode.String}, "c", "test"));
        MockServerFixture.SpannerMock.AddOrUpdateStatementResult("SELECT 'ab¢d';",
            StatementResult.CreateSingleColumnResultSet(new V1.Type{Code = TypeCode.String}, "c", "ab¢d"));
        MockServerFixture.SpannerMock.AddOrUpdateStatementResult(SelectNoRows,
            StatementResult.CreateSingleColumnResultSet(new V1.Type{Code = TypeCode.Int64}, "c"));
        MockServerFixture.SpannerMock.AddOrUpdateStatementResult(SelectNoRows[..^1],
            StatementResult.CreateSingleColumnResultSet(new V1.Type{Code = TypeCode.Int64}, "c"));
        MockServerFixture.SpannerMock.AddOrUpdateStatementResult("SELECT 42",
            StatementResult.CreateSingleColumnResultSet(new V1.Type{Code = TypeCode.Int64}, "c", 42));
        MockServerFixture.SpannerMock.AddOrUpdateStatementResult("SELECT 43",
            StatementResult.CreateSingleColumnResultSet(new V1.Type{Code = TypeCode.Int64}, "c", 43));
        MockServerFixture.SpannerMock.AddOrUpdateStatementResult(" SELECT 43",
            StatementResult.CreateSingleColumnResultSet(new V1.Type{Code = TypeCode.Int64}, "c", 43));
        MockServerFixture.SpannerMock.AddOrUpdateStatementResult("SELECT 42 UNION SELECT 43;",
            StatementResult.CreateSingleColumnResultSet(new V1.Type{Code = TypeCode.Int64}, "c", 42, 43));
        MockServerFixture.SpannerMock.AddOrUpdateStatementResult("SELECT 1 UNION SELECT 2;",
            StatementResult.CreateSingleColumnResultSet(new V1.Type{Code = TypeCode.Int64}, "c", 1, 2));
        MockServerFixture.SpannerMock.AddOrUpdateStatementResult(DeleteNoRows, StatementResult.CreateUpdateCount(0));
    }

    public string CreateSelectSql(DbType dbType, ValueKind kind)
    {
        return dbType switch
        {
            DbType.Binary => CreateSelectSqlBinary(kind),
            DbType.Boolean => CreateSelectSqlBoolean(kind),
            DbType.Date => CreateSelectSqlDate(kind),
            DbType.DateTime => CreateSelectSqlDateTime(kind),
            DbType.Decimal => CreateSelectSqlDecimal(kind),
            DbType.Double => CreateSelectSqlDouble(kind),
            DbType.Guid => CreateSelectSqlGuid(kind),
            DbType.Int64 => CreateSelectSqlInt64(kind),
            DbType.Single => CreateSelectSqlSingle(kind),
            DbType.String => CreateSelectSqlString(kind),
            _ => throw new NotImplementedException("Not implemented")
        };
    }

    private string CreateSelectSqlBinary(ValueKind kind)
    {
        var sql = "SELECT bytes_col FROM my_table;";
        switch (kind)
        {
            case ValueKind.Empty:
                MockServerFixture.SpannerMock.AddOrUpdateStatementResult(sql,
                    StatementResult.CreateSingleColumnResultSet(new V1.Type { Code = TypeCode.Bytes }, "bytes_col", Array.Empty<byte>()));
                break;
            case ValueKind.Zero:
                MockServerFixture.SpannerMock.AddOrUpdateStatementResult(sql,
                    StatementResult.CreateSingleColumnResultSet(new V1.Type { Code = TypeCode.Bytes }, "bytes_col", new byte[]{0}));
                break;
            case ValueKind.One:
                MockServerFixture.SpannerMock.AddOrUpdateStatementResult(sql,
                    StatementResult.CreateSingleColumnResultSet(new V1.Type { Code = TypeCode.Bytes }, "bytes_col", new byte[]{0x11}));
                break;
            case ValueKind.Maximum:
            case ValueKind.Minimum:
            case ValueKind.Null:
                MockServerFixture.SpannerMock.AddOrUpdateStatementResult(sql,
                    StatementResult.CreateSingleColumnResultSet(new V1.Type { Code = TypeCode.Bytes }, "bytes_col", DBNull.Value));
                break;
            default:
                throw new NotImplementedException("Not implemented");
        }
        return sql;
    }
    private string CreateSelectSqlBoolean(ValueKind kind)
    {
        var sql = "SELECT bool_col FROM my_table;";
        switch (kind)
        {
            case ValueKind.Maximum:
            case ValueKind.One:
                MockServerFixture.SpannerMock.AddOrUpdateStatementResult(sql,
                    StatementResult.CreateSingleColumnResultSet(new V1.Type { Code = TypeCode.Bool }, "bool_col", true));
                break;
            case ValueKind.Empty:
            case ValueKind.Minimum:
            case ValueKind.Zero:
                MockServerFixture.SpannerMock.AddOrUpdateStatementResult(sql,
                    StatementResult.CreateSingleColumnResultSet(new V1.Type { Code = TypeCode.Bool }, "bool_col", false));
                break;
            case ValueKind.Null:
                MockServerFixture.SpannerMock.AddOrUpdateStatementResult(sql,
                    StatementResult.CreateSingleColumnResultSet(new V1.Type { Code = TypeCode.Bool }, "bool_col", DBNull.Value));
                break;
            default:
                throw new NotImplementedException("Not implemented");
        }
        return sql;
    }
    
    private string CreateSelectSqlDate(ValueKind kind)
    {
        var sql = "SELECT date_col FROM my_table;";
        switch (kind)
        {
            case ValueKind.One:
                MockServerFixture.SpannerMock.AddOrUpdateStatementResult(sql,
                    StatementResult.CreateSingleColumnResultSet(new V1.Type { Code = TypeCode.Date }, "date_col", "1111-11-11"));
                break;
            case ValueKind.Maximum:
                MockServerFixture.SpannerMock.AddOrUpdateStatementResult(sql,
                    StatementResult.CreateSingleColumnResultSet(new V1.Type { Code = TypeCode.Date }, "date_col", "9999-12-31"));
                break;
            case ValueKind.Minimum:
                MockServerFixture.SpannerMock.AddOrUpdateStatementResult(sql,
                    StatementResult.CreateSingleColumnResultSet(new V1.Type { Code = TypeCode.Date }, "date_col", "0001-01-01"));
                break;
            case ValueKind.Zero:
            case ValueKind.Empty:
            case ValueKind.Null:
                MockServerFixture.SpannerMock.AddOrUpdateStatementResult(sql,
                    StatementResult.CreateSingleColumnResultSet(new V1.Type { Code = TypeCode.Date }, "date_col", DBNull.Value));
                break;
            default:
                throw new NotImplementedException("Not implemented");
        }
        return sql;
    }
    
    private string CreateSelectSqlDateTime(ValueKind kind)
    {
        var sql = "SELECT timestamp_col FROM my_table;";
        switch (kind)
        {
            case ValueKind.One:
                MockServerFixture.SpannerMock.AddOrUpdateStatementResult(sql,
                    StatementResult.CreateSingleColumnResultSet(new V1.Type { Code = TypeCode.Timestamp }, "timestamp_col", "1111-11-11T11:11:11.111000000Z"));
                break;
            case ValueKind.Maximum:
                MockServerFixture.SpannerMock.AddOrUpdateStatementResult(sql,
                    StatementResult.CreateSingleColumnResultSet(new V1.Type { Code = TypeCode.Timestamp }, "timestamp_col", "9999-12-31T23:59:59.999000000Z"));
                break;
            case ValueKind.Minimum:
                MockServerFixture.SpannerMock.AddOrUpdateStatementResult(sql,
                    StatementResult.CreateSingleColumnResultSet(new V1.Type { Code = TypeCode.Timestamp }, "timestamp_col", "0001-01-01T00:00:00Z"));
                break;
            case ValueKind.Zero:
            case ValueKind.Empty:
            case ValueKind.Null:
                MockServerFixture.SpannerMock.AddOrUpdateStatementResult(sql,
                    StatementResult.CreateSingleColumnResultSet(new V1.Type { Code = TypeCode.Timestamp }, "timestamp_col", DBNull.Value));
                break;
            default:
                throw new NotImplementedException("Not implemented");
        }
        return sql;
    }
    
    private string CreateSelectSqlDecimal(ValueKind kind)
    {
        var sql = "SELECT numeric_col FROM my_table;";
        switch (kind)
        {
            case ValueKind.Zero:
                MockServerFixture.SpannerMock.AddOrUpdateStatementResult(sql,
                    StatementResult.CreateSingleColumnResultSet(new V1.Type { Code = TypeCode.Numeric }, "numeric_col", "0"));
                break;
            case ValueKind.One:
                MockServerFixture.SpannerMock.AddOrUpdateStatementResult(sql,
                    StatementResult.CreateSingleColumnResultSet(new V1.Type { Code = TypeCode.Numeric }, "numeric_col", "1"));
                break;
            case ValueKind.Maximum:
                MockServerFixture.SpannerMock.AddOrUpdateStatementResult(sql,
                    StatementResult.CreateSingleColumnResultSet(new V1.Type { Code = TypeCode.Numeric }, "numeric_col", "99999999999999999999.999999999999999"));
                break;
            case ValueKind.Minimum:
                MockServerFixture.SpannerMock.AddOrUpdateStatementResult(sql,
                    StatementResult.CreateSingleColumnResultSet(new V1.Type { Code = TypeCode.Numeric }, "numeric_col", "0.000000000000001"));
                break;
            case ValueKind.Empty:
            case ValueKind.Null:
                MockServerFixture.SpannerMock.AddOrUpdateStatementResult(sql,
                    StatementResult.CreateSingleColumnResultSet(new V1.Type { Code = TypeCode.Numeric }, "numeric_col", DBNull.Value));
                break;
            default:
                throw new NotImplementedException("Not implemented");
        }
        return sql;
    }
    
    private string CreateSelectSqlDouble(ValueKind kind)
    {
        var sql = "SELECT float64_col FROM my_table;";
        switch (kind)
        {
            case ValueKind.Zero:
                MockServerFixture.SpannerMock.AddOrUpdateStatementResult(sql,
                    StatementResult.CreateSingleColumnResultSet(new V1.Type { Code = TypeCode.Float64 }, "float64_col", 0.0d));
                break;
            case ValueKind.One:
                MockServerFixture.SpannerMock.AddOrUpdateStatementResult(sql,
                    StatementResult.CreateSingleColumnResultSet(new V1.Type { Code = TypeCode.Float64 }, "float64_col", 1.0d));
                break;
            case ValueKind.Maximum:
                MockServerFixture.SpannerMock.AddOrUpdateStatementResult(sql,
                    StatementResult.CreateSingleColumnResultSet(new V1.Type { Code = TypeCode.Float64 }, "float64_col", 1.79e308d));
                break;
            case ValueKind.Minimum:
                MockServerFixture.SpannerMock.AddOrUpdateStatementResult(sql,
                    StatementResult.CreateSingleColumnResultSet(new V1.Type { Code = TypeCode.Float64 }, "float64_col", 2.23e-308d));
                break;
            case ValueKind.Empty:
            case ValueKind.Null:
                MockServerFixture.SpannerMock.AddOrUpdateStatementResult(sql,
                    StatementResult.CreateSingleColumnResultSet(new V1.Type { Code = TypeCode.Float64 }, "float64_col", DBNull.Value));
                break;
            default:
                throw new NotImplementedException("Not implemented");
        }
        return sql;
    }
        
    private string CreateSelectSqlGuid(ValueKind kind)
    {
        var sql = "SELECT uuid_col FROM my_table;";
        switch (kind)
        {
            case ValueKind.Zero:
                MockServerFixture.SpannerMock.AddOrUpdateStatementResult(sql,
                    StatementResult.CreateSingleColumnResultSet(new V1.Type { Code = TypeCode.Uuid }, "uuid_col", "00000000-0000-0000-0000-000000000000"));
                break;
            case ValueKind.One:
                MockServerFixture.SpannerMock.AddOrUpdateStatementResult(sql,
                    StatementResult.CreateSingleColumnResultSet(new V1.Type { Code = TypeCode.Uuid }, "uuid_col", "11111111-1111-1111-1111-111111111111"));
                break;
            case ValueKind.Maximum:
                MockServerFixture.SpannerMock.AddOrUpdateStatementResult(sql,
                    StatementResult.CreateSingleColumnResultSet(new V1.Type { Code = TypeCode.Uuid }, "uuid_col", "ccddeeff-aabb-8899-7766-554433221100"));
                break;
            case ValueKind.Minimum:
                MockServerFixture.SpannerMock.AddOrUpdateStatementResult(sql,
                    StatementResult.CreateSingleColumnResultSet(new V1.Type { Code = TypeCode.Uuid }, "uuid_col", "33221100-5544-7766-9988-aabbccddeeff"));
                break;
            case ValueKind.Empty:
            case ValueKind.Null:
                MockServerFixture.SpannerMock.AddOrUpdateStatementResult(sql,
                    StatementResult.CreateSingleColumnResultSet(new V1.Type { Code = TypeCode.Uuid }, "uuid_col", DBNull.Value));
                break;
            default:
                throw new NotImplementedException("Not implemented");
        }
        return sql;
    }

    private string CreateSelectSqlInt64(ValueKind kind)
    {
        var sql = "SELECT int64_col FROM my_table;";
        switch (kind)
        {
            case ValueKind.Zero:
                MockServerFixture.SpannerMock.AddOrUpdateStatementResult(sql,
                    StatementResult.CreateSingleColumnResultSet(new V1.Type { Code = TypeCode.Int64 }, "int64_col", 0L));
                break;
            case ValueKind.One:
                MockServerFixture.SpannerMock.AddOrUpdateStatementResult(sql,
                    StatementResult.CreateSingleColumnResultSet(new V1.Type { Code = TypeCode.Int64 }, "int64_col", 1L));
                break;
            case ValueKind.Maximum:
                MockServerFixture.SpannerMock.AddOrUpdateStatementResult(sql,
                    StatementResult.CreateSingleColumnResultSet(new V1.Type { Code = TypeCode.Int64 }, "int64_col", long.MaxValue));
                break;
            case ValueKind.Minimum:
                MockServerFixture.SpannerMock.AddOrUpdateStatementResult(sql,
                    StatementResult.CreateSingleColumnResultSet(new V1.Type { Code = TypeCode.Int64 }, "int64_col", long.MinValue));
                break;
            case ValueKind.Empty:
            case ValueKind.Null:
                MockServerFixture.SpannerMock.AddOrUpdateStatementResult(sql,
                    StatementResult.CreateSingleColumnResultSet(new V1.Type { Code = TypeCode.Int64 }, "int64_col", DBNull.Value));
                break;
            default:
                throw new NotImplementedException("Not implemented");
        }
        return sql;
    }
    
    private string CreateSelectSqlSingle(ValueKind kind)
    {
        var sql = "SELECT float32_col FROM my_table;";
        switch (kind)
        {
            case ValueKind.Zero:
                MockServerFixture.SpannerMock.AddOrUpdateStatementResult(sql,
                    StatementResult.CreateSingleColumnResultSet(new V1.Type { Code = TypeCode.Float32 }, "float32_col", 0.0f));
                break;
            case ValueKind.One:
                MockServerFixture.SpannerMock.AddOrUpdateStatementResult(sql,
                    StatementResult.CreateSingleColumnResultSet(new V1.Type { Code = TypeCode.Float32 }, "float32_col", 1.0f));
                break;
            case ValueKind.Maximum:
                MockServerFixture.SpannerMock.AddOrUpdateStatementResult(sql,
                    StatementResult.CreateSingleColumnResultSet(new V1.Type { Code = TypeCode.Float32 }, "float32_col", 3.40e38f));
                break;
            case ValueKind.Minimum:
                MockServerFixture.SpannerMock.AddOrUpdateStatementResult(sql,
                    StatementResult.CreateSingleColumnResultSet(new V1.Type { Code = TypeCode.Float32 }, "float32_col", 1.18e-38f));
                break;
            case ValueKind.Empty:
            case ValueKind.Null:
                MockServerFixture.SpannerMock.AddOrUpdateStatementResult(sql,
                    StatementResult.CreateSingleColumnResultSet(new V1.Type { Code = TypeCode.Float32 }, "float32_col", DBNull.Value));
                break;
            default:
                throw new NotImplementedException("Not implemented");
        }
        return sql;
    }

    private string CreateSelectSqlString(ValueKind kind)
    {
        var sql = "SELECT string_col FROM my_table;";
        switch (kind)
        {
            case ValueKind.Zero:
                MockServerFixture.SpannerMock.AddOrUpdateStatementResult(sql,
                    StatementResult.CreateSingleColumnResultSet(new V1.Type { Code = TypeCode.String }, "string_col", "0"));
                break;
            case ValueKind.One:
                MockServerFixture.SpannerMock.AddOrUpdateStatementResult(sql,
                    StatementResult.CreateSingleColumnResultSet(new V1.Type { Code = TypeCode.String }, "string_col", "1"));
                break;
            case ValueKind.Empty:
                MockServerFixture.SpannerMock.AddOrUpdateStatementResult(sql,
                    StatementResult.CreateSingleColumnResultSet(new V1.Type { Code = TypeCode.String }, "string_col", ""));
                break;
            case ValueKind.Maximum:
            case ValueKind.Minimum:
            case ValueKind.Null:
                MockServerFixture.SpannerMock.AddOrUpdateStatementResult(sql,
                    StatementResult.CreateSingleColumnResultSet(new V1.Type { Code = TypeCode.String }, "string_col", DBNull.Value));
                break;
            default:
                throw new NotImplementedException("Not implemented");
        }
        return sql;
    }

    public string CreateSelectSql(byte[] value)
    {
        var sql = "SELECT bytes_col FROM my_table;";
        MockServerFixture.SpannerMock.AddOrUpdateStatementResult(sql,
            StatementResult.CreateSingleColumnResultSet(new V1.Type{Code = TypeCode.Bytes}, "bytes_col", value));

        return sql;
    }
    
    protected void MarkDisposed()
    {
        _disposed = true;
    }

    public void Dispose()
    {
        Dispose(true);
        GC.SuppressFinalize(this);
    }
        
    protected virtual void Dispose(bool disposing)
    {
        if (_disposed)
        {
            return;
        }
        try
        {
            MockServerFixture.Dispose();
            // var source = new CancellationTokenSource();
            // source.CancelAfter(1000);
            // Task.Run(() => SpannerPool.CloseSpannerLibWhenAllConnectionsClosedAsync(source.Token), source.Token).Wait(source.Token);
        }
        finally
        {
            _disposed = true;
        }
    }
}
