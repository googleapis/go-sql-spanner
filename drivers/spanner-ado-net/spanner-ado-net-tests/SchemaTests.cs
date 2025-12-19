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
using System.Text.RegularExpressions;
using TypeCode = Google.Cloud.Spanner.V1.TypeCode;

namespace Google.Cloud.Spanner.DataProvider.Tests;

public class SchemaTests : AbstractMockServerTests
{
    [Test]
    public async Task MetaDataCollections()
    {
        await using var conn = await OpenConnectionAsync();

        var metaDataCollections = await conn.GetSchemaAsync(DbMetaDataCollectionNames.MetaDataCollections);
        Assert.That(metaDataCollections.Rows, Has.Count.GreaterThan(0));

        foreach (var row in metaDataCollections.Rows.OfType<DataRow>())
        {
            var collectionName = (string)row!["CollectionName"];
            Assert.That(await conn.GetSchemaAsync(collectionName), Is.Not.Null, $"Collection {collectionName} advertise in MetaDataCollections but is null");
        }
    }
    
    [Test]
    public async Task NoParameter()
    {
        await using var conn = await OpenConnectionAsync();

        var dataTable1 = conn.GetSchema();
        var collections1 = dataTable1.Rows
            .Cast<DataRow>()
            .Select(r => (string)r["CollectionName"])
            .ToList();

        var dataTable2 = conn.GetSchema(DbMetaDataCollectionNames.MetaDataCollections);
        var collections2 = dataTable2.Rows
            .Cast<DataRow>()
            .Select(r => (string)r["CollectionName"])
            .ToList();

        Assert.That(collections1, Is.EquivalentTo(collections2));
    }
    
    [Test]
    public async Task CaseInsensitiveCollectionName()
    {
        await using var conn = await OpenConnectionAsync();

        var dataTable1 = conn.GetSchema(DbMetaDataCollectionNames.MetaDataCollections);
        var collections1 = dataTable1.Rows
            .Cast<DataRow>()
            .Select(r => (string)r["CollectionName"])
            .ToList();

        var dataTable2 = conn.GetSchema("METADATACOLLECTIONS");
        var collections2 = dataTable2.Rows
            .Cast<DataRow>()
            .Select(r => (string)r["CollectionName"])
            .ToList();

        var dataTable3 = conn.GetSchema("metadatacollections");
        var collections3 = dataTable3.Rows
            .Cast<DataRow>()
            .Select(r => (string)r["CollectionName"])
            .ToList();

        var dataTable4 = conn.GetSchema("MetaDataCollections");
        var collections4 = dataTable4.Rows
            .Cast<DataRow>()
            .Select(r => (string)r["CollectionName"])
            .ToList();

        var dataTable5 = conn.GetSchema("METADATACOLLECTIONS", null!);
        var collections5 = dataTable5.Rows
            .Cast<DataRow>()
            .Select(r => (string)r["CollectionName"])
            .ToList();

        var dataTable6 = conn.GetSchema("metadatacollections", null!);
        var collections6 = dataTable6.Rows
            .Cast<DataRow>()
            .Select(r => (string)r["CollectionName"])
            .ToList();

        var dataTable7 = conn.GetSchema("MetaDataCollections", null!);
        var collections7 = dataTable7.Rows
            .Cast<DataRow>()
            .Select(r => (string)r["CollectionName"])
            .ToList();

        Assert.That(collections1, Is.EquivalentTo(collections2));
        Assert.That(collections1, Is.EquivalentTo(collections3));
        Assert.That(collections1, Is.EquivalentTo(collections4));
        Assert.That(collections1, Is.EquivalentTo(collections5));
        Assert.That(collections1, Is.EquivalentTo(collections6));
        Assert.That(collections1, Is.EquivalentTo(collections7));
    }
    
    [Test]
    public async Task DataSourceInformation()
    {
        await using var conn = await OpenConnectionAsync();
        var dataTable = conn.GetSchema(DbMetaDataCollectionNames.MetaDataCollections);
        var metadata = dataTable.Rows
            .Cast<DataRow>()
            .Single(r => r["CollectionName"].Equals("DataSourceInformation"));
        Assert.That(metadata["NumberOfRestrictions"], Is.Zero);
        Assert.That(metadata["NumberOfIdentifierParts"], Is.Zero);

        var dataSourceInfo = conn.GetSchema(DbMetaDataCollectionNames.DataSourceInformation);
        var row = dataSourceInfo.Rows.Cast<DataRow>().Single();

        Assert.That(row["DataSourceProductName"], Is.EqualTo("Spanner"));
        Assert.That(row["DataSourceProductVersion"], Is.EqualTo("1.0.0"));
        Assert.That(row["DataSourceProductVersionNormalized"], Is.EqualTo("001.000.0000"));

        Assert.That(Regex.Match("`some_identifier`", (string)row["QuotedIdentifierPattern"]).Groups[1].Value,
            Is.EqualTo("some_identifier"));
    }
    
    [Test]
    public async Task DataTypes()
    {
        await using var connection = await OpenConnectionAsync();

        var dataTable = connection.GetSchema(DbMetaDataCollectionNames.MetaDataCollections);
        var metadata = dataTable.Rows
            .Cast<DataRow>()
            .Single(r => r["CollectionName"].Equals("DataTypes"));
        Assert.That(metadata["NumberOfRestrictions"], Is.Zero);
        Assert.That(metadata["NumberOfIdentifierParts"], Is.Zero);

        var dataTypes = connection.GetSchema(DbMetaDataCollectionNames.DataTypes);

        var boolRow = dataTypes.Rows.Cast<DataRow>().Single(r => r["TypeName"].Equals("Bool"));
        Assert.That(boolRow["DataType"], Is.EqualTo("System.Boolean"));
        Assert.That(boolRow["ProviderDbType"], Is.EqualTo((int)TypeCode.Bool));
        Assert.That(boolRow["IsUnsigned"], Is.EqualTo(DBNull.Value));

        var bytesRow = dataTypes.Rows.Cast<DataRow>().Single(r => r["TypeName"].Equals("Bytes"));
        Assert.That(bytesRow["DataType"], Is.EqualTo("System.Byte[]"));
        Assert.That(bytesRow["ProviderDbType"], Is.EqualTo((int)TypeCode.Bytes));
        Assert.That(bytesRow["IsUnsigned"], Is.EqualTo(DBNull.Value));
        Assert.That(bytesRow["IsBestMatch"], Is.True);

        var dateRow = dataTypes.Rows.Cast<DataRow>().Single(r => r["TypeName"].Equals("Date"));
        Assert.That(dateRow["DataType"], Is.EqualTo("System.DateOnly"));
        Assert.That(dateRow["ProviderDbType"], Is.EqualTo((int)TypeCode.Date));
        Assert.That(dateRow["IsUnsigned"], Is.EqualTo(DBNull.Value));
        Assert.That(dateRow["IsBestMatch"], Is.True);

        var enumRow = dataTypes.Rows.Cast<DataRow>().Single(r => r["TypeName"].Equals("Enum"));
        Assert.That(enumRow["DataType"], Is.EqualTo("System.Int64"));
        Assert.That(enumRow["ProviderDbType"], Is.EqualTo((int)TypeCode.Enum));
        Assert.That(enumRow["IsUnsigned"], Is.EqualTo(DBNull.Value));
        Assert.That(enumRow["IsBestMatch"], Is.False);

        var float32Row = dataTypes.Rows.Cast<DataRow>().Single(r => r["TypeName"].Equals("Float32"));
        Assert.That(float32Row["DataType"], Is.EqualTo("System.Single"));
        Assert.That(float32Row["ProviderDbType"], Is.EqualTo((int)TypeCode.Float32));
        Assert.That(float32Row["IsUnsigned"], Is.False);
        Assert.That(float32Row["IsBestMatch"], Is.True);

        var float64Row = dataTypes.Rows.Cast<DataRow>().Single(r => r["TypeName"].Equals("Float64"));
        Assert.That(float64Row["DataType"], Is.EqualTo("System.Double"));
        Assert.That(float64Row["ProviderDbType"], Is.EqualTo((int)TypeCode.Float64));
        Assert.That(float64Row["IsUnsigned"], Is.False);
        Assert.That(float64Row["IsBestMatch"], Is.True);

        var int64Row = dataTypes.Rows.Cast<DataRow>().Single(r => r["TypeName"].Equals("Int64"));
        Assert.That(int64Row["DataType"], Is.EqualTo("System.Int64"));
        Assert.That(int64Row["ProviderDbType"], Is.EqualTo((int)TypeCode.Int64));
        Assert.That(int64Row["IsUnsigned"], Is.False);
        Assert.That(int64Row["IsBestMatch"], Is.True);

        var intervalRow = dataTypes.Rows.Cast<DataRow>().Single(r => r["TypeName"].Equals("Interval"));
        Assert.That(intervalRow["DataType"], Is.EqualTo("System.TimeSpan"));
        Assert.That(intervalRow["ProviderDbType"], Is.EqualTo((int)TypeCode.Interval));
        Assert.That(intervalRow["IsUnsigned"], Is.EqualTo(DBNull.Value));
        Assert.That(intervalRow["IsBestMatch"], Is.True);

        var jsonRow = dataTypes.Rows.Cast<DataRow>().Single(r => r["TypeName"].Equals("Json"));
        Assert.That(jsonRow["DataType"], Is.EqualTo("System.String"));
        Assert.That(jsonRow["ProviderDbType"], Is.EqualTo((int)TypeCode.Json));
        Assert.That(jsonRow["IsUnsigned"], Is.EqualTo(DBNull.Value));
        Assert.That(jsonRow["IsBestMatch"], Is.False);

        var numericRow = dataTypes.Rows.Cast<DataRow>().Single(r => r["TypeName"].Equals("Numeric"));
        Assert.That(numericRow["DataType"], Is.EqualTo("System.Decimal"));
        Assert.That(numericRow["ProviderDbType"], Is.EqualTo((int)TypeCode.Numeric));
        Assert.That(numericRow["IsUnsigned"], Is.False);
        Assert.That(numericRow["IsBestMatch"], Is.True);

        var protoRow = dataTypes.Rows.Cast<DataRow>().Single(r => r["TypeName"].Equals("Proto"));
        Assert.That(protoRow["DataType"], Is.EqualTo("System.Byte[]"));
        Assert.That(protoRow["ProviderDbType"], Is.EqualTo((int)TypeCode.Proto));
        Assert.That(protoRow["IsUnsigned"], Is.EqualTo(DBNull.Value));
        Assert.That(protoRow["IsBestMatch"], Is.False);

        var stringRow = dataTypes.Rows.Cast<DataRow>().Single(r => r["TypeName"].Equals("String"));
        Assert.That(stringRow["DataType"], Is.EqualTo("System.String"));
        Assert.That(stringRow["ProviderDbType"], Is.EqualTo((int)TypeCode.String));
        Assert.That(stringRow["IsUnsigned"], Is.EqualTo(DBNull.Value));
        Assert.That(stringRow["IsBestMatch"], Is.True);

        var timestampRow = dataTypes.Rows.Cast<DataRow>().Single(r => r["TypeName"].Equals("Timestamp"));
        Assert.That(timestampRow["DataType"], Is.EqualTo("System.DateTime"));
        Assert.That(timestampRow["ProviderDbType"], Is.EqualTo((int)TypeCode.Timestamp));
        Assert.That(timestampRow["IsUnsigned"], Is.EqualTo(DBNull.Value));
        Assert.That(timestampRow["IsBestMatch"], Is.True);

        var uuidRow = dataTypes.Rows.Cast<DataRow>().Single(r => r["TypeName"].Equals("Uuid"));
        Assert.That(uuidRow["DataType"], Is.EqualTo("System.Guid"));
        Assert.That(uuidRow["ProviderDbType"], Is.EqualTo((int)TypeCode.Uuid));
        Assert.That(uuidRow["IsUnsigned"], Is.EqualTo(DBNull.Value));
        Assert.That(uuidRow["IsBestMatch"], Is.True);
    }

    [Test]
    public async Task Restrictions()
    {
        await using var conn = await OpenConnectionAsync();
        var restrictions = conn.GetSchema(DbMetaDataCollectionNames.Restrictions);
        Assert.That(restrictions.Rows, Has.Count.GreaterThan(0));
    }

    [Test]
    public async Task ReservedWords()
    {
        await using var conn = await OpenConnectionAsync();
        var reservedWords = conn.GetSchema(DbMetaDataCollectionNames.ReservedWords);
        Assert.That(reservedWords.Rows, Has.Count.GreaterThan(0));
    }

}