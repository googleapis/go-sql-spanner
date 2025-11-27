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
using System.Data;
using System.Data.Common;
using Google.Api.Gax;
using TypeCode = Google.Cloud.Spanner.V1.TypeCode;

namespace Google.Cloud.Spanner.DataProvider;

internal sealed class SpannerSchemaProvider(SpannerConnection connection)
{
	internal DataTable GetSchema() => GetSchema("MetaDataCollections", null);

	internal DataTable GetSchema(string collectionName, string?[]? restrictionValues)
	{
		GaxPreconditions.CheckNotNull(collectionName, nameof(collectionName));
		var dataTable = new DataTable();
		if (string.Equals(collectionName, DbMetaDataCollectionNames.MetaDataCollections, StringComparison.OrdinalIgnoreCase))
		{
			FillMetaDataCollections(dataTable, restrictionValues);
		}
		else if (string.Equals(collectionName, DbMetaDataCollectionNames.DataSourceInformation, StringComparison.OrdinalIgnoreCase))
		{
			FillDataSourceInformation(dataTable, restrictionValues);
		}
		else if (string.Equals(collectionName, DbMetaDataCollectionNames.DataTypes, StringComparison.OrdinalIgnoreCase))
		{
			FillDataTypes(dataTable, restrictionValues);
		}
		else if (string.Equals(collectionName, DbMetaDataCollectionNames.ReservedWords, StringComparison.OrdinalIgnoreCase))
		{
			FillReservedWords(dataTable, restrictionValues);
		}
		else if (string.Equals(collectionName, DbMetaDataCollectionNames.Restrictions, StringComparison.OrdinalIgnoreCase))
		{
			FillRestrictions(dataTable, restrictionValues);
		}
		else
		{
			throw new ArgumentException($"Invalid collection name: '{collectionName}'.", nameof(collectionName));
		}
		return dataTable;
	}

	private void FillMetaDataCollections(DataTable dataTable, string?[]? restrictionValues)
	{
		GaxPreconditions.CheckArgument(restrictionValues == null || restrictionValues.Length == 0, nameof(restrictionValues), "restrictionValues is not supported for schema 'MetaDataCollections'.");

		dataTable.TableName = DbMetaDataCollectionNames.MetaDataCollections;
		dataTable.Columns.AddRange(
		[
			new("CollectionName", typeof(string)),
			new("NumberOfRestrictions", typeof(int)),
			new("NumberOfIdentifierParts", typeof(int)),
		]);

		dataTable.Rows.Add(DbMetaDataCollectionNames.MetaDataCollections, 0, 0);
		dataTable.Rows.Add(DbMetaDataCollectionNames.DataSourceInformation, 0, 0);
		dataTable.Rows.Add(DbMetaDataCollectionNames.DataTypes, 0, 0);
		dataTable.Rows.Add(DbMetaDataCollectionNames.ReservedWords, 0, 0);
		dataTable.Rows.Add(DbMetaDataCollectionNames.Restrictions, 0, 0);
	}
	
	private void FillDataSourceInformation(DataTable dataTable, string?[]? restrictionValues)
	{
		GaxPreconditions.CheckArgument(restrictionValues == null || restrictionValues.Length == 0, nameof(restrictionValues), "restrictionValues is not supported for schema 'DataSourceInformation'.");
		
		dataTable.TableName = DbMetaDataCollectionNames.DataSourceInformation;
		dataTable.Columns.AddRange(
		[
			new("CompositeIdentifierSeparatorPattern", typeof(string)),
			new("DataSourceProductName", typeof(string)),
			new("DataSourceProductVersion", typeof(string)),
			new("DataSourceProductVersionNormalized", typeof(string)),
			new("GroupByBehavior", typeof(GroupByBehavior)),
			new("IdentifierPattern", typeof(string)),
			new("IdentifierCase", typeof(IdentifierCase)),
			new("OrderByColumnsInSelect", typeof(bool)),
			new("ParameterMarkerFormat", typeof(string)),
			new("ParameterMarkerPattern", typeof(string)),
			new("ParameterNameMaxLength", typeof(int)),
			new("QuotedIdentifierPattern", typeof(string)),
			new("QuotedIdentifierCase", typeof(IdentifierCase)),
			new("ParameterNamePattern", typeof(string)),
			new("StatementSeparatorPattern", typeof(string)),
			new("StringLiteralPattern", typeof(string)),
			new("SupportedJoinOperators", typeof(SupportedJoinOperators)),
		]);
		
		var row = dataTable.NewRow();
		row["CompositeIdentifierSeparatorPattern"] = @"\.";
		row["DataSourceProductName"] = "Spanner";
		row["DataSourceProductVersion"] = connection.ServerVersion;
		row["DataSourceProductVersionNormalized"] = connection.ServerVersionNormalizedString;
		row["GroupByBehavior"] = GroupByBehavior.Unrelated;
		row["IdentifierPattern"] = @"(^\[\p{Lo}\p{Lu}\p{Ll}][\p{Lo}\p{Lu}\p{Ll}\p{Nd}_]*$)";
		row["IdentifierCase"] = IdentifierCase.Insensitive;
		row["OrderByColumnsInSelect"] = false;
		row["ParameterMarkerFormat"] = "{0}";
		row["ParameterMarkerPattern"] = "(@[A-Za-z0-9_]*)";
		row["ParameterNameMaxLength"] = 128;
		row["QuotedIdentifierPattern"] = @"(([^`]|\\`)+)";
		row["QuotedIdentifierCase"] = IdentifierCase.Insensitive;
		row["ParameterNamePattern"] = @"[\p{Lo}\p{Lu}\p{Ll}\p{Lm}][\p{Lo}\p{Lu}\p{Ll}\p{Lm}\p{Nd}_]*";
		row["StatementSeparatorPattern"] = ";";
		row["StringLiteralPattern"] = @"'(([^']|'')*)'";
		row["SupportedJoinOperators"] =
			SupportedJoinOperators.FullOuter |
			SupportedJoinOperators.Inner |
			SupportedJoinOperators.LeftOuter |
			SupportedJoinOperators.RightOuter;
		dataTable.Rows.Add(row);
	}

	private void FillDataTypes(DataTable dataTable, string?[]? restrictionValues)
	{
		GaxPreconditions.CheckArgument(restrictionValues == null || restrictionValues.Length == 0, nameof(restrictionValues), "restrictionValues is not supported for schema 'DataTypes'.");
		
		dataTable.TableName = DbMetaDataCollectionNames.DataTypes;
		dataTable.Columns.AddRange(
		[
			new("TypeName", typeof(string)),
			new("ProviderDbType", typeof(int)),
			new("ColumnSize", typeof(long)),
			new("CreateFormat", typeof(string)),
			new("CreateParameters", typeof(string)),
			new("DataType", typeof(string)),
			new("IsAutoIncrementable", typeof(bool)),
			new("IsBestMatch", typeof(bool)),
			new("IsCaseSensitive", typeof(bool)),
			new("IsFixedLength", typeof(bool)),
			new("IsFixedPrecisionScale", typeof(bool)),
			new("IsLong", typeof(bool)),
			new("IsNullable", typeof(bool)),
			new("IsSearchable", typeof(bool)),
			new("IsSearchableWithLike", typeof(bool)),
			new("IsUnsigned", typeof(bool)),
			new("MaximumScale", typeof(short)),
			new("MinimumScale", typeof(short)),
			new("IsConcurrencyType", typeof(bool)),
			new("IsLiteralSupported", typeof(bool)),
			new("LiteralPrefix", typeof(string)),
			new("LiteralSuffix", typeof(string)),
			new("NativeDataType", typeof(string)),
		]);
		
		foreach (var code in Enum.GetValues<TypeCode>())
		{
			// These are not supported as stored column types.
			if (code == TypeCode.Unspecified || code == TypeCode.Struct)
			{
				continue;
			}
			// TODO: Add arrays
			if (code == TypeCode.Array)
			{
				continue;
			}

			var clrType = TypeConversion.GetSystemType(code);
			var clrTypeName = clrType.ToString();
			// Both STRING and JSON are mapped to System.String. STRING is the best match.
			// Both ENUM and INT64 are mapped to System.Int64. INT64 is the best match.
			// Both PROTO and BYTES are mapped to System.Byte[]. BYTES is the best match.
			var isBestMatch = code != TypeCode.Json && code != TypeCode.Enum && code != TypeCode.Proto;
			var dataTypeName = code.ToString();
			var isAutoIncrementable = code == TypeCode.Int64;
			var isFixedLength = code != TypeCode.String && code != TypeCode.Bytes && code != TypeCode.Json && code != TypeCode.Proto;
			var createFormat = isFixedLength
				? dataTypeName
				: dataTypeName + "({0})";
			var createParameters = isFixedLength ? "" : "length";
			var isFixedPrecisionScale = isFixedLength;
			var isLong = false;
			var columnSize = 0;
			var isCaseSensitive = code == TypeCode.String || code == TypeCode.Json;
			var isSearchableWithLike = code == TypeCode.String;
			object isUnsigned = code == TypeCode.Int64 || code == TypeCode.Float32 || code == TypeCode.Float64 ||
			                    code == TypeCode.Numeric ? false : DBNull.Value;
			var literalPrefix = $" {code.ToString()}";

			var row = dataTable.NewRow();
			row["TypeName"] = dataTypeName;
			row["ProviderDbType"] = (int) code;
			row["ColumnSize"] = columnSize;
			row["CreateFormat"] = createFormat;
			row["CreateParameters"] = createParameters;
			row["DataType"] = clrTypeName;
			row["IsAutoIncrementable"] = isAutoIncrementable;
			row["IsBestMatch"] = isBestMatch;
			row["IsCaseSensitive"] = isCaseSensitive;
			row["IsFixedLength"] = isFixedLength;
			row["IsFixedPrecisionScale"] = isFixedPrecisionScale;
			row["IsLong"] = isLong;
			row["IsNullable"] = true;
			row["IsSearchable"] = true;
			row["IsSearchableWithLike"] = isSearchableWithLike;
			row["IsUnsigned"] = isUnsigned;
			row["MaximumScale"] = DBNull.Value;
			row["MinimumScale"] = DBNull.Value;
			row["IsConcurrencyType"] = false;
			row["IsLiteralSupported"] = true;
			row["LiteralPrefix"] = literalPrefix;
			row["LiteralSuffix"] = DBNull.Value;
			row["NativeDataType"] = DBNull.Value;
			
			dataTable.Rows.Add(row);
		}
	}
	private static void FillRestrictions(DataTable dataTable, string?[]? restrictionValues)
	{
		GaxPreconditions.CheckArgument(restrictionValues == null || restrictionValues.Length == 0, nameof(restrictionValues), "restrictionValues is not supported for schema 'Restrictions'.");

		dataTable.TableName = DbMetaDataCollectionNames.Restrictions;
		dataTable.Columns.AddRange(
		[
			new("CollectionName", typeof(string)),
			new("RestrictionName", typeof(string)),
			new("RestrictionDefault", typeof(string)),
			new("RestrictionNumber", typeof(int)),
		]);

		dataTable.Rows.Add("Columns", "Catalog", "TABLE_CATALOG", 1);
		dataTable.Rows.Add("Columns", "Schema", "TABLE_SCHEMA", 2);
		dataTable.Rows.Add("Columns", "Table", "TABLE_NAME", 3);
		dataTable.Rows.Add("Columns", "Column", "COLUMN_NAME", 4);
		dataTable.Rows.Add("Tables", "Catalog", "TABLE_CATALOG", 1);
		dataTable.Rows.Add("Tables", "Schema", "TABLE_SCHEMA", 2);
		dataTable.Rows.Add("Tables", "Table", "TABLE_NAME", 3);
		dataTable.Rows.Add("Tables", "TableType", "TABLE_TYPE", 4);
		dataTable.Rows.Add("Foreign Keys", "Catalog", "TABLE_CATALOG", 1);
		dataTable.Rows.Add("Foreign Keys", "Schema", "TABLE_SCHEMA", 2);
		dataTable.Rows.Add("Foreign Keys", "Table", "TABLE_NAME", 3);
		dataTable.Rows.Add("Foreign Keys", "Constraint Name", "CONSTRAINT_NAME", 4);
		dataTable.Rows.Add("Indexes", "Catalog", "TABLE_CATALOG", 1);
		dataTable.Rows.Add("Indexes", "Schema", "TABLE_SCHEMA", 2);
		dataTable.Rows.Add("Indexes", "Table", "TABLE_NAME", 3);
		dataTable.Rows.Add("Indexes", "Name", "INDEX_NAME", 4);
		dataTable.Rows.Add("IndexColumns", "Catalog", "TABLE_CATALOG", 1);
		dataTable.Rows.Add("IndexColumns", "Schema", "TABLE_SCHEMA", 2);
		dataTable.Rows.Add("IndexColumns", "Table", "TABLE_NAME", 3);
		dataTable.Rows.Add("IndexColumns", "Name", "INDEX_NAME", 4);
		dataTable.Rows.Add("IndexColumns", "Column", "COLUMN_NAME", 5);
	}

	private static void FillReservedWords(DataTable dataTable, string?[]? restrictionValues)
	{
		GaxPreconditions.CheckArgument(restrictionValues == null || restrictionValues.Length == 0, nameof(restrictionValues), "restrictionValues is not supported for schema 'ReservedWords'.");
		
		dataTable.TableName = DbMetaDataCollectionNames.ReservedWords;
		dataTable.Columns.AddRange(
		[
			new("ReservedWord", typeof(string)),
		]);
		
		var keywords = new []
		{
			"ALL",
			"AND",
			"ANY",
			"ARRAY",
			"AS",
			"ASC",
			"AT",
			"BETWEEN",
			"BY",
			"CASE",
			"CAST",
			"CHECK",
			"COLUMN",
			"COMMIT",
			"CONSTRAINT",
			"CREATE",
			"CROSS",
			"CUBE",
			"CURRENT",
			"DEFAULT",
			"DELETE",
			"DESC",
			"DESCENDING",
			"DISTINCT",
			"DROP",
			"ELSE",
			"END",
			"ESCAPE",
			"EXCEPT",
			"EXISTS",
			"FALSE",
			"FETCH",
			"FOLLOWING",
			"FOR",
			"FOREIGN",
			"FROM",
			"FULL",
			"GROUP",
			"GROUPING",
			"HAVING",
			"IN",
			"INNER",
			"INSERT",
			"INTERSECT",
			"INTERVAL",
			"INTO",
			"IS",
			"JOIN",
			"LEFT",
			"LIKE",
			"LIMIT",
			"NOT",
			"NULL",
			"ON",
			"OR",
			"ORDER",
			"OUTER",
			"PARTITION",
			"PRECEDING",
			"PRIMARY",
			"REFERENCES",
			"RIGHT",
			"ROLLUP",
			"ROW",
			"ROWS",
			"SELECT",
			"SET",
			"SOME",
			"TABLE",
			"THEN",
			"TO",
			"TRUE",
			"UNBOUNDED",
			"UNION",
			"UNNEST",
			"UPDATE",
			"USING",
			"VALUES",
			"WHEN",
			"WHERE",
			"WINDOW",
			"WITH"
		};
		foreach (string word in keywords)
		{
			dataTable.Rows.Add(word);
		}
	}
}
