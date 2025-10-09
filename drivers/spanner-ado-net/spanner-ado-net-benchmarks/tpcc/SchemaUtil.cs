using Google.Cloud.Spanner.Admin.Database.V1;

namespace Google.Cloud.Spanner.DataProvider.Benchmarks.tpcc;

static class SchemaUtil
{
    internal static async Task CreateSchemaAsync(SpannerConnection connection, DatabaseDialect dialect, CancellationToken cancellationToken)
    {
        await using var cmd =  connection.CreateCommand();
        cmd.CommandText = "select count(1) "
                          + "from information_schema.tables "
                          + "where "
                          + (dialect == DatabaseDialect.Postgresql ? "table_schema='public' and " : "table_schema='' and ")
                          + "table_name in ('warehouse', 'district', 'customer', 'history', 'orders', 'new_orders', 'order_line', 'stock', 'item')";
        var count = await cmd.ExecuteScalarAsync(cancellationToken);
        if (count is long and 9)
        {
            return;
        }

        var commands = SchemaDefinition.CreateTablesPostgreSql.Split(";");
        foreach (var command in commands)
        {
            if (command.Trim() == "")
            {
                continue;
            }
            cmd.CommandText = command;
            await cmd.ExecuteNonQueryAsync(cancellationToken);
        }
    }

    internal static async Task DropSchemaAsync(SpannerConnection connection, CancellationToken cancellationToken)
    {
        await using var cmd = connection.CreateCommand();
        var commands = SchemaDefinition.DropTables.Split(";");
        foreach (var command in commands)
        {
            if (command.Trim() == "")
            {
                continue;
            }
            cmd.CommandText = command;
            await cmd.ExecuteNonQueryAsync(cancellationToken);
        }
    }
}