namespace Google.Cloud.Spanner.DataProvider.Samples.Snippets;

// Sample showing how to work with the different data types that are supported by Spanner:
// 1. How to set data of each type as a statement parameter.
// 2. How to get data from columns of each type.
public static class DataTypesSample
{
    private const string CreateAllTypesTable = @"
        CREATE TABLE IF NOT EXISTS AllTypes (
            id int64 primary key,
            col_bool bool,
            col_bytes bytes(max),
            col_date date,
            col_float32 float32,
            col_float64 float64,
            col_int64 int64,
            --col_interval interval,
            col_json json,
            col_numeric numeric,
            col_string string(max),
            col_timestamp timestamp,
    )";
    
    public static async Task Run(string connectionString)
    {
        await using var connection = new SpannerConnection(connectionString);
        await connection.OpenAsync();
        
        // Create a table that contains all data types.
        await using var createCommand = connection.CreateCommand();
        createCommand.CommandText = CreateAllTypesTable;
        await createCommand.ExecuteNonQueryAsync();
        
        // Insert a row into the table using query parameters.
        await using var insertCommand = connection.CreateCommand();
        insertCommand.CommandText =
            "insert or update into AllTypes " +
            "(id, col_bool, col_bytes, col_date, col_float32, col_float64, col_int64, /*col_interval,*/ col_json, col_numeric, col_string, col_timestamp) " +
            "values (@id, @bool, @bytes, @date, @float32, @float64, @int64, /*@interval,*/ @json, @numeric, @string, @timestamp)";
        insertCommand.Parameters.AddWithValue("id", 1);
        // Use bool for BOOL values.
        insertCommand.Parameters.AddWithValue("bool", true);
        // Use byte[] for BYTES values.
        insertCommand.Parameters.AddWithValue("bytes", new byte[] { 1, 2, 3 });
        // Use DateOnly for DATE values.
        insertCommand.Parameters.AddWithValue("date", DateOnly.FromDateTime(DateTime.Now));
        // Use float for FLOAT32 values.
        insertCommand.Parameters.AddWithValue("float32", 3.14f);
        // Use double for FLOAT64 values.
        insertCommand.Parameters.AddWithValue("float64", 3.14d);
        // Use long for INT64 values.
        insertCommand.Parameters.AddWithValue("int64", 100L);
        // Use TimeSpan for INTERVAL values.
        // TODO: Enable the following line when the Emulator supports INTERVAL.
        // insertCommand.Parameters.AddWithValue("interval", TimeSpan.FromMinutes(1));
        // Use strings for JSON values.
        insertCommand.Parameters.AddWithValue("json", "{\"key\": \"value\"}");
        // Use decimal for NUMERIC values.
        insertCommand.Parameters.AddWithValue("numeric", 3.14m);
        // Use string for STRING values.
        insertCommand.Parameters.AddWithValue("string", "test-string");
        // Use DateTime for TIMESTAMP values.
        insertCommand.Parameters.AddWithValue("timestamp", DateTime.Now);
        
        long inserted = await insertCommand.ExecuteNonQueryAsync();
        Console.WriteLine($"Inserted: {inserted}");
        
        // Retrieve the row that was just inserted.
        await using var selectCommand = connection.CreateCommand("select * from AllTypes order by id");
        await using var reader = await selectCommand.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            for (var col = 0; col < reader.FieldCount; col++)
            {
                Console.WriteLine($"{reader.GetName(col)}: {reader.GetValue(col)}");
            }
        }
    }

}