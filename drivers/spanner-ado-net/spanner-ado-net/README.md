# Spanner ADO.NET Data Provider

__ALPHA: This library is still in development. It is not yet ready for production use.__

ADO.NET Data Provider for Spanner. This library implements the standard ADO.NET interfaces and classes
and exposes an API that is similar to ADO.NET data providers for other relational database systems.

## Usage

Create a connection string using a `SpannerConnectionStringBuilder`:

```csharp
var builder = new SpannerConnectionStringBuilder
{
    Project = "my-project",
    Instance = "my-instance",
    Database = "my-database",
    DefaultIsolationLevel = IsolationLevel.ReadCommitted,
};
await using var connection = new SpannerConnection(builder.ConnectionString);
await connection.OpenAsync();
await using var command = connection.CreateCommand();
command.CommandText = "SELECT 'Hello World' as Message";
await using var reader = await command.ExecuteReaderAsync();
while (await reader.ReadAsync())
{
    Console.WriteLine($"Greeting from Spanner: {reader.GetString(0)}");
}
```

### Emulator

The driver can also connect to the Spanner Emulator. The easiest way to do this is to set the `AutoConfigEmulator`
property to true. This instructs the driver to connect to the Emulator on `localhost:9010` and to automatically
create the Spanner instance and database in the connection string if these do not already exist.

```csharp
var builder = new SpannerConnectionStringBuilder
{
    Project = "my-project",
    Instance = "my-instance",
    Database = "my-database",
    DefaultIsolationLevel = IsolationLevel.ReadCommitted,
    // Setting AutoConfigEmulator=true instructs the driver to connect to the Spanner emulator on 'localhost:9010',
    // and to create the instance and database on the emulator if these do not already exist.
    AutoConfigEmulator = true,
};
await using var connection = new SpannerConnection(builder.ConnectionString);
await connection.OpenAsync();
await using var command = connection.CreateCommand();
command.CommandText = "SELECT 'Hello World' as Message";
await using var reader = await command.ExecuteReaderAsync();
while (await reader.ReadAsync())
{
    Console.WriteLine($"Greeting from Spanner: {reader.GetString(0)}");
}
```

## Examples

See the [spanner-ado-net-samples](../spanner-ado-net-samples/Snippets) project for ready-to-run examples for how to use
various Spanner features with this driver.
