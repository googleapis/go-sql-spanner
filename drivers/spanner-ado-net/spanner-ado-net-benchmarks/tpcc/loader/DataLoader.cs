using System.Globalization;
using System.Xml;

namespace Google.Cloud.Spanner.DataProvider.Benchmarks.tpcc.loader;

public class DataLoader
{
    private readonly SpannerConnection _connection;
    private readonly int _numWarehouses;
    private readonly int _numDistrictsPerWarehouse;
    private readonly int _numCustomersPerDistrict;
    private readonly int _numItems;
    
    public DataLoader(
        SpannerConnection connection,
        int numWarehouses,
        int numDistrictsPerWarehouse = 10,
        int numCustomersPerDistrict = 3000,
        int numItems = 100_000)
    {
        _connection = connection;
        _numWarehouses = numWarehouses;
        _numDistrictsPerWarehouse = numDistrictsPerWarehouse;
        _numCustomersPerDistrict = numCustomersPerDistrict;
        _numItems = numItems;
    }

    public async Task LoadAsync(CancellationToken cancellationToken)
    {
        Console.WriteLine("Loading warehouses...");
        var warehouseLoader = new WarehouseLoader(_connection, _numWarehouses);
        await warehouseLoader.LoadAsync(cancellationToken);
        Console.WriteLine("Loading items...");
        var itemLoader = new ItemLoader(_connection, _numItems);
        await  itemLoader.LoadAsync(cancellationToken);
        Console.WriteLine("Loading districts...");
        var districtLoader = new DistrictLoader(_connection, _numWarehouses, _numDistrictsPerWarehouse);
        await districtLoader.LoadAsync(cancellationToken);
        Console.WriteLine("Loading customers...");
        var customerLoader = new CustomerLoader(_connection, _numWarehouses, _numDistrictsPerWarehouse, _numCustomersPerDistrict);
        await customerLoader.LoadAsync(cancellationToken);
        Console.WriteLine("Loading stock...");
        var stockLoader = new StockLoader(_connection, _numWarehouses, _numItems);
        await stockLoader.LoadAsync(cancellationToken);
    }
    
    public static long ReverseBitsUnsigned(ulong n)
    {
        // Step 1: Swap adjacent bits
        n = ((n >> 1) & 0x5555555555555555UL) | ((n & 0x5555555555555555UL) << 1);
        // Step 2: Swap adjacent pairs of bits
        n = ((n >> 2) & 0x3333333333333333UL) | ((n & 0x3333333333333333UL) << 2);
        // Step 3: Swap adjacent nibbles (4 bits)
        n = ((n >> 4) & 0x0F0F0F0F0F0F0F0FUL) | ((n & 0x0F0F0F0F0F0F0F0FUL) << 4);
        // Step 4: Swap adjacent bytes
        n = ((n >> 8) & 0x00FF00FF00FF00FFUL) | ((n & 0x00FF00FF00FF00FFUL) << 8);
        // Step 5: Swap adjacent 2-byte words
        n = ((n >> 16) & 0x0000FFFF0000FFFFUL) | ((n & 0x0000FFFF0000FFFFUL) << 16);
        // Step 6: Swap the high and low 4-byte words (32 bits)
        n = (n >> 32) | (n << 32);
        return (long) n;
    }
    
    internal static string RandomString(int length)
    {
        const string chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
        return new string(Enumerable.Repeat(chars, length)
            .Select(s => s[Random.Shared.Next(s.Length)]).ToArray());
    }

    internal static string RandomDecimal(int min, int max)
    {
        var d = (decimal) Random.Shared.Next(min, max) / 100;
        return d.ToString("F", CultureInfo.InvariantCulture);
    }

    internal static string RandomTimestamp()
    {
        var ts = DateTime.UtcNow.AddTicks(-Random.Shared.NextInt64(10 * 365 * TimeSpan.TicksPerDay));
        return XmlConvert.ToString(Convert.ToDateTime(ts, CultureInfo.InvariantCulture),
            XmlDateTimeSerializationMode.Utc);
    }
}
