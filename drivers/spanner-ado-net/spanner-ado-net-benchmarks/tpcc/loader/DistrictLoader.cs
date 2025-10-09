using Google.Cloud.Spanner.V1;
using Google.Protobuf.WellKnownTypes;

namespace Google.Cloud.Spanner.DataProvider.Benchmarks.tpcc.loader;

internal class DistrictLoader
{
    private readonly SpannerConnection _connection;

    private readonly int _warehouseCount;
    
    private readonly int _districtsPerWarehouse;

    internal DistrictLoader(SpannerConnection connection, int warehouseCount, int districtsPerWarehouse)
    {
        _connection = connection;
        _warehouseCount = warehouseCount;
        _districtsPerWarehouse = districtsPerWarehouse;
    }

    internal async Task LoadAsync(CancellationToken cancellationToken = default)
    {
        var count = await CountAsync(cancellationToken);
        if (count >= _warehouseCount * _districtsPerWarehouse)
        {
            return;
        }
        for (var warehouse = 0; warehouse < _warehouseCount; warehouse++)
        {
            var group = new BatchWriteRequest.Types.MutationGroup
            {
                Mutations = { Capacity = 1 }
            };
            group.Mutations.Add(CreateMutation(warehouse, _districtsPerWarehouse));
            await _connection.WriteMutationsAsync(group, cancellationToken);
        }
    }

    private async Task<long> CountAsync(CancellationToken cancellationToken = default)
    {
        await using var command = _connection.CreateCommand();
        command.CommandText = "SELECT COUNT(1) FROM district";
        var result = await command.ExecuteScalarAsync(cancellationToken);
        return result == null ? 0L : (long) result;
    }

    private Mutation CreateMutation(int warehouse, int rows)
    {
        var mutation = new Mutation
        {
            InsertOrUpdate = new Mutation.Types.Write
            {
                Table = "district",
                Columns = { "d_id", "w_id", "d_name", "d_street_1", "d_street_2", "d_city", "d_state", "d_zip", "d_tax", "d_ytd" },
                Values =
                {
                    Capacity = _districtsPerWarehouse,
                }
            }
        };
        for (var i = 0; i < rows; i++)
        {
            mutation.InsertOrUpdate.Values.Add(CreateRandomDistrict(warehouse, i));
        }
        return mutation;
    }

    private ListValue CreateRandomDistrict(int warehouse, int index)
    {
        var row = new ListValue
        {
            Values =
            {
                Capacity = 10
            }
        };
        row.Values.Add(Value.ForString($"{DataLoader.ReverseBitsUnsigned((ulong) index)}"));
        row.Values.Add(Value.ForString($"{DataLoader.ReverseBitsUnsigned((ulong) warehouse)}"));
        row.Values.Add(Value.ForString($"W#{warehouse}D#{index}"));
        row.Values.Add(Value.ForString(DataLoader.RandomString(20)));
        row.Values.Add(Value.ForString(DataLoader.RandomString(20)));
        row.Values.Add(Value.ForString(DataLoader.RandomString(20)));
        row.Values.Add(Value.ForString(DataLoader.RandomString(2)));
        row.Values.Add(Value.ForString(DataLoader.RandomString(9)));
        row.Values.Add(Value.ForString(DataLoader.RandomDecimal(0, 21)));
        row.Values.Add(Value.ForString("0.0"));
        return row;
    }
}