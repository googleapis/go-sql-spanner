using System.Globalization;
using Google.Cloud.Spanner.V1;
using Google.Protobuf.WellKnownTypes;

namespace Google.Cloud.Spanner.DataProvider.Benchmarks.tpcc.loader;

internal class CustomerLoader
{
    private readonly SpannerConnection _connection;

    private readonly int _warehouseCount;
    
    private readonly int _districtsPerWarehouse;

    private readonly int _customersPerDistrict;

    internal CustomerLoader(SpannerConnection connection, int warehouseCount, int districtsPerWarehouse, int customersPerDistrict)
    {
        _connection = connection;
        _warehouseCount = warehouseCount;
        _districtsPerWarehouse = districtsPerWarehouse;
        _customersPerDistrict = customersPerDistrict;
    }

    internal async Task LoadAsync(CancellationToken cancellationToken = default)
    {
        var count = await CountAsync(cancellationToken);
        if (count >= _warehouseCount * _districtsPerWarehouse * _customersPerDistrict)
        {
            return;
        }
        
        for (var warehouse = 0; warehouse < _warehouseCount; warehouse++)
        {
            for (var district = 0; district < _districtsPerWarehouse; district++)
            {
                var group = new BatchWriteRequest.Types.MutationGroup
                {
                    Mutations = { Capacity = 1 }
                };
                group.Mutations.Add(CreateMutation(warehouse, district, _customersPerDistrict));
                await _connection.WriteMutationsAsync(group, cancellationToken);
            }
        }
    }

    private async Task<long> CountAsync(CancellationToken cancellationToken = default)
    {
        await using var command = _connection.CreateCommand();
        command.CommandText = "SELECT COUNT(1) FROM customer";
        var result = await command.ExecuteScalarAsync(cancellationToken);
        return result == null ? 0L : (long) result;
    }

    private Mutation CreateMutation(int warehouse, int district, int rows)
    {
        var mutation = new Mutation
        {
            InsertOrUpdate = new Mutation.Types.Write
            {
                Table = "customer",
                Columns = { "c_id", "d_id", "w_id", "c_first", "c_middle", "c_last", "c_street_1", "c_street_2",
                    "c_city", "c_state", "c_zip", "c_phone", "c_since", "c_credit", "c_credit_lim", "c_discount",
                    "c_balance", "c_ytd_payment", "c_payment_cnt", "c_delivery_cnt", "c_data",
                },
                Values =
                {
                    Capacity = _customersPerDistrict,
                }
            }
        };
        for (var i = 0; i < rows; i++)
        {
            mutation.InsertOrUpdate.Values.Add(CreateRandomCustomer(warehouse, district, i));
        }
        return mutation;
    }

    private ListValue CreateRandomCustomer(int warehouse, int district, int index)
    {
        var row = new ListValue
        {
            Values =
            {
                Capacity = 22
            }
        };
        row.Values.Add(Value.ForString($"{DataLoader.ReverseBitsUnsigned((ulong) index)}"));
        row.Values.Add(Value.ForString($"{DataLoader.ReverseBitsUnsigned((ulong) district)}"));
        row.Values.Add(Value.ForString($"{DataLoader.ReverseBitsUnsigned((ulong) warehouse)}"));
        row.Values.Add(Value.ForString(DataLoader.RandomString(16)));
        row.Values.Add(Value.ForString(DataLoader.RandomString(2)));
        row.Values.Add(Value.ForString(LastNameGenerator.GenerateLastName(index)));
        row.Values.Add(Value.ForString(DataLoader.RandomString(20)));
        row.Values.Add(Value.ForString(DataLoader.RandomString(20)));
        row.Values.Add(Value.ForString(DataLoader.RandomString(20)));
        row.Values.Add(Value.ForString(DataLoader.RandomString(2)));
        row.Values.Add(Value.ForString(DataLoader.RandomString(9)));
        row.Values.Add(Value.ForString(DataLoader.RandomString(16)));
        row.Values.Add(Value.ForString(DataLoader.RandomTimestamp()));
        row.Values.Add(Value.ForString(Random.Shared.Next(2) == 0 ? "GC" : "BC"));
        row.Values.Add(Value.ForString(Random.Shared.Next(100, 5000).ToString(CultureInfo.InvariantCulture)));
        row.Values.Add(Value.ForString(DataLoader.RandomDecimal(1, 40)));
        row.Values.Add(Value.ForString("0.0"));
        row.Values.Add(Value.ForString("0.0"));
        row.Values.Add(Value.ForString("0"));
        row.Values.Add(Value.ForString("0"));
        row.Values.Add(Value.ForString(DataLoader.RandomString(500)));
        
        return row;
    }
}