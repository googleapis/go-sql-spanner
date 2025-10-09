using Google.Cloud.Spanner.V1;
using Google.Protobuf.WellKnownTypes;

namespace Google.Cloud.Spanner.DataProvider.Benchmarks.tpcc.loader;

internal class StockLoader
{
    private static readonly int RowsPerGroup = 1000;
    
    private readonly SpannerConnection _connection;

    private readonly int _warehouseCount;
    
    private readonly int _numItems;

    internal StockLoader(SpannerConnection connection, int warehouseCount, int numItems)
    {
        _connection = connection;
        _warehouseCount = warehouseCount;
        _numItems = numItems;
    }

    internal async Task LoadAsync(CancellationToken cancellationToken = default)
    {
        var count = await CountAsync(cancellationToken);
        if (count >= _warehouseCount * _numItems)
        {
            return;
        }
        for (var warehouse = 0; warehouse < _warehouseCount; warehouse++)
        {
            for (var item=0; item<_numItems; item += RowsPerGroup)
            {
                var group = new BatchWriteRequest.Types.MutationGroup
                {
                    Mutations = { Capacity = 1 }
                };
                group.Mutations.Add(CreateMutation(warehouse, item, RowsPerGroup));
                await _connection.WriteMutationsAsync(group, cancellationToken);
            }
        }
    }

    private async Task<long> CountAsync(CancellationToken cancellationToken = default)
    {
        await using var command = _connection.CreateCommand();
        command.CommandText = "SELECT COUNT(1) FROM stock";
        var result = await command.ExecuteScalarAsync(cancellationToken);
        return result == null ? 0L : (long) result;
    }

    private Mutation CreateMutation(int warehouse, int item, int rows)
    {
        var mutation = new Mutation
        {
            InsertOrUpdate = new Mutation.Types.Write
            {
                Table = "stock",
                Columns = { "s_i_id", "w_id", "s_quantity", "s_dist_01", "s_dist_02", "s_dist_03", "s_dist_04", "s_dist_05",
                    "s_dist_06", "s_dist_07", "s_dist_08",  "s_dist_09", "s_dist_10", "s_ytd", "s_order_cnt", "s_remote_cnt", "s_data" },
                Values =
                {
                    Capacity = _numItems,
                }
            }
        };
        for (var i = 0; i < rows; i++)
        {
            mutation.InsertOrUpdate.Values.Add(CreateRandomStock(warehouse, item, i));
        }
        return mutation;
    }

    private ListValue CreateRandomStock(int warehouse, int item, int index)
    {
        var row = new ListValue
        {
            Values =
            {
                Capacity = 10
            }
        };
        // s_i_id int not null,
        // w_id int not null,
        // s_quantity int,
        // s_dist_01 varchar(24),
        // s_dist_02 varchar(24),
        // s_dist_03 varchar(24),
        // s_dist_04 varchar(24),
        // s_dist_05 varchar(24),
        // s_dist_06 varchar(24),
        // s_dist_07 varchar(24),
        // s_dist_08 varchar(24),
        // s_dist_09 varchar(24),
        // s_dist_10 varchar(24),
        // s_ytd decimal,
        // s_order_cnt int,
        // s_remote_cnt int,
        // s_data varchar(50),
        row.Values.Add(Value.ForString($"{DataLoader.ReverseBitsUnsigned((ulong) (item + index))}"));
        row.Values.Add(Value.ForString($"{DataLoader.ReverseBitsUnsigned((ulong) warehouse)}"));
        row.Values.Add(Value.ForString(Random.Shared.Next(1, 500).ToString()));
        row.Values.Add(Value.ForString(DataLoader.RandomString(24)));
        row.Values.Add(Value.ForString(DataLoader.RandomString(24)));
        row.Values.Add(Value.ForString(DataLoader.RandomString(24)));
        row.Values.Add(Value.ForString(DataLoader.RandomString(24)));
        row.Values.Add(Value.ForString(DataLoader.RandomString(24)));
        row.Values.Add(Value.ForString(DataLoader.RandomString(24)));
        row.Values.Add(Value.ForString(DataLoader.RandomString(24)));
        row.Values.Add(Value.ForString(DataLoader.RandomString(24)));
        row.Values.Add(Value.ForString(DataLoader.RandomString(24)));
        row.Values.Add(Value.ForString(DataLoader.RandomString(24)));
        row.Values.Add(Value.ForString("0.0"));
        row.Values.Add(Value.ForString("0"));
        row.Values.Add(Value.ForString("0"));
        row.Values.Add(Value.ForString(DataLoader.RandomString(50)));
        return row;
    }
}