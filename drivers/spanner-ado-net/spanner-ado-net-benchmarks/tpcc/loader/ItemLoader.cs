using Google.Cloud.Spanner.V1;
using Google.Protobuf.WellKnownTypes;

namespace Google.Cloud.Spanner.DataProvider.Benchmarks.tpcc.loader;

internal class ItemLoader
{
    private static readonly int RowsPerGroup = 1000;
    
    private readonly SpannerConnection _connection;
    
    private readonly int _rowCount;

    internal ItemLoader(SpannerConnection connection, int rowCount)
    {
        _connection = connection;
        _rowCount = rowCount;
    }

    internal async Task LoadAsync(CancellationToken cancellationToken = default)
    {
        var count = await CountAsync(cancellationToken);
        if (count >= _rowCount)
        {
            return;
        }
        
        var batch = 0;
        var remaining = _rowCount;
        while (remaining > 0)
        {
            var group = new BatchWriteRequest.Types.MutationGroup
            {
                Mutations = { Capacity = 1 }
            };
            var rows = Math.Min(RowsPerGroup, remaining);
            group.Mutations.Add(CreateMutation(batch, rows));
            await _connection.WriteMutationsAsync(group, cancellationToken);
            remaining -= rows;
            batch++;
        }
    }

    private async Task<long> CountAsync(CancellationToken cancellationToken = default)
    {
        await using var command = _connection.CreateCommand();
        command.CommandText = "SELECT COUNT(1) FROM item";
        var result = await command.ExecuteScalarAsync(cancellationToken);
        return result == null ? 0L : (long) result;
    }

    private Mutation CreateMutation(int batch, int rows)
    {
        var mutation = new Mutation
        {
            InsertOrUpdate = new Mutation.Types.Write
            {
                Table = "item",
                Columns = { "i_id", "i_im_id", "i_name", "i_price", "i_data" },
                Values =
                {
                    Capacity = _rowCount,
                }
            }
        };
        for (var i = 0; i < rows; i++)
        {
            mutation.InsertOrUpdate.Values.Add(CreateRandomItem(batch, i));
        }
        return mutation;
    }

    private ListValue CreateRandomItem(int batch, int index)
    {
        var row = new ListValue
        {
            Values =
            {
                Capacity = 5
            }
        };
        var id = (long)batch * RowsPerGroup + index;
        row.Values.Add(Value.ForString($"{DataLoader.ReverseBitsUnsigned((ulong) id)}"));
        row.Values.Add(Value.ForString($"{Random.Shared.Next(1, 2000001)}"));
        row.Values.Add(Value.ForString(DataLoader.RandomString(24)));
        row.Values.Add(Value.ForString(DataLoader.RandomDecimal(100, 10001)));
        row.Values.Add(Value.ForString(DataLoader.RandomString(50)));
        return row;
    }
}