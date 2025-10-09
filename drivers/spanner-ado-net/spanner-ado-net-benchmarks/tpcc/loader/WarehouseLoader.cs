using Google.Cloud.Spanner.V1;
using Google.Protobuf.WellKnownTypes;

namespace Google.Cloud.Spanner.DataProvider.Benchmarks.tpcc.loader;

internal class WarehouseLoader
{
    private readonly SpannerConnection _connection;
    
    private readonly int _rowCount;

    internal WarehouseLoader(SpannerConnection connection, int rowCount)
    {
        _connection = connection;
        _rowCount = rowCount;
    }

    internal Task<CommitResponse?> LoadAsync(CancellationToken cancellationToken = default)
    {
        return _connection.WriteMutationsAsync(new BatchWriteRequest.Types.MutationGroup
        {
            Mutations = { CreateMutation() }
        }, cancellationToken);
    }

    private Mutation CreateMutation()
    {
        var mutation = new Mutation
        {
            InsertOrUpdate = new Mutation.Types.Write
            {
                Table = "warehouse",
                Columns = { "w_id", "w_name", "w_street_1", "w_street_2", "w_city", "w_state", "w_zip", "w_tax", "w_ytd" },
                Values =
                {
                    Capacity = _rowCount,
                }
            }
        };
        for (var i = 0; i < _rowCount; i++)
        {
            mutation.InsertOrUpdate.Values.Add(CreateRandomWarehouse(i));
        }
        return mutation;
    }

    private ListValue CreateRandomWarehouse(int index)
    {
        var row = new ListValue
        {
            Values =
            {
                Capacity = 9
            }
        };
        row.Values.Add(Value.ForString($"{DataLoader.ReverseBitsUnsigned((ulong) index)}"));
        row.Values.Add(Value.ForString($"W#{index}"));
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