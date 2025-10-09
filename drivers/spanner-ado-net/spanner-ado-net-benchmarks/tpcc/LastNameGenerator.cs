namespace Google.Cloud.Spanner.DataProvider.Benchmarks.tpcc;

public static class LastNameGenerator
{
    private static readonly string[] Parts = ["BAR", "OUGHT", "ABLE", "PRI", "PRES", "ESE", "ANTI", "CALLY", "ATION", "EING"];
    
    public static string GenerateLastName(long rowIndex) {
        int row;
        if (rowIndex < 1000L)
        {
            row = (int) rowIndex;
        }
        else
        {
            row = Random.Shared.Next(1000);
        }
        return Parts[row / 100] +
               Parts[row / 10 % 10] +
               Parts[row % 10];
    }

}