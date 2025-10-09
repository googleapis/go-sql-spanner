using System.Data.Common;

namespace Google.Cloud.Spanner.DataProvider.Benchmarks.tpcc;

public class RowNotFoundException(string message) : DbException(message);