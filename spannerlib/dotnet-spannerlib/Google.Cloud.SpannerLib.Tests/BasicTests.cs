namespace Google.Cloud.SpannerLib.Tests;

public class BasicTests
{
    [SetUp]
    public void Setup()
    {
    }

    [Test]
    public void TestCreatePool()
    {
        var pool = Spanner.CreatePool("projects/my-project/instances/my-instance/databases/my-database");
        Spanner.ClosePool(pool);
    }
}
