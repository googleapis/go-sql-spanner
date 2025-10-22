namespace Google.Cloud.SpannerLib.Tests;

public static class TestUtils
{
    private const string Chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";

    public static string GenerateRandomString(int length)
    {
        return new string(Enumerable.Repeat(Chars, length)
            .Select(s => s[Random.Shared.Next(s.Length)]).ToArray());
    }
}
