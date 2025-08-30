using System;

namespace Google.Cloud.SpannerLib;

public class Pool : AbstractLibObject
{
    private static readonly bool DefaultUseNativeLib = true;
    private static readonly string? UseNativeLibEnvVar = Environment.GetEnvironmentVariable("USE_NATIVE_LIB");
    private static readonly bool UseNativeLib =
        UseNativeLibEnvVar?.Equals("true", StringComparison.InvariantCultureIgnoreCase) ?? DefaultUseNativeLib;

    private static Lazy<ISpanner> _spanner = new(CreateSpanner);

    private static ISpanner CreateSpanner()
    {
        ISpanner spanner;
        if (UseNativeLib)
        {
            spanner = new SharedLibSpanner();
        }
        else
        {
            spanner = new GrpcLibSpanner();
        }
        return spanner;
    }

    public static Pool Create(string dsn)
    {
        return Create(_spanner.Value, dsn);
    }

    public static Pool Create(ISpanner spanner, string dsn)
    {
        return spanner.CreatePool(dsn);
    }

    internal Pool(ISpanner spanner, long id) : base(spanner, id)
    {
    }

    public Connection CreateConnection()
    {
        CheckDisposed();
        return Spanner.CreateConnection(this);
    }

    protected override void CloseLibObject()
    {
        Spanner.ClosePool(this);
    }
}