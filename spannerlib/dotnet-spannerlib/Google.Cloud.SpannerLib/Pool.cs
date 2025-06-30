using System;

namespace Google.Cloud.SpannerLib
{

    public class Pool : AbstractLibObject
    {
        internal long Id { get; }

        public static Pool Create(string dsn)
        {
            return Spanner.CreatePool(dsn);
        }

        internal Pool(long id)
        {
            Id = id;
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
}