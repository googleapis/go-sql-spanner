using System;

namespace Google.Cloud.SpannerLib
{

    public class LibPool : AbstractLibObject
    {
        internal long Id { get; }

        public static LibPool Create(string dsn)
        {
            return Spanner.CreatePool(dsn);
        }

        internal LibPool(long id)
        {
            Id = id;
        }

        public LibConnection CreateConnection()
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