namespace Google.Cloud.SpannerLib
{

    internal class LibPool
    {
        internal long Id { get; }

        internal LibPool(long id)
        {
            Id = id;
        }

        public void Close()
        {
            Spanner.ClosePool(this);
        }
    }
}