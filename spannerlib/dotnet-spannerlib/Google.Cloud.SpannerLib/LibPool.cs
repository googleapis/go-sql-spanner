namespace Google.Cloud.SpannerLib
{

    public class LibPool
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