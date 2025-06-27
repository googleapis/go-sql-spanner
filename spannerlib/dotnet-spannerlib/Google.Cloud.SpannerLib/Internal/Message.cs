namespace Google.Cloud.SpannerLib.Internal
{
    internal unsafe struct Message
    {
#pragma warning disable CS0649 // Field is never assigned to, and will always have its default value
        public long Pinner;
        public int Code;
        public long ObjectId;
        public int Length;
        public void* Pointer;
#pragma warning restore CS0649 // Field is never assigned to, and will always have its default value
    }
}