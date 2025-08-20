using System;

namespace Google.Cloud.SpannerLib
{
    public abstract class AbstractLibObject : IDisposable
    {
        internal ISpanner Spanner { get; }
        internal long Id { get; }
        
        private bool _disposed;

        protected void CheckDisposed()
        {
            if (_disposed)
            {
                throw new ObjectDisposedException(GetType().Name);
            }
        }

        internal AbstractLibObject(ISpanner spanner, long id)
        {
            Spanner = spanner;
            Id = id;
        } 

        ~AbstractLibObject()
        {
            Dispose(false);
        }

        protected void MarkDisposed()
        {
            _disposed = true;
        }

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }
        
        public void Close()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }
        
        protected virtual void Dispose(bool disposing)
        {
            if (_disposed)
            {
                return;
            }
            try
            {
                if (Id > 0)
                {
                    CloseLibObject();
                }
            }
            finally
            {
                _disposed = true;
            }
        }

        protected abstract void CloseLibObject();
    }
}