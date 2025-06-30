using System;

namespace Google.Cloud.SpannerLib
{
    public abstract class AbstractLibObject : IDisposable
    {
        private bool _disposed;

        protected void CheckDisposed()
        {
            if (_disposed)
            {
                throw new ObjectDisposedException(GetType().Name);
            }
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
                CloseLibObject();
            }
            finally
            {
                _disposed = true;
            }
        }

        protected abstract void CloseLibObject();
    }
}