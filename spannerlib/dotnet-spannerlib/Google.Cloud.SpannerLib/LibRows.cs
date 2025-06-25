using System;
using Google.Cloud.Spanner.V1;
using Google.Protobuf.WellKnownTypes;

namespace Google.Cloud.SpannerLib
{

    internal class LibRows
    {
        private Lazy<ResultSetStats?> _stats;

        internal LibConnection LibConnection { get; private set; }
        internal long Id { get; }

        internal ResultSetMetadata? Metadata { get; }

        private ResultSetStats? Stats => _stats.Value;

        internal long UpdateCount
        {
            get
            {
                var stats = Stats;
                if (stats == null)
                {
                    return -1;
                }

                if (stats.HasRowCountExact)
                {
                    return (int)stats.RowCountExact;
                }

                if (stats.HasRowCountLowerBound)
                {
                    return (int)stats.RowCountLowerBound;
                }

                return -1;
            }
        }

        internal LibRows(LibConnection libConnection, long id)
        {
            LibConnection = libConnection;
            Id = id;
            Metadata = Spanner.Metadata(this);
            _stats = new(() => Spanner.Stats(this));
        }

        internal ListValue? Next()
        {
            var res = Spanner.Next(this);
            if (res == null && !_stats.IsValueCreated)
            {
                // initialize stats.
                _ = _stats.Value;
            }

            return res;
        }

        internal void Close()
        {
            Spanner.CloseRows(this);
        }
    }
}