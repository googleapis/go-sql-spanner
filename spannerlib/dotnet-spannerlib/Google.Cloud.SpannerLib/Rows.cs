using System;
using Google.Cloud.Spanner.V1;
using Google.Protobuf.WellKnownTypes;

namespace Google.Cloud.SpannerLib
{

    public class Rows : AbstractLibObject
    {
        private Lazy<ResultSetStats?> _stats;

        internal Connection Connection { get; private set; }
        internal long Id { get; }

        public ResultSetMetadata? Metadata { get; }

        private ResultSetStats? Stats => _stats.Value;

        public long UpdateCount
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

        internal Rows(Connection connection, long id)
        {
            Connection = connection;
            Id = id;
            Metadata = Spanner.Metadata(this);
            _stats = new(() => Spanner.Stats(this));
        }

        public ListValue? Next()
        {
            var res = Spanner.Next(this);
            if (res == null && !_stats.IsValueCreated)
            {
                // initialize stats.
                _ = _stats.Value;
            }

            return res;
        }

        protected override void CloseLibObject()
        {
            Spanner.CloseRows(this);
        }
    }
}