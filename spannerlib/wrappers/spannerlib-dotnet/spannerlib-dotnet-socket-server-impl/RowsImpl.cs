using System.Net.Sockets;
using Google.Cloud.Spanner.V1;
using Google.Cloud.SpannerLib.SocketServer.Protocol;
using Google.Protobuf.WellKnownTypes;

namespace Google.Cloud.SpannerLib.SocketServer;

internal class RowsImpl : Rows
{
    internal RowsImpl Create(ConnectionImpl connection)
    {
        var stream = connection.Stream;
        var id = Encoding.ReadLong(stream);
        var metadataBytes = Encoding.ReadBytes(stream);
        var metadata = ResultSetMetadata.Parser.ParseFrom(metadataBytes);
        return new RowsImpl(connection, id, metadata);
    }
    
    private readonly ConnectionImpl _connection;
    
    private readonly NetworkStream _stream;

    public override ResultSetMetadata? Metadata { get; }
    
    private ResultSetStats _stats;

    internal RowsImpl(ConnectionImpl connection, long id, ResultSetMetadata metadata) : base(connection, id)
    {
        _connection = connection;
        _stream = connection.Stream;
        Metadata = metadata;
    }

    public override ListValue? Next()
    {
        var hasMoreRows = Encoding.ReadBool(_stream);
        if (!hasMoreRows)
        {
            
        }
    }

    private ResultSetStats ReadStats()
    {
        var bytes = Encoding.ReadBytes(_stream);
        return ResultSetStats.Parser.ParseFrom(bytes);
    }
}