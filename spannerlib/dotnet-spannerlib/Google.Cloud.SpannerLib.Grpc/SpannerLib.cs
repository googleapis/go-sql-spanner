using System;
using System.Diagnostics;
using System.IO;
using System.Net.Http;
using System.Net.Sockets;
using System.Runtime.InteropServices;
using System.Threading;
using Google.Cloud.Spanner.V1;
using Google.Cloud.SpannerLib.V1;
using Google.Protobuf.WellKnownTypes;
using Grpc.Core;
using Grpc.Net.Client;
using BeginTransactionRequest = Google.Cloud.SpannerLib.V1.BeginTransactionRequest;
using ExecuteBatchDmlRequest = Google.Cloud.Spanner.V1.ExecuteBatchDmlRequest;
using Transaction = Google.Cloud.SpannerLib.V1.Transaction;

namespace Google.Cloud.SpannerLib.Grpc
{
    public class SpannerLib : IDisposable
    {
        private readonly Process _process;
        private readonly string _fileName;
        private readonly V1.SpannerLib.SpannerLibClient _client;
        private readonly GrpcChannel _channel;
        
        private bool _disposed;

        public SpannerLib()
        {
            (_fileName, _process) = StartGrpcServer();
            (_client, _channel) = CreateClient(_fileName);
        }
        
        ~SpannerLib()
        {
            Dispose(false);
        }

        private static string GetBinaryFileName()
        {
            if (File.Exists("runtimes/any/native/grpc_server"))
            {
                return "runtimes/any/native/grpc_server";
            }
            if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
            {
            }
            else if (RuntimeInformation.IsOSPlatform(OSPlatform.Linux))
            {
                switch (RuntimeInformation.OSArchitecture)
                {
                    case Architecture.X64:
                        return "runtimes/linux-x64/native/grpc_server";
                }
            }
            else if (RuntimeInformation.IsOSPlatform(OSPlatform.OSX))
            {
                switch (RuntimeInformation.ProcessArchitecture)
                {
                    case Architecture.Arm64:
                        return "runtimes/osx-arm64/native/grpc_server";
                }
            }
            throw new PlatformNotSupportedException();
        }
        
        private static Tuple<string, Process> StartGrpcServer()
        {
            // Generate a random temp file name that will be used for the Unix domain socket communication.
            var unixDomainSocketFileName = Path.GetTempPath() + Guid.NewGuid();
            var binaryFileName = GetBinaryFileName();
            var info = new ProcessStartInfo
            {
                Arguments = unixDomainSocketFileName,
                UseShellExecute = false,
                FileName = binaryFileName,
                RedirectStandardOutput = true,
            };
            // Start the process as a child process. The process will automatically stop when the
            // parent process stops.
            var process = Process.Start(info);
            if (process == null)
            {
                throw new InvalidOperationException("Failed to start spanner");
            }
            while (!File.Exists(unixDomainSocketFileName))
            {
                Thread.Sleep(1);
            }
            // Return the name of the Unix domain socket.
            return Tuple.Create(unixDomainSocketFileName, process);
        }
        
        private static Tuple<V1.SpannerLib.SpannerLibClient, GrpcChannel> CreateClient(string file)
        {
            var channel = ForUnixSocket(file);
            return Tuple.Create(new V1.SpannerLib.SpannerLibClient(channel), channel);
        }
        
        private static GrpcChannel ForUnixSocket(string fileName)
        {
            var endpoint = new UnixDomainSocketEndPoint(fileName);
            return GrpcChannel.ForAddress("http://localhost", new GrpcChannelOptions {
                HttpHandler = new SocketsHttpHandler {
                    EnableMultipleHttp2Connections = true,
                    ConnectCallback = async (_, cancellationToken) => {
                        var socket = new Socket(AddressFamily.Unix, SocketType.Stream, ProtocolType.Unspecified);
                        try {
                            await socket.ConnectAsync(endpoint, cancellationToken).ConfigureAwait(false);
                            return new NetworkStream(socket, true);
                        } catch {
                            socket.Dispose();
                            throw;
                        }
                    }
                }
            });
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
                _channel.Dispose();
                _process.Dispose();
            }
            finally
            {
                _disposed = true;
            }
        }

        private void CheckDisposed()
        {
            if (_disposed)
            {
                throw new ObjectDisposedException(GetType().Name);
            }
        }

        public Pool CreatePool(string dsn)
        {
            CheckDisposed();
            return _client.CreatePool(new CreatePoolRequest { Dsn = dsn });
        }

        public void ClosePool(Pool pool)
        {
            CheckDisposed();
            _client.ClosePool(pool);
        }

        public Connection CreateConnection(Pool pool)
        {
            CheckDisposed();
            return _client.CreateConnection(new CreateConnectionRequest { Pool = pool });
        }

        public void CloseConnection(Connection connection)
        {
            CheckDisposed();
            _client.CloseConnection(connection);
        }

        public CommitResponse Apply(Connection connection, BatchWriteRequest.Types.MutationGroup mutations)
        {
            CheckDisposed();
            return _client.Apply(new ApplyRequest { Connection = connection, Mutations = mutations });
        }

        public void BufferWrite(Transaction transaction, BatchWriteRequest.Types.MutationGroup mutations)
        {
            CheckDisposed();
            _client.BufferWrite(new BufferWriteRequest { Transaction = transaction, Mutations = mutations });
        }

        public Rows Execute(Connection connection, ExecuteSqlRequest statement)
        {
            CheckDisposed();
            return _client.Execute(new ExecuteRequest { Connection = connection, ExecuteSqlRequest = statement });
        }

        public AsyncServerStreamingCall<PartialResultSet> ExecuteStreaming(Connection connection, ExecuteSqlRequest statement)
        {
            CheckDisposed();
            return _client.ExecuteStreaming(new ExecuteRequest { Connection = connection, ExecuteSqlRequest = statement });
        }

        public Rows ExecuteTransaction(Transaction transaction, ExecuteSqlRequest statement)
        {
            CheckDisposed();
            return _client.ExecuteTransaction(new ExecuteTransactionRequest { Transaction = transaction, ExecuteSqlRequest = statement });
        }

        public ExecuteBatchDmlResponse ExecuteBatchDml(Connection connection, ExecuteBatchDmlRequest statements)
        {
            CheckDisposed();
            return _client.ExecuteBatchDml(new V1.ExecuteBatchDmlRequest { Connection = connection, ExecuteBatchDmlRequest_ = statements });
        }

        public ResultSetMetadata Metadata(Rows rows)
        {
            CheckDisposed();
            return _client.Metadata(rows);
        }

        public ListValue Next(Rows rows)
        {
            CheckDisposed();
            return _client.Next(rows);
        }

        public ResultSetStats ResultSetStats(Rows rows)
        {
            CheckDisposed();
            return _client.ResultSetStats(rows);
        }

        public void CloseRows(Rows rows)
        {
            CheckDisposed();
            _client.CloseRows(rows);
        }

        public Transaction BeginTransaction(Connection connection, TransactionOptions transactionOptions)
        {
            CheckDisposed();
            return _client.BeginTransaction(new BeginTransactionRequest
                { Connection = connection, TransactionOptions = transactionOptions });
        }

        public CommitResponse Commit(Transaction transaction)
        {
            CheckDisposed();
            return _client.Commit(transaction);
        }

        public void Rollback(Transaction transaction)
        {
            CheckDisposed();
            _client.Rollback(transaction);
        }

        public AsyncDuplexStreamingCall<ConnectionStreamRequest, ConnectionStreamResponse> CreateStream()
        {
            CheckDisposed();
            return _client.ConnectionStream();
        }
    }
}