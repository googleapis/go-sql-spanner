// Copyright 2025 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Google.Cloud.Spanner.V1;
using Google.Cloud.SpannerLib.V1;
using Google.Rpc;
using Grpc.Core;
using BeginTransactionRequest = Google.Cloud.SpannerLib.V1.BeginTransactionRequest;
using Status = Google.Rpc.Status;

namespace Google.Cloud.SpannerLib.Grpc;

/// <summary>
/// GrpcConnection is a gRPC-specific implementation of a SpannerLib Connection. This class opens a bidirectional
/// gRPC stream to the gRPC server in SpannerLib. This stream can be used to execute multiple statements on Spanner
/// with the lowest possible latency.
/// </summary>
internal class GrpcBidiConnection(GrpcLibSpanner spanner, Pool pool, long id) : Connection(pool, id)
{
    private AsyncDuplexStreamingCall<ConnectionStreamRequest, ConnectionStreamResponse>? _stream;

    /// <summary>
    /// Returns a bidirectional gRPC stream for this connection. The stream is created the first time this property
    /// is used.
    /// </summary>
    private AsyncDuplexStreamingCall<ConnectionStreamRequest, ConnectionStreamResponse> Stream
    {
        get
        {
            _stream ??= spanner.Client.ConnectionStream();
            return _stream;
        }
    }

    protected override void CloseLibObject()
    {
        base.CloseLibObject();
        _stream?.Dispose();
    }
    
    /// <summary>
    /// Executes the given request using a bidirectional gRPC stream and waits for a response on the same stream.
    /// </summary>
    private ConnectionStreamResponse ExecuteBidi(ConnectionStreamRequest request)
    {
        var connectionStream = Stream;
        Task.Run(() => connectionStream.RequestStream.WriteAsync(request)).GetAwaiter().GetResult();
        if (!Task.Run(() => connectionStream.ResponseStream.MoveNext(CancellationToken.None)).GetAwaiter().GetResult())
        {
            // This should never happen assuming that the gRPC server is well-behaved.
            throw new SpannerException(new Status { Code = (int)Code.Internal, Message = "No response received" });
        }
        if (connectionStream.ResponseStream.Current.Status.Code != (int)Code.Ok)
        {
            throw new SpannerException(connectionStream.ResponseStream.Current.Status);
        }
        return connectionStream.ResponseStream.Current;
    }

    /// <summary>
    /// Executes the given request using a bidirectional gRPC stream and waits for a response on the same stream.
    /// </summary>
    private async Task<ConnectionStreamResponse> ExecuteBidiAsync(
        ConnectionStreamRequest request, CancellationToken cancellationToken)
    {
        var connectionStream = Stream;
        await connectionStream.RequestStream.WriteAsync(request, cancellationToken).ConfigureAwait(false);
        if (!await connectionStream.ResponseStream.MoveNext(cancellationToken).ConfigureAwait(false))
        {
            // This should never happen assuming that the gRPC server is well-behaved.
            throw new SpannerException(new Status { Code = (int)Code.Internal, Message = "No response received" });
        }
        if (connectionStream.ResponseStream.Current.Status.Code != (int)Code.Ok)
        {
            throw new SpannerException(connectionStream.ResponseStream.Current.Status);
        }
        return connectionStream.ResponseStream.Current;
    }

    public override void BeginTransaction(TransactionOptions transactionOptions)
    {
        ExecuteBidi(CreateBeginTransactionRequest(transactionOptions));
    }

    public override Task BeginTransactionAsync(TransactionOptions transactionOptions, CancellationToken cancellationToken = default)
    {
        return ExecuteBidiAsync(CreateBeginTransactionRequest(transactionOptions), cancellationToken);
    }

    private ConnectionStreamRequest CreateBeginTransactionRequest(TransactionOptions transactionOptions)
    {
        return new ConnectionStreamRequest
        {
            BeginTransactionRequest = new BeginTransactionRequest
            {
                Connection = GrpcLibSpanner.ToProto(this),
                TransactionOptions = transactionOptions,
            }
        };
    }

    public override CommitResponse? Commit()
    {
        var response = ExecuteBidi(CreateCommitRequest());
        return response.CommitResponse.CommitTimestamp != null ? response.CommitResponse : null;
    }

    public override async Task<CommitResponse?> CommitAsync(CancellationToken cancellationToken = default)
    {
        var response = await ExecuteBidiAsync(CreateCommitRequest(), cancellationToken).ConfigureAwait(false);
        return response.CommitResponse.CommitTimestamp != null ? response.CommitResponse : null;
    }

    private ConnectionStreamRequest CreateCommitRequest()
    {
        return new ConnectionStreamRequest
        {
            CommitRequest = GrpcLibSpanner.ToProto(this),
        };
    }

    public override void Rollback()
    {
        ExecuteBidi(CreateRollbackRequest());
    }

    public override Task RollbackAsync(CancellationToken cancellationToken = default)
    {
        return ExecuteBidiAsync(CreateRollbackRequest(), cancellationToken);
    }
    
    private ConnectionStreamRequest CreateRollbackRequest()
    {
        return new ConnectionStreamRequest
        {
            RollbackRequest = GrpcLibSpanner.ToProto(this),
        };
    }

    public override Rows Execute(ExecuteSqlRequest statement, int prefetchRows = 0)
    {
        var response = ExecuteBidi(CreateExecuteRequest(statement, prefetchRows));
        return StreamingRows.Create(spanner, this, response.ExecuteResponse);
    }

    public override async Task<Rows> ExecuteAsync(ExecuteSqlRequest statement, int prefetchRows = 0, CancellationToken cancellationToken = default)
    {
        var response = await ExecuteBidiAsync(CreateExecuteRequest(statement, prefetchRows), cancellationToken).ConfigureAwait(false);
        return await StreamingRows.CreateAsync(spanner, this, response.ExecuteResponse, cancellationToken).ConfigureAwait(false);
    }

    private ConnectionStreamRequest CreateExecuteRequest(ExecuteSqlRequest statement, int prefetchRows = 0)
    {
        return new ConnectionStreamRequest
        {
            ExecuteRequest = new ExecuteRequest
            {
                Connection = GrpcLibSpanner.ToProto(this),
                ExecuteSqlRequest = statement,
                FetchOptions = new FetchOptions
                {
                    NumRows = prefetchRows,
                },
            },
        };
    }

    public override long[] ExecuteBatch(List<ExecuteBatchDmlRequest.Types.Statement> statements)
    {
        var response = ExecuteBidi(CreateExecuteBatchRequest(statements));
        return ISpannerLib.ToUpdateCounts(response.ExecuteBatchResponse);
    }

    public override async Task<long[]> ExecuteBatchAsync(List<ExecuteBatchDmlRequest.Types.Statement> statements, CancellationToken cancellationToken = default)
    {
        var response = await ExecuteBidiAsync(CreateExecuteBatchRequest(statements), cancellationToken).ConfigureAwait(false);
        return ISpannerLib.ToUpdateCounts(response.ExecuteBatchResponse);
    }

    private ConnectionStreamRequest CreateExecuteBatchRequest(
        IEnumerable<ExecuteBatchDmlRequest.Types.Statement> statements)
    {
        return new ConnectionStreamRequest
        {
            ExecuteBatchRequest = new ExecuteBatchRequest
            {
                Connection = GrpcLibSpanner.ToProto(this),
                ExecuteBatchDmlRequest = new ExecuteBatchDmlRequest
                {
                    Statements = { statements },
                },
            },
        };
    }

    public override CommitResponse? WriteMutations(BatchWriteRequest.Types.MutationGroup mutations)
    {
        var response = ExecuteBidi(CreateWriteMutationsRequest(mutations));
        return response.WriteMutationsResponse?.CommitTimestamp != null ? response.WriteMutationsResponse : null;
    }
    
    public override async Task<CommitResponse?> WriteMutationsAsync(BatchWriteRequest.Types.MutationGroup mutations, CancellationToken cancellationToken = default)
    {
        var response = await ExecuteBidiAsync(CreateWriteMutationsRequest(mutations), cancellationToken).ConfigureAwait(false);
        return response.WriteMutationsResponse?.CommitTimestamp != null ? response.WriteMutationsResponse : null;
    }

    private ConnectionStreamRequest CreateWriteMutationsRequest(BatchWriteRequest.Types.MutationGroup mutations)
    {
        return new ConnectionStreamRequest
        {
            WriteMutationsRequest = new WriteMutationsRequest
            {
                Connection = GrpcLibSpanner.ToProto(this),
                Mutations = mutations,
            }
        };
    }
}
