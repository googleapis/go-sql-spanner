/*
 * Copyright 2025 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.google.cloud.spannerlib;

import com.google.cloud.spannerlib.v1.BeginTransactionRequest;
import com.google.cloud.spannerlib.v1.CreateConnectionRequest;
import com.google.cloud.spannerlib.v1.CreatePoolRequest;
import com.google.cloud.spannerlib.v1.ExecuteBatchRequest;
import com.google.cloud.spannerlib.v1.ExecuteRequest;
import com.google.cloud.spannerlib.v1.NextRequest;
import com.google.cloud.spannerlib.v1.RowData;
import com.google.cloud.spannerlib.v1.SpannerLibGrpc;
import com.google.cloud.spannerlib.v1.SpannerLibGrpc.SpannerLibBlockingV2Stub;
import com.google.cloud.spannerlib.v1.WriteMutationsRequest;
import com.google.protobuf.ListValue;
import com.google.rpc.Status;
import com.google.spanner.v1.BatchWriteRequest.MutationGroup;
import com.google.spanner.v1.CommitResponse;
import com.google.spanner.v1.ExecuteBatchDmlRequest;
import com.google.spanner.v1.ExecuteBatchDmlResponse;
import com.google.spanner.v1.ExecuteSqlRequest;
import com.google.spanner.v1.ResultSetMetadata;
import com.google.spanner.v1.ResultSetStats;
import com.google.spanner.v1.TransactionOptions;
import io.grpc.Channel;
import io.grpc.ManagedChannel;
import io.grpc.StatusException;
import io.grpc.stub.BlockingClientCall;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;

/** This implementation communicates with SpannerLib through a gRPC interface. */
public class GrpcSpannerLibraryImpl implements SpannerLibrary {
  private final Channel channel;
  private final SpannerLibBlockingV2Stub stub;
  private final boolean useStreamingRows;

  private final List<Channel> channels;
  private final List<SpannerLibBlockingV2Stub> stubs;

  public GrpcSpannerLibraryImpl(Channel channel, boolean useStreamingRows) {
    this.channel = channel;
    this.stub = SpannerLibGrpc.newBlockingV2Stub(channel);
    this.useStreamingRows = useStreamingRows;

    this.channels = null;
    this.stubs = null;
  }

  public GrpcSpannerLibraryImpl(List<Channel> channels) {
    this.channel = channels.get(0);
    this.stub = SpannerLibGrpc.newBlockingV2Stub(channels.get(0));
    this.useStreamingRows = true;

    this.channels = channels;
    this.stubs =
        channels.stream().map(SpannerLibGrpc::newBlockingV2Stub).collect(Collectors.toList());
  }

  static SpannerLibException toSpannerLibException(StatusException exception) {
    String message =
        exception.getStatus().getDescription() != null
            ? exception.getStatus().getDescription()
            : exception.getMessage();
    return new SpannerLibException(
        Status.newBuilder()
            .setCode(exception.getStatus().getCode().value())
            .setMessage(message)
            .build());
  }

  private static com.google.cloud.spannerlib.v1.Pool toProto(Pool pool) {
    return com.google.cloud.spannerlib.v1.Pool.newBuilder().setId(pool.getId()).build();
  }

  private static com.google.cloud.spannerlib.v1.Connection toProto(Connection connection) {
    return com.google.cloud.spannerlib.v1.Connection.newBuilder()
        .setPool(toProto(connection.getPool()))
        .setId(connection.getId())
        .build();
  }

  private static com.google.cloud.spannerlib.v1.Rows toProto(Rows rows) {
    return com.google.cloud.spannerlib.v1.Rows.newBuilder()
        .setConnection(toProto(rows.getConnection()))
        .setId(rows.getId())
        .build();
  }

  @Override
  public void close() {
    if (this.channels != null) {
      for (Channel channel : channels) {
        if (channel instanceof ManagedChannel) {
          ((ManagedChannel) channel).shutdown();
        }
      }
    }
    if (this.channel instanceof ManagedChannel) {
      ((ManagedChannel) this.channel).shutdown();
    }
  }

  @Override
  public Pool createPool(String connectionString) {
    try {
      return new Pool(
          this,
          stub.createPool(
                  CreatePoolRequest.newBuilder()
                      .setUserAgentSuffix(USER_AGENT_SUFFIX)
                      .setConnectionString(connectionString)
                      .build())
              .getId());
    } catch (StatusException exception) {
      throw toSpannerLibException(exception);
    }
  }

  @Override
  public void closePool(Pool pool) {
    try {
      //noinspection ResultOfMethodCallIgnored
      stub.closePool(toProto(pool));
    } catch (StatusException exception) {
      throw toSpannerLibException(exception);
    }
  }

  @Override
  public Connection createConnection(Pool pool) {
    try {
      return new Connection(
          pool,
          stub.createConnection(CreateConnectionRequest.newBuilder().setPool(toProto(pool)).build())
              .getId());
    } catch (StatusException exception) {
      throw toSpannerLibException(exception);
    }
  }

  @Override
  public void closeConnection(Connection connection) {
    try {
      //noinspection ResultOfMethodCallIgnored
      stub.closeConnection(toProto(connection));
    } catch (StatusException exception) {
      throw toSpannerLibException(exception);
    }
  }

  @Override
  public CommitResponse writeMutations(Connection connection, MutationGroup mutations) {
    try {
      CommitResponse response =
          stub.writeMutations(
              WriteMutationsRequest.newBuilder()
                  .setConnection(toProto(connection))
                  .setMutations(mutations)
                  .build());
      if (!response.hasCommitTimestamp()) {
        return null;
      }
      return response;
    } catch (StatusException exception) {
      throw toSpannerLibException(exception);
    }
  }

  @Override
  public void beginTransaction(Connection connection, TransactionOptions options) {
    try {
      //noinspection ResultOfMethodCallIgnored
      stub.beginTransaction(
          BeginTransactionRequest.newBuilder()
              .setConnection(toProto(connection))
              .setTransactionOptions(options)
              .build());
    } catch (StatusException exception) {
      throw toSpannerLibException(exception);
    }
  }

  @Override
  public CommitResponse commit(Connection connection) {
    try {
      return stub.commit(toProto(connection));
    } catch (StatusException exception) {
      throw toSpannerLibException(exception);
    }
  }

  @Override
  public void rollback(Connection connection) {
    try {
      //noinspection ResultOfMethodCallIgnored
      stub.rollback(toProto(connection));
    } catch (StatusException exception) {
      throw toSpannerLibException(exception);
    }
  }

  @Override
  public Rows execute(Connection connection, ExecuteSqlRequest request) {
    if (useStreamingRows) {
      return executeStreaming(connection, request);
    }
    try {
      return new Rows(
          connection,
          stub.execute(
                  ExecuteRequest.newBuilder()
                      .setConnection(toProto(connection))
                      .setExecuteSqlRequest(request)
                      .build())
              .getId());
    } catch (StatusException exception) {
      throw toSpannerLibException(exception);
    }
  }

  private Rows executeStreaming(Connection connection, ExecuteSqlRequest request) {
    SpannerLibBlockingV2Stub stub = this.stub;
    if (stubs != null) {
      stub = stubs.get(ThreadLocalRandom.current().nextInt(stubs.size()));
    }
    BlockingClientCall<?, RowData> stream =
        stub.executeStreaming(
            ExecuteRequest.newBuilder()
                .setConnection(toProto(connection))
                .setExecuteSqlRequest(request)
                .build());
    StreamingRows rows = new StreamingRows(connection, stream);
    connection.registerStream(rows);
    return rows;
  }

  @Override
  public ResultSetMetadata getMetadata(Rows rows) {
    try {
      return stub.metadata(toProto(rows));
    } catch (StatusException exception) {
      throw toSpannerLibException(exception);
    }
  }

  @Override
  public ListValue next(Rows rows) {
    try {
      ListValue values = stub.next(NextRequest.newBuilder().setRows(toProto(rows)).build());
      if (values.getValuesList().isEmpty()) {
        return null;
      }
      return values;
    } catch (StatusException exception) {
      throw toSpannerLibException(exception);
    }
  }

  @Override
  public ResultSetStats getResultSetStats(Rows rows) {
    try {
      return stub.resultSetStats(toProto(rows));
    } catch (StatusException exception) {
      throw toSpannerLibException(exception);
    }
  }

  @Override
  public ResultSetMetadata nextResultSet(Rows rows) {
    try {
      return stub.nextResultSet(toProto(rows));
    } catch (StatusException exception) {
      throw toSpannerLibException(exception);
    }
  }

  @Override
  public void closeRows(Rows rows) {
    try {
      //noinspection ResultOfMethodCallIgnored
      stub.closeRows(toProto(rows));
    } catch (StatusException exception) {
      throw toSpannerLibException(exception);
    }
  }

  @Override
  public ExecuteBatchDmlResponse executeBatch(
      Connection connection, ExecuteBatchDmlRequest request) {
    try {
      return stub.executeBatch(
          ExecuteBatchRequest.newBuilder()
              .setConnection(toProto(connection))
              .setExecuteBatchDmlRequest(request)
              .build());
    } catch (StatusException exception) {
      throw toSpannerLibException(exception);
    }
  }
}
