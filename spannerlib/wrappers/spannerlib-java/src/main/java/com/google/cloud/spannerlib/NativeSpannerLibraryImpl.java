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

import static com.google.cloud.spannerlib.internal.SpannerLibrary.executeAndRelease;

import com.google.cloud.spannerlib.Rows.Encoding;
import com.google.cloud.spannerlib.internal.GoString;
import com.google.cloud.spannerlib.internal.MessageHandler;
import com.google.cloud.spannerlib.internal.WrappedGoBytes;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.ListValue;
import com.google.spanner.v1.BatchWriteRequest.MutationGroup;
import com.google.spanner.v1.CommitResponse;
import com.google.spanner.v1.ExecuteBatchDmlRequest;
import com.google.spanner.v1.ExecuteBatchDmlResponse;
import com.google.spanner.v1.ExecuteSqlRequest;
import com.google.spanner.v1.ResultSetMetadata;
import com.google.spanner.v1.ResultSetStats;
import com.google.spanner.v1.TransactionOptions;
import java.nio.ByteBuffer;

/** This implementation communicates with SpannerLib using native library calls. */
public class NativeSpannerLibraryImpl implements SpannerLibrary {
  private static final NativeSpannerLibraryImpl INSTANCE = new NativeSpannerLibraryImpl();

  public static NativeSpannerLibraryImpl getInstance() {
    return INSTANCE;
  }

  private final com.google.cloud.spannerlib.internal.SpannerLibrary library =
      com.google.cloud.spannerlib.internal.SpannerLibrary.getInstance();

  private NativeSpannerLibraryImpl() {}

  @Override
  public void close() {
    // no-op
  }

  @Override
  public Pool createPool(String connectionString) {
    try (MessageHandler message =
        library.execute(lib -> lib.CreatePool(new GoString(connectionString)))) {
      return new Pool(this, message.getObjectId());
    }
  }

  @Override
  public void closePool(Pool pool) {
    executeAndRelease(library, library -> library.ClosePool(pool.getId()));
  }

  @Override
  public Connection createConnection(Pool pool) {
    try (MessageHandler message =
        library.execute(library -> library.CreateConnection(pool.getId()))) {
      return new Connection(pool, message.getObjectId());
    }
  }

  @Override
  public void closeConnection(Connection connection) {
    executeAndRelease(
        library,
        library -> library.CloseConnection(connection.getPool().getId(), connection.getId()));
  }

  @Override
  public CommitResponse writeMutations(Connection connection, MutationGroup mutations) {
    try (WrappedGoBytes serializedRequest = WrappedGoBytes.serialize(mutations);
        MessageHandler message =
            library.execute(
                library ->
                    library.WriteMutations(
                        connection.getPool().getId(),
                        connection.getId(),
                        serializedRequest.getGoBytes()))) {
      if (message.getLength() == 0) {
        return null;
      }
      ByteBuffer buffer = message.getValue().getByteBuffer(0, message.getLength());
      return CommitResponse.parseFrom(buffer);
    } catch (InvalidProtocolBufferException decodeException) {
      throw new RuntimeException(decodeException);
    }
  }

  @Override
  public void beginTransaction(Connection connection, TransactionOptions options) {
    try (WrappedGoBytes serializedOptions = WrappedGoBytes.serialize(options)) {
      executeAndRelease(
          library,
          library ->
              library.BeginTransaction(
                  connection.getPool().getId(),
                  connection.getId(),
                  serializedOptions.getGoBytes()));
    }
  }

  @Override
  public CommitResponse commit(Connection connection) {
    try (MessageHandler message =
        library.execute(
            library -> library.Commit(connection.getPool().getId(), connection.getId()))) {
      // Return null in case there is no CommitResponse.
      if (message.getLength() == 0) {
        return null;
      }
      ByteBuffer buffer = message.getValue().getByteBuffer(0, message.getLength());
      return CommitResponse.parseFrom(buffer);
    } catch (InvalidProtocolBufferException decodeException) {
      throw new RuntimeException(decodeException);
    }
  }

  @Override
  public void rollback(Connection connection) {
    executeAndRelease(
        library, library -> library.Rollback(connection.getPool().getId(), connection.getId()));
  }

  @Override
  public Rows execute(Connection connection, ExecuteSqlRequest request) {
    try (WrappedGoBytes serializedRequest = WrappedGoBytes.serialize(request);
        MessageHandler message =
            library.execute(
                library ->
                    library.Execute(
                        connection.getPool().getId(),
                        connection.getId(),
                        serializedRequest.getGoBytes()))) {
      return new Rows(connection, message.getObjectId());
    }
  }

  @Override
  public ResultSetMetadata getMetadata(Rows rows) {
    try (MessageHandler message =
        library.execute(
            library ->
                library.Metadata(
                    rows.getConnection().getPool().getId(),
                    rows.getConnection().getId(),
                    rows.getId()))) {
      if (message.getLength() == 0) {
        return ResultSetMetadata.getDefaultInstance();
      }
      ByteBuffer buffer = message.getValue().getByteBuffer(0, message.getLength());
      return ResultSetMetadata.parseFrom(buffer);
    } catch (InvalidProtocolBufferException decodeException) {
      throw new RuntimeException(decodeException);
    }
  }

  @Override
  public ListValue next(Rows rows) {
    try (MessageHandler message =
        library.execute(
            library ->
                library.Next(
                    rows.getConnection().getPool().getId(),
                    rows.getConnection().getId(),
                    rows.getId(),
                    /* numRows= */ 1,
                    Encoding.PROTOBUF.ordinal()))) {
      // An empty message means that we have reached the end of the iterator.
      if (message.getLength() == 0) {
        return null;
      }
      ByteBuffer buffer = message.getValue().getByteBuffer(0, message.getLength());
      return ListValue.parseFrom(buffer);
    } catch (InvalidProtocolBufferException decodeException) {
      throw new RuntimeException(decodeException);
    }
  }

  @Override
  public ResultSetStats getResultSetStats(Rows rows) {
    try (MessageHandler message =
        library.execute(
            library ->
                library.ResultSetStats(
                    rows.getConnection().getPool().getId(),
                    rows.getConnection().getId(),
                    rows.getId()))) {
      if (message.getLength() == 0) {
        return ResultSetStats.getDefaultInstance();
      }
      ByteBuffer buffer = message.getValue().getByteBuffer(0, message.getLength());
      return ResultSetStats.parseFrom(buffer);
    } catch (InvalidProtocolBufferException decodeException) {
      throw new RuntimeException(decodeException);
    }
  }

  @Override
  public void closeRows(Rows rows) {
    executeAndRelease(
        library,
        library ->
            library.CloseRows(
                rows.getConnection().getPool().getId(),
                rows.getConnection().getId(),
                rows.getId()));
  }

  @Override
  public ExecuteBatchDmlResponse executeBatch(
      Connection connection, ExecuteBatchDmlRequest request) {
    try (WrappedGoBytes serializedRequest = WrappedGoBytes.serialize(request);
        MessageHandler message =
            library.execute(
                library ->
                    library.ExecuteBatch(
                        connection.getPool().getId(),
                        connection.getId(),
                        serializedRequest.getGoBytes()))) {
      ByteBuffer buffer = message.getValue().getByteBuffer(0, message.getLength());
      return ExecuteBatchDmlResponse.parseFrom(buffer);
    } catch (InvalidProtocolBufferException decodeException) {
      throw new RuntimeException(decodeException);
    }
  }
}
