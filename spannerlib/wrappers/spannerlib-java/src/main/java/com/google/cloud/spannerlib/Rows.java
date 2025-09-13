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

import com.google.cloud.spannerlib.internal.MessageHandler;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.ListValue;
import com.google.spanner.v1.ResultSetMetadata;
import com.google.spanner.v1.ResultSetStats;
import java.nio.ByteBuffer;
import java.sql.Statement;

public class Rows extends AbstractLibraryObject {
  public enum Encoding {
    PROTOBUF,
  }

  private final Connection connection;

  Rows(Connection connection, long id) {
    super(connection.getLibrary(), id);
    this.connection = connection;
  }

  @Override
  public void close() {
    executeAndRelease(
        getLibrary(),
        library -> library.CloseRows(connection.getPool().getId(), connection.getId(), getId()));
  }

  public ResultSetMetadata getMetadata() {
    try (MessageHandler message =
        getLibrary()
            .execute(
                library ->
                    library.Metadata(connection.getPool().getId(), connection.getId(), getId()))) {
      if (message.getLength() == 0) {
        return ResultSetMetadata.getDefaultInstance();
      }
      ByteBuffer buffer = message.getValue().getByteBuffer(0, message.getLength());
      return ResultSetMetadata.parseFrom(buffer);
    } catch (InvalidProtocolBufferException decodeException) {
      throw new RuntimeException(decodeException);
    }
  }

  public ResultSetStats getResultSetStats() {
    try (MessageHandler message =
        getLibrary()
            .execute(
                library ->
                    library.ResultSetStats(
                        connection.getPool().getId(), connection.getId(), getId()))) {
      if (message.getLength() == 0) {
        return ResultSetStats.getDefaultInstance();
      }
      ByteBuffer buffer = message.getValue().getByteBuffer(0, message.getLength());
      return ResultSetStats.parseFrom(buffer);
    } catch (InvalidProtocolBufferException decodeException) {
      throw new RuntimeException(decodeException);
    }
  }

  public long getUpdateCount() {
    ResultSetStats stats = getResultSetStats();
    if (stats.hasRowCountExact()) {
      return stats.getRowCountExact();
    } else if (stats.hasRowCountLowerBound()) {
      return stats.getRowCountLowerBound();
    }
    return Statement.SUCCESS_NO_INFO;
  }

  /** Returns the next row in this {@link Rows} instance, or null if there are no more rows. */
  public ListValue next() {
    try (MessageHandler message =
        getLibrary()
            .execute(
                library ->
                    library.Next(
                        connection.getPool().getId(),
                        connection.getId(),
                        getId(),
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
}
