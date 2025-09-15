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
import com.google.cloud.spannerlib.internal.WrappedGoBytes;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.spanner.v1.CommitResponse;
import com.google.spanner.v1.ExecuteSqlRequest;
import com.google.spanner.v1.TransactionOptions;
import java.nio.ByteBuffer;

/** A {@link Connection} that has been created by SpannerLib. */
public class Connection extends AbstractLibraryObject {
  private final Pool pool;

  Connection(Pool pool, long id) {
    super(pool.getLibrary(), id);
    this.pool = pool;
  }

  public Pool getPool() {
    return this.pool;
  }

  /** Starts a transaction on this connection. */
  public void beginTransaction(TransactionOptions options) {
    try (WrappedGoBytes serializedOptions = WrappedGoBytes.serialize(options)) {
      executeAndRelease(
          getLibrary(),
          library ->
              library.BeginTransaction(pool.getId(), getId(), serializedOptions.getGoBytes()));
    }
  }

  /**
   * Commits the current transaction on this connection and returns the {@link CommitResponse} or
   * null if there is no {@link CommitResponse} (e.g. for read-only transactions).
   */
  public CommitResponse commit() {
    try (MessageHandler message =
        getLibrary().execute(library -> library.Commit(pool.getId(), getId()))) {
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

  /** Rollbacks the current transaction on this connection. */
  public void rollback() {
    executeAndRelease(getLibrary(), library -> library.Rollback(pool.getId(), getId()));
  }

  /** Executes the given SQL statement on this connection. */
  public Rows execute(ExecuteSqlRequest request) {
    try (WrappedGoBytes serializedRequest = WrappedGoBytes.serialize(request);
        MessageHandler message =
            getLibrary()
                .execute(
                    library ->
                        library.Execute(pool.getId(), getId(), serializedRequest.getGoBytes()))) {
      return new Rows(this, message.getObjectId());
    }
  }

  /** Closes this connection. Any active transaction on the connection is rolled back. */
  @Override
  public void close() {
    executeAndRelease(getLibrary(), library -> library.CloseConnection(pool.getId(), getId()));
  }
}
