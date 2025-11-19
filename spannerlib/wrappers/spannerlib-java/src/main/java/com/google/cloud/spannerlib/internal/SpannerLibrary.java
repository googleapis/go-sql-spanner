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

package com.google.cloud.spannerlib.internal;

import com.google.cloud.spannerlib.SpannerLibException;
import com.sun.jna.Library;
import com.sun.jna.Native;
import java.util.function.Function;

/**
 * {@link SpannerLibrary} is the Java interface that corresponds with the public API of the native
 * SpannerLib library.
 */
public interface SpannerLibrary extends Library {
  SpannerLibrary LIBRARY = Native.load("spanner", SpannerLibrary.class);

  /** Returns the singleton instance of the library. */
  static SpannerLibrary getInstance() {
    return LIBRARY;
  }

  /**
   * Executes a library function and automatically releases any memory that was returned by the
   * library call when the supplied function finishes.
   */
  static void executeAndRelease(
      SpannerLibrary library, Function<SpannerLibrary, Message> function) {
    try (MessageHandler message = new MessageHandler(library, function.apply(library))) {
      message.throwIfError();
    }
  }

  /** Executes a library function and returns the result as a {@link MessageHandler}. */
  default MessageHandler execute(Function<SpannerLibrary, Message> function)
      throws SpannerLibException {
    return new MessageHandler(this, function.apply(this));
  }

  /** Releases the memory that is pointed to by the pinner. */
  int Release(long pinner);

  /** Creates a new Pool. */
  Message CreatePool(GoString dsn);

  /** Closes the given Pool. */
  Message ClosePool(long id);

  /** Creates a new Connection in the given Pool. */
  Message CreateConnection(long poolId);

  /** Closes the given Connection. */
  Message CloseConnection(long poolId, long connectionId);

  /**
   * Writes a group of mutations on Spanner. The mutations are buffered in the current read/write
   * transaction if the connection has an active read/write transaction. Otherwise, the mutations
   * are written directly to Spanner in a new read/write transaction. Returns a {@link
   * com.google.spanner.v1.CommitResponse} if the mutations were written directly to Spanner, and an
   * empty message if the mutations were only buffered in the current transaction.
   */
  Message WriteMutations(long poolId, long connectionId, GoBytes mutations);

  /** Starts a new transaction on the given Connection. */
  Message BeginTransaction(long poolId, long connectionId, GoBytes transactionOptions);

  /**
   * Commits the current transaction on the given Connection and returns a {@link
   * com.google.spanner.v1.CommitResponse}.
   */
  Message Commit(long poolId, long connectionId);

  /** Rollbacks the current transaction on the given Connection. */
  Message Rollback(long poolId, long connectionId);

  /** Executes a SQL statement on the given Connection. */
  Message Execute(long poolId, long connectionId, GoBytes executeSqlRequest);

  /**
   * Executes a batch of DML or DDL statements on the given Connection. Returns an {@link
   * com.google.spanner.v1.ExecuteBatchDmlResponse} for both DML and DDL batches.
   */
  Message ExecuteBatch(long poolId, long connectionId, GoBytes executeBatchDmlRequest);

  /** Returns the {@link com.google.spanner.v1.ResultSetMetadata} of the given Rows object. */
  Message Metadata(long poolId, long connectionId, long rowsId);

  /** Returns the next row from the given Rows object. */
  Message Next(long poolId, long connectionId, long rowsId, int numRows, int encoding);

  /** Returns the {@link com.google.spanner.v1.ResultSetStats} of the given Rows object. */
  Message ResultSetStats(long poolId, long connectionId, long rowsId);

  /**
   * Returns the {@link com.google.spanner.v1.ResultSetMetadata} of the next result set in the given
   * Rows object, or null if there is no next result set.
   */
  Message NextResultSet(long poolId, long connectionId, long rowsId);

  /** Closes the given Rows object. */
  Message CloseRows(long poolId, long connectionId, long rowsId);
}
