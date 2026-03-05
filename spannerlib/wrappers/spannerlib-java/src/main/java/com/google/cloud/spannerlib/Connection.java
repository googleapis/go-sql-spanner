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

import com.google.spanner.v1.BatchWriteRequest.MutationGroup;
import com.google.spanner.v1.CommitResponse;
import com.google.spanner.v1.ExecuteBatchDmlRequest;
import com.google.spanner.v1.ExecuteBatchDmlResponse;
import com.google.spanner.v1.ExecuteSqlRequest;
import com.google.spanner.v1.TransactionOptions;
import java.util.ArrayList;
import java.util.List;

/** A {@link Connection} that has been created by SpannerLib. */
public class Connection extends AbstractLibraryObject {
  private final Pool pool;
  private final List<StreamingRows> streams = new ArrayList<>();

  Connection(Pool pool, long id) {
    super(pool.getLibrary(), id);
    this.pool = pool;
  }

  void registerStream(StreamingRows stream) {
    this.streams.add(stream);
  }

  void deregisterStream(StreamingRows stream) {
    this.streams.remove(stream);
  }

  /** Closes this connection. Any active transaction on the connection is rolled back. */
  @Override
  public void close() {
    synchronized (this.streams) {
      for (StreamingRows stream : this.streams) {
        stream.cancel("connection closed");
      }
    }
    getLibrary().closeConnection(this);
  }

  public Pool getPool() {
    return this.pool;
  }

  /**
   * Writes a group of mutations to Spanner. The mutations are buffered in the current read/write
   * transaction if the connection has an active read/write transaction. Otherwise, the mutations
   * are written directly to Spanner using a new read/write transaction. Returns a {@link
   * CommitResponse} if the mutations were written directly to Spanner, and otherwise null if the
   * mutations were buffered in the current transaction.
   */
  public CommitResponse WriteMutations(MutationGroup mutations) {
    return getLibrary().writeMutations(this, mutations);
  }

  /** Starts a transaction on this connection. */
  public void beginTransaction(TransactionOptions options) {
    getLibrary().beginTransaction(this, options);
  }

  /**
   * Commits the current transaction on this connection and returns the {@link CommitResponse} or
   * null if there is no {@link CommitResponse} (e.g. for read-only transactions).
   */
  public CommitResponse commit() {
    return getLibrary().commit(this);
  }

  /** Rollbacks the current transaction on this connection. */
  public void rollback() {
    getLibrary().rollback(this);
  }

  /** Executes the given SQL statement on this connection. */
  public Rows execute(ExecuteSqlRequest request) {
    return getLibrary().execute(this, request);
  }

  /**
   * Executes the given batch of DML or DDL statements on this connection. The statements must all
   * be of the same type.
   */
  public ExecuteBatchDmlResponse executeBatch(ExecuteBatchDmlRequest request) {
    return getLibrary().executeBatch(this, request);
  }
}
