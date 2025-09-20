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

import com.google.protobuf.ListValue;
import com.google.spanner.v1.BatchWriteRequest.MutationGroup;
import com.google.spanner.v1.CommitResponse;
import com.google.spanner.v1.ExecuteBatchDmlRequest;
import com.google.spanner.v1.ExecuteBatchDmlResponse;
import com.google.spanner.v1.ExecuteSqlRequest;
import com.google.spanner.v1.ResultSetMetadata;
import com.google.spanner.v1.ResultSetStats;
import com.google.spanner.v1.TransactionOptions;
import java.io.Closeable;

/**
 * This is the generic interface that must be implemented by SpannerLib implementations that use the
 * various supported communication models (e.g. shared library or gRPC).
 */
public interface SpannerLibrary extends Closeable {

  Pool createPool(String connectionString);

  void closePool(Pool pool);

  Connection createConnection(Pool pool);

  void closeConnection(Connection connection);

  CommitResponse writeMutations(Connection connection, MutationGroup mutations);

  void beginTransaction(Connection connection, TransactionOptions options);

  CommitResponse commit(Connection connection);

  void rollback(Connection connection);

  Rows execute(Connection connection, ExecuteSqlRequest request);

  ResultSetMetadata getMetadata(Rows rows);

  ListValue next(Rows rows);

  ResultSetStats getResultSetStats(Rows rows);

  void closeRows(Rows rows);

  ExecuteBatchDmlResponse executeBatch(Connection connection, ExecuteBatchDmlRequest request);
}
