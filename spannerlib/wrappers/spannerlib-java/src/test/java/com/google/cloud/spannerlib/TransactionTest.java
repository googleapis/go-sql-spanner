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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import com.google.cloud.spanner.MockSpannerServiceImpl.StatementResult;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.connection.AbstractMockServerTest;
import com.google.cloud.spanner.connection.RandomResultSetGenerator;
import com.google.common.collect.ImmutableMap;
import com.google.protobuf.Struct;
import com.google.protobuf.Value;
import com.google.spanner.v1.BeginTransactionRequest;
import com.google.spanner.v1.CommitRequest;
import com.google.spanner.v1.ExecuteSqlRequest;
import com.google.spanner.v1.ResultSet;
import com.google.spanner.v1.ResultSetMetadata;
import com.google.spanner.v1.ResultSetStats;
import com.google.spanner.v1.RollbackRequest;
import com.google.spanner.v1.StructType;
import com.google.spanner.v1.TransactionOptions;
import com.google.spanner.v1.TransactionOptions.ReadOnly;
import com.google.spanner.v1.Type;
import com.google.spanner.v1.TypeCode;
import io.grpc.Status.Code;
import org.junit.Test;

public class TransactionTest extends AbstractMockServerTest {
  @Test
  public void testBeginAndCommit() {
    String dsn =
        String.format(
            "localhost:%d/projects/p/instances/i/databases/d?usePlainText=true", getPort());
    try (Pool pool = Pool.createPool(dsn);
        Connection connection = pool.createConnection()) {
      connection.beginTransaction(TransactionOptions.getDefaultInstance());
      connection.commit();

      // TODO: The library should take a shortcut and just skip committing empty transactions.
      assertEquals(1, mockSpanner.countRequestsOfType(BeginTransactionRequest.class));
      assertEquals(1, mockSpanner.countRequestsOfType(CommitRequest.class));
    }
  }

  @Test
  public void testBeginAndRollback() {
    String dsn =
        String.format(
            "localhost:%d/projects/p/instances/i/databases/d?usePlainText=true", getPort());
    try (Pool pool = Pool.createPool(dsn);
        Connection connection = pool.createConnection()) {
      connection.beginTransaction(TransactionOptions.getDefaultInstance());
      connection.rollback();

      assertEquals(0, mockSpanner.countRequestsOfType(BeginTransactionRequest.class));
      assertEquals(0, mockSpanner.countRequestsOfType(CommitRequest.class));
      assertEquals(0, mockSpanner.countRequestsOfType(RollbackRequest.class));
    }
  }

  @Test
  public void testReadWriteTransaction() {
    String updateSql = "update my_table set my_val=@value where id=@id";
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.newBuilder(updateSql).bind("value").to("foo").bind("id").to(1L).build(),
            ResultSet.newBuilder()
                .setMetadata(
                    ResultSetMetadata.newBuilder()
                        .setRowType(StructType.newBuilder().build())
                        .build())
                .setStats(ResultSetStats.newBuilder().setRowCountExact(1L).build())
                .build()));

    String dsn =
        String.format(
            "localhost:%d/projects/p/instances/i/databases/d?usePlainText=true", getPort());
    try (Pool pool = Pool.createPool(dsn);
        Connection connection = pool.createConnection()) {
      connection.beginTransaction(TransactionOptions.getDefaultInstance());
      connection.execute(
          ExecuteSqlRequest.newBuilder()
              .setSql(updateSql)
              .setParams(
                  Struct.newBuilder()
                      .putFields("value", Value.newBuilder().setStringValue("foo").build())
                      .putFields("id", Value.newBuilder().setStringValue("1").build())
                      .build())
              .putAllParamTypes(
                  ImmutableMap.of(
                      "value", Type.newBuilder().setCode(TypeCode.STRING).build(),
                      "id", Type.newBuilder().setCode(TypeCode.INT64).build()))
              .build());
      connection.commit();

      // There should be no BeginTransaction requests, as the transaction start is inlined with the
      // first statement.
      assertEquals(0, mockSpanner.countRequestsOfType(BeginTransactionRequest.class));
      assertEquals(1, mockSpanner.countRequestsOfType(ExecuteSqlRequest.class));
      ExecuteSqlRequest request = mockSpanner.getRequestsOfType(ExecuteSqlRequest.class).get(0);
      assertTrue(request.hasTransaction());
      assertTrue(request.getTransaction().hasBegin());
      assertTrue(request.getTransaction().getBegin().hasReadWrite());
      assertEquals(1, mockSpanner.countRequestsOfType(CommitRequest.class));
    }
  }

  @Test
  public void testReadOnlyTransaction() {
    String sql = "select * from random";
    int numRows = 5;
    RandomResultSetGenerator generator = new RandomResultSetGenerator(numRows);
    mockSpanner.putStatementResult(StatementResult.query(Statement.of(sql), generator.generate()));

    String dsn =
        String.format(
            "localhost:%d/projects/p/instances/i/databases/d?usePlainText=true", getPort());
    try (Pool pool = Pool.createPool(dsn);
        Connection connection = pool.createConnection()) {
      connection.beginTransaction(
          TransactionOptions.newBuilder().setReadOnly(ReadOnly.newBuilder().build()).build());
      try (Rows rows = connection.execute(ExecuteSqlRequest.newBuilder().setSql(sql).build())) {
        int rowCount = 0;
        while (rows.next() != null) {
          rowCount++;
        }
        assertEquals(numRows, rowCount);
      }
      connection.commit();
    }

    // There should be no BeginTransaction requests, as the transaction start is inlined with the
    // first statement.
    assertEquals(0, mockSpanner.countRequestsOfType(BeginTransactionRequest.class));
    assertEquals(1, mockSpanner.countRequestsOfType(ExecuteSqlRequest.class));
    ExecuteSqlRequest request = mockSpanner.getRequestsOfType(ExecuteSqlRequest.class).get(0);
    assertTrue(request.hasTransaction());
    assertTrue(request.getTransaction().hasBegin());
    assertTrue(request.getTransaction().getBegin().hasReadOnly());
    // There should be no CommitRequests on the server, as committing a read-only transaction is a
    // no-op on Spanner.
    assertEquals(0, mockSpanner.countRequestsOfType(CommitRequest.class));
  }

  @Test
  public void testBeginTwice() {
    String dsn =
        String.format(
            "localhost:%d/projects/p/instances/i/databases/d?usePlainText=true", getPort());
    try (Pool pool = Pool.createPool(dsn);
        Connection connection = pool.createConnection()) {
      // Try to start two transactions on a connection.
      connection.beginTransaction(TransactionOptions.getDefaultInstance());
      SpannerLibException exception =
          assertThrows(
              SpannerLibException.class,
              () -> connection.beginTransaction(TransactionOptions.getDefaultInstance()));
      assertEquals(Code.FAILED_PRECONDITION.value(), exception.getStatus().getCode());
    }
  }
}
