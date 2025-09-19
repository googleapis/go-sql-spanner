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
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import com.google.common.collect.ImmutableList;
import com.google.protobuf.ListValue;
import com.google.protobuf.Value;
import com.google.rpc.Code;
import com.google.spanner.v1.BatchWriteRequest.MutationGroup;
import com.google.spanner.v1.BeginTransactionRequest;
import com.google.spanner.v1.CommitRequest;
import com.google.spanner.v1.CommitResponse;
import com.google.spanner.v1.CreateSessionRequest;
import com.google.spanner.v1.Mutation;
import com.google.spanner.v1.Mutation.Write;
import com.google.spanner.v1.TransactionOptions;
import com.google.spanner.v1.TransactionOptions.ReadOnly;
import org.junit.Test;

public class ConnectionTest extends AbstractSpannerLibTest {

  @Test
  public void testCreateConnection() {
    String dsn =
        String.format(
            "localhost:%d/projects/p/instances/i/databases/d?usePlainText=true", getPort());
    try (Pool pool = Pool.createPool(library, dsn);
        Connection connection = pool.createConnection()) {
      assertTrue(connection.getId() > 0);
      assertEquals(1, mockSpanner.countRequestsOfType(CreateSessionRequest.class));
    }
  }

  @Test
  public void testCreateTwoConnections() {
    String dsn =
        String.format(
            "localhost:%d/projects/p/instances/i/databases/d?usePlainText=true", getPort());
    try (Pool pool = Pool.createPool(library, dsn)) {
      try (Connection connection1 = pool.createConnection();
          Connection connection2 = pool.createConnection()) {
        assertTrue(connection1.getId() > 0);
        assertTrue(connection2.getId() > 0);
        assertNotEquals(connection1.getId(), connection2.getId());
        assertEquals(1, mockSpanner.countRequestsOfType(CreateSessionRequest.class));
      }
    }
  }

  @Test
  public void testWriteMutations() {
    String dsn =
        String.format(
            "localhost:%d/projects/p/instances/i/databases/d?usePlainText=true", getPort());
    try (Pool pool = Pool.createPool(library, dsn);
        Connection connection = pool.createConnection()) {
      CommitResponse response =
          connection.WriteMutations(
              MutationGroup.newBuilder()
                  .addMutations(
                      Mutation.newBuilder()
                          .setInsert(
                              Write.newBuilder()
                                  .addAllColumns(ImmutableList.of("id", "value"))
                                  .addValues(
                                      ListValue.newBuilder()
                                          .addValues(Value.newBuilder().setStringValue("1").build())
                                          .addValues(
                                              Value.newBuilder().setStringValue("One").build())
                                          .build())
                                  .addValues(
                                      ListValue.newBuilder()
                                          .addValues(Value.newBuilder().setStringValue("2").build())
                                          .addValues(
                                              Value.newBuilder().setStringValue("Two").build())
                                          .build())
                                  .build())
                          .build())
                  .addMutations(
                      Mutation.newBuilder()
                          .setInsertOrUpdate(
                              Write.newBuilder()
                                  .addAllColumns(ImmutableList.of("id", "value"))
                                  .addValues(
                                      ListValue.newBuilder()
                                          .addValues(Value.newBuilder().setStringValue("0").build())
                                          .addValues(
                                              Value.newBuilder().setStringValue("Zero").build())
                                          .build())
                                  .build())
                          .build())
                  .build());
      assertNotNull(response);
      assertNotNull(response.getCommitTimestamp());

      assertEquals(1, mockSpanner.countRequestsOfType(BeginTransactionRequest.class));
      assertEquals(1, mockSpanner.countRequestsOfType(CommitRequest.class));
      CommitRequest request = mockSpanner.getRequestsOfType(CommitRequest.class).get(0);
      assertEquals(2, request.getMutationsCount());
      assertEquals(2, request.getMutations(0).getInsert().getValuesCount());
      assertEquals(1, request.getMutations(1).getInsertOrUpdate().getValuesCount());
    }
  }

  @Test
  public void testWriteMutationsInTransaction() {
    String dsn =
        String.format(
            "localhost:%d/projects/p/instances/i/databases/d?usePlainText=true", getPort());
    try (Pool pool = Pool.createPool(library, dsn);
        Connection connection = pool.createConnection()) {
      connection.beginTransaction(TransactionOptions.getDefaultInstance());
      CommitResponse response =
          connection.WriteMutations(
              MutationGroup.newBuilder()
                  .addMutations(
                      Mutation.newBuilder()
                          .setInsert(
                              Write.newBuilder()
                                  .addAllColumns(ImmutableList.of("id", "value"))
                                  .addValues(
                                      ListValue.newBuilder()
                                          .addValues(Value.newBuilder().setStringValue("1").build())
                                          .addValues(
                                              Value.newBuilder().setStringValue("One").build())
                                          .build())
                                  .build())
                          .build())
                  .build());
      // The mutations are only buffered in the current transaction, so there should be no response.
      assertNull(response);

      // Committing the transaction should return a CommitResponse.
      response = connection.commit();
      assertNotNull(response);
      assertNotNull(response.getCommitTimestamp());

      assertEquals(1, mockSpanner.countRequestsOfType(BeginTransactionRequest.class));
      assertEquals(1, mockSpanner.countRequestsOfType(CommitRequest.class));
      CommitRequest request = mockSpanner.getRequestsOfType(CommitRequest.class).get(0);
      assertEquals(1, request.getMutationsCount());
      assertEquals(1, request.getMutations(0).getInsert().getValuesCount());
    }
  }

  @Test
  public void testWriteMutationsInReadOnlyTransaction() {
    String dsn =
        String.format(
            "localhost:%d/projects/p/instances/i/databases/d?usePlainText=true", getPort());
    try (Pool pool = Pool.createPool(library, dsn);
        Connection connection = pool.createConnection()) {
      connection.beginTransaction(
          TransactionOptions.newBuilder().setReadOnly(ReadOnly.newBuilder().build()).build());
      SpannerLibException exception =
          assertThrows(
              SpannerLibException.class,
              () ->
                  connection.WriteMutations(
                      MutationGroup.newBuilder()
                          .addMutations(
                              Mutation.newBuilder()
                                  .setInsert(
                                      Write.newBuilder()
                                          .addAllColumns(ImmutableList.of("id", "value"))
                                          .addValues(
                                              ListValue.newBuilder()
                                                  .addValues(
                                                      Value.newBuilder()
                                                          .setStringValue("1")
                                                          .build())
                                                  .addValues(
                                                      Value.newBuilder()
                                                          .setStringValue("One")
                                                          .build())
                                                  .build())
                                          .build())
                                  .build())
                          .build()));
      assertEquals(Code.FAILED_PRECONDITION.getNumber(), exception.getStatus().getCode());
    }
  }
}
