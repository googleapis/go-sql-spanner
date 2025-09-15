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
import static org.junit.Assert.assertFalse;

import com.google.cloud.spanner.MockSpannerServiceImpl.StatementResult;
import com.google.cloud.spanner.connection.AbstractMockServerTest;
import com.google.common.collect.ImmutableMap;
import com.google.longrunning.Operation;
import com.google.protobuf.AbstractMessage;
import com.google.protobuf.Any;
import com.google.protobuf.Empty;
import com.google.protobuf.Struct;
import com.google.protobuf.Value;
import com.google.spanner.admin.database.v1.UpdateDatabaseDdlMetadata;
import com.google.spanner.admin.database.v1.UpdateDatabaseDdlRequest;
import com.google.spanner.v1.CommitRequest;
import com.google.spanner.v1.ExecuteBatchDmlRequest;
import com.google.spanner.v1.ExecuteBatchDmlRequest.Statement;
import com.google.spanner.v1.ExecuteBatchDmlResponse;
import com.google.spanner.v1.ExecuteSqlRequest;
import com.google.spanner.v1.Type;
import com.google.spanner.v1.TypeCode;
import java.util.List;
import org.junit.Test;

public class BatchTest extends AbstractMockServerTest {

  @Test
  public void testBatchDml() {
    String insert = "insert into test (id, value) values (@id, @value)";
    mockSpanner.putStatementResult(
        StatementResult.update(
            com.google.cloud.spanner.Statement.newBuilder(insert)
                .bind("id")
                .to(1L)
                .bind("value")
                .to("One")
                .build(),
            1L));
    mockSpanner.putStatementResult(
        StatementResult.update(
            com.google.cloud.spanner.Statement.newBuilder(insert)
                .bind("id")
                .to(2L)
                .bind("value")
                .to("Two")
                .build(),
            1L));

    String dsn =
        String.format(
            "localhost:%d/projects/p/instances/i/databases/d?usePlainText=true", getPort());
    try (Pool pool = Pool.createPool(dsn);
        Connection connection = pool.createConnection()) {
      ExecuteBatchDmlResponse response =
          connection.executeBatch(
              ExecuteBatchDmlRequest.newBuilder()
                  .addStatements(
                      Statement.newBuilder()
                          .setSql(insert)
                          .setParams(
                              Struct.newBuilder()
                                  .putFields("id", Value.newBuilder().setStringValue("1").build())
                                  .putFields(
                                      "value", Value.newBuilder().setStringValue("One").build())
                                  .build())
                          .putAllParamTypes(
                              ImmutableMap.of(
                                  "id", Type.newBuilder().setCode(TypeCode.INT64).build(),
                                  "value", Type.newBuilder().setCode(TypeCode.STRING).build()))
                          .build())
                  .addStatements(
                      Statement.newBuilder()
                          .setSql(insert)
                          .setParams(
                              Struct.newBuilder()
                                  .putFields("id", Value.newBuilder().setStringValue("2").build())
                                  .putFields(
                                      "value", Value.newBuilder().setStringValue("Two").build())
                                  .build())
                          .putAllParamTypes(
                              ImmutableMap.of(
                                  "id", Type.newBuilder().setCode(TypeCode.INT64).build(),
                                  "value", Type.newBuilder().setCode(TypeCode.STRING).build()))
                          .build())
                  .build());

      assertEquals(2, response.getResultSetsCount());
      assertEquals(1L, response.getResultSets(0).getStats().getRowCountExact());
      assertEquals(1L, response.getResultSets(1).getStats().getRowCountExact());
    }

    assertEquals(1, mockSpanner.countRequestsOfType(ExecuteBatchDmlRequest.class));
    assertEquals(1, mockSpanner.countRequestsOfType(CommitRequest.class));
    assertEquals(0, mockSpanner.countRequestsOfType(ExecuteSqlRequest.class));
  }

  @Test
  public void testBatchDdl() {
    // Set up a DDL response on the mock server.
    mockDatabaseAdmin.addResponse(
        Operation.newBuilder()
            .setDone(true)
            .setResponse(Any.pack(Empty.getDefaultInstance()))
            .setMetadata(Any.pack(UpdateDatabaseDdlMetadata.getDefaultInstance()))
            .build());

    String dsn =
        String.format(
            "localhost:%d/projects/p/instances/i/databases/d?usePlainText=true", getPort());
    try (Pool pool = Pool.createPool(dsn);
        Connection connection = pool.createConnection()) {
      ExecuteBatchDmlResponse response =
          connection.executeBatch(
              ExecuteBatchDmlRequest.newBuilder()
                  .addStatements(
                      Statement.newBuilder()
                          .setSql("create table my_table (id int64 primary key, value string(max))")
                          .build())
                  .addStatements(
                      Statement.newBuilder()
                          .setSql("create index my_index on my_table (value)")
                          .build())
                  .build());

      assertEquals(2, response.getResultSetsCount());
      assertFalse(response.getResultSets(0).getStats().hasRowCountExact());
    }

    List<AbstractMessage> requests = mockDatabaseAdmin.getRequests();
    assertEquals(1, requests.size());
    UpdateDatabaseDdlRequest request = (UpdateDatabaseDdlRequest) requests.get(0);
    assertEquals(2, request.getStatementsCount());
  }
}
