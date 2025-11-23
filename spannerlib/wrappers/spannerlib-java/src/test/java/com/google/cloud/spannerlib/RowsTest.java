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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import com.google.cloud.spanner.Dialect;
import com.google.cloud.spanner.MockSpannerServiceImpl.StatementResult;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.connection.RandomResultSetGenerator;
import com.google.common.collect.ImmutableMap;
import com.google.longrunning.Operation;
import com.google.protobuf.Any;
import com.google.protobuf.Empty;
import com.google.protobuf.ListValue;
import com.google.protobuf.Struct;
import com.google.spanner.admin.database.v1.UpdateDatabaseDdlMetadata;
import com.google.spanner.v1.CommitRequest;
import com.google.spanner.v1.ExecuteSqlRequest;
import com.google.spanner.v1.ResultSet;
import com.google.spanner.v1.ResultSetMetadata;
import com.google.spanner.v1.ResultSetStats;
import com.google.spanner.v1.StructType;
import com.google.spanner.v1.StructType.Field;
import com.google.spanner.v1.Type;
import com.google.spanner.v1.TypeCode;
import java.util.concurrent.ThreadLocalRandom;
import org.junit.Test;

public class RowsTest extends AbstractSpannerLibTest {

  @Test
  public void testExecuteSelect1() {
    String dsn =
        String.format(
            "localhost:%d/projects/p/instances/i/databases/d?usePlainText=true", getPort());
    try (Pool pool = Pool.createPool(library, dsn);
        Connection connection = pool.createConnection()) {
      try (Rows rows =
          connection.execute(ExecuteSqlRequest.newBuilder().setSql("SELECT 1").build())) {
        ListValue row;
        int numRows = 0;
        while ((row = rows.next()) != null) {
          numRows++;
          assertEquals(1, row.getValuesList().size());
          assertTrue(row.getValues(0).hasStringValue());
          assertEquals("1", row.getValues(0).getStringValue());
        }
        assertEquals(1, numRows);
      }
    }
  }

  @Test
  public void testEmptyResult() {
    String sql = "SELECT * FROM (SELECT 1) WHERE FALSE";
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.of(sql),
            ResultSet.newBuilder().setMetadata(SELECT1_RESULTSET.getMetadata()).build()));

    String dsn =
        String.format(
            "localhost:%d/projects/p/instances/i/databases/d?usePlainText=true", getPort());
    try (Pool pool = Pool.createPool(library, dsn);
        Connection connection = pool.createConnection()) {
      try (Rows rows = connection.execute(ExecuteSqlRequest.newBuilder().setSql(sql).build())) {
        assertEquals(1, rows.getMetadata().getRowType().getFieldsCount());
        assertNull(rows.next());
      }
    }
  }

  @Test
  public void testRandomResults() {
    String sql = "select * from random";
    int numRows = 100;
    RandomResultSetGenerator generator = new RandomResultSetGenerator(numRows);
    int numCols = RandomResultSetGenerator.generateAllTypes(Dialect.GOOGLE_STANDARD_SQL).length;
    mockSpanner.putStatementResult(StatementResult.query(Statement.of(sql), generator.generate()));

    String dsn =
        String.format(
            "localhost:%d/projects/p/instances/i/databases/d?usePlainText=true", getPort());
    try (Pool pool = Pool.createPool(library, dsn);
        Connection connection = pool.createConnection()) {
      try (Rows rows = connection.execute(ExecuteSqlRequest.newBuilder().setSql(sql).build())) {
        ListValue row;
        int rowCount = 0;
        ResultSetMetadata metadata = rows.getMetadata();
        assertEquals(numCols, metadata.getRowType().getFieldsCount());
        while ((row = rows.next()) != null) {
          rowCount++;
          assertEquals(numCols, row.getValuesList().size());
        }
        assertEquals(numRows, rowCount);
      }
    }

    assertEquals(1, mockSpanner.countRequestsOfType(ExecuteSqlRequest.class));
    ExecuteSqlRequest request = mockSpanner.getRequestsOfType(ExecuteSqlRequest.class).get(0);
    assertEquals(sql, request.getSql());
    assertTrue(request.hasTransaction());
    assertTrue(request.getTransaction().hasSingleUse());
    assertTrue(request.getTransaction().getSingleUse().hasReadOnly());
    assertTrue(request.getTransaction().getSingleUse().getReadOnly().hasStrong());
  }

  @Test
  public void testStopHalfway() {
    String sql = "select * from random";
    int numRows = 10;
    RandomResultSetGenerator generator = new RandomResultSetGenerator(numRows);
    int numCols = RandomResultSetGenerator.generateAllTypes(Dialect.GOOGLE_STANDARD_SQL).length;
    mockSpanner.putStatementResult(StatementResult.query(Statement.of(sql), generator.generate()));

    int stopAfterRows = ThreadLocalRandom.current().nextInt(1, numRows - 1);
    String dsn =
        String.format(
            "localhost:%d/projects/p/instances/i/databases/d?usePlainText=true", getPort());
    try (Pool pool = Pool.createPool(library, dsn);
        Connection connection = pool.createConnection()) {
      try (Rows rows = connection.execute(ExecuteSqlRequest.newBuilder().setSql(sql).build())) {
        ListValue row;
        int rowCount = 0;
        ResultSetMetadata metadata = rows.getMetadata();
        assertEquals(numCols, metadata.getRowType().getFieldsCount());
        while ((row = rows.next()) != null) {
          rowCount++;
          assertEquals(numCols, row.getValuesList().size());
          if (rowCount == stopAfterRows) {
            break;
          }
        }
      }
    }

    assertEquals(1, mockSpanner.countRequestsOfType(ExecuteSqlRequest.class));
    ExecuteSqlRequest request = mockSpanner.getRequestsOfType(ExecuteSqlRequest.class).get(0);
    assertEquals(sql, request.getSql());
    assertTrue(request.hasTransaction());
    assertTrue(request.getTransaction().hasSingleUse());
    assertTrue(request.getTransaction().getSingleUse().hasReadOnly());
    assertTrue(request.getTransaction().getSingleUse().getReadOnly().hasStrong());
  }

  @Test
  public void testExecuteDml() {
    String sql = "update my_table set my_val=1 where id=2";
    // Set up the result as a ResultSet, as the Java mock Spanner server does not return the
    // transaction ID correctly when ExecuteStreamingSql is used for an update count result.
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.of(sql),
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
    try (Pool pool = Pool.createPool(library, dsn);
        Connection connection = pool.createConnection()) {
      // The Execute method is used for all types of statements.
      // The return type is always Rows.
      try (Rows rows = connection.execute(ExecuteSqlRequest.newBuilder().setSql(sql).build())) {
        // A DML statement without a THEN RETURN clause does not return any rows.
        assertNull(rows.next());
        // The ResultSetStats contains the update count. The Rows wrapper contains a util method
        // for getting it.
        assertEquals(1L, rows.getUpdateCount());
      }
    }

    assertEquals(1, mockSpanner.countRequestsOfType(ExecuteSqlRequest.class));
    ExecuteSqlRequest request = mockSpanner.getRequestsOfType(ExecuteSqlRequest.class).get(0);
    assertEquals(sql, request.getSql());
    assertTrue(request.hasTransaction());
    assertTrue(request.getTransaction().hasBegin());
    assertTrue(request.getTransaction().getBegin().hasReadWrite());
    assertEquals(1, mockSpanner.countRequestsOfType(CommitRequest.class));
  }

  @Test
  public void testExecuteDdl() {
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
    try (Pool pool = Pool.createPool(library, dsn);
        Connection connection = pool.createConnection()) {
      // The Execute method is used for all types of statements.
      // The input type is always an ExecuteSqlRequest, even for DDL statements.
      // The return type is always Rows.
      try (Rows rows =
          connection.execute(
              ExecuteSqlRequest.newBuilder()
                  .setSql(
                      "create table my_table (" + "id int64 primary key, " + "value string(max))")
                  .build())) {
        // A DDL statement does not return any rows.
        assertNull(rows.next());
        // There is no update count for DDL statements.
        // The library returns the Java standard constant for the update count for DDL statements.
        assertEquals(java.sql.Statement.SUCCESS_NO_INFO, rows.getUpdateCount());
      }
    }

    assertEquals(0, mockSpanner.countRequestsOfType(ExecuteSqlRequest.class));
    assertEquals(1, mockDatabaseAdmin.getRequests().size());
  }

  @Test
  public void testExecuteCustomSql() {
    // The Execute method can be used to execute any type of SQL statement, including statements
    // that are handled internally by the Spanner library. This includes for example statements to
    // start and commit a transaction.

    String selectSql = "select value from my_val where id=@id";
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.newBuilder(selectSql).bind("id").to(1L).build(),
            ResultSet.newBuilder()
                .setMetadata(
                    ResultSetMetadata.newBuilder()
                        .setRowType(
                            StructType.newBuilder()
                                .addFields(
                                    Field.newBuilder()
                                        .setType(Type.newBuilder().setCode(TypeCode.STRING).build())
                                        .setName("value")
                                        .build())
                                .build())
                        .build())
                .addRows(
                    ListValue.newBuilder()
                        .addValues(
                            com.google.protobuf.Value.newBuilder().setStringValue("bar").build())
                        .build())
                .build()));

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
    try (Pool pool = Pool.createPool(library, dsn);
        Connection connection = pool.createConnection()) {
      // The Execute method is used for all types of statements.
      // This starts a new transaction on the connection.
      try (Rows rows = connection.execute(ExecuteSqlRequest.newBuilder().setSql("begin").build())) {
        assertNull(rows.next());
        assertEquals(java.sql.Statement.SUCCESS_NO_INFO, rows.getUpdateCount());
      }

      // Execute a parameterized query using the current transaction.
      try (Rows rows =
          connection.execute(
              ExecuteSqlRequest.newBuilder()
                  .setSql(selectSql)
                  .setParams(
                      Struct.newBuilder()
                          .putFields(
                              "id",
                              com.google.protobuf.Value.newBuilder().setStringValue("1").build())
                          .build())
                  .putAllParamTypes(
                      ImmutableMap.of("id", Type.newBuilder().setCode(TypeCode.INT64).build()))
                  .build())) {
        ListValue row = rows.next();
        assertNotNull(row);
        assertEquals(1, row.getValuesList().size());
        assertTrue(row.getValues(0).hasStringValue());
        assertEquals("bar", row.getValues(0).getStringValue());
        assertNull(rows.next());
      }

      // Execute a DML statement using the current transaction.
      try (Rows rows =
          connection.execute(
              ExecuteSqlRequest.newBuilder()
                  .setSql(updateSql)
                  .setParams(
                      Struct.newBuilder()
                          .putFields(
                              "id",
                              com.google.protobuf.Value.newBuilder().setStringValue("1").build())
                          .putFields(
                              "value",
                              com.google.protobuf.Value.newBuilder().setStringValue("foo").build())
                          .build())
                  .putAllParamTypes(
                      ImmutableMap.of(
                          "id", Type.newBuilder().setCode(TypeCode.INT64).build(),
                          "value", Type.newBuilder().setCode(TypeCode.STRING).build()))
                  .build())) {
        // There should be no rows.
        assertNull(rows.next());
        // There should be an update count.
        assertEquals(1, rows.getUpdateCount());
      }

      // Commit the transaction.
      try (Rows rows =
          connection.execute(ExecuteSqlRequest.newBuilder().setSql("commit").build())) {
        assertNull(rows.next());
        assertEquals(java.sql.Statement.SUCCESS_NO_INFO, rows.getUpdateCount());
      }
    }

    assertEquals(2, mockSpanner.countRequestsOfType(ExecuteSqlRequest.class));
    ExecuteSqlRequest selectRequest = mockSpanner.getRequestsOfType(ExecuteSqlRequest.class).get(0);
    assertEquals(selectSql, selectRequest.getSql());
    assertTrue(selectRequest.hasTransaction());
    // The library uses inline-begin-transaction, even if a transaction is started by executing a
    // BEGIN statement.
    assertTrue(selectRequest.getTransaction().hasBegin());
    assertTrue(selectRequest.getTransaction().getBegin().hasReadWrite());

    ExecuteSqlRequest updateRequest = mockSpanner.getRequestsOfType(ExecuteSqlRequest.class).get(1);
    assertEquals(updateSql, updateRequest.getSql());
    assertTrue(updateRequest.hasTransaction());
    assertTrue(updateRequest.getTransaction().hasId());

    assertEquals(1, mockSpanner.countRequestsOfType(CommitRequest.class));
  }

  @Test
  public void testMultiStatement() {
    String random1 = "select * from random1";
    String random2 = "select * from random2";
    int numRows = 10;
    RandomResultSetGenerator generator = new RandomResultSetGenerator(numRows);
    int numCols = RandomResultSetGenerator.generateAllTypes(Dialect.GOOGLE_STANDARD_SQL).length;
    mockSpanner.putStatementResult(
        StatementResult.query(Statement.of(random1), generator.generate()));
    mockSpanner.putStatementResult(
        StatementResult.query(Statement.of(random2), generator.generate()));

    String dsn =
        String.format(
            "localhost:%d/projects/p/instances/i/databases/d?usePlainText=true", getPort());
    try (Pool pool = Pool.createPool(library, dsn);
        Connection connection = pool.createConnection()) {
      try (Rows rows =
          connection.execute(
              ExecuteSqlRequest.newBuilder()
                  .setSql(String.format("%s;%s", random1, random2))
                  .build())) {
        ListValue row;
        ResultSetMetadata metadata = rows.getMetadata();
        assertEquals(numCols, metadata.getRowType().getFieldsCount());

        int resultSetCount = 0;
        do {
          int rowCount = 0;
          while ((row = rows.next()) != null) {
            rowCount++;
            assertEquals(numCols, row.getValuesList().size());
          }
          assertEquals(numRows, rowCount);
          resultSetCount++;
        } while (rows.nextResultSet());
        assertEquals(2, resultSetCount);
      }
    }

    assertEquals(2, mockSpanner.countRequestsOfType(ExecuteSqlRequest.class));
    ExecuteSqlRequest request1 = mockSpanner.getRequestsOfType(ExecuteSqlRequest.class).get(0);
    assertEquals(random1, request1.getSql());
    ExecuteSqlRequest request2 = mockSpanner.getRequestsOfType(ExecuteSqlRequest.class).get(1);
    assertEquals(random2, request2.getSql());
  }

  @Test
  public void testMultiStatement_MoveToNextResultSetHalfway() {
    String random1 = "select * from random1";
    String random2 = "select * from random2";
    int numRows = 20;
    int readRowsPerResultSet = ThreadLocalRandom.current().nextInt(numRows);
    RandomResultSetGenerator generator = new RandomResultSetGenerator(numRows);
    int numCols = RandomResultSetGenerator.generateAllTypes(Dialect.GOOGLE_STANDARD_SQL).length;
    mockSpanner.putStatementResult(
        StatementResult.query(Statement.of(random1), generator.generate()));
    mockSpanner.putStatementResult(
        StatementResult.query(Statement.of(random2), generator.generate()));

    int totalReadRows = 0;
    String dsn =
        String.format(
            "localhost:%d/projects/p/instances/i/databases/d?usePlainText=true", getPort());
    try (Pool pool = Pool.createPool(library, dsn);
        Connection connection = pool.createConnection()) {
      try (Rows rows =
          connection.execute(
              ExecuteSqlRequest.newBuilder()
                  .setSql(String.format("%s;%s", random1, random2))
                  .build())) {
        ListValue row;
        ResultSetMetadata metadata = rows.getMetadata();
        assertEquals(numCols, metadata.getRowType().getFieldsCount());

        int resultSetCount = 0;
        do {
          int rowCount = 0;
          while ((row = rows.next()) != null && rowCount < readRowsPerResultSet) {
            rowCount++;
            totalReadRows++;
            assertEquals(numCols, row.getValuesList().size());
          }
          assertEquals(readRowsPerResultSet, rowCount);
          resultSetCount++;
        } while (rows.nextResultSet());
        assertEquals(2, resultSetCount);
      }
    }

    assertEquals(readRowsPerResultSet * 2, totalReadRows);

    assertEquals(2, mockSpanner.countRequestsOfType(ExecuteSqlRequest.class));
    ExecuteSqlRequest request1 = mockSpanner.getRequestsOfType(ExecuteSqlRequest.class).get(0);
    assertEquals(random1, request1.getSql());
    ExecuteSqlRequest request2 = mockSpanner.getRequestsOfType(ExecuteSqlRequest.class).get(1);
    assertEquals(random2, request2.getSql());
  }
}
