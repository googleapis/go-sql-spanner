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
import com.google.spanner.v1.ResultSetMetadata;
import com.google.spanner.v1.ResultSetStats;
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

  public Connection getConnection() {
    return connection;
  }

  @Override
  public void close() {
    getLibrary().closeRows(this);
  }

  public ResultSetMetadata getMetadata() {
    return getLibrary().getMetadata(this);
  }

  public ResultSetStats getResultSetStats() {
    return getLibrary().getResultSetStats(this);
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
    return getLibrary().next(this);
  }
}
