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

import static com.google.cloud.spannerlib.GrpcSpannerLibraryImpl.toSpannerLibException;

import com.google.cloud.spannerlib.v1.RowData;
import com.google.common.base.Preconditions;
import com.google.protobuf.ListValue;
import com.google.rpc.Code;
import com.google.spanner.v1.ResultSetMetadata;
import com.google.spanner.v1.ResultSetStats;
import io.grpc.StatusException;
import io.grpc.stub.BlockingClientCall;

public class StreamingRows extends Rows {
  private final BlockingClientCall<?, RowData> stream;
  private boolean done;
  private ListValue pendingRow;
  private ResultSetMetadata metadata;
  private ResultSetStats stats;
  private boolean pendingNextResultSetCall;

  StreamingRows(Connection connection, BlockingClientCall<?, RowData> stream) {
    super(connection, 0L);
    this.stream = Preconditions.checkNotNull(stream);
    this.pendingRow = next();
  }

  @Override
  public void close() {
    if (done) {
      return;
    }
    markDone();
    cancel("Rows closed");
  }

  void cancel(String message) {
    this.stream.cancel(message, null);
  }

  private void markDone() {
    this.done = true;
    getConnection().deregisterStream(this);
  }

  @Override
  public ListValue next() {
    if (this.pendingNextResultSetCall) {
      return null;
    }
    if (this.pendingRow != null) {
      ListValue row = pendingRow;
      this.pendingRow = null;
      return row;
    }
    try {
      RowData rowData = stream.read();
      if (rowData == null) {
        markDone();
        return null;
      }
      if (rowData.hasMetadata()) {
        this.metadata = rowData.getMetadata();
      }
      if (rowData.hasStats()) {
        this.stats = rowData.getStats();
      }
      if (rowData.getDataCount() == 0) {
        if (rowData.getHasMoreResults()) {
          pendingNextResultSetCall = true;
        } else {
          markDone();
        }
        return null;
      }
      return rowData.getData(0);
    } catch (StatusException exception) {
      markDone();
      throw toSpannerLibException(exception);
    } catch (InterruptedException exception) {
      throw new SpannerLibException(Code.CANCELLED, "next() was cancelled", exception);
    }
  }

  @Override
  public ResultSetMetadata getMetadata() {
    return this.metadata;
  }

  @Override
  public ResultSetStats getResultSetStats() {
    if (this.stats == null) {
      throw new SpannerLibException(
          Code.FAILED_PRECONDITION, "stats can only be fetched once all data has been fetched");
    }
    return this.stats;
  }

  @Override
  public boolean nextResultSet() {
    if (this.done) {
      return false;
    }
    // Read data until we reach the next result set.
    //noinspection StatementWithEmptyBody
    while (!this.pendingNextResultSetCall && next() != null) {}
    if (this.pendingNextResultSetCall) {
      this.pendingNextResultSetCall = false;
      return true;
    }
    return false;
  }
}
