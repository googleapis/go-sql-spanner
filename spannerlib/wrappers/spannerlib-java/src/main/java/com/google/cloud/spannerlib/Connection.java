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
import com.google.spanner.v1.ExecuteSqlRequest;

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

  @Override
  public void close() {
    executeAndRelease(getLibrary(), library -> library.CloseConnection(pool.getId(), getId()));
  }
}
