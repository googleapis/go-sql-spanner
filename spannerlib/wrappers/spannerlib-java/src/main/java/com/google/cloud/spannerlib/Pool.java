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

/**
 * A {@link Pool} that has been created by SpannerLib. A {@link Pool} can create any number of
 * {@link Connection} instances. All {@link Connection} instances share the same underlying Spanner
 * client.
 */
public class Pool extends AbstractLibraryObject {

  /** Creates a new {@link Pool} using the given connection string. */
  public static Pool createPool(SpannerLibrary library, String connectionString) {
    return library.createPool(connectionString);
  }

  Pool(SpannerLibrary library, long id) {
    super(library, id);
  }

  @Override
  public void close() {
    getLibrary().closePool(this);
  }

  /** Creates a new {@link Connection} in this {@link Pool}. */
  public Connection createConnection() {
    return getLibrary().createConnection(this);
  }
}
