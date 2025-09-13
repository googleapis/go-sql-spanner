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

package com.google.cloud.spannerlib.internal;

import com.google.cloud.spannerlib.SpannerLibException;
import com.sun.jna.Library;
import com.sun.jna.Native;
import java.util.function.Function;

/**
 * {@link SpannerLibrary} is the Java interface that corresponds with the public API of the native
 * SpannerLib library.
 */
public interface SpannerLibrary extends Library {
  SpannerLibrary LIBRARY = Native.load("spanner", SpannerLibrary.class);

  /** Returns the singleton instance of the library. */
  static SpannerLibrary getInstance() {
    return LIBRARY;
  }

  /**
   * Executes a library function and automatically releases any memory that was returned by the
   * library call when the supplied function finishes.
   */
  static void executeAndRelease(
      SpannerLibrary library, Function<SpannerLibrary, Message> function) {
    try (MessageHandler message = new MessageHandler(library, function.apply(library))) {
      message.throwIfError();
    }
  }

  /** Executes a library function and returns the result as a {@link MessageHandler}. */
  default MessageHandler execute(Function<SpannerLibrary, Message> function)
      throws SpannerLibException {
    return new MessageHandler(this, function.apply(this));
  }

  /** Releases the memory that is pointed to by the pinner. */
  int Release(long pinner);

  /** Creates a new Pool. */
  Message CreatePool(GoString dsn);

  /** Closes the given Pool. */
  Message ClosePool(long id);

  /** Creates a new Connection in the given Pool. */
  Message CreateConnection(long poolId);

  /** Closes the given Connection. */
  Message CloseConnection(long poolId, long connectionId);

  /** Executes a SQL statement on the given Connection. */
  Message Execute(long poolId, long connectionId, GoBytes executeSqlRequest);

  /** Returns the {@link com.google.spanner.v1.ResultSetMetadata} of the given Rows object. */
  Message Metadata(long poolId, long connectionId, long rowsId);

  /** Returns the next row from the given Rows object. */
  Message Next(long poolId, long connectionId, long rowsId, int numRows, int encoding);

  /** Returns the {@link com.google.spanner.v1.ResultSetStats} of the given Rows object. */
  Message ResultSetStats(long poolId, long connectionId, long rowsId);

  /** Closes the given Rows object. */
  Message CloseRows(long poolId, long connectionId, long rowsId);
}
