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
 * {@link AbstractLibraryObject} is the base class for all objects that are created by SpannerLib,
 * such as {@link Pool} and {@link Connection}.
 */
abstract class AbstractLibraryObject implements AutoCloseable {
  private final SpannerLibrary library;
  private final long id;

  AbstractLibraryObject(SpannerLibrary library, long id) {
    this.library = library;
    this.id = id;
  }

  SpannerLibrary getLibrary() {
    return this.library;
  }

  long getId() {
    return this.id;
  }
}
