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

import com.sun.jna.Pointer;
import com.sun.jna.Structure;
import java.util.Arrays;
import java.util.List;

/** {@link Message} is the Java definition of the return type for all functions in SpannerLib. */
public class Message extends Structure implements Structure.ByValue {
  /**
   * The ID of the memory that was returned by a function and that must be released once the Java
   * library is done with the data.
   */
  public long pinner;

  /** The result code of a function call. */
  public int code;

  /** The ID of the object that was created by the function call. */
  public long objectId;

  /** The length of the data that was returned. */
  public int length;

  /** A pointer to the actual data, or null if no data was returned. */
  public Pointer pointer;

  @Override
  protected List<String> getFieldOrder() {
    return Arrays.asList("pinner", "code", "objectId", "length", "pointer");
  }
}
