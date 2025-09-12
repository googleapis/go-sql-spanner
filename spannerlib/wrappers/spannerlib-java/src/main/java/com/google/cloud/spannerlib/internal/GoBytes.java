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
import com.google.protobuf.CodedOutputStream;
import com.google.protobuf.Message;
import com.google.rpc.Code;
import com.google.rpc.Status;
import com.sun.jna.Native;
import com.sun.jna.Pointer;
import com.sun.jna.Structure;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;

/** {@link GoBytes} is the Java representation of a Go byte slice ([]byte). */
public class GoBytes extends Structure implements Structure.ByReference, AutoCloseable {
  // JNA does not allow these fields to be final.

  /** The pointer to the actual data. */
  public Pointer p;

  /** The number of bytes in the slice (the length). */
  public long n;

  /** The capacity of the slice. This is always equal to the length when created in Java. */
  public long c;

  /** Serializes a protobuf message into a Go byte slice. */
  public static GoBytes serialize(Message message) {
    int size = message.getSerializedSize();
    ByteBuffer buffer = ByteBuffer.allocateDirect(size);
    try {
      message.writeTo(CodedOutputStream.newInstance(buffer));
    } catch (IOException exception) {
      throw new SpannerLibException(
          Status.newBuilder()
              .setCode(Code.INTERNAL_VALUE)
              .setMessage("Failed to serialize message")
              .build(),
          exception);
    }
    return new GoBytes(buffer, size);
  }

  GoBytes(ByteBuffer buffer, long size) {
    this.p = Native.getDirectBufferPointer(buffer);
    this.n = size;
    this.c = size;
  }

  @Override
  public void close() {}

  @Override
  protected List<String> getFieldOrder() {
    return Arrays.asList("p", "n", "c");
  }
}
