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
import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * {@link WrappedGoBytes} is an {@link AutoCloseable} helper class for working with {@link GoBytes}.
 */
public class WrappedGoBytes implements AutoCloseable {
  private final GoBytes goBytes;
  private final ByteBuffer buffer;

  /** Serializes a protobuf {@link Message} into a {@link WrappedGoBytes} instance. */
  public static WrappedGoBytes serialize(Message message) {
    int size = message.getSerializedSize();
    byte[] bytes = message.toByteArray();
    ByteBuffer buffer = ByteBuffer.allocateDirect(size);
    try {
      message.writeTo(CodedOutputStream.newInstance(buffer));
    } catch (IOException exception) {
      throw new SpannerLibException(Code.INTERNAL, "Failed to serialize message", exception);
    }
    return new WrappedGoBytes(new GoBytes(buffer, size), buffer);
  }

  private WrappedGoBytes(GoBytes goBytes, ByteBuffer buffer) {
    this.goBytes = goBytes;
    this.buffer = buffer;
  }

  public GoBytes getGoBytes() {
    return goBytes;
  }

  @Override
  public void close() {
    this.buffer.clear();
  }
}
