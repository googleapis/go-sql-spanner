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
import com.google.common.base.Preconditions;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.rpc.Code;
import com.google.rpc.Status;
import com.sun.jna.Pointer;
import java.nio.ByteBuffer;

/** {@link MessageHandler} is a helper class for handling a return value from a library call. */
public class MessageHandler implements AutoCloseable {
  private final SpannerLibrary library;
  private final Message message;
  private boolean closed = false;

  MessageHandler(SpannerLibrary library, Message message) {
    this.library = Preconditions.checkNotNull(library);
    this.message = Preconditions.checkNotNull(message);
  }

  /** Throws a {@link SpannerLibException} if the {@link Message} contains an error code. */
  void throwIfError() {
    if (hasError()) {
      throw new SpannerLibException(getError());
    }
  }

  @Override
  public void close() {
    if (closed) {
      return;
    }
    closed = true;
    // Release the memory that was reserved for this result (if any).
    if (message.pinner != 0) {
      int res = this.library.Release(message.pinner);
      if (res != 0) {
        throw new SpannerLibException(Code.INTERNAL, "Failed to release message");
      }
    }
  }

  /** Returns the result code of this message. */
  public int getCode() {
    return this.message.code;
  }

  /** Returns the length of the data that was returned (if any). */
  public int getLength() {
    return this.message.length;
  }

  /**
   * Returns a pointer to the data that was returned (if any). Throws a {@link SpannerLibException}
   * if the function returned an error code.
   */
  public Pointer getValue() {
    throwIfError();
    return this.message.pointer;
  }

  /**
   * Returns the ID of the object that was created (if any). Throws a {@link SpannerLibException} if
   * the function returned an error code.
   */
  public long getObjectId() {
    throwIfError();
    return this.message.objectId;
  }

  /** Returns true if this message contains an error code. */
  public boolean hasError() {
    return message.code != 0;
  }

  /** Returns the error that is encoded in this message, or null if there is no error. */
  public Status getError() {
    if (message.code == 0) {
      return null;
    }
    ByteBuffer buffer = message.pointer.getByteBuffer(0, message.length);
    try {
      return Status.parseFrom(buffer);
    } catch (InvalidProtocolBufferException exception) {
      throw new SpannerLibException(Code.INTERNAL, "Failed to parse status", exception);
    }
  }
}
