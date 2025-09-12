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

import com.google.rpc.Code;
import com.google.rpc.Status;

public class SpannerLibException extends RuntimeException {
  private final Status status;

  public SpannerLibException(Status status) {
    super(status.getMessage());
    this.status = status;
  }

  public SpannerLibException(Status status, Throwable cause) {
    super(status.getMessage(), cause);
    this.status = status;
  }

  public SpannerLibException(Code code, String message) {
    super(message);
    this.status = Status.newBuilder().setCode(code.getNumber()).setMessage(message).build();
  }

  public SpannerLibException(Code code, String message, Throwable cause) {
    super(message, cause);
    this.status = Status.newBuilder().setCode(code.getNumber()).setMessage(message).build();
  }

  public Status getStatus() {
    return status;
  }
}
