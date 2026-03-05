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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import com.google.cloud.spanner.MockSpannerServiceImpl.SimulatedExecutionTime;
import com.google.rpc.Code;
import com.google.spanner.v1.CreateSessionRequest;
import io.grpc.Status;
import org.junit.Test;

public class PoolTest extends AbstractSpannerLibTest {

  @Test
  public void testCreatePool() {
    String dsn =
        String.format(
            "localhost:%d/projects/p/instances/i/databases/d?usePlainText=true", getPort());
    try (Pool pool = Pool.createPool(library, dsn)) {
      assertTrue(pool.getId() > 0);
      assertEquals(1, mockSpanner.countRequestsOfType(CreateSessionRequest.class));
    }
  }

  @Test
  public void testCreatePoolFails() {
    mockSpanner.setCreateSessionExecutionTime(
        SimulatedExecutionTime.ofStickyException(
            Status.PERMISSION_DENIED.withDescription("Not allowed").asRuntimeException()));

    String dsn =
        String.format(
            "localhost:%d/projects/p/instances/i/databases/d?usePlainText=true", getPort());
    SpannerLibException exception =
        assertThrows(SpannerLibException.class, () -> Pool.createPool(library, dsn));
    assertEquals(Code.PERMISSION_DENIED.getNumber(), exception.getStatus().getCode());
  }
}
