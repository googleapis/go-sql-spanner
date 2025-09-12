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
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

import com.google.cloud.spanner.connection.AbstractMockServerTest;
import com.google.spanner.v1.CreateSessionRequest;
import org.junit.Test;

public class ConnectionTest extends AbstractMockServerTest {

  @Test
  public void testCreateConnection() {
    String dsn =
        String.format(
            "localhost:%d/projects/p/instances/i/databases/d?usePlainText=true", getPort());
    try (Pool pool = Pool.createPool(dsn);
        Connection connection = pool.createConnection()) {
      assertTrue(connection.getId() > 0);
      assertEquals(1, mockSpanner.countRequestsOfType(CreateSessionRequest.class));
    }
  }

  @Test
  public void testCreateTwoConnections() {
    String dsn =
        String.format(
            "localhost:%d/projects/p/instances/i/databases/d?usePlainText=true", getPort());
    try (Pool pool = Pool.createPool(dsn)) {
      try (Connection connection1 = pool.createConnection();
          Connection connection2 = pool.createConnection()) {
        assertTrue(connection1.getId() > 0);
        assertTrue(connection2.getId() > 0);
        assertNotEquals(connection1.getId(), connection2.getId());
        assertEquals(1, mockSpanner.countRequestsOfType(CreateSessionRequest.class));
      }
    }
  }
}
