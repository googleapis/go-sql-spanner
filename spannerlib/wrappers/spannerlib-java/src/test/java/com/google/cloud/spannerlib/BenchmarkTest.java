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

import com.google.common.base.Stopwatch;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.protobuf.ListValue;
import com.google.protobuf.Struct;
import com.google.protobuf.Value;
import com.google.spanner.v1.ExecuteSqlRequest;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import org.junit.Ignore;
import org.junit.Test;

public class BenchmarkTest extends AbstractSpannerLibTest {
  static class Stats {
    public final Duration min;
    public final Duration p50;
    public final Duration p90;
    public final Duration p95;
    public final Duration p99;
    public final Duration max;
    public final Duration avg;

    public Stats(
        Duration min,
        Duration p50,
        Duration p90,
        Duration p95,
        Duration p99,
        Duration max,
        Duration avg) {
      this.min = min;
      this.p50 = p50;
      this.p90 = p90;
      this.p95 = p95;
      this.p99 = p99;
      this.max = max;
      this.avg = avg;
    }

    @Override
    public String toString() {
      return String.format(
          "Min: %s\n"
              + "P50: %s\n"
              + "P90: %s\n"
              + "P95: %s\n"
              + "P99: %s\n"
              + "Max: %s\n"
              + "Avg: %s\n",
          min, p50, p90, p95, p99, max, avg);
    }
  }

  @Test
  @Ignore("For local testing")
  public void testBenchmarkRealSpanner() throws Exception {
    try (Pool pool =
        Pool.createPool(
            library,
            "projects/appdev-soda-spanner-staging/instances/knut-test-ycsb/databases/knut-test-db")) {
      Duration warmupDuration = readRandomRows(pool, 10);
      System.out.printf("Warmup duration: %s%n", warmupDuration);

      for (int i = 0; i < 10; i++) {
        int numRows = (i + 1) * 10;
        Duration duration = readRandomRows(pool, numRows);
        System.out.printf("Duration (%d): %s%n", numRows, duration);
      }

      int numThreads = 100;
      int numTasks = 200;
      ListeningExecutorService executor =
          MoreExecutors.listeningDecorator(Executors.newCachedThreadPool());
      List<ListenableFuture<Duration>> futures = new ArrayList<>(numTasks);
      for (int i = 0; i < numTasks; i++) {
        futures.add(executor.submit(() -> readRandomRows(pool, 10)));
      }
      Futures.allAsList(futures).get();
      Stats stats = calculateStats(new ArrayList<>(Futures.allAsList(futures).get()));
      System.out.println();
      System.out.printf("Num tasks: %d\n", numTasks);
      System.out.println(stats);
    }
  }

  private static Stats calculateStats(List<Duration> durations) {
    durations.sort(Duration::compareTo);
    return new Stats(
        durations.get(0),
        durations.get(durations.size() * 50 / 100),
        durations.get(durations.size() * 90 / 100),
        durations.get(durations.size() * 95 / 100),
        durations.get(durations.size() * 99 / 100),
        durations.get(durations.size() - 1),
        Duration.ofNanos(
            (long) durations.stream().mapToLong(Duration::toNanos).average().orElse(0.0d)));
  }

  private static Duration readRandomRows(Pool pool, int maxRows) {
    HashSet<Integer> set = new HashSet<Integer>();
    ListValue.Builder builder = ListValue.newBuilder();
    for (int c = 0; c < maxRows; c++) {
      int id = ThreadLocalRandom.current().nextInt(1, 1_000_001);
      set.add(id);
      builder.addValues(Value.newBuilder().setStringValue(String.valueOf(id)).build());
    }
    try (Connection connection = pool.createConnection()) {
      Stopwatch stopwatch = Stopwatch.createStarted();
      ExecuteSqlRequest request =
          ExecuteSqlRequest.newBuilder()
              .setSql("select * from all_types where col_bigint = any($1)")
              .setParams(
                  Struct.newBuilder()
                      .putFields("p1", Value.newBuilder().setListValue(builder.build()).build())
                      .build())
              .build();
      int count = 0;
      try (Rows rows = connection.execute(request)) {
        ListValue row;
        while ((row = rows.next()) != null) {
          assertEquals(10, row.getValuesList().size());
          count++;
        }
        assertEquals(set.size(), count);
      }
      return stopwatch.elapsed();
    }
  }
}
