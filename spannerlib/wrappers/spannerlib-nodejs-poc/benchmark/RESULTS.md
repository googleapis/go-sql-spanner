# Benchmark Results

## Environment
- **Database**: `projects/span-cloud-testing/instances/gargsurbhi-testing/databases/test-db-koffi-136`

**Action Benchmarked**: A Single Point Read operation involving executing a SQL query (`SELECT ... WHERE SingerId = 1`), reading the first result row, and closing the result stream.

## Mitata Latency Benchmark

**cpu: Intel(R) Xeon(R) CPU @ 2.80GHz**
**runtime: node v22.22.2 (x64-linux)**

| Wrapper | Time (Avg) | Min … Max | p75 | p99 | p999 |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **Koffi** | 4'554 µs/iter | 2'985 µs … 6'313 µs | 5'295 µs | 6'288 µs | 6'313 µs |
| **N-API** | 4'608 µs/iter | 2'689 µs … 6'519 µs | 5'122 µs | 6'132 µs | 6'519 µs |
| **IPC** | 7'090 µs/iter | 4'831 µs … 11'449 µs | 8'010 µs | 11'449 µs | 11'449 µs |

## Summary of Manual Benchmark (Average of 5 Runs)

This table summarizes a manual benchmark designed to measure throughput and memory efficiency under sustained load.

Each run consists of 50,000 iterations of this action. The table displays the average results across 5 independent runs for each wrapper.

| Wrapper | Throughput | Avg Latency | p50 | p90 | p99 | Heap Diff (Avg) |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| **Koffi** | 230.82 ops/sec | 4.33 ms | 4.46 ms | 5.35 ms | 7.04 ms | 4.72 MB |
| **N-API** | 239.63 ops/sec | 4.17 ms | 4.30 ms | 5.23 ms | 6.62 ms | 4.06 MB |
| **IPC** | 171.31 ops/sec | 5.84 ms | 5.85 ms | 6.93 ms | 10.28 ms | 10.21 MB |

## Detailed Runs

### Koffi

| Run | Start Time | End Time | Throughput | Avg Latency | p50 | p90 | p99 | Heap Diff |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| Run 1 | 2026-04-17T09:05:36.744Z | 2026-04-17T09:09:15.783Z | 228.27 ops/sec | 4.38 ms | 4.50 ms | 5.39 ms | 7.35 ms | 2.63 MB |
| Run 2 | 2026-04-17T09:09:17.828Z | 2026-04-17T09:12:54.407Z | 230.86 ops/sec | 4.33 ms | 4.47 ms | 5.35 ms | 6.81 ms | 3.09 MB |
| Run 3 | 2026-04-17T09:12:56.451Z | 2026-04-17T09:16:31.826Z | 232.15 ops/sec | 4.31 ms | 4.44 ms | 5.31 ms | 6.82 ms | 6.09 MB |
| Run 4 | 2026-04-17T09:16:33.872Z | 2026-04-17T09:20:08.088Z | 233.41 ops/sec | 4.28 ms | 4.42 ms | 5.29 ms | 6.73 ms | 6.08 MB |
| Run 5 | 2026-04-17T09:20:10.133Z | 2026-04-17T09:23:48.067Z | 229.43 ops/sec | 4.36 ms | 4.47 ms | 5.38 ms | 7.47 ms | 5.71 MB |
| **Average** | - | - | 230.82 ops/sec | 4.33 ms | 4.46 ms | 5.35 ms | 7.04 ms | 4.72 MB |

### N-API

| Run | Start Time | End Time | Throughput | Avg Latency | p50 | p90 | p99 | Heap Diff |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| Run 1 | 2026-04-17T09:24:50.107Z | 2026-04-17T09:28:24.288Z | 233.45 ops/sec | 4.28 ms | 4.41 ms | 5.34 ms | 6.81 ms | 1.66 MB |
| Run 2 | 2026-04-17T09:28:26.330Z | 2026-04-17T09:31:56.971Z | 237.37 ops/sec | 4.21 ms | 4.35 ms | 5.24 ms | 6.47 ms | 2.83 MB |
| Run 3 | 2026-04-17T09:31:59.016Z | 2026-04-17T09:35:23.629Z | 244.36 ops/sec | 4.09 ms | 4.23 ms | 5.14 ms | 6.40 ms | 5.80 MB |
| Run 4 | 2026-04-17T09:35:25.673Z | 2026-04-17T09:38:53.506Z | 240.58 ops/sec | 4.16 ms | 4.26 ms | 5.22 ms | 6.84 ms | 5.21 MB |
| Run 5 | 2026-04-17T09:38:55.551Z | 2026-04-17T09:42:21.827Z | 242.39 ops/sec | 4.12 ms | 4.24 ms | 5.20 ms | 6.58 ms | 4.82 MB |
| **Average** | - | - | 239.63 ops/sec | 4.17 ms | 4.30 ms | 5.23 ms | 6.62 ms | 4.06 MB |

### IPC

| Run | Start Time | End Time | Throughput | Avg Latency | p50 | p90 | p99 | Heap Diff |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| Run 1 | 2026-04-17T09:43:23.923Z | 2026-04-17T09:48:16.443Z | 170.93 ops/sec | 5.85 ms | 5.85 ms | 6.94 ms | 10.36 ms | 13.09 MB |
| Run 2 | 2026-04-17T09:48:18.488Z | 2026-04-17T09:53:11.760Z | 170.49 ops/sec | 5.86 ms | 5.87 ms | 6.97 ms | 10.68 ms | 8.24 MB |
| Run 3 | 2026-04-17T09:53:13.800Z | 2026-04-17T09:58:03.738Z | 172.45 ops/sec | 5.80 ms | 5.82 ms | 6.89 ms | 10.28 ms | 6.35 MB |
| Run 4 | 2026-04-17T09:58:05.778Z | 2026-04-17T10:02:58.023Z | 171.09 ops/sec | 5.84 ms | 5.87 ms | 6.94 ms | 10.09 ms | 14.67 MB |
| Run 5 | 2026-04-17T10:03:00.069Z | 2026-04-17T10:07:51.418Z | 171.62 ops/sec | 5.83 ms | 5.84 ms | 6.89 ms | 9.97 ms | 8.72 MB |
| **Average** | - | - | 171.31 ops/sec | 5.84 ms | 5.85 ms | 6.93 ms | 10.28 ms | 10.21 MB |

