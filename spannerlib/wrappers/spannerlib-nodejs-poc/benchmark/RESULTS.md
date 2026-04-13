# Benchmark Results

## Environment
- **Database**: `projects/span-cloud-testing/instances/gargsurbhi-testing/databases/test-db-koffi-136`

## Mitata Latency Benchmark
benchmark                    time (avg)             (min … max)       p75       p99      p999
--------------------------------------------------------------- -----------------------------
Koffi Single Point Read   4'228 µs/iter   (2'763 µs … 7'062 µs)  4'795 µs  6'234 µs  7'062 µs
N-API Single Point Read   4'223 µs/iter   (2'745 µs … 6'530 µs)  4'777 µs  5'694 µs  6'530 µs
IPC Single Point Read     6'744 µs/iter  (5'166 µs … 11'075 µs)  7'296 µs 11'075 µs 11'075 µs

## Summary of Manual Benchmark (Average of 5 Runs)

| Wrapper | Throughput | Avg Latency | Min Latency | Max Latency | Heap Diff (Avg) |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **Koffi** | 247.50 ops/sec | 4.04 ms | 2.19 ms | 120.20 ms | 0.48 MB |
| **N-API** | 251.79 ops/sec | 3.97 ms | 2.08 ms | 114.84 ms | 0.29 MB |
| **IPC** | 178.48 ops/sec | 5.60 ms | 3.55 ms | 62.48 ms | 1.11 MB |

## Detailed Runs

### Koffi

| Run | Start Time | End Time | Throughput | Avg Latency | Min Latency | Max Latency | Heap Diff |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| Run 1 | 2026-04-13T16:09:56.696Z | 2026-04-13T16:13:17.847Z | 248.57 ops/sec | 4.02 ms | 2.10 ms | 211.72 ms | 0.63 MB |
| Run 2 | 2026-04-13T16:13:19.850Z | 2026-04-13T16:16:39.698Z | 250.19 ops/sec | 4.00 ms | 2.21 ms | 50.32 ms | 0.60 MB |
| Run 3 | 2026-04-13T16:16:41.700Z | 2026-04-13T16:20:04.955Z | 246.00 ops/sec | 4.06 ms | 2.22 ms | 46.13 ms | 0.50 MB |
| Run 4 | 2026-04-13T16:20:06.957Z | 2026-04-13T16:23:30.452Z | 245.71 ops/sec | 4.07 ms | 2.23 ms | 83.57 ms | 0.59 MB |
| Run 5 | 2026-04-13T16:23:32.455Z | 2026-04-13T16:26:54.852Z | 247.04 ops/sec | 4.05 ms | 2.18 ms | 209.29 ms | 0.05 MB |
| **Average** | - | - | 247.50 ops/sec | 4.04 ms | 2.19 ms | 120.20 ms | 0.48 MB |

### N-API

| Run | Start Time | End Time | Throughput | Avg Latency | Min Latency | Max Latency | Heap Diff |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| Run 1 | 2026-04-13T16:27:56.860Z | 2026-04-13T16:31:15.625Z | 251.55 ops/sec | 3.97 ms | 2.07 ms | 212.07 ms | -1.32 MB |
| Run 2 | 2026-04-13T16:31:17.628Z | 2026-04-13T16:34:37.128Z | 250.63 ops/sec | 3.99 ms | 2.07 ms | 211.28 ms | 0.60 MB |
| Run 3 | 2026-04-13T16:34:39.131Z | 2026-04-13T16:37:58.728Z | 250.50 ops/sec | 3.99 ms | 2.10 ms | 49.52 ms | -0.15 MB |
| Run 4 | 2026-04-13T16:38:00.729Z | 2026-04-13T16:41:19.177Z | 251.96 ops/sec | 3.97 ms | 2.10 ms | 48.44 ms | -0.19 MB |
| Run 5 | 2026-04-13T16:41:21.180Z | 2026-04-13T16:44:37.807Z | 254.29 ops/sec | 3.93 ms | 2.07 ms | 52.89 ms | 2.50 MB |
| **Average** | - | - | 251.79 ops/sec | 3.97 ms | 2.08 ms | 114.84 ms | 0.29 MB |

### IPC

| Run | Start Time | End Time | Throughput | Avg Latency | Min Latency | Max Latency | Heap Diff |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| Run 1 | 2026-04-13T16:45:39.821Z | 2026-04-13T16:50:15.184Z | 181.58 ops/sec | 5.51 ms | 3.54 ms | 100.94 ms | 7.74 MB |
| Run 2 | 2026-04-13T16:50:17.187Z | 2026-04-13T16:54:54.569Z | 180.26 ops/sec | 5.55 ms | 3.56 ms | 53.70 ms | -0.07 MB |
| Run 3 | 2026-04-13T16:54:56.572Z | 2026-04-13T16:59:37.603Z | 177.92 ops/sec | 5.62 ms | 3.57 ms | 52.15 ms | -0.16 MB |
| Run 4 | 2026-04-13T16:59:39.606Z | 2026-04-13T17:04:22.795Z | 176.56 ops/sec | 5.66 ms | 3.57 ms | 55.32 ms | -5.46 MB |
| Run 5 | 2026-04-13T17:04:24.798Z | 2026-04-13T17:09:08.731Z | 176.10 ops/sec | 5.68 ms | 3.50 ms | 50.27 ms | 3.48 MB |
| **Average** | - | - | 178.48 ops/sec | 5.60 ms | 3.55 ms | 62.48 ms | 1.11 MB |

