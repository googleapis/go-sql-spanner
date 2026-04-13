# Benchmark Results

## Environment
- **Database**: `projects/span-cloud-testing/instances/gargsurbhi-testing/databases/test-db-koffi-136`

## Mitata Latency Benchmark

```
cpu: Apple M4 Pro
runtime: node v22.17.1 (arm64-darwin)

benchmark                    time (avg)             (min … max)       p75       p99      p999
--------------------------------------------------------------- -----------------------------
Koffi Single Point Read  66'049 µs/iter (62'838 µs … 69'484 µs) 68'705 µs 69'484 µs 69'484 µs
N-API Single Point Read  71'349 µs/iter (66'112 µs … 83'420 µs) 76'782 µs 83'420 µs 83'420 µs
IPC Single Point Read    75'546 µs/iter (66'407 µs … 97'423 µs) 91'583 µs 97'423 µs 97'423 µs
```

## Summary of Manual Benchmark (Average of 5 Runs)

| Wrapper | Throughput | Avg Latency | Min Latency | Max Latency | Heap Diff (Avg) |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **Koffi** | 13.83 ops/sec | 72.48 ms | 60.38 ms | 159.86 ms | 0.73 MB |
| **N-API** | 13.97 ops/sec | 71.81 ms | 59.34 ms | 267.20 ms | 0.55 MB |
| **IPC** | 13.24 ops/sec | 75.80 ms | 63.78 ms | 215.10 ms | 13.28 MB |

## Detailed Runs

### Koffi

| Run | Start Time | End Time | Throughput | Avg Latency | Min Latency | Max Latency | Heap Diff |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| Run 1 | 2026-04-13T06:26:00.195Z | 2026-04-13T06:26:35.666Z | 14.10 ops/sec | 70.94 ms | 60.02 ms | 369.14 ms | 0.68 MB |
| Run 2 | 2026-04-13T06:26:37.677Z | 2026-04-13T06:27:11.543Z | 14.76 ops/sec | 67.73 ms | 59.99 ms | 95.69 ms | 0.76 MB |
| Run 3 | 2026-04-13T06:27:13.556Z | 2026-04-13T06:27:51.062Z | 13.33 ops/sec | 75.01 ms | 61.05 ms | 108.63 ms | 0.72 MB |
| Run 4 | 2026-04-13T06:27:53.073Z | 2026-04-13T06:28:31.692Z | 12.95 ops/sec | 77.24 ms | 61.14 ms | 106.80 ms | 0.71 MB |
| Run 5 | 2026-04-13T06:28:33.703Z | 2026-04-13T06:29:09.433Z | 13.99 ops/sec | 71.46 ms | 59.70 ms | 119.04 ms | 0.78 MB |
| **Average** | - | - | 13.83 ops/sec | 72.48 ms | 60.38 ms | 159.86 ms | 0.73 MB |

### N-API

| Run | Start Time | End Time | Throughput | Avg Latency | Min Latency | Max Latency | Heap Diff |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| Run 1 | 2026-04-13T06:30:09.445Z | 2026-04-13T06:30:45.556Z | 13.85 ops/sec | 72.22 ms | 59.20 ms | 633.28 ms | 0.56 MB |
| Run 2 | 2026-04-13T06:30:47.561Z | 2026-04-13T06:31:20.317Z | 15.26 ops/sec | 65.51 ms | 59.25 ms | 131.44 ms | 0.53 MB |
| Run 3 | 2026-04-13T06:31:22.329Z | 2026-04-13T06:31:57.787Z | 14.10 ops/sec | 70.92 ms | 58.95 ms | 129.25 ms | 0.56 MB |
| Run 4 | 2026-04-13T06:31:59.797Z | 2026-04-13T06:32:36.084Z | 13.78 ops/sec | 72.57 ms | 59.58 ms | 192.24 ms | 0.55 MB |
| Run 5 | 2026-04-13T06:32:38.096Z | 2026-04-13T06:33:17.018Z | 12.85 ops/sec | 77.84 ms | 59.74 ms | 249.79 ms | 0.55 MB |
| **Average** | - | - | 13.97 ops/sec | 71.81 ms | 59.34 ms | 267.20 ms | 0.55 MB |

### IPC

| Run | Start Time | End Time | Throughput | Avg Latency | Min Latency | Max Latency | Heap Diff |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| Run 1 | 2026-04-13T06:34:17.030Z | 2026-04-13T06:34:58.429Z | 12.08 ops/sec | 82.80 ms | 63.94 ms | 608.08 ms | 8.40 MB |
| Run 2 | 2026-04-13T06:35:00.440Z | 2026-04-13T06:35:39.422Z | 12.83 ops/sec | 77.96 ms | 64.92 ms | 134.13 ms | 15.16 MB |
| Run 3 | 2026-04-13T06:35:41.437Z | 2026-04-13T06:36:17.121Z | 14.01 ops/sec | 71.37 ms | 62.60 ms | 112.61 ms | 15.49 MB |
| Run 4 | 2026-04-13T06:36:19.130Z | 2026-04-13T06:36:54.629Z | 14.09 ops/sec | 71.00 ms | 62.87 ms | 118.40 ms | 13.68 MB |
| Run 5 | 2026-04-13T06:36:56.642Z | 2026-04-13T06:37:34.589Z | 13.18 ops/sec | 75.89 ms | 64.54 ms | 102.27 ms | 13.66 MB |
| **Average** | - | - | 13.24 ops/sec | 75.80 ms | 63.78 ms | 215.10 ms | 13.28 MB |

