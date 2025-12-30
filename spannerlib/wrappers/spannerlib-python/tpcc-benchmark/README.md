# Spanner TPC-C Benchmark (Python)

This package contains a TPC-C inspired benchmark implementation for Google Cloud Spanner using the Python DBAPI. It supports benchmarking both the standard `google-cloud-spanner` DBAPI and the new `spanner-python-driver`.

## Prerequisites

- Python 3.10+
- Google Cloud Spanner Instance and Database created.
- `GOOGLE_APPLICATION_CREDENTIALS` set to your service account key.

## Installation

```bash
pip install -r requirements.txt
```

## Running the Benchmark

You can run the benchmark using `nox` for a convenient, reproducible environment.

### Using Nox (Recommended)

The `nox` session handles dependency installation and allows you to easily switch between drivers.

**Environment Variables:**

You can set these environment variables to avoid passing them as arguments every time:

*   `SPANNER_PROJECT`: Your GCP Project ID.
*   `SPANNER_INSTANCE`: Your Spanner Instance ID.
*   `SPANNER_DATABASE`: Your Spanner Database ID.
*   `GOOGLE_APPLICATION_CREDENTIALS`: Path to your service account key (Required).

**Run with default settings (using env vars):**

```bash
nox -s benchmark
```

**Run with explicit arguments:**

```bash
nox -s benchmark -- --project my-project --instance my-instance --database my-db --load
```

**Select a specific driver:**

By default, it runs against both drivers. To run against a specific one:

```bash
# Run against the standard Google Cloud Spanner DBAPI
nox -s benchmark(driver="google.cloud.spanner_dbapi")

# Run against the new Spanner Python Driver
nox -s benchmark(driver="google.cloud.spanner_python_driver")
```

### Running Directly with Python

```bash
python tpcc-benchmark/run_benchmark.py \
  --project my-project \
  --instance my-instance \
  --database my-database \
  --driver google.cloud.spanner_python_driver \
  --load \
  --duration 60
```

## Benchmark Options

*   `--load`: If set, the benchmark will create the schema and load initial data before running the workload. **Warning:** This may take some time.
*   `--duration`: Duration of the benchmark run in seconds (default: 60).
*   `--driver`: logic to select the driver.
    *   `google.cloud.spanner_dbapi`: The existing Spanner DBAPI.
    *   `google.cloud.spanner_python_driver`: The new Spanner Python Driver (requires separate installation or local path).

## Project Structure

*   `tpcc_benchmark/`: Source code package.
    *   `spanner_tpcc.py`: Core TPC-C transaction logic (New Order, Payment).
    *   `run_benchmark.py`: Main entry point and driver selector.
*   `noxfile.py`: Automation for linting, formatting, and running benchmarks.
*   `requirements.txt`: Python dependencies.
*   `schema.ddl`: Database schema definition.
