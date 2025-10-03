# SPANNERLIB-PY: A High-Performance Python Wrapper for the Go Spanner Client Shared lib 🐍

## Introduction
spannerlib-py provides a high-performance, idiomatic Python interface for Google Cloud Spanner by wrapping the official Go Client Shared library.

The Go library is compiled into a C-shared library (.so), and this project uses ctypes to call it directly from Python, aiming to combine Go's performance with Python's ease of use.

## Running Tests

### Unit Tests

To run the unit tests, navigate to the root directory of this wrapper (`spannerlib_py`) and run:

```bash
python3 -m unittest tests/unit/test_spannerlib_wrapper.py
```

### System Tests

The system tests require a Cloud Spanner Emulator instance running.

1.  **Pull and Run the Emulator:**
    ```bash
    docker pull gcr.io/cloud-spanner-emulator/emulator
    docker run -p 9010:9010 -p 9020:9020 -d gcr.io/cloud-spanner-emulator/emulator
    ```

2.  **Set Environment Variable:**
    Ensure the `SPANNER_EMULATOR_HOST` environment variable is set:
    ```bash
    export SPANNER_EMULATOR_HOST=localhost:9010
    ```

3.  **Create Test Instance and Database:**
    You need the `gcloud` CLI installed and configured.
    ```bash
    gcloud spanner instances create test-instance --config=emulator-config --description="Test Instance" --nodes=1
    gcloud spanner databases create testdb --instance=test-instance
    ```

4.  **Run the System Tests:**
    Navigate to the root directory of this wrapper (`spannerlib_py`) and run:
    ```bash
    python3 -m unittest tests/system/test_spannerlib_wrapper.py
    ```
