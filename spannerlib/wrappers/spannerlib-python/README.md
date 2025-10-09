# SPANNERLIB-PY: A High-Performance Python Wrapper for the Go Spanner Client Shared lib 🐍

## Introduction
spannerlib-py provides a high-performance, idiomatic Python interface for Google Cloud Spanner by wrapping the official Go Client Shared library.

The Go library is compiled into a C-shared library (.so), and this project uses ctypes to call it directly from Python, aiming to combine Go's performance with Python's ease of use.

**Code Structure**

```bash
spannerlib-python/
|___google/cloud/spannerlib/
    |_____internal - SpannerLib wrapper
|___tests/
    |___unit/ - Unit tests
    |___system/ - System tests
|___samples
README.md
noxfile.py
myproject.toml - Project config for packaging
```

**Lint support**

```bash
nox -s format lint
```

## Running Tests

### Unit Tests

To run the unit tests, navigate to the root directory of this wrapper (`spannerlib-python`) and run:

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
    Navigate to the root directory of this wrapper (`spannerlib-python`) and run:
    ```bash
    python3 -m unittest tests/system/test_spannerlib_wrapper.py
    ```

## Build and install

**Install locally**
```bash
pip3 install -e .
```

**Package**
```bash
pip3 install build
python3 -m build
```

**Validate** 
```bash
pip3 install twine
twine check dist/*
unzip -l dist/spannerlib-0.1.0-py3-none-any.whl
tar -tvzf dist/spannerlib-0.1.0.tar.gz 
``` 
