# SPANNERLIB-PYTHON: A High-Performance Python Wrapper for the Go Spanner Client Shared lib

> **NOTICE:** This is an internal library that can make breaking changes without prior notice.

## Introduction
The `spannerlib-python` wrapper provides a high-performance, idiomatic Python interface for Google Cloud Spanner by wrapping the official Go Client Shared library.

The Go library is compiled into a C-shared library, and this project calls it directly from Python, aiming to combine Go's performance with Python's ease of use.

## Code Structure

```bash
spannerlib-python/
|___google/cloud/spannerlib/
    |___internal - SpannerLib wrapper
        |___lib - Spannerlib artifacts
|___tests/
    |___unit/ - Unit tests
    |___system/ - System tests
|___samples
README.md
noxfile.py
pyproject.toml - Project config for packaging
```

## NOX Setup

1. Create virtual environment

**Mac/Linux**
```bash
pip install virtualenv
virtualenv <your-env>
source <your-env>/bin/activate
```

**Windows**
```bash
pip install virtualenv
virtualenv <your-env>
<your-env>\Scripts\activate
```

**Install Dependencies**
```bash
pip install -r requirements.txt
```

To run the nox tests, navigate to the root directory of this wrapper (`spannerlib-python`) and run:

**format/Lint**

```bash
nox -s format lint
```

**Unit Tests**

```bash
nox -s unit
```

Run specific tests
```bash
# file
nox -s unit-3.13 -- tests/unit/test_connection.py
# class
nox -s unit-3.13 -- tests/unit/test_connection.py::TestConnection
# method
nox -s unit-3.13 -- tests/unit/test_connection.py::TestConnection::test_close_connection_propagates_error
```

**System Tests**

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

    ```bash
    nox -s system
    ```

## Build and install

**Package**

Create python wheel

```bash
pip3 install build
python3 -m build
```

**Validate Package**

```bash
pip3 install twine
twine check dist/*
unzip -l dist/spannerlib-*-*.whl
tar -tvzf dist/spannerlib-*.tar.gz
```

**Install locally**

```bash
pip3 install -e .
```


