# Generated gRPC Code

This directory contains generated gRPC Python code for Google Cloud Spanner services.

## Why are these files here?

These files are generated from the [googleapis](https://github.com/googleapis/googleapis) protocol buffer definitions. We check them into source control to:
1.  Avoid requiring `grpcio-tools` and the `googleapis` repository as build-time dependencies for users.
2.  Ensure consistent generated code across different environments.
3.  Simplify the installation process for the mock server.

## How to regenerate

To regenerate these files (e.g., to update to a newer version of the Spanner protos), run the following command from the `spanner-mockserver` directory:

```bash
nox -s generate_grpc
```

This command will:
1.  Clone the `googleapis` repository.
2.  Run `grpc_tools.protoc` to generate the Python code.
3.  Copy the relevant `*_pb2_grpc.py` files to this directory.

## Steps to generate spanner_pb2_grpc.py

```shell
pip install grpcio-tools
git clone git@github.com:googleapis/googleapis.git
cd googleapis
python -m grpc_tools.protoc \
    -I . \
    --python_out=. --pyi_out=. --grpc_python_out=. \
    ./google/spanner/v1/*.proto
```

## Steps to generate spanner_database_admin_pb2_grpc.py

```shell
pip install grpcio-tools
git clone git@github.com:googleapis/googleapis.git
cd googleapis
python -m grpc_tools.protoc \
    -I . \
    --python_out=. --pyi_out=. --grpc_python_out=. \
    ./google/spanner/admin/database/v1/*.proto
```
