# Spanner Go Sql Examples

This directory contains samples for how to use the Spanner go-sql driver. Each sample can be executed
as a standalone application without the need for any prior setup, other than that Docker must be installed
on your system. Each sample will automatically:
1. Download and start the [Spanner Emulator](https://cloud.google.com/spanner/docs/emulator) in a Docker container.
2. Create a sample database and execute the sample on the sample database.
3. Shutdown the Docker container that is running the emulator.

Running a sample is done by navigating to the corresponding sample directory and executing the following command:

```shell
# Change the 'helloword' directory below to any of the samples in this directory.
cd helloworld
go run main.go
```

## Prerequisites

Your system must have [Docker installed](https://docs.docker.com/get-docker/) for these samples to be executed,
as each sample will automatically start the Spanner Emulator in a Docker container.