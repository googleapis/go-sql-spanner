# Spannerlib-Python System Tests

## Setup

## Emulator
To run these E2E tests against a Cloud Spanner Emulator:

1. Start the emulator: gcloud emulators spanner start
```shell
docker pull gcr.io/cloud-spanner-emulator/emulator

docker run -p 9010:9010 -p 9020:9020 -d gcr.io/cloud-spanner-emulator/emulator
```
2. Set the environment variable:
```shell
export SPANNER_EMULATOR_HOST=localhost:9010
```
3. Create a test instance and database in the emulator:
```shell
gcloud spanner instances create test-instance --config=emulator-config --description="Test Instance" --nodes=1

gcloud spanner databases create test-db --instance=test-instance
```
4. Run the tests: 
```shell
python3 -m unittest tests/system/test_pool.py
```
-or-
```shell
nox -s system
```
