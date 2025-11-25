#  Copyright 2025 Google LLC
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#        http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
"""Helper functions for system tests."""

import os

TEST_ON_PROD = os.environ.get("SPANNERLIB_TEST_ON_PROD", False)
EMULATOR_TEST_CONNECTION_STRING = (
    "localhost:9010"
    "/projects/test-project"
    "/instances/test-instance"
    "/databases/testdb"
    "?autoConfigEmulator=true"
)

PROJECT_ID = os.environ.get("SPANNER_PROJECT_ID", "NA")
INSTANCE_ID = os.environ.get("SPANNER_INSTANCE_ID", "NA")
DATABASE_ID = os.environ.get("SPANNER_DATABASE_ID", "NA")

PROD_TEST_CONNECTION_STRING = (
    f"projects/{PROJECT_ID}/instances/{INSTANCE_ID}/databases/{DATABASE_ID}"
)


def setup_test_env() -> None:
    if not TEST_ON_PROD:
        # Set environment variable for Spanner Emulator
        os.environ["SPANNER_EMULATOR_HOST"] = "localhost:9010"
        print(
            f"Set SPANNER_EMULATOR_HOST to {os.environ['SPANNER_EMULATOR_HOST']}"
        )
    print(f"Using Connection String: {get_test_connection_string()}")


def get_test_connection_string() -> str:
    return (
        PROD_TEST_CONNECTION_STRING
        if TEST_ON_PROD
        else EMULATOR_TEST_CONNECTION_STRING
    )
