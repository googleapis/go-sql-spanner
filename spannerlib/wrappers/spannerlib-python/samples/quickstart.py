#!/usr/bin/env python

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

import os

from _helper import format_results
from google.cloud.spanner_v1 import ExecuteSqlRequest  # noqa: E402

from google.cloud.spannerlib import Pool, SpannerLibError  # noqa: E402

EMULATOR_TEST_CONNECTION_STRING = (
    "localhost:9010"
    "/projects/test-project"
    "/instances/test-instance"
    "/databases/testdb"
    "?autoConfigEmulator=true"
)


def setup_env():
    # Set environment variable for Spanner Emulator if not set
    if not os.environ.get("SPANNER_EMULATOR_HOST"):
        os.environ["SPANNER_EMULATOR_HOST"] = "localhost:9010"
        print(
            f"Set SPANNER_EMULATOR_HOST to {os.environ['SPANNER_EMULATOR_HOST']}"
        )


def run_quickstart(test_connection_string):
    try:
        pool = Pool.create_pool(test_connection_string)
        print(f"Successfully created pool with ID: {pool.id}")

        print("Attempting to create a connection from the pool...")
        try:
            with pool.create_connection() as conn:
                print(f"Successfully created connection with ID: {conn.id}")
                print("Connection test successful!")
                print("Attempting to execute a statement on the connection...")

                sql = "SELECT 1 as one, 'hello' as greeting;"
                print(f"Executing SQL: {sql}")
                request = ExecuteSqlRequest(sql=sql)

                rows = conn.execute(request)

                metadata = rows.metadata()

                rows_data = []
                row = rows.next()
                while row is not None:
                    rows_data.append(row)
                    row = rows.next()

                if rows_data:
                    print("\nResults:")
                    print(format_results(metadata, rows_data))
                    print("")
                else:
                    print("No rows returned.")

                rows.close()
        except SpannerLibError as e:
            print(f"Error creating or using connection: {e}")

        print("Closing the pool...")
        pool.close()
        print("Pool closed.")

    except SpannerLibError as e:
        print(f"Error creating pool: {e}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")


if __name__ == "__main__":
    run_quickstart(EMULATOR_TEST_CONNECTION_STRING)
