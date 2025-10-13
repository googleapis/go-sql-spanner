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

from _helper import cleanup, count_rows, setup, setup_env
from google.cloud.spanner_v1 import ExecuteBatchDmlRequest, ExecuteSqlRequest

from google.cloud.spannerlib import Pool, SpannerLibError

EMULATOR_TEST_CONNECTION_STRING = (
    "localhost:9010"
    "/projects/test-project"
    "/instances/test-instance"
    "/databases/testdb"
    "?autoConfigEmulator=true"
)


def run_execute_batch_sample(test_connection_string):
    try:
        pool = Pool.create_pool(test_connection_string)
        print(f"Successfully created pool with ID: {pool.id}")

        print("Attempting to create a connection from the pool...")
        conn = None  # Initialize conn to None
        try:
            conn = pool.create_connection()
            print(f"Successfully created connection with ID: {conn.id}")

            setup(conn)

            print("\nAttempting to run ExecuteBatchDmlRequest...")
            try:
                statements = [
                    ExecuteBatchDmlRequest.Statement(
                        sql="INSERT INTO Singers (SingerId, FirstName, LastName) VALUES (100, 'Batch', 'User1')"
                    ),
                    ExecuteBatchDmlRequest.Statement(
                        sql="INSERT INTO Singers (SingerId, FirstName, LastName) VALUES (101, 'Batch', 'User2')"
                    ),
                ]
                request = ExecuteBatchDmlRequest(statements=statements)

                response = conn.execute_batch(request)
                print(f"ExecuteBatchDmlResponse: {response}")

                if response.status.code == 0:
                    print("Batch DML executed successfully.")
                    for i, result_set in enumerate(response.result_sets):
                        print(
                            f"  Statement {i}: Rows affected: {result_set.stats.row_count_exact}"
                        )
                else:
                    print(f"Batch DML failed with status: {response.status}")

            except SpannerLibError as e:
                print(f"Error during execute_batch: {e}")

            # Verify
            rows = conn.execute(
                ExecuteSqlRequest(
                    sql="SELECT * FROM Singers WHERE SingerId >= 100"
                )
            )
            print(
                "\nVerification Query: SELECT * FROM Singers WHERE SingerId >= 100"
            )
            print(f"Found {count_rows(rows)} rows.")
            rows.close()

            print("\nAttempting to run ExecuteBatchDmlRequest with an error...")
            try:
                statements = [
                    ExecuteBatchDmlRequest.Statement(
                        sql="INSERT INTO Singers (SingerId, FirstName, LastName) VALUES (200, 'Good', 'Batch')"
                    ),
                    ExecuteBatchDmlRequest.Statement(
                        sql="INSERT INTO NonExistentTable (Id) VALUES (1)"  # This will fail
                    ),
                ]
                request = ExecuteBatchDmlRequest(statements=statements)

                conn.execute_batch(request)
            except SpannerLibError as e:
                print(f"Error during execute_batch as expected: {e}")

            # Verify the transaction was rolled back
            rows = conn.execute(
                ExecuteSqlRequest(
                    sql="SELECT * FROM Singers WHERE SingerId >= 200"
                )
            )
            print(
                "\nVerification Query: SELECT * FROM Singers WHERE SingerId >= 200"
            )
            print(f"Found {count_rows(rows)} rows (should be 0).")
            rows.close()

        except SpannerLibError as e:
            print(f"Error creating or using connection: {e}")
        finally:
            if conn:
                cleanup(conn)
                conn.close()
                print("\nConnection closed.")

        print("Closing the pool...")
        pool.close()
        print("Pool closed.")

    except SpannerLibError as e:
        print(f"Error creating pool: {e}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")


if __name__ == "__main__":
    setup_env()
    run_execute_batch_sample(EMULATOR_TEST_CONNECTION_STRING)
