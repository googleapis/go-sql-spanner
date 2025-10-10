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

from _helper import cleanup, setup, setup_env
from google.cloud.spanner_v1 import ExecuteSqlRequest  # noqa: E402

from google.cloud.spannerlib import Pool, SpannerLibError  # noqa: E402

EMULATOR_TEST_CONNECTION_STRING = (
    "localhost:9010"
    "/projects/test-project"
    "/instances/test-instance"
    "/databases/testdb"
    "?autoConfigEmulator=true"
)


def run_dml_stats_sample(test_connection_string):
    try:
        pool = Pool.create_pool(test_connection_string)
        print(f"Successfully created pool with ID: {pool.id}")

        print("Attempting to create a connection from the pool...")
        conn = None  # Initialize conn to None
        try:
            conn = pool.create_connection()
            print(f"Successfully created connection with ID: {conn.id}")

            setup(conn)

            print("\nAttempting to execute an INSERT statement...")
            insert_sql = "INSERT INTO Singers (SingerId, FirstName, LastName) VALUES (1, 'Marc', 'Richards')"
            print(f"Executing SQL: {insert_sql}")
            insert_request = ExecuteSqlRequest(sql=insert_sql)
            insert_rows = conn.execute(insert_request)
            stats = insert_rows.result_set_stats()
            if stats:
                print(f"Insert count: {stats.row_count_exact}")
            insert_rows.close()

            print("\nAttempting to execute an UPDATE statement...")
            update_sql = (
                "UPDATE Singers SET LastName = 'Richardson' WHERE SingerId = 1"
            )
            print(f"Executing SQL: {update_sql}")
            update_request = ExecuteSqlRequest(sql=update_sql)
            update_rows = conn.execute(update_request)
            stats = update_rows.result_set_stats()
            if stats:
                print(f"Update count: {stats.row_count_exact}")
            update_rows.close()

            print("\nAttempting to execute a DELETE statement...")
            delete_sql = "DELETE FROM Singers WHERE SingerId = 1"  # noqa: E501
            print(f"Executing SQL: {delete_sql}")
            delete_request = ExecuteSqlRequest(sql=delete_sql)
            delete_rows = conn.execute(delete_request)
            stats = delete_rows.result_set_stats()
            if stats:
                print(f"Delete count: {stats.row_count_exact}")
            delete_rows.close()

        except SpannerLibError as e:
            print(f"Error during DML operations: {e}")
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
    run_dml_stats_sample(EMULATOR_TEST_CONNECTION_STRING)
