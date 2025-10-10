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
from google.cloud.spanner_v1 import ExecuteSqlRequest

from google.cloud.spannerlib import Pool, SpannerLibError

EMULATOR_TEST_CONNECTION_STRING = (
    "localhost:9010"
    "/projects/test-project"
    "/instances/test-instance"
    "/databases/testdb"
    "?autoConfigEmulator=true"
)


def run_transaction_sample(test_connection_string):
    try:
        pool = Pool.create_pool(test_connection_string)
        print(f"Successfully created pool with ID: {pool.id}")

        print("Attempting to create a connection from the pool...")
        conn = None  # Initialize conn to None
        try:
            conn = pool.create_connection()
            print(f"Successfully created connection with ID: {conn.id}")

            setup(conn)

            print("\nAttempting to run a transaction | BEGIN/ROLLBACK...")
            try:
                conn.begin_transaction()
                print("Transaction started.")
                conn.execute(
                    ExecuteSqlRequest(
                        sql="INSERT INTO Singers (SingerId, FirstName, LastName) VALUES (1, 'Catalina', 'Smith')"
                    )
                )
                conn.rollback()
                print("Transaction rolled back.")
            except SpannerLibError as e:
                print(f"Error during transaction: {e}")

            # Verify the transaction
            rows = conn.execute(ExecuteSqlRequest(sql="SELECT * FROM Singers"))
            print("\nVerification Query: SELECT * FROM Singers")
            print(f"Found {count_rows(rows)} rows.")
            rows.close()

            print("\nAttempting to run a transaction | BEGIN/COMMIT...")
            try:
                conn.begin_transaction()
                print("Transaction started.")
                conn.execute(
                    ExecuteSqlRequest(
                        sql="INSERT INTO Singers (SingerId, FirstName, LastName) VALUES (2, 'Catalina', 'Smith')"
                    )
                )
                print("Executed first INSERT.")
                conn.execute(
                    ExecuteSqlRequest(
                        sql="INSERT INTO Singers (SingerId, FirstName, LastName) VALUES (3, 'Alice', 'Trentor')"
                    )
                )
                print("Executed second INSERT.")
                conn.commit()
                print("Transaction committed successfully.")
            except SpannerLibError as e:
                print(f"Error during transaction: {e}")

            # Verify the transaction
            rows = conn.execute(ExecuteSqlRequest(sql="SELECT * FROM Singers"))
            print("\nVerification Query: SELECT * FROM Singers")
            print(f"Found {count_rows(rows)} rows.")
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
    run_transaction_sample(EMULATOR_TEST_CONNECTION_STRING)
