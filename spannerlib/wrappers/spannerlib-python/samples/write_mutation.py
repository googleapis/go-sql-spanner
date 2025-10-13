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
from google.cloud.spanner_v1 import (
    BatchWriteRequest,
    ExecuteSqlRequest,
    Mutation,
)
from google.protobuf.struct_pb2 import ListValue, Value

from google.cloud.spannerlib import Pool, SpannerLibError

EMULATOR_TEST_CONNECTION_STRING = (
    "localhost:9010"
    "/projects/test-project"
    "/instances/test-instance"
    "/databases/testdb"
    "?autoConfigEmulator=true"
)


def run_write_mutation_sample(test_connection_string):
    try:
        pool = Pool.create_pool(test_connection_string)
        print(f"Successfully created pool with ID: {pool.id}")

        print("Attempting to create a connection from the pool...")
        conn = None  # Initialize conn to None
        try:
            conn = pool.create_connection()
            print(f"Successfully created connection with ID: {conn.id}")

            setup(conn)

            print("\nAttempting to run write_mutation...")
            try:
                mutation = Mutation(
                    insert=Mutation.Write(
                        table="Singers",
                        columns=["SingerId", "FirstName", "LastName"],
                        values=[
                            ListValue(
                                values=[
                                    Value(string_value="200"),
                                    Value(string_value="Mutation"),
                                    Value(string_value="User1"),
                                ]
                            )
                        ],
                    )
                )
                mutation_group = BatchWriteRequest.MutationGroup(
                    mutations=[mutation]
                )

                response = conn.write_mutations(mutation_group)
                print(f"write_mutations response: {response}")

            except SpannerLibError as e:
                print(f"Error during write_mutations: {e}")

            # Verify
            rows = conn.execute(
                ExecuteSqlRequest(
                    sql="SELECT * FROM Singers WHERE SingerId >= 200"
                )
            )
            print(
                "\nVerification Query: SELECT * FROM Singers WHERE SingerId >= 200"
            )
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
    run_write_mutation_sample(EMULATOR_TEST_CONNECTION_STRING)
