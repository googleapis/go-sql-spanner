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
"""Helper script to setup Spanner Emulator schema."""

import os

from google.cloud import spanner


def setup_env():
    if os.environ.get("SPANNERLIB_TEST_ON_PROD"):
        return

    project_id = "test-project"
    instance_id = "test-instance"
    database_id = "testdb"

    client = spanner.Client(project=project_id)
    instance = client.instance(instance_id)
    if not instance.exists():
        instance.create(configuration_name="emulator-config")

    database = instance.database(database_id)
    if not database.exists():
        database.create()

    # Create table
    try:
        op = database.update_ddl(
            [
                """CREATE TABLE Singers (
                SingerId INT64 NOT NULL,
                FirstName STRING(1024),
                LastName STRING(1024),
            ) PRIMARY KEY (SingerId)"""
            ]
        )
        op.result()
    except Exception as e:
        if "Duplicate name" in str(e):
            print("Table Singers already exists.")
        else:
            raise
    print("Schema setup complete.")


def teardown():
    if os.environ.get("SPANNERLIB_TEST_ON_PROD"):
        return

    project_id = "test-project"
    instance_id = "test-instance"
    database_id = "testdb"

    client = spanner.Client(project=project_id)
    instance = client.instance(instance_id)
    if not instance.exists():
        return

    database = instance.database(database_id)
    if not database.exists():
        return

    # Drop table
    try:
        op = database.update_ddl(["DROP TABLE Singers"])
        op.result()
    except Exception as e:
        if "Table not found" in str(e) or "NotFound" in str(e):
            print("Table Singers does not exist.")
        else:
            # Ignore errors if table doesn't exist or other issues during cleanup
            print(f"Error during cleanup: {e}")
    print("Schema teardown complete.")


if __name__ == "__main__":
    import sys

    if len(sys.argv) > 1 and sys.argv[1] == "teardown":
        teardown()
    else:
        setup_env()
