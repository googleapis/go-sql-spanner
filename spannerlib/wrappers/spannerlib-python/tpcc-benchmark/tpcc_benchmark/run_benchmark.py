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
import argparse
import datetime
import importlib
import random
import time

from google.cloud import spanner, spanner_admin_database_v1
from spanner_tpcc import SpannerTPCCDriver

# Constants
NUM_ITEMS = 1000
DISTRICTS_PER_WAREHOUSE = 10
CUSTOMERS_PER_DISTRICT = 30


def get_connect_func(driver_name):
    """
    Dynamically imports the connect function based on the driver name.

    Args:
        driver_name (str): The name of the driver module
                           ('google.cloud.spanner_dbapi' or
                            'google.cloud.spanner_python_driver').

    Returns:
        function: The `connect` function from the specified driver.

    Raises:
        ValueError: If an unknown driver name is provided.
    """
    if driver_name == "google.cloud.spanner_dbapi":
        from google.cloud.spanner_dbapi import connect

        return connect
    elif driver_name == "google.cloud.spanner_python_driver":
        # Assumes the new driver package is installed and exposes connect
        module = importlib.import_module("google.cloud.spanner_python_driver")
        return module.connect
    else:
        raise ValueError(f"Unknown driver: {driver_name}")


def clean_data(instance_id, database_id, project_id):
    """
    Drops all tables defined in clean_schema.ddl.

    Args:
        instance_id (str): Spanner instance ID.
        database_id (str): Spanner database ID.
        project_id (str): GCP project ID.
    """
    print("Cleaning Data (Dropping Tables)...")
    import os

    clean_schema_path = os.path.join(
        os.path.dirname(__file__), "clean_schema.ddl"
    )
    try:
        with open(clean_schema_path, "r") as f:
            ddl_statements = [
                s.strip() for s in f.read().split(";") if s.strip()
            ]
    except FileNotFoundError:
        print(f"Error: {clean_schema_path} not found.")
        return

    client = spanner_admin_database_v1.DatabaseAdminClient()
    db_path = client.database_path(project_id, instance_id, database_id)

    try:
        op = client.update_database_ddl(
            request={"database": db_path, "statements": ddl_statements}
        )
        op.result(timeout=300)
        print("Tables dropped successfully.")
    except Exception as e:
        print(f"Failed to drop tables: {e}")

        print(
            "Note: If tables have foreign keys or interleaving, "
            "drop order matters."
        )


def load_data(conn, instance_id, database_id, project_id):
    """
    Creates schema and loads initial TPC-C data into the database.

    Args:
        conn (object): The DBAPI connection object.
        instance_id (str): Spanner instance ID.
        database_id (str): Spanner database ID.
        project_id (str): GCP project ID.
    """
    # Schema creation uses Admin API (Driver Independent)
    print("Ensuring Schema...")
    import os

    schema_path = os.path.join(os.path.dirname(__file__), "schema.ddl")
    with open(schema_path, "r") as f:
        ddl_statements = [s.strip() for s in f.read().split(";") if s.strip()]

    client = spanner_admin_database_v1.DatabaseAdminClient()
    db_path = client.database_path(project_id, instance_id, database_id)

    try:
        op = client.update_database_ddl(
            request={"database": db_path, "statements": ddl_statements}
        )
        op.result(timeout=300)
    except Exception as e:
        print(f"Schema update failed: {e}")
        # If schema update fails, we likely can't proceed
        # with loading data especially if we just cleaned.
        raise e

    # Data Loading uses the selected DBAPI Driver
    print("Loading Data...")
    conn.autocommit = False
    cursor = conn.cursor()

    try:
        # (Simplified loading logic reused from previous example)
        print("Loading Items...")
        for i in range(1, NUM_ITEMS + 1):
            cursor.execute(
                "INSERT INTO Item "
                "(I_ID, I_IM_ID, I_NAME, I_PRICE, I_DATA) "
                "VALUES (%s, %s, %s, %s, %s)",
                (
                    i,
                    random.randint(1, 10000),
                    "Item-" + str(i),
                    random.random() * 100,
                    "data",
                ),
            )

        print("Loading Warehouse...")
        w_id = 1
        cursor.execute(
            "INSERT INTO Warehouse "
            "(W_ID, W_NAME, W_TAX, W_YTD) "
            "VALUES (%s, %s, %s, %s)",
            (w_id, "W1", 0.1, 300000.0),
        )

        print("Loading Stock...")
        for i in range(1, NUM_ITEMS + 1):
            cursor.execute(
                "INSERT INTO Stock "
                "(W_ID, S_I_ID, S_QUANTITY, S_DIST_01, "
                "S_YTD, S_ORDER_CNT, S_REMOTE_CNT, S_DATA) "
                "VALUES (%s, %s, %s, %s, %s, %s, %s, %s)",
                (w_id, i, 100, "dist1", 0, 0, 0, "data"),
            )

        print("Loading Districts & Customers...")
        for d_id in range(1, DISTRICTS_PER_WAREHOUSE + 1):
            cursor.execute(
                "INSERT INTO District "
                "(W_ID, D_ID, D_NAME, D_TAX, D_YTD, D_NEXT_O_ID) "
                "VALUES (%s, %s, %s, %s, %s, %s)",
                (w_id, d_id, f"Dist-{d_id}", 0.1, 30000.0, 3001),
            )

            for c_id in range(1, CUSTOMERS_PER_DISTRICT + 1):
                cursor.execute(
                    "INSERT INTO Customer "
                    "(W_ID, D_ID, C_ID, C_LAST, C_FIRST, C_CREDIT, "
                    "C_DISCOUNT, C_BALANCE, C_YTD_PAYMENT, C_PAYMENT_CNT, "
                    "C_DELIVERY_CNT)"
                    "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
                    (
                        w_id,
                        d_id,
                        c_id,
                        f"Last-{c_id}",
                        f"First-{c_id}",
                        "GC",
                        0.1,
                        -10.0,
                        10.0,
                        1,
                        0,
                    ),
                )

        conn.commit()
        print("Data Loading Complete.")
    except Exception as e:
        print(f"Data loading failed: {e}")
        conn.rollback()
        # Do not close connection here, we reuse it


def run_benchmark(conn, duration_sec):
    """
    Runs the TPC-C benchmark for a specified duration.

    Args:
        conn (object): The DBAPI connection object.
        duration_sec (int): Duration to run the benchmark in seconds.
    """
    driver = SpannerTPCCDriver(conn)

    start_time = time.time()
    tx_count = 0

    print(f"Running benchmark for {duration_sec} seconds...")

    while time.time() - start_time < duration_sec:
        w_id = 1
        d_id = random.randint(1, DISTRICTS_PER_WAREHOUSE)
        c_id = random.randint(1, CUSTOMERS_PER_DISTRICT)

        if random.random() < 0.5:
            # New Order
            ol_cnt = random.randint(5, 15)
            order_lines = [
                {
                    "ol_i_id": random.randint(1, NUM_ITEMS),
                    "ol_supply_w_id": w_id,
                    "ol_quantity": 5,
                }
                for _ in range(ol_cnt)
            ]
            driver.do_new_order(
                w_id, d_id, c_id, order_lines, datetime.datetime.now()
            )
        else:
            # Payment
            driver.do_payment(w_id, d_id, c_id, 100.00, datetime.datetime.now())

        tx_count += 1

    # Do not close connection here, we might reuse it or close in main
    tps = tx_count / duration_sec
    print(f"Benchmark Finished. Total Transactions: {tx_count}. TPS: {tps:.2f}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Spanner TPC-C DBAPI Benchmark"
    )
    parser.add_argument(
        "--driver",
        choices=[
            "google.cloud.spanner_dbapi",
            "google.cloud.spanner_python_driver",
        ],
        default="google.cloud.spanner_dbapi",
        help="Which DBAPI driver to use",
    )
    parser.add_argument("--project", required=True)
    parser.add_argument("--instance", required=True)
    parser.add_argument("--database", required=True)
    parser.add_argument("--clean", action="store_true")
    parser.add_argument("--load", action="store_true")
    parser.add_argument("--duration", type=int, default=60)

    args = parser.parse_args()

    # Select Driver
    # Create Connection ONCE
    print(f"Connecting using {args.driver}...")
    connect_func = get_connect_func(args.driver)

    conn = None
    if args.driver == "google.cloud.spanner_dbapi":
        # Create a Client with metrics disabled to prevent export errors on exit
        client = spanner.Client(
            project=args.project, disable_builtin_metrics=True
        )
        conn = connect_func(args.instance, args.database, client=client)
    else:
        conn = connect_func(args.instance, args.database, project=args.project)

    try:
        # Clean Data (if requested)
        if args.clean:
            # clean_data uses Admin API, doesn't need conn directly
            # but we kept it separate
            clean_data(args.instance, args.database, args.project)

        # Load Data (if requested)
        if args.load:
            load_data(conn, args.instance, args.database, args.project)

        # Run Benchmark
        run_benchmark(conn, args.duration)

    finally:
        conn.close()
