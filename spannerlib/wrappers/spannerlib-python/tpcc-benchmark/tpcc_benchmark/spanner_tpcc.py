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

# Constants
NUM_ITEMS = 1000
DISTRICTS_PER_WAREHOUSE = 10
# Reduced for quick testing (Standard is 3000)
CUSTOMERS_PER_DISTRICT = 30
# Reduced for quick testing (Standard is 3000)
ORDERS_PER_DISTRICT = 30


class SpannerTPCCDriver:
    """
    Driver to execute TPC-C transactions against Spanner.

    This class encapsulates the logic for running TPC-C New Order and Payment
    transactions using a provided DBAPI connection.
    """

    def __init__(self, connection):
        """
        Initialize the driver with a database connection.

        Args:
            connection: A PEP 249 compliant database connection object.
        """
        self.conn = connection
        self.conn.autocommit = False  # TPC-C requires transactions

    def close(self):
        """Close the underlying database connection."""
        self.conn.close()

    def do_new_order(self, w_id, d_id, c_id, order_lines, o_entry_d):
        """
        Executes the New Order Transaction.

        Simulates a customer entering a new order with multiple items.
        It updates stock, creates an order, and inserts order lines.

        Args:
            w_id (int): Warehouse ID.
            d_id (int): District ID.
            c_id (int): Customer ID.
            order_lines (list[dict]): List of items in the order.
                Each dict must contain:
                - 'ol_i_id': Item ID
                - 'ol_supply_w_id': Supplying Warehouse ID
                - 'ol_quantity': Quantity
            o_entry_d (datetime): Timestamp of the order entry.

        Returns:
            str: "NewOrder Success" or error message.
        """
        cursor = self.conn.cursor()
        try:
            # 1. Get Warehouse Tax
            cursor.execute(
                "SELECT W_TAX FROM Warehouse WHERE W_ID = %s", (w_id,)
            )
            _ = cursor.fetchone()[0]

            # 2. Get District Tax and increment Next Order ID
            cursor.execute(
                "SELECT D_TAX, D_NEXT_O_ID "
                "FROM District WHERE W_ID = %s AND D_ID = %s",
                (w_id, d_id),
            )
            d_tax, d_next_o_id = cursor.fetchone()

            cursor.execute(
                "UPDATE District SET D_NEXT_O_ID = D_NEXT_O_ID + 1 "
                "WHERE W_ID = %s AND D_ID = %s",
                (w_id, d_id),
            )

            # 3. Get Customer Discount
            cursor.execute(
                "SELECT C_DISCOUNT FROM Customer "
                "WHERE W_ID = %s AND D_ID = %s AND C_ID = %s",
                (w_id, d_id, c_id),
            )
            _ = cursor.fetchone()[0]

            # 4. Create Order
            all_local = (
                1
                if all(ol["ol_supply_w_id"] == w_id for ol in order_lines)
                else 0
            )
            cursor.execute(
                "INSERT INTO Orders "
                "(O_ID, D_ID, W_ID, O_C_ID, "
                "O_ENTRY_D, O_OL_CNT, O_ALL_LOCAL)"
                "VALUES (%s, %s, %s, %s, %s, %s, %s)",
                (
                    d_next_o_id,
                    d_id,
                    w_id,
                    c_id,
                    o_entry_d,
                    len(order_lines),
                    all_local,
                ),
            )

            # 5. Create Order Lines and Update Stock
            total_amount = 0
            for i, ol in enumerate(order_lines):
                # Get Item Info
                cursor.execute(
                    "SELECT I_PRICE, I_NAME, I_DATA "
                    "FROM Item WHERE I_ID = %s",
                    (ol["ol_i_id"],),
                )
                item_row = cursor.fetchone()
                if not item_row:
                    # TPC-C spec says 1% of transactions should roll back here
                    self.conn.rollback()
                    return "Item Not Found (Rollback)"
                i_price, i_name, i_data = item_row

                # Get Stock Info
                cursor.execute(
                    "SELECT S_QUANTITY, S_DATA, S_DIST_%02d "
                    "FROM Stock WHERE S_I_ID = %%s AND W_ID = %%s" % (d_id,),
                    (ol["ol_i_id"], ol["ol_supply_w_id"]),
                )
                s_qty, s_data, s_dist = cursor.fetchone()

                # Update Stock
                if s_qty - ol["ol_quantity"] >= 10:
                    s_qty -= ol["ol_quantity"]
                else:
                    s_qty = s_qty - ol["ol_quantity"] + 91

                cursor.execute(
                    "UPDATE Stock "
                    "SET S_QUANTITY = %s, S_YTD = S_YTD + %s, "
                    "S_ORDER_CNT = S_ORDER_CNT + 1 "
                    "WHERE S_I_ID = %s AND W_ID = %s",
                    (
                        s_qty,
                        ol["ol_quantity"],
                        ol["ol_i_id"],
                        ol["ol_supply_w_id"],
                    ),
                )

                ol_amount = ol["ol_quantity"] * i_price
                total_amount += ol_amount

                cursor.execute(
                    "INSERT INTO OrderLine "
                    "(O_ID, D_ID, W_ID, OL_NUMBER, OL_I_ID, "
                    "OL_SUPPLY_W_ID, OL_DELIVERY_D, OL_QUANTITY, "
                    "OL_AMOUNT, OL_DIST_INFO)"
                    "VALUES (%s, %s, %s, %s, %s, %s, NULL, %s, %s, %s)",
                    (
                        d_next_o_id,
                        d_id,
                        w_id,
                        i + 1,
                        ol["ol_i_id"],
                        ol["ol_supply_w_id"],
                        ol["ol_quantity"],
                        ol_amount,
                        s_dist,
                    ),
                )

            self.conn.commit()
            return "NewOrder Success"
        except Exception as e:
            self.conn.rollback()
            return f"NewOrder Failed: {e}"

    def do_payment(self, w_id, d_id, c_id, h_amount, h_date):
        """
        Executes the Payment Transaction.

        Updates warehouse, district, and customer statistics
        to reflect a payment. It also inserts a history record.

        Args:
            w_id (int): Warehouse ID.
            d_id (int): District ID.
            c_id (int): Customer ID.
            h_amount (float): Payment amount.
            h_date (datetime): Timestamp of the payment.

        Returns:
            str: "Payment Success" or error message.
        """
        cursor = self.conn.cursor()
        try:
            # Update Warehouse
            cursor.execute(
                "UPDATE Warehouse SET W_YTD = W_YTD + %s WHERE W_ID = %s",
                (h_amount, w_id),
            )
            cursor.execute(
                "SELECT "
                "W_NAME, W_STREET_1, W_STREET_2, W_CITY, W_STATE, W_ZIP "
                "FROM Warehouse WHERE W_ID = %s",
                (w_id,),
            )
            w_row = cursor.fetchone()

            # Update District
            cursor.execute(
                "UPDATE District SET D_YTD = D_YTD + %s "
                "WHERE W_ID = %s AND D_ID = %s",
                (h_amount, w_id, d_id),
            )
            cursor.execute(
                "SELECT "
                "D_NAME, D_STREET_1, D_STREET_2, D_CITY, D_STATE, D_ZIP "
                "FROM District WHERE W_ID = %s AND D_ID = %s",
                (w_id, d_id),
            )
            d_row = cursor.fetchone()

            # Update Customer
            cursor.execute(
                "UPDATE Customer "
                "SET C_BALANCE = C_BALANCE - %s, "
                "C_YTD_PAYMENT = C_YTD_PAYMENT + %s, "
                "C_PAYMENT_CNT = C_PAYMENT_CNT + 1 "
                "WHERE W_ID = %s AND D_ID = %s AND C_ID = %s",
                (h_amount, h_amount, w_id, d_id, c_id),
            )

            # Insert History
            h_data = f"{w_row[0]}    {d_row[0]}"
            cursor.execute(
                "INSERT INTO History "
                "(H_C_D_ID, H_C_W_ID, H_C_ID, H_D_ID, "
                "H_W_ID, H_DATE, H_AMOUNT, H_DATA)"
                "VALUES (%s, %s, %s, %s, %s, %s, %s, %s)",
                (d_id, w_id, c_id, d_id, w_id, h_date, h_amount, h_data),
            )

            self.conn.commit()
            return "Payment Success"
        except Exception as e:
            self.conn.rollback()
            return f"Payment Failed: {e}"
