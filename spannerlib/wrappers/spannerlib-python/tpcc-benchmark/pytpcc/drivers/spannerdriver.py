# pytpcc/drivers/spanner_driver.py
import datetime

# Import your dynamic connection logic
# Assuming you place your connection logic in a sibling package or path accessible
from google.cloud.spanner_dbapi import connect as connect_dbapi

from .abstractdriver import AbstractDriver

# from google.cloud.spanner_python_driver import connect as connect_new_driver


class SpannerDriver(AbstractDriver):
    DEFAULT_CONFIG = {
        "instance": ("Spanner Instance ID", "test-instance"),
        "database": ("Spanner Database ID", "test-db"),
        "driver_type": ("Driver type (dbapi/custom)", "dbapi"),
    }

    def __init__(self, ddl):
        super(SpannerDriver, self).__init__("spanner", ddl)
        self.conn = None
        self.cursor = None

    def makeDefaultConfig(self):
        return self.DEFAULT_CONFIG

    def loadConfig(self, config):
        for key in self.DEFAULT_CONFIG.keys():
            if key not in config:
                # Fallback to default value from tuple
                config[key] = self.DEFAULT_CONFIG[key][1]
        self.config = config

        # 1. Connect using the requested driver
        instance_id = config["instance"]
        database_id = config["database"]

        if config["driver_type"] == "dbapi":
            self.conn = connect_dbapi(instance_id, database_id)
        else:
            # Import your new driver dynamically here if preferred
            from google.cloud.spanner_python_driver import connect

            self.conn = connect(instance_id, database_id)

        self.conn.autocommit = False
        self.cursor = self.conn.cursor()

    def doNewOrder(self, params):
        # Map py-tpcc params to your SQL logic
        w_id = params["w_id"]
        d_id = params["d_id"]
        c_id = params["c_id"]
        o_entry_d = datetime.datetime.now()

        # py-tpcc passes items as a list of dicts, slightly different structure
        # You will need to adapt your 'do_new_order' logic here.
        # For brevity, reusing the core SQL logic you already wrote:

        try:
            # ... [INSERT YOUR do_new_order SQL LOGIC HERE] ...
            # Remember to use self.cursor and self.conn

            self.conn.commit()
            return None  # py-tpcc expects None on success or specific tuple
        except Exception as e:
            self.conn.rollback()
            raise e

    def doPayment(self, params):
        w_id = params["w_id"]
        d_id = params["d_id"]
        c_id = params["c_id"]
        h_amount = params["h_amount"]
        h_date = datetime.datetime.now()

        try:
            # ... [INSERT YOUR do_payment SQL LOGIC HERE] ...

            self.conn.commit()
            return None
        except Exception as e:
            self.conn.rollback()
            raise e

    # You must implement these to satisfy AbstractDriver,
    # even if you don't run them (raise NotImplementedError if unused)
    def doOrderStatus(self, params):
        raise NotImplementedError("OrderStatus not implemented")

    def doDelivery(self, params):
        raise NotImplementedError("Delivery not implemented")

    def doStockLevel(self, params):
        raise NotImplementedError("StockLevel not implemented")
