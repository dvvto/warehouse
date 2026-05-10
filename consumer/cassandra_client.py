import logging
import time
from datetime import datetime, timezone

from cassandra.cluster import Cluster, ExecutionProfile, EXEC_PROFILE_DEFAULT
from cassandra.policies import RoundRobinPolicy

log = logging.getLogger(__name__)


def now_ms():
    return datetime.now(timezone.utc)


class CassandraClient:
    def __init__(self, host, port, keyspace, retries=20, delay=5):
        self.host = host
        self.port = port
        self.keyspace = keyspace
        self.session = None
        self._connect(retries, delay)
        self._prepare_statements()

    def _connect(self, retries, delay):
        profile = ExecutionProfile(load_balancing_policy=RoundRobinPolicy())
        for i in range(retries):
            try:
                cluster = Cluster(
                    [self.host],
                    port=self.port,
                    execution_profiles={EXEC_PROFILE_DEFAULT: profile},
                    protocol_version=4,
                )
                self.session = cluster.connect(self.keyspace)
                log.info("Connected to Cassandra")
                return
            except Exception as e:
                log.warning(f"Cassandra not ready ({i+1}/{retries}): {e}")
                time.sleep(delay)
        raise RuntimeError("Cassandra not available after retries")

    def _prepare_statements(self):
        self.stmt_check_event = self.session.prepare(
            "SELECT event_id FROM processed_events WHERE event_id = ?"
        )
        self.stmt_insert_event = self.session.prepare(
            "INSERT INTO processed_events (event_id, event_type, processed_at) VALUES (?, ?, ?)"
        )
        self.stmt_get_inv_pz = self.session.prepare(
            "SELECT available_quantity, reserved_quantity FROM inventory_by_product_zone "
            "WHERE product_id = ? AND zone_id = ?"
        )
        self.stmt_upsert_inv_pz = self.session.prepare(
            "INSERT INTO inventory_by_product_zone "
            "(product_id, zone_id, sku, available_quantity, reserved_quantity, updated_at) "
            "VALUES (?, ?, ?, ?, ?, ?)"
        )
        self.stmt_upsert_inv_p = self.session.prepare(
            "INSERT INTO inventory_by_product "
            "(product_id, zone_id, sku, available_quantity, reserved_quantity, updated_at) "
            "VALUES (?, ?, ?, ?, ?, ?)"
        )
        self.stmt_upsert_inv_z = self.session.prepare(
            "INSERT INTO inventory_by_zone "
            "(zone_id, product_id, sku, available_quantity, reserved_quantity, updated_at) "
            "VALUES (?, ?, ?, ?, ?, ?)"
        )
        self.stmt_get_order = self.session.prepare(
            "SELECT status FROM orders WHERE order_id = ?"
        )
        self.stmt_upsert_order = self.session.prepare(
            "INSERT INTO orders (order_id, status, created_at, updated_at, items) "
            "VALUES (?, ?, ?, ?, ?)"
        )
        self.stmt_update_order_status = self.session.prepare(
            "UPDATE orders SET status = ?, updated_at = ? WHERE order_id = ?"
        )

    def is_processed(self, event_id):
        row = self.session.execute(self.stmt_check_event, (event_id,)).one()
        return row is not None

    def mark_processed(self, event_id, event_type):
        self.session.execute(
            self.stmt_insert_event, (event_id, event_type, now_ms())
        )

    def get_inventory(self, product_id, zone_id):
        row = self.session.execute(self.stmt_get_inv_pz, (product_id, zone_id)).one()
        if row:
            return row.available_quantity, row.reserved_quantity
        return 0, 0

    def update_inventory(self, product_id, zone_id, sku, available, reserved):
        ts = now_ms()
        self.session.execute(
            self.stmt_upsert_inv_pz, (product_id, zone_id, sku, available, reserved, ts)
        )
        self.session.execute(
            self.stmt_upsert_inv_p, (product_id, zone_id, sku, available, reserved, ts)
        )
        self.session.execute(
            self.stmt_upsert_inv_z, (zone_id, product_id, sku, available, reserved, ts)
        )

    def get_order(self, order_id):
        return self.session.execute(self.stmt_get_order, (order_id,)).one()

    def create_order(self, order_id, items_json):
        ts = now_ms()
        self.session.execute(
            self.stmt_upsert_order, (order_id, "CREATED", ts, ts, items_json)
        )

    def update_order_status(self, order_id, status):
        self.session.execute(
            self.stmt_update_order_status, (status, now_ms(), order_id)
        )
