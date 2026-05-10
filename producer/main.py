import json
import logging
import os
import time
import uuid
from io import BytesIO

import fastavro
import requests
from confluent_kafka import Producer

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
log = logging.getLogger(__name__)

KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
SCHEMA_REGISTRY_URL = os.environ.get("SCHEMA_REGISTRY_URL", "http://localhost:8081")
TOPIC = os.environ.get("KAFKA_TOPIC", "warehouse-events")
SCHEMA_PATH = os.path.join(os.path.dirname(__file__), "schemas", "warehouse_event.avsc")


def load_schema():
    with open(SCHEMA_PATH) as f:
        return json.load(f)


def wait_for_kafka(bootstrap_servers, retries=30, delay=5):
    from confluent_kafka.admin import AdminClient
    for i in range(retries):
        try:
            client = AdminClient({"bootstrap.servers": bootstrap_servers})
            meta = client.list_topics(timeout=5)
            log.info("Kafka is ready")
            return
        except Exception as e:
            log.warning(f"Kafka not ready ({i+1}/{retries}): {e}")
            time.sleep(delay)
    raise RuntimeError("Kafka not available after retries")


def wait_for_schema_registry(url, retries=30, delay=5):
    for i in range(retries):
        try:
            r = requests.get(f"{url}/subjects", timeout=5)
            if r.status_code == 200:
                log.info("Schema Registry is ready")
                return
        except Exception as e:
            log.warning(f"Schema Registry not ready ({i+1}/{retries}): {e}")
        time.sleep(delay)
    raise RuntimeError("Schema Registry not available after retries")


def register_schema(url, subject, schema):
    payload = {"schema": json.dumps(schema)}
    r = requests.post(
        f"{url}/subjects/{subject}/versions",
        headers={"Content-Type": "application/vnd.schemaregistry.v1+json"},
        json=payload,
        timeout=10,
    )
    r.raise_for_status()
    schema_id = r.json()["id"]
    log.info(f"Schema registered with id={schema_id}")
    return schema_id


def serialize_avro(schema, schema_id, record):
    parsed = fastavro.parse_schema(schema)
    buf = BytesIO()
    # confluent wire format: magic byte + 4-byte schema id + avro bytes
    buf.write(b"\x00")
    buf.write(schema_id.to_bytes(4, "big"))
    fastavro.schemaless_writer(buf, parsed, record)
    return buf.getvalue()


def make_event(event_type, product_id, zone_id=None, target_zone_id=None,
               quantity=0, order_id=None, sku=None, order_items=None):
    return {
        "event_id": str(uuid.uuid4()),
        "event_type": event_type,
        "timestamp": int(time.time() * 1000),
        "product_id": product_id,
        "zone_id": zone_id,
        "target_zone_id": target_zone_id,
        "quantity": quantity,
        "order_id": order_id,
        "sku": sku,
        "order_items": order_items,
    }


def delivery_report(err, msg):
    if err:
        log.error(f"Delivery failed: {err}")
    else:
        log.info(f"Delivered to {msg.topic()} [{msg.partition()}] offset={msg.offset()}")


def send(producer, topic, schema, schema_id, event):
    data = serialize_avro(schema, schema_id, event)
    producer.produce(topic, value=data, callback=delivery_report)
    producer.poll(0)


def run():
    wait_for_kafka(KAFKA_BOOTSTRAP)
    wait_for_schema_registry(SCHEMA_REGISTRY_URL)

    schema = load_schema()
    schema_id = register_schema(SCHEMA_REGISTRY_URL, f"{TOPIC}-value", schema)

    producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP})

    products = [
        ("prod-001", "SKU-WIDGET-A"),
        ("prod-002", "SKU-GADGET-B"),
        ("prod-003", "SKU-TOOL-C"),
    ]
    zones = ["zone-A1", "zone-B2", "zone-C3"]

    cycle = 0
    while True:
        cycle += 1
        log.info(f"Starting event cycle {cycle}")

        # Receive products into zones
        for product_id, sku in products:
            for zone_id in zones[:2]:
                event = make_event(
                    "PRODUCT_RECEIVED",
                    product_id=product_id,
                    zone_id=zone_id,
                    quantity=100,
                    sku=sku,
                )
                send(producer, TOPIC, schema, schema_id, event)
                time.sleep(0.3)

        time.sleep(1)

        # Reserve some products
        for product_id, _ in products:
            event = make_event(
                "PRODUCT_RESERVED",
                product_id=product_id,
                zone_id="zone-A1",
                quantity=20,
            )
            send(producer, TOPIC, schema, schema_id, event)
            time.sleep(0.3)

        time.sleep(1)

        # Create orders
        order_id = f"order-{cycle:03d}"
        order_items = [
            {"product_id": "prod-001", "zone_id": "zone-A1", "quantity": 5},
            {"product_id": "prod-002", "zone_id": "zone-A1", "quantity": 3},
        ]
        event = make_event(
            "ORDER_CREATED",
            product_id="prod-001",
            order_id=order_id,
            quantity=0,
            order_items=order_items,
        )
        send(producer, TOPIC, schema, schema_id, event)
        time.sleep(1)

        # Move products between zones
        event = make_event(
            "PRODUCT_MOVED",
            product_id="prod-003",
            zone_id="zone-A1",
            target_zone_id="zone-C3",
            quantity=10,
        )
        send(producer, TOPIC, schema, schema_id, event)
        time.sleep(0.5)

        # Inventory count
        event = make_event(
            "INVENTORY_COUNTED",
            product_id="prod-001",
            zone_id="zone-B2",
            quantity=95,
        )
        send(producer, TOPIC, schema, schema_id, event)
        time.sleep(0.5)

        # Complete order
        event = make_event(
            "ORDER_COMPLETED",
            product_id="prod-001",
            order_id=order_id,
            quantity=0,
            order_items=order_items,
        )
        send(producer, TOPIC, schema, schema_id, event)
        time.sleep(0.5)

        # Ship some products
        event = make_event(
            "PRODUCT_SHIPPED",
            product_id="prod-002",
            zone_id="zone-B2",
            quantity=15,
        )
        send(producer, TOPIC, schema, schema_id, event)
        time.sleep(0.5)

        # Release reserved products
        event = make_event(
            "PRODUCT_RELEASED",
            product_id="prod-003",
            zone_id="zone-A1",
            quantity=10,
        )
        send(producer, TOPIC, schema, schema_id, event)
        time.sleep(0.5)

        producer.flush()
        log.info(f"Cycle {cycle} complete, sleeping 10s")
        time.sleep(10)


if __name__ == "__main__":
    run()
