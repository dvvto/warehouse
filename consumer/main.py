import json
import logging
import os
import time
from io import BytesIO

import fastavro
import requests
from confluent_kafka import Consumer, KafkaError

from cassandra_client import CassandraClient
from handlers import dispatch

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
log = logging.getLogger(__name__)

KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
SCHEMA_REGISTRY_URL = os.environ.get("SCHEMA_REGISTRY_URL", "http://localhost:8081")
TOPIC = os.environ.get("KAFKA_TOPIC", "warehouse-events")
GROUP_ID = os.environ.get("KAFKA_GROUP_ID", "warehouse-state-consumer")
CASSANDRA_HOST = os.environ.get("CASSANDRA_HOST", "localhost")
CASSANDRA_PORT = int(os.environ.get("CASSANDRA_PORT", "9042"))
CASSANDRA_KEYSPACE = os.environ.get("CASSANDRA_KEYSPACE", "warehouse")

_schema_cache = {}


def wait_for_kafka(bootstrap_servers, retries=30, delay=5):
    from confluent_kafka.admin import AdminClient
    for i in range(retries):
        try:
            client = AdminClient({"bootstrap.servers": bootstrap_servers})
            client.list_topics(timeout=5)
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


def fetch_schema(registry_url, schema_id):
    if schema_id in _schema_cache:
        return _schema_cache[schema_id]
    r = requests.get(f"{registry_url}/schemas/ids/{schema_id}", timeout=10)
    r.raise_for_status()
    schema = json.loads(r.json()["schema"])
    parsed = fastavro.parse_schema(schema)
    _schema_cache[schema_id] = parsed
    return parsed


def deserialize_avro(registry_url, raw_bytes):
    # confluent wire format: 0x00 + 4-byte schema_id + avro payload
    if raw_bytes[0] != 0:
        raise ValueError("Not a Confluent Avro message")
    schema_id = int.from_bytes(raw_bytes[1:5], "big")
    schema = fetch_schema(registry_url, schema_id)
    buf = BytesIO(raw_bytes[5:])
    return fastavro.schemaless_reader(buf, schema)


def run():
    wait_for_kafka(KAFKA_BOOTSTRAP)
    wait_for_schema_registry(SCHEMA_REGISTRY_URL)

    db = CassandraClient(CASSANDRA_HOST, CASSANDRA_PORT, CASSANDRA_KEYSPACE)

    consumer = Consumer({
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "group.id": GROUP_ID,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": "false",
    })
    consumer.subscribe([TOPIC])
    log.info(f"Subscribed to {TOPIC} as group {GROUP_ID}")

    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    log.debug(f"End of partition {msg.partition()} offset {msg.offset()}")
                else:
                    log.error(f"Kafka error: {msg.error()}")
                continue

            try:
                event = deserialize_avro(SCHEMA_REGISTRY_URL, msg.value())
            except Exception as e:
                log.error(f"Deserialization failed offset={msg.offset()}: {e}")
                consumer.commit(message=msg)
                continue

            event_id = event["event_id"]
            event_type = event["event_type"]
            offset = msg.offset()
            partition = msg.partition()

            log.info(
                f"Received event_id={event_id} event_type={event_type} "
                f"partition={partition} offset={offset}"
            )

            try:
                if db.is_processed(event_id):
                    log.info(f"Skipping duplicate event_id={event_id}")
                    consumer.commit(message=msg)
                    continue

                dispatch(db, event)
                db.mark_processed(event_id, event_type)

                log.info(
                    f"Processed event_id={event_id} event_type={event_type} "
                    f"partition={partition} offset={offset}"
                )
            except Exception as e:
                log.error(f"Failed to process event_id={event_id}: {e}")
                # don't commit - will retry on restart (at-least-once)
                continue

            consumer.commit(message=msg)

    except KeyboardInterrupt:
        log.info("Shutting down")
    finally:
        consumer.close()


if __name__ == "__main__":
    run()
