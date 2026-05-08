"""
Kafka Consumer for longterm_listings CDC events
Prints events
"""

import json
import os
from kafka import KafkaConsumer

# Kafka settings
KAFKA_TOPIC = "longterm_listings_changes"
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"


def print_event(msg):
    """Print CDC event in readable format"""
    try:
        payload = json.loads(msg.value)
        op = payload.get("op", "unknown")

        op_map = {"c": "INSERT", "u": "UPDATE", "d": "DELETE", "r": "READ"}
        change_type = op_map.get(op, "UNKNOWN")

        print(f"CDC Event: {change_type}")
        print(f"Topic: {msg.topic}")
        print(f"Partition: {msg.partition}, Offset: {msg.offset}")

        if payload.get("before"):
            print(f"BEFORE: {json.dumps(payload['before'], indent=2)}")
        if payload.get("after"):
            print(f"AFTER: {json.dumps(payload['after'], indent=2)}")

    except Exception as e:
        print(f"Error parsing message: {e}")
        print(f"Raw message: {msg.value}")


def main():
    print(f"Starting Kafka consumer...")
    print(f"Listening to topic: {KAFKA_TOPIC}")
    print(f"Bootstrap servers: {KAFKA_BOOTSTRAP_SERVERS}")
    print("Waiting for CDC events...\n")

    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        group_id="longterm_listings_consumer",
        value_deserializer=lambda x: x.decode("utf-8"),
    )

    for msg in consumer:
        print_event(msg)


if __name__ == "__main__":
    main()
