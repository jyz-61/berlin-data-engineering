# Kafka Consumer to DB — Step 3
# Reads from longterm_listings_changes topic
# Inserts into enginering_alerts.cdc_updates


import json
import os
import psycopg2
from kafka import KafkaConsumer

DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = "ep-weathered-bird-adi4s1o1-pooler.c-2.us-east-1.aws.neon.tech"
DB_NAME = "layered_berlin"
KAFKA_TOPIC = "longterm_listings_changes"
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"


def get_db_connection():
    return psycopg2.connect(
        f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}/{DB_NAME}"
        f"?sslmode=require&channel_binding=require"
    )


def process_message(msg, cur):
    payload = json.loads(msg.value)
    op = payload.get("op")
    op_map = {"c": "insert", "u": "update", "d": "delete"}
    change_type = op_map.get(op, "unknown")

    # For delete use before, otherwise use after
    data = payload.get("after") if op != "d" else payload.get("before")
    data = data or {}

    cur.execute(
        """
        INSERT INTO enginering_alerts.cdc_updates
        (table_name, change_type, record_id, title, price, location, processed)
        VALUES (%s, %s, %s, %s, %s, %s, FALSE)
    """,
        (
            "longterm_listings",
            change_type,
            data.get("id"),
            data.get("name") or data.get("title"),
            data.get("price_euro") or data.get("price"),
            data.get("address") or data.get("location"),
        ),
    )
    print(f"Inserted {change_type} event for record {data.get('id')}")
    return True


def main():
    print("Connecting to database...")
    conn = get_db_connection()
    cur = conn.cursor()
    print("Connected!")

    print(f"Listening to Kafka topic: {KAFKA_TOPIC}")
    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        group_id="cdc_to_db_consumer",
        value_deserializer=lambda x: x.decode("utf-8"),
    )

    for msg in consumer:
        try:
            process_message(msg, cur)
            conn.commit()
        except Exception as e:
            print(f"Error: {e}")
            conn.rollback()


if __name__ == "__main__":
    main()
