"""
Airflow DAG for processing long_term_listings CDC changes
Consumes Kafka messages, stores in cdc_updates table,
and sends alerts for significant changes
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import json
import os
import psycopg2
from kafka import KafkaConsumer

# Database settings
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = "ep-weathered-bird-adi4s1o1-pooler.c-2.us-east-1.aws.neon.tech"
DB_NAME = "layered_berlin"

# Kafka settings
KAFKA_TOPIC = "layered_berlin.berlin_source_data.long_term_listings"
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"

# Default DAG arguments
default_args = {
    "owner": "data_team",
    "depends_on_past": False,
    "start_date": datetime(2025, 1, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


def get_db_connection():
    """Create database connection"""
    return psycopg2.connect(
        f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}/{DB_NAME}"
        f"?sslmode=require&channel_binding=require"
    )


def consume_kafka_messages(**context):
    """
    Task 1: Consume messages from Kafka topic
    Reads CDC events from long_term_listings topic
    """
    print(f"Connecting to Kafka at {KAFKA_BOOTSTRAP_SERVERS}")

    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        group_id="airflow_longterm_listings",
        consumer_timeout_ms=10000,  # Stop after 10 seconds of no messages
        value_deserializer=lambda x: x.decode("utf-8"),
    )

    messages = []
    for msg in consumer:
        messages.append(msg.value)

    consumer.close()
    print(f"Consumed {len(messages)} messages from Kafka")

    # Push messages to XCom for next task
    context["ti"].xcom_push(key="kafka_messages", value=messages)
    return len(messages)


def process_messages(**context):
    """
    Task 2: Process Kafka messages
    Parses CDC events and inserts into cdc_updates table
    """
    messages = context["ti"].xcom_pull(key="kafka_messages")

    if not messages:
        print("No messages to process")
        return 0

    conn = get_db_connection()
    cur = conn.cursor()

    processed = 0
    change_type_map = {"c": "INSERT", "u": "UPDATE", "d": "DELETE", "r": "INSERT"}

    for msg_value in messages:
        try:
            payload = json.loads(msg_value)
            op = payload.get("op")
            change_type = change_type_map.get(op, "UNKNOWN")
            data = payload.get("after") or payload.get("before") or {}

            cur.execute(
                """
                INSERT INTO berlin_source_data.cdc_updates
                (table_name, change_type, record_id, title, price,
                 location, district, neighborhood, number_of_rooms,
                 surface_m2, processed, captured_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
            """,
                (
                    "long_term_listings",
                    change_type,
                    data.get("id", "unknown"),
                    data.get("name"),
                    data.get("price_euro"),
                    data.get("address"),
                    data.get("district"),
                    data.get("neighborhood"),
                    data.get("number_of_rooms"),
                    data.get("surface_m2"),
                    False,
                ),
            )
            processed += 1
        except Exception as e:
            print(f"Error processing message: {e}")

    conn.commit()
    cur.close()
    conn.close()
    print(f"Processed {processed} messages")
    return processed


def send_alerts(**context):
    """
    Task 3: Send alerts for significant changes
    Checks for high-value listings or significant price changes
    """
    conn = get_db_connection()
    cur = conn.cursor()

    # Check for high-value listings (price > 5000)
    cur.execute("""
        SELECT record_id, title, price, change_type
        FROM berlin_source_data.cdc_updates
        WHERE processed = FALSE
        AND price > 5000
        AND captured_at > NOW() - INTERVAL '5 minutes';
    """)

    high_value = cur.fetchall()

    if high_value:
        print(f"ALERT: {len(high_value)} high-value listing changes detected!")
        for row in high_value:
            print(f"  {row[3]}: {row[1]} - €{row[2]}")
    else:
        print("No significant changes detected")

    cur.close()
    conn.close()
    return len(high_value)


def flag_processed_records(**context):
    """
    Task 4: Mark processed records in cdc_updates table
    """
    conn = get_db_connection()
    cur = conn.cursor()

    cur.execute("""
        UPDATE berlin_source_data.cdc_updates
        SET processed = TRUE,
            processed_at = NOW()
        WHERE processed = FALSE
        AND captured_at > NOW() - INTERVAL '5 minutes';
    """)

    updated = cur.rowcount
    conn.commit()
    cur.close()
    conn.close()
    print(f"Flagged {updated} records as processed")
    return updated


# Define DAG
with DAG(
    "long_term_listings_cdc",
    default_args=default_args,
    description="Process CDC changes from long_term_listings table",
    schedule_interval=timedelta(minutes=5),
    catchup=False,
    tags=["listings", "cdc", "kafka"],
) as dag:

    # Task 1: Consume Kafka messages
    t1_consume = PythonOperator(
        task_id="consume_kafka_messages",
        python_callable=consume_kafka_messages,
        provide_context=True,
    )

    # Task 2: Process messages into cdc_updates
    t2_process = PythonOperator(
        task_id="process_messages",
        python_callable=process_messages,
        provide_context=True,
    )

    # Task 3: Send alerts for significant changes
    t3_alerts = PythonOperator(
        task_id="send_alerts", python_callable=send_alerts, provide_context=True
    )

    # Task 4: Flag processed records
    t4_flag = PythonOperator(
        task_id="flag_processed_records",
        python_callable=flag_processed_records,
        provide_context=True,
    )

    # Task dependencies
    t1_consume >> t2_process >> t3_alerts >> t4_flag
