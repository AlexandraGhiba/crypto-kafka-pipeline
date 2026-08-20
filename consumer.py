import json
import os
from datetime import datetime

import duckdb
from kafka import KafkaConsumer


KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_TOPIC = "crypto-trades"
DB_PATH = "data/crypto.duckdb"


def init_database():
    os.makedirs("data", exist_ok=True)

    conn = duckdb.connect(DB_PATH)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS raw_crypto_trades (
            timestamp TIMESTAMP,
            symbol VARCHAR,
            price DOUBLE,
            quantity DOUBLE,
            trade_id BIGINT
        )
    """)

    conn.close()


def main():
    init_database()

    conn = duckdb.connect(DB_PATH)

    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id="duckdb-consumer-v2",
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        value_deserializer=lambda value: json.loads(
            value.decode("utf-8")
        ),
    )

    print("[CONSUMER] Kafka -> DuckDB")
    print("[CONSUMER] Waiting for messages...")

    try:
        for message in consumer:
            trade = message.value

            timestamp = datetime.fromtimestamp(
                trade["timestamp"] / 1000
            )

            conn.execute(
                """
                INSERT INTO raw_crypto_trades
                VALUES (?, ?, ?, ?, ?)
                """,
                [
                    timestamp,
                    trade["symbol"],
                    trade["price"],
                    trade["quantity"],
                    trade["trade_id"],
                ],
            )

        print(
            f"[CONSUMER] Saved | "
            f"{trade['symbol']} | "
            f"price={trade['price']} | "
            f"qty={trade['quantity']} | "
            f"trade_id={trade['trade_id']}"
        )

    except KeyboardInterrupt:
        print("\n[CONSUMER] Stopping...")

    finally:
        conn.close()
        consumer.close()


if __name__ == "__main__":
    main()
