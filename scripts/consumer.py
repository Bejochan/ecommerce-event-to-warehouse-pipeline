import argparse
import json
import os
import sys
import time
from pathlib import Path

# Add scripts directory to sys.path
sys.path.insert(0, str(Path(__file__).resolve().parent))

from confluent_kafka import Consumer
from validate import OrderModel
from pydantic import ValidationError
import duckdb


def main():
    parser = argparse.ArgumentParser(description="Kafka Streaming Consumer for E-Commerce Orders with Quality Gate")
    parser.add_argument("--broker", default=os.getenv("KAFKA_BROKER", "localhost:9092"), help="Kafka broker host:port")
    parser.add_argument("--topic", default=os.getenv("KAFKA_TOPIC", "orders"), help="Kafka topic name")
    parser.add_argument("--group", default=os.getenv("KAFKA_GROUP", "orders-group"), help="Consumer group ID")
    parser.add_argument("--max-messages", type=int, default=None, help="Maximum messages to consume before stopping")
    parser.add_argument("--timeout", type=float, default=1.5, help="Poll timeout in seconds")
    args = parser.parse_args()

    conf = {
        "bootstrap.servers": args.broker,
        "group.id": args.group,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    }

    consumer = Consumer(conf)
    consumer.subscribe([args.topic])
    print(f"[CONSUMER] Berlangganan topic '{args.topic}' dengan Consumer Group '{args.group}'...")

    db_path = Path(__file__).resolve().parent.parent / "warehouse" / "warehouse.duckdb"
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = duckdb.connect(str(db_path))

    # Pastikan tabel streaming_orders ada
    conn.execute("""
        CREATE TABLE IF NOT EXISTS streaming_orders (
            order_id VARCHAR PRIMARY KEY,
            customer_id VARCHAR,
            order_ts TIMESTAMP,
            product_category VARCHAR,
            quantity INTEGER,
            unit_price DOUBLE,
            discount_pct DOUBLE,
            payment_method VARCHAR,
            status VARCHAR,
            net_sales DOUBLE,
            ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    """)

    rejected_path = Path(__file__).resolve().parent.parent / "data" / "rejected" / "stream_rejected.csv"
    rejected_path.parent.mkdir(parents=True, exist_ok=True)

    count_valid = 0
    count_rejected = 0
    total_processed = 0

    try:
        while True:
            msg = consumer.poll(args.timeout)
            if msg is None:
                # Idle break for batch demo
                print(f"[CONSUMER] Menunggu pesan streaming baru (timeout {args.timeout}s)...")
                time.sleep(1.0)
                continue
            if msg.error():
                print(f"[ERROR] {msg.error()}")
                continue

            raw_val = msg.value().decode("utf-8")
            event = json.loads(raw_val)
            total_processed += 1

            # Validasi Data Contract
            try:
                validated = OrderModel.model_validate(event)
                net_sales = validated.quantity * validated.unit_price * (1.0 - validated.discount_pct)
                
                conn.execute("""
                    INSERT OR REPLACE INTO streaming_orders (
                        order_id, customer_id, order_ts, product_category,
                        quantity, unit_price, discount_pct, payment_method,
                        status, net_sales
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
                """, [
                    validated.order_id, validated.customer_id, validated.order_ts,
                    validated.product_category, validated.quantity, validated.unit_price,
                    validated.discount_pct, validated.payment_method, validated.status,
                    net_sales
                ])
                count_valid += 1
                print(f"[ACCEPT] Order {validated.order_id} valid! Net sales: ${net_sales:.2f} (Partition: {msg.partition()}, Offset: {msg.offset()})")

            except ValidationError as exc:
                count_rejected += 1
                reason = "; ".join([f"{err['loc'][0]}: {err['msg']}" for err in exc.errors()])
                print(f"[REJECT] Order {event.get('order_id')} INVALID: {reason}")
                with open(rejected_path, "a", encoding="utf-8") as f:
                    f.write(f"{event.get('order_id')},{reason}\n")

            # Manual commit offset (At-least-once delivery)
            consumer.commit(message=msg, asynchronous=False)

            if args.max_messages and total_processed >= args.max_messages:
                print(f"[CONSUMER] Mencapai batas maksimum pesan ({args.max_messages}). Selesai.")
                break

    except KeyboardInterrupt:
        print(f"\n[CONSUMER] Dihentikan oleh user. Total Valid: {count_valid}, Total Rejected: {count_rejected}")
    finally:
        consumer.close()
        conn.close()


if __name__ == "__main__":
    main()
