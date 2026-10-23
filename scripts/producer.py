import argparse
import json
import os
import time
from pathlib import Path
import pandas as pd
from confluent_kafka import Producer


def main():
    parser = argparse.ArgumentParser(description="Kafka Streaming Producer for E-Commerce Orders")
    parser.add_argument("--broker", default=os.getenv("KAFKA_BROKER", "localhost:9092"), help="Kafka broker host:port")
    parser.add_argument("--topic", default=os.getenv("KAFKA_TOPIC", "orders"), help="Kafka topic name")
    parser.add_argument("--source", default="data/orders.csv", help="Source CSV dataset")
    parser.add_argument("--demo-duplicate", action="store_true", help="Send duplicate event to test sink idempotency")
    args = parser.parse_args()

    print(f"[PRODUCER] Menghubungkan ke Kafka broker {args.broker}...")
    producer = Producer({"bootstrap.servers": args.broker})

    orders_path = Path(__file__).parent.parent / args.source
    df = pd.read_csv(orders_path)
    print(f"[PRODUCER] Membaca {len(df)} event transaksi dari {orders_path}")

    for idx, row in df.iterrows():
        event = row.to_dict()
        # Clean NaN
        event = {k: (None if pd.isna(v) else v) for k, v in event.items()}
        key = str(event.get("order_id", "unknown"))

        producer.produce(
            args.topic,
            key=key,
            value=json.dumps(event).encode("utf-8"),
        )
        producer.poll(0)
        print(f"[PRODUCER] Terkirim: order={key} category={event.get('product_category')} status={event.get('status')} (key={key})")
        time.sleep(0.05)

    if args.demo_duplicate:
        print("\n[PRODUCER] --- DEMO DUPLIKASI PESAN ---")
        dup_row = df.iloc[0].to_dict()
        dup_row = {k: (None if pd.isna(v) else v) for k, v in dup_row.items()}
        key = str(dup_row.get("order_id", "O1001"))
        print(f"[PRODUCER] Mengirim pesan duplikat secara sengaja: order_id={key}")
        producer.produce(
            args.topic,
            key=key,
            value=json.dumps(dup_row).encode("utf-8"),
        )
        producer.poll(0)

    producer.flush()
    print("[PRODUCER] Seluruh event transaksi streaming selesai dikirim.")


if __name__ == "__main__":
    main()

