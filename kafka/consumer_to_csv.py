from kafka import KafkaConsumer
import json, csv, os

os.makedirs("../data/raw", exist_ok=True)

consumer = KafkaConsumer(
    "orders",
    bootstrap_servers="localhost:9092",
    auto_offset_reset="earliest",
    group_id="group_csv",
    value_deserializer=lambda m: json.loads(m.decode())
)

with open("../data/raw/orders.csv", "a", newline="") as f:
    writer = csv.writer(f)
    for msg in consumer:
        o = msg.value
        writer.writerow([o["order_id"], o["amount"], o["country"], o["timestamp"]])
        print("Consumed:", o)
