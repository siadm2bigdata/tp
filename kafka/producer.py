from kafka import KafkaProducer
import json, time, random

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode()
)

for i in range(10):
    order = {
        "order_id": i,
        "amount": round(random.uniform(10, 500), 2),
        "country": random.choice(["FR", "DE", "US"]),
        "timestamp": time.time()
    }
    producer.send("orders", order)
    print("Produced:", order)
    time.sleep(0.5)

producer.flush()
