from kafka import KafkaProducer
import json
import time
import random

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda x: json.dumps(x).encode('utf-8')
)

for i in range(20):
    data = {
        "user_id": i,
        "event": "click",
        "value": random.randint(100, 999),
        "timestamp": time.time()
    }
    producer.send("test-topic", value=data)
    print("Sent:", data)
    time.sleep(0.5)

producer.close()
