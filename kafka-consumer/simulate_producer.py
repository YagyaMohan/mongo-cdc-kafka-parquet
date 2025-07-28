from kafka import KafkaProducer
import json
import time
import random

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda x: json.dumps(x).encode('utf-8')
)

for i in range(15):
    data = {"id": i, "event": "login", "value": random.randint(1, 100)}
    producer.send("test-topic", value=data)
    print("Sent:", data)
    time.sleep(1)

producer.close()
