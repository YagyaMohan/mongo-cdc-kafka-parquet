from pymongo import MongoClient
from kafka import KafkaProducer
import json
from bson import ObjectId, Timestamp

class JSONEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, ObjectId):
            return str(o)
        if isinstance(o, Timestamp):
            return str(o)
        return super().default(o)

producer = KafkaProducer(bootstrap_servers='localhost:9092')

client = MongoClient("mongodb://localhost:27017/?replicaSet=rs0")
collection = client["test_db"]["events"]

print("Watching MongoDB for changes...")

try:
    with collection.watch() as stream:
        for change in stream:
            message = json.dumps(change, cls=JSONEncoder).encode('utf-8')
            print("Sending:", change)
            producer.send('test-topic', message)
            producer.flush()

except Exception as e:
    print("Error:", e)
