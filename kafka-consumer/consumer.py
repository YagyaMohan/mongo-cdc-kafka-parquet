from kafka import KafkaConsumer
import json
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from datetime import datetime
import os

consumer = KafkaConsumer(
    'test-topic',
    bootstrap_servers='172.17.0.1:9092',
    auto_offset_reset='earliest',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

print("Listening for messages...")

buffer = []

for msg in consumer:
    buffer.append(msg.value)

    if len(buffer) >= 5:
        df = pd.DataFrame(buffer)
        os.makedirs("parquet_output", exist_ok=True)
        filename = f"parquet_output/output_{datetime.now().strftime('%Y%m%d_%H%M%S')}.parquet"
        table = pa.Table.from_pandas(df)
        pq.write_table(table, filename)
        print(f"Saved: {filename}")
        buffer.clear()
