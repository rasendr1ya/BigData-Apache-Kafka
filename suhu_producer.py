from kafka import KafkaProducer
import json
import time
import random

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

gudang_ids = ['G1', 'G2', 'G3']

while True:
    for gid in gudang_ids:
        suhu = random.randint(75, 90)
        data = {"gudang_id": gid, "suhu": suhu}
        producer.send('sensor-suhu-gudang', value=data)
        print("Suhu terkirim:", data)
    time.sleep(1)