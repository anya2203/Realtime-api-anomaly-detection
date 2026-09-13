from kafka import KafkaConsumer
import json
from collections import defaultdict
import time

consumer = KafkaConsumer(
    'api_logs',
    bootstrap_servers='localhost:9092',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

request_count = defaultdict(int)
start_time = time.time()

for message in consumer:
    data = message.value
    ip = data['ip']

    request_count[ip] += 1

    print(f"Received: {data}")

    # Check every 5 seconds
    if time.time() - start_time > 5:
        print("\n--- ANALYSIS WINDOW ---")

        for ip, count in request_count.items():
            print(f"{ip}: {count} requests")

            if count > 15:
                print(f"🚨 ALERT: Possible bot attack from {ip}")

        print("-----------------------\n")

        request_count.clear()
        start_time = time.time()
