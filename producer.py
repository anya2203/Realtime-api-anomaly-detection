from kafka import KafkaProducer
import json
import time
import random

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

ips = ["192.168.1.1", "10.0.0.2", "172.16.0.5"]

while True:
    data = {
        "ip": random.choice(ips),
        "endpoint": "/login",
        "timestamp": time.time()
    }

    producer.send("api_logs", data)
    print("Sent:", data)

    if random.random() < 0.1:
        print("⚠️ ATTACK SIMULATION")
        for _ in range(20):
            producer.send("api_logs", {
                "ip": "ATTACKER",
                "endpoint": "/login",
                "timestamp": time.time()
            })

    time.sleep(1)
