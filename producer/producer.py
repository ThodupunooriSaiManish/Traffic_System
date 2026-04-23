from kafka import KafkaProducer
import json
import time
import random

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda x: json.dumps(x).encode('utf-8')
)

locations = ["Signal A", "Signal B", "Signal C"]

while True:
    vehicles = random.randint(10, 100)
    speed = random.randint(5, 60)

    if vehicles > 70:
        status = "Heavy Traffic"
    elif vehicles > 40:
        status = "Moderate"
    else:
        status = "Low"

    data = {
        "location": random.choice(locations),
        "vehicles": vehicles,
        "speed": speed,
        "status": status,
        "timestamp": time.time()

    }

    print("Sending:", data)
    producer.send('traffic-data', value=data)
    time.sleep(2)