from kafka import KafkaConsumer
import json

consumer = KafkaConsumer(
    'traffic-data',
    bootstrap_servers='localhost:9092',
    value_deserializer=lambda x: json.loads(x.decode('utf-8')),
    auto_offset_reset='latest',   # start from latest messages
    enable_auto_commit=True
)

print("🚀 Kafka Consumer Started...\n")

for message in consumer:
    data = message.value

    # 🚨 Alert logic (same as backend for consistency)
    if data["speed"] < 10 and data["vehicles"] > 70:
        data["alert"] = "🚨 Accident"
    else:
        data["alert"] = "Normal"

    # 📊 Print nicely
    print(f"""
📍 Location : {data['location']}
🚗 Vehicles : {data['vehicles']}
⚡ Speed    : {data['speed']}
🚦 Status   : {data['status']}
🚨 Alert    : {data['alert']}
-------------------------------
""")