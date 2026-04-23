from flask import Flask, jsonify
from flask_cors import CORS
from kafka import KafkaConsumer
import json
import threading
from threading import Lock
import os

app = Flask(__name__)
CORS(app)

# 🔐 Thread safety lock
lock = Lock()

# 📦 In-memory storage
data_store = []
aggregation = {}

# 📁 File path for Spark
DATA_FILE = os.path.join(os.path.dirname(__file__), "../data/traffic.json")

# 🔹 Ensure data folder exists
os.makedirs(os.path.dirname(DATA_FILE), exist_ok=True)

# 🔹 Aggregation function
def update_aggregation(data):
    loc = data["location"]

    if loc not in aggregation:
        aggregation[loc] = {
            "total_vehicles": 0,
            "count": 0,
            "max": 0
        }

    aggregation[loc]["total_vehicles"] += data["vehicles"]
    aggregation[loc]["count"] += 1
    aggregation[loc]["max"] = max(
        aggregation[loc]["max"], data["vehicles"]
    )

# 🔹 Kafka Consumer Thread
def consume_data():
    consumer = KafkaConsumer(
        'traffic-data',
        bootstrap_servers='localhost:9092',
        value_deserializer=lambda x: json.loads(x.decode('utf-8')),
        auto_offset_reset='latest',
        enable_auto_commit=True
    )

    print("🚀 Kafka Consumer Started...\n")

    for message in consumer:
        data = message.value

        # 🚨 Alert logic
        if data["speed"] < 10 and data["vehicles"] > 70:
            data["alert"] = "🚨 Accident"
        else:
            data["alert"] = "Normal"

        with lock:
            # ✅ Store in memory
            data_store.append(data)
            update_aggregation(data)

            # Limit size
            if len(data_store) > 50:
                data_store.pop(0)

            # ✅ Save to file (for Spark)
            with open(DATA_FILE, "a") as f:
                f.write(json.dumps(data) + "\n")

# 🔹 Start Kafka consumer thread
thread = threading.Thread(target=consume_data)
thread.daemon = True
thread.start()

# 🔹 APIs

@app.route('/')
def home():
    return "Kafka Traffic Backend Running 🚀"

@app.route('/traffic', methods=['GET'])
def get_data():
    with lock:
        return jsonify(data_store)

@app.route('/aggregate', methods=['GET'])
def get_aggregate():
    with lock:
        result = []

        for loc, val in aggregation.items():
            avg = val["total_vehicles"] / val["count"]

            result.append({
                "location": loc,
                "avg_vehicles": round(avg, 2),
                "max_vehicles": val["max"],
                "count": val["count"]
            })

        return jsonify(result)

# 🔥 Reset API (Demo Feature)
@app.route('/reset', methods=['GET'])
def reset():
    global data_store, aggregation

    with lock:
        data_store = []
        aggregation = {}

        # Clear file also
        open(DATA_FILE, "w").close()

    return "Data Reset Done ✅"

# 🔥 Export API (Optional)
@app.route('/export', methods=['GET'])
def export():
    with lock:
        return jsonify(aggregation)

# 🔹 Run app
if __name__ == '__main__':
    app.run(debug=True)