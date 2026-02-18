import json, time, random
from datetime import datetime, timezone
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

start_time = time.time()

while True:
    hours_passed = (time.time() - start_time) / 3600
    temp = 20.0 + (0.5 * hours_passed) + random.uniform(-0.2, 0.2)
    
    data = {
        "sensor_id": "TEMP_01",
        "temperature": round(temp, 2),
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "location": "Warehouse_A"
    }
    producer.send("temperature-sensors", data)
    time.sleep(5)
