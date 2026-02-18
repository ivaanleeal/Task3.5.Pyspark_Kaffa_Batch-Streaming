import json, time, random
from datetime import datetime, timezone
from kafka import KafkaProducer

producer = KafkaProducer(bootstrap_servers="localhost:9092",
                         value_serializer=lambda v: json.dumps(v).encode("utf-8"))

while True:
    now = datetime.now(timezone.utc)
    # Alta humedad de 18:00 a 06:00
    is_night = now.hour >= 18 or now.hour < 6
    base_hum = 70.0 if is_night else 40.0
    
    data = {
        "sensor_id": "HUM_01",
        "humidity": round(base_hum + random.uniform(-5, 5), 2),
        "timestamp": now.isoformat(),
        "location": "Warehouse_A"
    }
    producer.send("humidity-sensors", data)
    time.sleep(5)
